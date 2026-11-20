"""SPX historical tick maintenance.

Provides the resumable session collector, the backfill and daily job
orchestration, and the pure session and timestamp conversions they use.
"""

import datetime
import math
import time
from zoneinfo import ZoneInfo

import config
import interface_ibkr
from interface_ticks import (
    ensure_sessions,
    get_session,
    list_sessions,
    save_session,
    session_metrics,
    session_schedule,
    upsert_ticks,
)

_EASTERN = ZoneInfo('America/New_York')
_UTC = datetime.timezone.utc
_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
_REQUEST_FORMAT = '%Y%m%d-%H:%M:%S'

_FATAL_ERROR_CODES = frozenset({10187, 10314})
_RECONNECT_ERROR_CODES = frozenset({502, 504, 1100, 1101})
_NO_DATA_ERROR_CODES = frozenset({162})
_IGNORED_ERROR_CODES = frozenset({2104, 2106, 2158})
_COMPLETE_SESSIONS = frozenset({'complete', 'unavailable'})
_INCOMPLETE_STATUSES = (
    'pending',
    'retry',
    'failed',
    'in_progress',
    'partial',
)
_DAILY_DISCOVERY_DAYS = 14
_DAILY_PROCESS_DAYS = 7


class FatalTickError(RuntimeError):
    """A request error that retrying cannot fix; the run must stop."""


def _as_datetime(value):
    if isinstance(value, datetime.datetime):
        return value
    return datetime.datetime.fromisoformat(str(value))


def _et_naive(value):
    """Return ``value`` as a naive datetime in Eastern time.

    Naive input is interpreted as Eastern time; aware input is converted.
    """
    moment = _as_datetime(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_EASTERN)
    return moment.astimezone(_EASTERN).replace(tzinfo=None)


def _valid_price(value):
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(price) or price <= 0:
        return None
    return price


def et_to_utc_start(value):
    """Return an ET-naive datetime as a ``YYYYMMDD-HH:MM:SS`` UTC string."""
    moment = _as_datetime(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_EASTERN)
    return moment.astimezone(_UTC).strftime(_REQUEST_FORMAT)


def page_to_rows(ticks, session_open, session_close):
    """Convert one tick page to ascending ``(date_text, price)`` rows.

    Tick times are UTC-aware and are stored as naive Eastern text. Non-finite
    or non-positive prices and timestamps outside the inclusive session window
    are dropped. Duplicate timestamps keep the last value in page order.
    """
    window_open = _et_naive(session_open)
    window_close = _et_naive(session_close)

    rows = {}
    for tick in ticks:
        price = _valid_price(tick.price)
        if price is None:
            continue
        moment = _et_naive(tick.time)
        if moment < window_open or moment > window_close:
            continue
        rows[moment] = price

    return [
        (moment.strftime(_TIMESTAMP_FORMAT), rows[moment])
        for moment in sorted(rows)
    ]


def _error_from_event(args):
    """Return ``(code, message)`` from an ib_insync ``errorEvent`` payload."""
    if len(args) >= 3:
        code, message = args[1], args[2]
    elif len(args) == 2:
        code, message = args[0], args[1]
    else:
        return None
    try:
        code = int(code)
    except (TypeError, ValueError):
        return None
    return code, str(message)


def request_tick_page(ibkr, contract, start_utc):
    """Request one historical tick page and return ``(ticks, errors)``.

    A temporary ``errorEvent`` listener collects the request's error events;
    informational connection notices are ignored.
    """
    errors = []

    def collect_error(*args):
        error = _error_from_event(args)
        if error is None or error[0] in _IGNORED_ERROR_CODES:
            return
        errors.append(error)

    ibkr.errorEvent += collect_error
    try:
        ticks = interface_ibkr.getHistoricalTicks(
            ibkr,
            contract,
            start_utc,
            numberOfTicks=config.tick_request_size,
        )
    finally:
        ibkr.errorEvent -= collect_error

    return list(ticks or []), errors


def classify_errors(errors):
    """Classify request errors as ``fatal``, ``reconnect``, ``failed``,
    ``no_data``, or ``None``.

    Precedence is fatal, reconnect, failed, then no_data.
    """
    codes = {code for code, _ in errors}
    if codes & _FATAL_ERROR_CODES:
        return 'fatal'
    if codes & _RECONNECT_ERROR_CODES:
        return 'reconnect'
    if codes - _NO_DATA_ERROR_CODES:
        return 'failed'
    if codes & _NO_DATA_ERROR_CODES:
        return 'no_data'
    return None


def _error_text(errors):
    return '; '.join('%s: %s' % (code, message) for code, message in errors)


def _now_text():
    return datetime.datetime.now(_EASTERN).replace(tzinfo=None).strftime(
        _TIMESTAMP_FORMAT
    )


def _parse_timestamp(value):
    return datetime.datetime.strptime(value, _TIMESTAMP_FORMAT)


def _write_transaction(conn, action):
    """Run ``action`` inside ``BEGIN IMMEDIATE`` and commit, or roll back."""
    conn.execute('BEGIN IMMEDIATE')
    try:
        result = action()
    except BaseException:
        if conn.in_transaction:
            conn.execute('ROLLBACK')
        raise
    conn.execute('COMMIT')
    return result


def _new_state(session):
    return {
        'symbol': session.get('symbol', config.tick_symbol),
        'session_date': session['session_date'],
        'session_open': session['session_open'],
        'session_close': session['session_close'],
        'status': session.get('status', 'pending'),
        'tick_count': session.get('tick_count', 0),
        'first_tick': session.get('first_tick'),
        'last_tick': session.get('last_tick'),
        'requests': session.get('requests', 0),
        'attempts': session.get('attempts', 0),
        'last_error': session.get('last_error'),
        'fetched_at': session.get('fetched_at'),
    }


def _metrics(conn, state):
    return session_metrics(
        conn,
        state['symbol'],
        state['session_open'],
        state['session_close'],
    )


def _save_failed(conn, state, requests, message):
    def action():
        failed = dict(state)
        failed.update(_metrics(conn, state))
        failed.update(
            status='failed',
            requests=requests,
            last_error=message,
            fetched_at=_now_text(),
        )
        save_session(conn, failed)
        return failed

    return _write_transaction(conn, action)


def _finalize(conn, state, requests, last_error=None):
    def action():
        finalized = dict(state)
        metrics = _metrics(conn, state)
        finalized.update(metrics)

        attempts = state.get('attempts', 0)
        if metrics['tick_count'] == 0:
            attempts += 1
            status = (
                'retry'
                if attempts < config.tick_max_attempts
                else 'unavailable'
            )
            finalized.update(
                status=status,
                attempts=attempts,
                first_tick=None,
                last_tick=None,
            )
        else:
            tolerance = datetime.timedelta(
                seconds=config.tick_complete_tolerance_seconds
            )
            close = _parse_timestamp(state['session_close'])
            last_tick = _parse_timestamp(metrics['last_tick'])
            finalized['status'] = (
                'complete' if last_tick >= close - tolerance else 'partial'
            )

        finalized.update(
            requests=requests,
            last_error=last_error,
            fetched_at=_now_text(),
        )
        save_session(conn, finalized)
        return finalized

    return _write_transaction(conn, action)


def collect_session(ibkr, conn, contract, session, force=False, resume=True,
                    reconnect=interface_ibkr.refreshConnection):
    """Collect one session's ticks and return its saved state.

    Complete and unavailable sessions are skipped unless ``force`` is set. An
    interrupted ``in_progress`` session resumes from ``last_tick + 1 second``
    when ``resume`` is true. Each page is upserted and checkpointed in its own
    transaction; the cursor advances by the last valid tick, never by the
    number of inserted rows.
    """
    state = get_session(
        conn, session.get('symbol', config.tick_symbol), session['session_date']
    )
    if state is None:
        state = _new_state(session)
        _write_transaction(conn, lambda: save_session(conn, state))

    if state['status'] in _COMPLETE_SESSIONS and not force:
        return state

    session_open = state['session_open']
    session_close = state['session_close']
    symbol = state['symbol']

    cursor = et_to_utc_start(session_open)
    if resume and state['status'] == 'in_progress' and state.get('last_tick'):
        resume_from = (
            _parse_timestamp(state['last_tick'])
            + datetime.timedelta(seconds=1)
        )
        cursor = et_to_utc_start(resume_from)

    max_requests = config.tick_max_requests_per_session
    max_reconnects = config.tick_connection_retries
    requests = 0

    while requests < max_requests:
        reconnects = 0
        while True:
            requests += 1
            ticks, errors = request_tick_page(ibkr, contract, cursor)
            outcome = classify_errors(errors)
            if outcome != 'reconnect':
                break
            if reconnects >= max_reconnects:
                return _save_failed(
                    conn, state, requests, _error_text(errors)
                )
            reconnects += 1
            refreshed = reconnect(ibkr)
            if refreshed is not None:
                ibkr = refreshed

        if outcome == 'fatal':
            message = _error_text(errors)
            _save_failed(conn, state, requests, message)
            raise FatalTickError(message)
        if outcome == 'failed':
            return _save_failed(conn, state, requests, _error_text(errors))
        if outcome == 'no_data':
            return _finalize(
                conn, state, requests, last_error=_error_text(errors)
            )

        if not ticks:
            break

        rows = page_to_rows(ticks, session_open, session_close)
        if not rows:
            break

        def save_page(rows=rows, requests=requests, state=state):
            upsert_ticks(conn, rows, symbol=symbol)
            page_state = dict(state)
            page_state.update(_metrics(conn, state))
            page_state.update(
                status='in_progress',
                requests=requests,
                last_error=None,
            )
            save_session(conn, page_state)
            return page_state

        state = _write_transaction(conn, save_page)

        last_tick = _parse_timestamp(rows[-1][0])
        close = _parse_timestamp(session_close)
        if last_tick >= close - datetime.timedelta(seconds=1):
            break

        next_cursor = et_to_utc_start(
            last_tick + datetime.timedelta(seconds=1)
        )
        if next_cursor <= cursor:
            break
        cursor = next_cursor

    return _finalize(conn, state, requests)


def _date_text(value):
    """Return a date-like value as ``YYYY-MM-DD`` text."""
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    return str(value)


def _select_sessions(conn, start_date, end_date, closed_before=None):
    """Return ``(candidates, skipped)`` for one inclusive date range.

    Candidates are incomplete sessions in ascending session order. Completed
    and unavailable sessions are counted as skipped. ``closed_before`` keeps
    only sessions whose close has been reached.
    """
    states = list_sessions(conn, start_date=start_date, end_date=end_date)
    if closed_before is not None:
        states = [
            state
            for state in states
            if _parse_timestamp(state['session_close']) <= closed_before
        ]
    skipped = sum(
        1 for state in states if state['status'] in _COMPLETE_SESSIONS
    )
    candidates = [
        state
        for state in states
        if state['status'] in _INCOMPLETE_STATUSES
    ]
    return candidates, skipped


def _collect_sessions(ibkr, conn, contract, sessions, deadline=None):
    """Collect sessions in order, stopping before the next one on expiry.

    Returns ``(processed, statuses, stopped_early)``. A ``KeyboardInterrupt``
    during a session stops the run cleanly; the collector's per-page
    checkpoint leaves that session ``in_progress``.
    """
    processed = 0
    statuses = []
    stopped_early = False

    for session in sessions:
        if deadline is not None and time.monotonic() >= deadline:
            stopped_early = True
            break
        try:
            state = collect_session(
                ibkr, conn, contract, session, force=False, resume=True
            )
        except KeyboardInterrupt:
            stopped_early = True
            break
        statuses.append(state['status'])
        processed += 1

    return processed, statuses, stopped_early


def _job_summary(job, start_date, end_date, processed, skipped, statuses,
                 stopped_early):
    counts = {}
    for status in statuses:
        counts[status] = counts.get(status, 0) + 1
    return {
        'job': job,
        'start_date': _date_text(start_date),
        'end_date': _date_text(end_date),
        'processed': processed,
        'skipped': skipped,
        'stopped_early': stopped_early,
        'status_counts': counts,
    }


def run_backfill(ibkr, conn, contract, start_date, end_date,
                 max_runtime_minutes=None):
    """Ensure and collect every incomplete session in an inclusive range.

    Expected rows are created first without changing existing statuses, then
    incomplete sessions are collected oldest-first. ``complete`` and
    ``unavailable`` sessions are skipped. The run stops before the next session
    when ``max_runtime_minutes`` is exhausted.

    The summary is ``job``, ``start_date``, ``end_date``, ``processed``,
    ``skipped``, ``stopped_early``, and ``status_counts`` (one count per
    status returned by the processed sessions).
    """
    deadline = None
    if max_runtime_minutes is not None:
        deadline = time.monotonic() + max_runtime_minutes * 60.0

    ensure_sessions(conn, session_schedule(start_date, end_date))
    sessions, skipped = _select_sessions(conn, start_date, end_date)
    processed, statuses, stopped_early = _collect_sessions(
        ibkr, conn, contract, sessions, deadline
    )
    return _job_summary(
        'backfill',
        start_date,
        end_date,
        processed,
        skipped,
        statuses,
        stopped_early,
    )


def run_daily(ibkr, conn, contract, now_et=None):
    """Collect recently closed incomplete sessions.

    Session discovery covers 14 calendar days, while processing covers the
    last 7 calendar days and only sessions whose close has been reached. A
    session that is still open today is skipped; after 16:00 ET the just-closed
    session is included. ``now_et`` is a naive Eastern datetime and defaults to
    the current Eastern time.

    The summary has the same shape as :func:`run_backfill`, with the process
    window as ``start_date`` and ``end_date``.
    """
    if now_et is None:
        moment = datetime.datetime.now(_EASTERN).replace(tzinfo=None)
    else:
        moment = _et_naive(now_et)
    today = moment.date()

    discovery_start = today - datetime.timedelta(
        days=_DAILY_DISCOVERY_DAYS - 1
    )
    ensure_sessions(
        conn,
        session_schedule(discovery_start.isoformat(), today.isoformat()),
    )

    process_start = today - datetime.timedelta(days=_DAILY_PROCESS_DAYS - 1)
    sessions, skipped = _select_sessions(
        conn,
        process_start.isoformat(),
        today.isoformat(),
        closed_before=moment,
    )
    processed, statuses, stopped_early = _collect_sessions(
        ibkr, conn, contract, sessions
    )
    return _job_summary(
        'daily',
        process_start.isoformat(),
        today.isoformat(),
        processed,
        skipped,
        statuses,
        stopped_early,
    )
