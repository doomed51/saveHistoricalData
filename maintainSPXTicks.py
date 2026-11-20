"""SPX historical tick maintenance.

Provides the resumable session collector, the backfill and daily job
orchestration, and the pure session and timestamp conversions they use.
"""

import argparse
import contextlib
import datetime
import logging
import math
import os
import sys
import time
from zoneinfo import ZoneInfo

import pandas as pd
from ib_insync import Index

import config
import interface_ibkr
from interface_ticks import (
    FINDING_COLUMNS,
    SESSION_TABLE,
    audit_exit_code,
    audit_sessions,
    connect_ticks,
    delete_invalid_rows,
    delete_unexpected_rows,
    ensure_schema,
    ensure_sessions,
    get_session,
    list_sessions,
    save_session,
    session_metrics,
    session_schedule,
    tick_table,
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

LOG_DIRECTORY = 'logs'
LOGGER_NAME = 'spx_ticks'
_LOG_FORMAT = '%(asctime)s %(levelname)s %(message)s'

_ERROR = 'error'
_WARNING = 'warning'
_INFO = 'info'
_UNAVAILABLE_FINDING = 'unavailable'


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


def _now_et():
    """Return the current time as a naive Eastern datetime."""
    return datetime.datetime.now(_EASTERN).replace(tzinfo=None)


def _now_text():
    return _now_et().strftime(_TIMESTAMP_FORMAT)


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


def _repair_session_dates(findings, include_warnings=False,
                          force_unavailable=False):
    """Return the session dates a repair must revisit, oldest first.

    Error findings always qualify. Warning findings qualify only with
    ``include_warnings``, and the informational ``unavailable`` finding only
    with ``force_unavailable``.
    """
    if findings is None or len(findings) == 0:
        return []

    severities = findings['severity']
    selected = severities == _ERROR
    if include_warnings:
        selected = selected | (severities == _WARNING)
    if force_unavailable:
        selected = selected | (
            (severities == _INFO)
            & (findings['finding'] == _UNAVAILABLE_FINDING)
        )
    return sorted(set(findings.loc[selected, 'session_date']))


def run_repair(ibkr, conn, contract, findings, include_warnings=False,
               force_unavailable=False):
    """Repair audited sessions with targeted cleanup and a forced re-fetch.

    Sessions are selected from ``findings``, an :func:`audit_sessions` frame:
    errors always, warnings when ``include_warnings`` is set, and the
    informational ``unavailable`` finding when ``force_unavailable`` is set.
    Inside the selected range, invalid prices (non-positive, non-finite, or
    NULL) and rows outside every expected session window are deleted first; a
    session's valid rows are left in place. The selected sessions are then
    collected again oldest-first
    through :func:`collect_session` with ``force=True`` and ``resume=False``,
    so an interrupted repair resumes through the normal collector path.

    The summary has ``job``, ``start_date``, ``end_date`` (the earliest and
    latest selected session dates), ``sessions`` (every selected session date,
    including a non-trading date that only needed cleanup), ``processed``
    (sessions re-fetched), ``status_counts``, ``cleanup`` (removed
    ``invalid_rows`` and ``unexpected_rows`` counts), and ``findings`` (the
    post-repair audit of the selected range). A selection with nothing to
    revisit returns an empty summary and empty findings.
    """
    selected = _repair_session_dates(
        findings, include_warnings, force_unavailable
    )
    if not selected:
        return {
            'job': 'repair',
            'start_date': None,
            'end_date': None,
            'sessions': [],
            'processed': 0,
            'status_counts': {},
            'cleanup': {'invalid_rows': 0, 'unexpected_rows': 0},
            'findings': pd.DataFrame(columns=list(FINDING_COLUMNS)),
        }

    start_date = selected[0]
    end_date = selected[-1]
    symbol = config.tick_symbol
    cleanup = {
        'invalid_rows': delete_invalid_rows(
            conn, symbol=symbol, start_date=start_date, end_date=end_date
        ),
        # The sweep is bounded to the audited range so dates outside it keep
        # their rows even though no window is supplied for them.
        'unexpected_rows': delete_unexpected_rows(
            conn,
            session_schedule(start_date, end_date),
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        ),
    }

    expected = {
        session['session_date']: session
        for session in session_schedule(start_date, end_date)
    }
    processed = 0
    statuses = []
    for session_date in selected:
        session = expected.get(session_date)
        if session is None:
            continue
        state = collect_session(
            ibkr, conn, contract, session, force=True, resume=False
        )
        statuses.append(state['status'])
        processed += 1

    counts = {}
    for status in statuses:
        counts[status] = counts.get(status, 0) + 1

    return {
        'job': 'repair',
        'start_date': start_date,
        'end_date': end_date,
        'sessions': list(selected),
        'processed': processed,
        'status_counts': counts,
        'cleanup': cleanup,
        'findings': audit_sessions(conn, start_date, end_date, symbol=symbol),
    }


def acquire_lock(path):
    """Take an exclusive non-blocking OS lock on ``path`` and return its handle.

    The lock is held by the open file handle, not a PID check, so a lock file
    left behind by a dead process never blocks a later run. Raises
    ``RuntimeError`` while another live process holds it. The parent directory
    is created when missing.
    """
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    handle = open(path, 'a+', encoding='utf-8')
    try:
        if os.name == 'nt':
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        raise RuntimeError(
            'another SPX tick process holds the lock at %s' % path
        ) from exc
    return handle


def release_lock(handle, path):
    """Release a lock from :func:`acquire_lock` and remove its file."""
    try:
        if os.name == 'nt':
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def _table_exists(conn, name):
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone() is not None


def _database_size(conn):
    """Return the main database file size, or ``None`` for an in-memory DB."""
    for _, name, path in conn.execute('PRAGMA database_list'):
        if name != 'main' or not path:
            continue
        try:
            return os.path.getsize(path)
        except OSError:
            return None
    return None


def status_summary(conn, symbol=config.tick_symbol):
    """Summarize stored state and coverage for one symbol.

    Returns ``symbol``; ``status_counts`` (one entry per stored session
    status); ``sessions`` (total state rows); ``earliest_complete`` and
    ``latest_complete`` (``YYYY-MM-DD`` bounds of complete sessions);
    ``tick_rows`` (every stored tick); and ``db_size_bytes`` (main database
    file size, or ``None`` for an in-memory database).
    """
    status_counts = {}
    earliest_complete = None
    latest_complete = None
    if _table_exists(conn, SESSION_TABLE):
        for status, count in conn.execute(
            'SELECT status, COUNT(*) FROM "%s"'
            ' WHERE symbol = ? GROUP BY status ORDER BY status'
            % SESSION_TABLE,
            (symbol,),
        ):
            status_counts[status] = count
        earliest_complete, latest_complete = conn.execute(
            'SELECT MIN(session_date), MAX(session_date) FROM "%s"'
            " WHERE symbol = ? AND status = 'complete'" % SESSION_TABLE,
            (symbol,),
        ).fetchone()

    tick_rows = 0
    table = tick_table(symbol)
    if _table_exists(conn, table):
        tick_rows = conn.execute(
            'SELECT COUNT(*) FROM "%s"' % table
        ).fetchone()[0]

    return {
        'symbol': symbol,
        'status_counts': status_counts,
        'sessions': sum(status_counts.values()),
        'earliest_complete': earliest_complete,
        'latest_complete': latest_complete,
        'tick_rows': tick_rows,
        'db_size_bytes': _database_size(conn),
    }


def configure_logging(log_directory=None, now_et=None):
    """Configure console and append-only daily file logging for CLI runs.

    The log file is ``<log_directory>/spx_ticks_YYYYMMDD.log`` in Eastern
    time, and ``log_directory`` defaults to :data:`LOG_DIRECTORY`. Returns the
    log file path. Existing handlers on this module's logger are closed first.
    """
    directory = LOG_DIRECTORY if log_directory is None else log_directory
    moment = _now_et() if now_et is None else _et_naive(now_et)
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(
        directory, 'spx_ticks_%s.log' % moment.strftime('%Y%m%d')
    )

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)

    formatter = logging.Formatter(_LOG_FORMAT)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logfile = logging.FileHandler(path, mode='a', encoding='utf-8')
    logfile.setFormatter(formatter)
    logger.addHandler(console)
    logger.addHandler(logfile)
    return path


def _latest_completed_session(now_et=None):
    """Return the latest NYSE session date whose close has been reached."""
    moment = _now_et() if now_et is None else _et_naive(now_et)
    start = moment.date() - datetime.timedelta(days=_DAILY_DISCOVERY_DAYS)
    closed = [
        session['session_date']
        for session in session_schedule(
            start.isoformat(), moment.date().isoformat()
        )
        if _parse_timestamp(session['session_close']) <= moment
    ]
    if not closed:
        raise RuntimeError('no completed NYSE session in the recent calendar')
    return closed[-1]


def _resolve_range(args):
    """Return the inclusive ``(start_date, end_date)`` for a CLI command."""
    if args.start is not None:
        start_date = args.start
    else:
        today = _now_et().date()
        start_date = (
            pd.Timestamp(today)
            - pd.DateOffset(years=config.tick_backfill_years)
        ).date().isoformat()
    end_date = args.end if args.end is not None else _latest_completed_session()
    return start_date, end_date


def _lock_path(db_path):
    return db_path + '.lock'


@contextlib.contextmanager
def _write_resources(db_path):
    """Yield ``(ibkr, conn, contract)`` for a locked write-mode run.

    The single-instance lock is taken before the database is opened, the
    schema is ensured, and the IBKR connection uses ``config.tick_client_id``.
    The connection, database, and lock are always released.
    """
    lock_path = _lock_path(db_path)
    lock = acquire_lock(lock_path)
    conn = None
    ibkr = None
    try:
        conn = connect_ticks(db_path)
        ensure_schema(conn)
        ibkr = interface_ibkr.setupConnection(clientId=config.tick_client_id)
        contract = Index(
            config.tick_symbol, config.tick_exchange, config.tick_currency
        )
        yield ibkr, conn, contract
    finally:
        try:
            if ibkr is not None:
                try:
                    ibkr.disconnect()
                except Exception:
                    logging.getLogger(LOGGER_NAME).exception(
                        'IBKR disconnect failed'
                    )
            if conn is not None:
                conn.close()
        finally:
            release_lock(lock, lock_path)


def _read_audit(db_path, start_date, end_date):
    conn = connect_ticks(db_path, readonly=True)
    try:
        return audit_sessions(conn, start_date, end_date)
    finally:
        conn.close()


def _severity_counts(findings):
    counts = {}
    for severity in findings['severity']:
        counts[severity] = counts.get(severity, 0) + 1
    return counts


def _log_job(logger, summary):
    logger.info(
        '%s %s..%s processed=%d skipped=%d stopped_early=%s statuses=%s',
        summary['job'], summary['start_date'], summary['end_date'],
        summary['processed'], summary['skipped'], summary['stopped_early'],
        summary['status_counts'],
    )


def _log_status(logger, summary):
    logger.info(
        'status symbol=%s sessions=%d ticks=%d db_size_bytes=%s',
        summary['symbol'], summary['sessions'], summary['tick_rows'],
        summary['db_size_bytes'],
    )
    logger.info('status counts=%s', summary['status_counts'])
    logger.info(
        'status complete sessions: earliest=%s latest=%s',
        summary['earliest_complete'], summary['latest_complete'],
    )


def _log_findings(logger, start_date, end_date, findings):
    logger.info(
        'audit %s..%s findings=%d severity=%s',
        start_date, end_date, len(findings), _severity_counts(findings),
    )
    for row in findings.itertuples(index=False):
        level = {
            'error': logging.ERROR,
            'warning': logging.WARNING,
        }.get(row.severity, logging.INFO)
        logger.log(
            level, '%s %s: %s', row.session_date, row.finding, row.detail
        )


def _log_repair(logger, summary):
    logger.info(
        'repair %s..%s sessions=%s processed=%d statuses=%s cleanup=%s',
        summary['start_date'], summary['end_date'], summary['sessions'],
        summary['processed'], summary['status_counts'], summary['cleanup'],
    )
    _log_findings(
        logger, summary['start_date'], summary['end_date'],
        summary['findings'],
    )


def _iso_date(value):
    try:
        return datetime.date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            '%s is not a YYYY-MM-DD date' % value
        ) from exc


def _build_parser():
    parser = argparse.ArgumentParser(
        prog='maintainSPXTicks.py',
        description='Maintain SPX historical ticks in SQLite.',
    )
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument(
        '--backfill', dest='command', action='store_const', const='backfill',
        help='collect every incomplete session in the date range',
    )
    modes.add_argument(
        '--daily', dest='command', action='store_const', const='daily',
        help='collect recently closed sessions in its own seven-day window',
    )
    modes.add_argument(
        '--audit', dest='command', action='store_const', const='audit',
        help='audit stored coverage without contacting IBKR',
    )
    modes.add_argument(
        '--repair', dest='command', action='store_const', const='repair',
        help='audit, then re-fetch the sessions that fail the audit',
    )
    modes.add_argument(
        '--status', dest='command', action='store_const', const='status',
        help='report stored status counts without contacting IBKR',
    )
    parser.add_argument(
        '--include-warnings', action='store_true',
        help='repair warning findings as well as errors',
    )
    parser.add_argument(
        '--force-unavailable', action='store_true',
        help='repair sessions that are unavailable after their retries',
    )
    parser.add_argument(
        '--strict', action='store_true',
        help='fail an audit on warnings as well as errors',
    )
    parser.add_argument(
        '--start', type=_iso_date,
        help='inclusive start date (default: %d years ago)'
        % config.tick_backfill_years,
    )
    parser.add_argument(
        '--end', type=_iso_date,
        help='inclusive end date (default: latest completed NYSE session)',
    )
    parser.add_argument(
        '--max-runtime-minutes', type=float,
        help='stop a backfill before the next session after this many minutes',
    )
    parser.add_argument(
        '--db-path', default=config.dbname_ticks,
        help='tick database path (default: %(default)s)',
    )
    return parser


def main(argv=None):
    """Run one maintenance command and return a process exit code."""
    args = _build_parser().parse_args(argv)
    configure_logging()
    logger = logging.getLogger(LOGGER_NAME)
    logger.info('command=%s db=%s', args.command, args.db_path)
    try:
        if args.command == 'status':
            conn = connect_ticks(args.db_path, readonly=True)
            try:
                summary = status_summary(conn)
            finally:
                conn.close()
            _log_status(logger, summary)
            return 0

        if args.command == 'audit':
            start_date, end_date = _resolve_range(args)
            findings = _read_audit(args.db_path, start_date, end_date)
            _log_findings(logger, start_date, end_date, findings)
            return audit_exit_code(findings, strict=args.strict)

        if args.command == 'daily':
            with _write_resources(args.db_path) as (ibkr, conn, contract):
                summary = run_daily(ibkr, conn, contract)
            _log_job(logger, summary)
            return 0

        if args.command == 'backfill':
            start_date, end_date = _resolve_range(args)
            with _write_resources(args.db_path) as (ibkr, conn, contract):
                summary = run_backfill(
                    ibkr, conn, contract, start_date, end_date,
                    max_runtime_minutes=args.max_runtime_minutes,
                )
            _log_job(logger, summary)
            return 0

        if args.command == 'repair':
            start_date, end_date = _resolve_range(args)
            with _write_resources(args.db_path) as (ibkr, conn, contract):
                findings = audit_sessions(conn, start_date, end_date)
                summary = run_repair(
                    ibkr, conn, contract, findings,
                    include_warnings=args.include_warnings,
                    force_unavailable=args.force_unavailable,
                )
            _log_repair(logger, summary)
            return audit_exit_code(summary['findings'], strict=args.strict)

        raise ValueError('unknown command %r' % args.command)
    except Exception:
        logger.exception('%s failed', args.command)
        return 1


if __name__ == '__main__':
    sys.exit(main())
