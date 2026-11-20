"""SQLite storage for SPX historical ticks and per-session fetch state.

Ticks are stored in one table per symbol (``<symbol>_index_ticks``) using the
same naive Eastern timestamp text as the price-history tables. Fetch state for
every expected session lives in the shared ``00-tick_sessions`` table.

Connections use autocommit (``isolation_level=None``); callers own explicit
transactions around page upserts and state changes.
"""

import os
import pathlib
import sqlite3
import statistics
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as market_calendars

import config

SESSION_TABLE = '00-tick_sessions'
_EASTERN = ZoneInfo('America/New_York')
_NYSE_CALENDAR = market_calendars.get_calendar('NYSE')

SESSION_COLUMNS = (
    'symbol',
    'session_date',
    'session_open',
    'session_close',
    'status',
    'tick_count',
    'first_tick',
    'last_tick',
    'requests',
    'attempts',
    'last_error',
    'fetched_at',
)

_SESSION_INSERT_SQL = (
    'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
    ' ON CONFLICT(symbol, session_date) DO UPDATE SET'
    ' session_open=excluded.session_open,'
    ' session_close=excluded.session_close,'
    ' status=excluded.status,'
    ' tick_count=excluded.tick_count,'
    ' first_tick=excluded.first_tick,'
    ' last_tick=excluded.last_tick,'
    ' requests=excluded.requests,'
    ' attempts=excluded.attempts,'
    ' last_error=excluded.last_error,'
    ' fetched_at=excluded.fetched_at'
)


def tick_table(symbol):
    """Return the tick table name for a symbol."""
    return '%s_index_ticks' % symbol


def _quote_identifier(name):
    return '"%s"' % name.replace('"', '""')


def _readonly_uri(db_path):
    if db_path.startswith('file:'):
        separator = '&' if '?' in db_path else '?'
        return db_path + separator + 'mode=ro'
    return pathlib.Path(os.path.abspath(db_path)).as_uri() + '?mode=ro'


def connect_ticks(db_path=config.dbname_ticks, readonly=False):
    """Open the tick database.

    Writers get WAL, ``synchronous=NORMAL``, and a 5000 ms busy timeout.
    Readers open through a ``mode=ro`` URI and ``PRAGMA query_only=ON``.
    """
    if db_path == ':memory:':
        conn = sqlite3.connect(':memory:', isolation_level=None)
        if readonly:
            conn.execute('PRAGMA query_only=ON')
        return conn

    if readonly:
        conn = sqlite3.connect(_readonly_uri(db_path), uri=True, isolation_level=None)
        conn.execute('PRAGMA query_only=ON')
        return conn

    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA busy_timeout=5000')
    return conn


def ensure_schema(conn, symbol=config.tick_symbol):
    """Create the tick table for a symbol and the shared session state table."""
    conn.execute(
        'CREATE TABLE IF NOT EXISTS %s ('
        ' date TEXT NOT NULL PRIMARY KEY,'
        ' price REAL NOT NULL'
        ') WITHOUT ROWID'
        % _quote_identifier(tick_table(symbol))
    )
    conn.execute(
        'CREATE TABLE IF NOT EXISTS %s ('
        ' symbol TEXT NOT NULL,'
        ' session_date TEXT NOT NULL,'
        ' session_open TEXT NOT NULL,'
        ' session_close TEXT NOT NULL,'
        ' status TEXT NOT NULL,'
        ' tick_count INTEGER NOT NULL DEFAULT 0,'
        ' first_tick TEXT,'
        ' last_tick TEXT,'
        ' requests INTEGER NOT NULL DEFAULT 0,'
        ' attempts INTEGER NOT NULL DEFAULT 0,'
        ' last_error TEXT,'
        ' fetched_at TEXT,'
        ' PRIMARY KEY (symbol, session_date)'
        ') WITHOUT ROWID'
        % _quote_identifier(SESSION_TABLE)
    )


def ensure_sessions(conn, sessions, symbol=config.tick_symbol):
    """Insert ``pending`` rows for expected sessions, leaving existing rows alone."""
    conn.executemany(
        'INSERT INTO %s'
        ' (symbol, session_date, session_open, session_close, status)'
        " VALUES (?, ?, ?, ?, 'pending')"
        ' ON CONFLICT(symbol, session_date) DO NOTHING'
        % _quote_identifier(SESSION_TABLE),
        [
            (
                symbol,
                session['session_date'],
                session['session_open'],
                session['session_close'],
            )
            for session in sessions
        ],
    )


def _calendar_et_text(timestamp):
    """Format a calendar timestamp as naive Eastern text."""
    return pd.Timestamp(timestamp).tz_convert(_EASTERN).strftime(
        '%Y-%m-%d %H:%M:%S'
    )


def session_schedule(start_date, end_date):
    """Return NYSE sessions in the inclusive ``[start_date, end_date]`` range.

    Each session is a dict of ``session_date``, ``session_open``, and
    ``session_close`` in naive Eastern time. Holidays and weekends are omitted,
    and early closes use the calendar's shortened hours.
    """
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    if end < start:
        return []

    # The calendar's end-date handling varies by version; pad the upper bound
    # and filter here so the returned range is always inclusive.
    schedule = _NYSE_CALENDAR.schedule(
        start_date=start,
        end_date=end + pd.Timedelta(days=1),
    )

    sessions = []
    for session_date, row in schedule.iterrows():
        day = pd.Timestamp(session_date).normalize()
        if day < start or day > end:
            continue
        sessions.append(
            {
                'session_date': day.strftime('%Y-%m-%d'),
                'session_open': _calendar_et_text(row['market_open']),
                'session_close': _calendar_et_text(row['market_close']),
            }
        )
    return sessions


def _rows_to_dicts(cursor):
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def get_session(conn, symbol, session_date):
    """Return the state row for a session, or ``None`` when it is missing."""
    cursor = conn.execute(
        'SELECT %s FROM %s WHERE symbol = ? AND session_date = ?'
        % (
            ', '.join(SESSION_COLUMNS),
            _quote_identifier(SESSION_TABLE),
        ),
        (symbol, session_date),
    )
    rows = _rows_to_dicts(cursor)
    return rows[0] if rows else None


def save_session(conn, state):
    """Insert or update one session state row.

    ``state`` requires ``symbol``, ``session_date``, ``session_open``,
    ``session_close``, and ``status``. Missing optional fields use the schema
    defaults, and every column is replaced on conflict.
    """
    values = [
        state['symbol'],
        state['session_date'],
        state['session_open'],
        state['session_close'],
        state['status'],
        state.get('tick_count', 0),
        state.get('first_tick'),
        state.get('last_tick'),
        state.get('requests', 0),
        state.get('attempts', 0),
        state.get('last_error'),
        state.get('fetched_at'),
    ]
    conn.execute(
        _SESSION_INSERT_SQL.format(
            table=_quote_identifier(SESSION_TABLE),
            columns=', '.join(SESSION_COLUMNS),
            placeholders=', '.join(['?'] * len(SESSION_COLUMNS)),
        ),
        values,
    )


def list_sessions(conn, symbol=config.tick_symbol, statuses=None,
                  start_date=None, end_date=None):
    """Return session state rows ordered by ``session_date`` ascending.

    ``start_date`` and ``end_date`` are inclusive ``YYYY-MM-DD`` bounds.
    """
    if statuses is not None and not statuses:
        return []

    sql = 'SELECT %s FROM %s WHERE symbol = ?' % (
        ', '.join(SESSION_COLUMNS),
        _quote_identifier(SESSION_TABLE),
    )
    params = [symbol]
    if statuses is not None:
        sql += ' AND status IN (%s)' % ', '.join(['?'] * len(statuses))
        params.extend(statuses)
    if start_date is not None:
        sql += ' AND session_date >= ?'
        params.append(start_date)
    if end_date is not None:
        sql += ' AND session_date <= ?'
        params.append(end_date)
    sql += ' ORDER BY session_date ASC'

    return _rows_to_dicts(conn.execute(sql, params))


def upsert_ticks(conn, rows, symbol=config.tick_symbol):
    """Insert or revise ``(date, price)`` rows and return the affected count."""
    cursor = conn.executemany(
        'INSERT INTO %s (date, price) VALUES (?, ?)'
        ' ON CONFLICT(date) DO UPDATE SET price=excluded.price'
        % _quote_identifier(tick_table(symbol)),
        [(date, price) for date, price in rows],
    )
    return cursor.rowcount


def session_metrics(conn, symbol, session_open, session_close):
    """Return ``tick_count``, ``first_tick``, and ``last_tick`` for a closed interval."""
    row = conn.execute(
        'SELECT COUNT(*), MIN(date), MAX(date) FROM %s'
        ' WHERE date >= ? AND date <= ?'
        % _quote_identifier(tick_table(symbol)),
        (session_open, session_close),
    ).fetchone()
    return {'tick_count': row[0], 'first_tick': row[1], 'last_tick': row[2]}


def get_ticks(conn, start, end, symbol=config.tick_symbol):
    """Return ticks in ``[start, end)`` as a ``date, price`` DataFrame.

    ``date`` is parsed to naive datetime values and rows are sorted ascending.
    """
    ticks = pd.read_sql_query(
        'SELECT date, price FROM %s WHERE date >= ? AND date < ? ORDER BY date ASC'
        % _quote_identifier(tick_table(symbol)),
        conn,
        params=(start, end),
        parse_dates=['date'],
    )
    return ticks


FINDING_COLUMNS = ('session_date', 'severity', 'finding', 'detail')

_ERROR = 'error'
_WARNING = 'warning'
_INFO = 'info'

_STATUS_FINDINGS = {
    'pending': ('not_started', _ERROR),
    'in_progress': ('in_progress', _ERROR),
    'partial': ('partial', _ERROR),
    'failed': ('failed', _ERROR),
    'retry': ('retry', _WARNING),
}


def session_max_gap_seconds(conn, symbol, session_open, session_close):
    """Return the largest interval between consecutive ticks in a session.

    The interval is measured in whole seconds over ticks inside the inclusive
    ``[session_open, session_close]`` window. A window with fewer than two rows
    has no internal interval and returns ``0.0``.
    """
    # strftime returns whole seconds as text, so the subtraction stays exact
    # for the second-resolution timestamps stored in the tick table.
    sql = (
        'SELECT MAX(gap_seconds) FROM ('
        " SELECT CAST(strftime('%s', date) AS INTEGER)"
        " - CAST(strftime('%s', LAG(date) OVER (ORDER BY date)) AS INTEGER)"
        ' AS gap_seconds FROM '
        + _quote_identifier(tick_table(symbol))
        + ' WHERE date >= ? AND date <= ?)'
    )
    row = conn.execute(sql, (session_open, session_close)).fetchone()
    return float(row[0]) if row and row[0] is not None else 0.0


def _session_row_stats(conn, symbol, session_open, session_close):
    """Return row counts and bounds for one session window from SQL."""
    row = conn.execute(
        'SELECT COUNT(*), MIN(date), MAX(date),'
        ' COALESCE(SUM(CASE WHEN price <= 0'
        ' OR price > 1.7976931348623157e308 THEN 1 ELSE 0 END), 0)'
        ' FROM %s WHERE date >= ? AND date <= ?'
        % _quote_identifier(tick_table(symbol)),
        (session_open, session_close),
    ).fetchone()
    return {
        'tick_count': row[0],
        'first_tick': row[1],
        'last_tick': row[2],
        'invalid_price': row[3],
    }


def _finding(session_date, severity, finding, detail):
    return {
        'session_date': session_date,
        'severity': severity,
        'finding': finding,
        'detail': detail,
    }


def _timestamp_text(value):
    return pd.Timestamp(value).strftime('%Y-%m-%d %H:%M:%S')


def _session_minutes(session_open, session_close):
    seconds = (
        pd.Timestamp(session_close) - pd.Timestamp(session_open)
    ).total_seconds()
    return seconds / 60.0


def _status_detail(status, state):
    if status == 'pending':
        return 'session is still pending after its close'
    if status == 'in_progress':
        return 'fetch did not finish'
    if status == 'partial':
        return 'fetch ended before the close tolerance'
    if status == 'retry':
        return 'no data on a prior attempt; will be retried'
    if status == 'failed':
        return state.get('last_error') or 'request error'
    if status == 'unavailable':
        return 'no data after %d attempts' % state.get('attempts', 0)
    return "unexpected status '%s'" % status


def audit_sessions(conn, start_date, end_date, symbol=config.tick_symbol):
    """Audit expected NYSE sessions against stored state and tick rows.

    Returns a DataFrame with ``session_date``, ``severity``, ``finding``, and
    ``detail`` columns. Completeness is derived from the calendar and the tick
    rows, never from the collector's status alone.
    """
    expected = session_schedule(start_date, end_date)
    states = {
        row['session_date']: row
        for row in list_sessions(
            conn, symbol=symbol, start_date=start_date, end_date=end_date
        )
    }

    day_start = _timestamp_text(pd.Timestamp(start_date).normalize())
    day_end = _timestamp_text(
        pd.Timestamp(end_date).normalize() + pd.Timedelta(days=1)
    )
    day_counts = {
        row[0]: row[1]
        for row in conn.execute(
            'SELECT substr(date, 1, 10), COUNT(*) FROM %s'
            ' WHERE date >= ? AND date < ? GROUP BY 1 ORDER BY 1'
            % _quote_identifier(tick_table(symbol)),
            (day_start, day_end),
        )
    }

    findings = []
    baselines = []
    expected_dates = {session['session_date'] for session in expected}

    for session in expected:
        session_date = session['session_date']
        session_open = session['session_open']
        session_close = session['session_close']
        stats = _session_row_stats(conn, symbol, session_open, session_close)

        outside = day_counts.get(session_date, 0) - stats['tick_count']
        if outside > 0:
            findings.append(
                _finding(
                    session_date,
                    _ERROR,
                    'unexpected_rows',
                    '%d row(s) outside the expected session window' % outside,
                )
            )

        state = states.get(session_date)
        if state is None:
            findings.append(
                _finding(
                    session_date,
                    _ERROR,
                    'missing_status',
                    'expected session has no state row',
                )
            )
            continue

        status = state['status']
        if status == 'unavailable':
            findings.append(
                _finding(
                    session_date,
                    _INFO,
                    'unavailable',
                    _status_detail(status, state),
                )
            )
            if stats['tick_count'] == 0:
                findings.append(
                    _finding(
                        session_date,
                        _INFO,
                        'no_rows_unavailable',
                        'expected: unavailable session has no rows',
                    )
                )
            continue

        if status != 'complete':
            finding, severity = _STATUS_FINDINGS.get(
                status, ('failed', _ERROR)
            )
            findings.append(
                _finding(
                    session_date,
                    severity,
                    finding,
                    _status_detail(status, state),
                )
            )
            continue

        if stats['tick_count'] == 0:
            findings.append(
                _finding(
                    session_date,
                    _ERROR,
                    'no_rows_complete',
                    'complete session has no rows in its window',
                )
            )
            continue

        invalid_baseline = False

        late_threshold = pd.Timestamp(session_open) + pd.Timedelta(
            seconds=config.tick_start_tolerance_seconds
        )
        first_tick = pd.Timestamp(stats['first_tick'])
        if first_tick > late_threshold:
            findings.append(
                _finding(
                    session_date,
                    _WARNING,
                    'late_start',
                    'first tick %s is later than %s'
                    % (stats['first_tick'], _timestamp_text(late_threshold)),
                )
            )

        early_threshold = pd.Timestamp(session_close) - pd.Timedelta(
            seconds=config.tick_complete_tolerance_seconds
        )
        last_tick = pd.Timestamp(stats['last_tick'])
        if last_tick < early_threshold:
            invalid_baseline = True
            findings.append(
                _finding(
                    session_date,
                    _ERROR,
                    'early_end',
                    'last tick %s is earlier than %s'
                    % (stats['last_tick'], _timestamp_text(early_threshold)),
                )
            )

        max_gap = session_max_gap_seconds(
            conn, symbol, session_open, session_close
        )
        if max_gap >= config.tick_gap_error_seconds:
            invalid_baseline = True
            findings.append(
                _finding(
                    session_date,
                    _ERROR,
                    'large_gap',
                    'largest interval between ticks is %d seconds' % max_gap,
                )
            )
        elif max_gap >= config.tick_gap_warning_seconds:
            findings.append(
                _finding(
                    session_date,
                    _WARNING,
                    'gap',
                    'largest interval between ticks is %d seconds' % max_gap,
                )
            )

        if stats['invalid_price'] > 0:
            invalid_baseline = True
            findings.append(
                _finding(
                    session_date,
                    _ERROR,
                    'invalid_price',
                    '%d row(s) with a non-positive or non-finite price'
                    % stats['invalid_price'],
                )
            )

        density = stats['tick_count'] / _session_minutes(
            session_open, session_close
        )
        if len(baselines) >= config.tick_density_min_baseline:
            median_density = statistics.median(baselines)
            if density < config.tick_density_warning_ratio * median_density:
                findings.append(
                    _finding(
                        session_date,
                        _WARNING,
                        'low_density',
                        'density %.4f ticks/minute is below half the trailing'
                        ' median %.4f' % (density, median_density),
                    )
                )

        if not invalid_baseline:
            baselines.append(density)
            baselines = baselines[-config.tick_density_baseline_sessions:]

    for session_date in sorted(day_counts):
        if session_date in expected_dates:
            continue
        findings.append(
            _finding(
                session_date,
                _ERROR,
                'unexpected_rows',
                '%d row(s) on a day with no expected session'
                % day_counts[session_date],
            )
        )

    if not findings:
        return pd.DataFrame(columns=list(FINDING_COLUMNS))
    return pd.DataFrame(findings, columns=list(FINDING_COLUMNS))


def audit_exit_code(findings, strict=False):
    """Return ``1`` when the audit should fail, otherwise ``0``.

    Any error fails the audit. Warnings fail only when ``strict`` is set;
    informational findings never fail it.
    """
    if findings is None or len(findings) == 0:
        return 0
    severities = findings['severity']
    if (severities == _ERROR).any():
        return 1
    if strict and (severities == _WARNING).any():
        return 1
    return 0
