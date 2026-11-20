import datetime
import math
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

import config
import interface_ticks
from interface_ticks import (
    connect_ticks,
    ensure_schema,
    get_session,
    save_session,
    session_schedule,
)
from maintainSPXTicks import (
    FatalTickError,
    classify_errors,
    collect_session,
    et_to_utc_start,
    page_to_rows,
    request_tick_page,
)

UTC = datetime.timezone.utc
EASTERN = ZoneInfo('America/New_York')
SESSION_DATE = '2026-11-18'
SESSION_OPEN = '2026-11-18 09:30:00'
SESSION_CLOSE = '2026-11-18 16:00:00'
CONTRACT = object()
SESSION = {
    'symbol': 'TEST',
    'session_date': SESSION_DATE,
    'session_open': SESSION_OPEN,
    'session_close': SESSION_CLOSE,
}


class Tick:
    """Minimal stand-in for an IBKR historical tick."""

    def __init__(self, time, price):
        self.time = time
        self.price = price


def _tick(time_text, price):
    time = datetime.datetime.strptime(time_text, '%Y-%m-%d %H:%M:%S')
    return Tick(time.replace(tzinfo=UTC), price)


def _et_datetime(time_text):
    return datetime.datetime.strptime(time_text, '%Y-%m-%d %H:%M:%S')


def _utc_from_request(start_utc):
    return (
        datetime.datetime.strptime(start_utc, '%Y%m%d-%H:%M:%S')
        .replace(tzinfo=UTC)
    )


def _et_tick(time_text, price):
    moment = _et_datetime(time_text).replace(tzinfo=EASTERN)
    return Tick(moment.astimezone(UTC), price)


def _ticks_between(start_text, end_text, count, base_price=1000.0):
    """Return ``count`` ET ticks spanning ``[start_text, end_text]``."""
    start = _et_datetime(start_text)
    end = _et_datetime(end_text)
    if count == 1:
        moments = [end]
    else:
        step = (end - start) / (count - 1)
        moments = [start + step * index for index in range(count - 1)]
        moments.append(end)
    return [
        Tick(moment.replace(tzinfo=EASTERN).astimezone(UTC), base_price + index)
        for index, moment in enumerate(moments)
    ]


def _ticks_to_close(count, base_price=1000.0):
    """Return ``count`` ET ticks spanning the session open to the close."""
    return _ticks_between(
        '2026-11-18 09:30:01',
        '2026-11-18 15:59:59',
        count,
        base_price=base_price,
    )


class CursorSource:
    """Fake page source that returns ticks at or after the request cursor."""

    def __init__(self, ticks):
        self.ticks = list(ticks)

    def __call__(self, start_utc, size):
        start = _utc_from_request(start_utc)
        available = [tick for tick in self.ticks if tick.time >= start]
        return available[:size], []


class ChunkSource:
    """Fake page source that returns prepared ``(ticks, errors)`` pages."""

    def __init__(self, pages):
        self.pages = list(pages)

    def __call__(self, start_utc, size):
        if not self.pages:
            return [], []
        return self.pages.pop(0)


class FakeErrorEvent:
    def __init__(self):
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self

    def __isub__(self, handler):
        self.handlers.remove(handler)
        return self

    def emit(self, *args):
        for handler in list(self.handlers):
            handler(*args)


class FakeIBKR:
    def __init__(self, page_source):
        self.page_source = page_source
        self.errorEvent = FakeErrorEvent()
        self.starts = []

    def isConnected(self):
        return True

    def reqHistoricalTicks(self, contract, startDateTime, endDateTime,
                           numberOfTicks, whatToShow, useRth, ignoreSize):
        self.starts.append(startDateTime)
        ticks, errors = self.page_source(startDateTime, numberOfTicks)
        for code, message in errors:
            self.errorEvent.emit(0, code, message, None)
        return ticks


class FakeReconnect:
    def __init__(self):
        self.calls = 0

    def __call__(self, ibkr):
        self.calls += 1
        return ibkr


class TickCollectorTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = connect_ticks(':memory:')
        ensure_schema(self.conn, symbol='TEST')
        self.session = dict(SESSION)
        pacer = patch('interface_ibkr._paceIbkrRequest')
        pacer.start()
        self.addCleanup(pacer.stop)

    def tearDown(self):
        self.conn.close()

    def _collect(self, page_source, **kwargs):
        ibkr = FakeIBKR(page_source)
        state = collect_session(
            ibkr, self.conn, CONTRACT, self.session, **kwargs
        )
        return ibkr, state


class TestSessionSchedule(unittest.TestCase):
    def test_schedule_excludes_holiday_and_uses_early_close(self):
        sessions = session_schedule('2025-11-27', '2025-11-28')

        self.assertEqual(
            sessions,
            [
                {
                    'session_date': '2025-11-28',
                    'session_open': '2025-11-28 09:30:00',
                    'session_close': '2025-11-28 13:00:00',
                }
            ],
        )

    def test_schedule_end_date_is_inclusive(self):
        sessions = session_schedule('2025-11-24', '2025-11-26')

        self.assertEqual(
            [session['session_date'] for session in sessions],
            ['2025-11-24', '2025-11-25', '2025-11-26'],
        )

    def test_schedule_uses_regular_session_hours(self):
        sessions = session_schedule('2026-11-18', '2026-11-18')

        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0]['session_open'], '2026-11-18 09:30:00')
        self.assertEqual(sessions[0]['session_close'], '2026-11-18 16:00:00')


class TestUtcRequestStart(unittest.TestCase):
    def test_utc_request_uses_actual_dst_offset(self):
        self.assertEqual(
            et_to_utc_start('2026-11-18 09:30:00'), '20261118-14:30:00'
        )
        self.assertEqual(
            et_to_utc_start('2026-06-18 09:30:00'), '20260618-13:30:00'
        )

    def test_utc_request_accepts_naive_datetime(self):
        value = datetime.datetime(2026, 11, 18, 9, 30, 0)

        self.assertEqual(et_to_utc_start(value), '20261118-14:30:00')


class TestPageToRows(unittest.TestCase):
    def test_page_to_rows_converts_filters_sorts_and_dedupes(self):
        ticks = [
            _tick('2026-11-18 14:35:00', 100.0),
            _tick('2026-11-18 14:32:00', 101.0),
            _tick('2026-11-18 14:35:00', 102.0),
            _tick('2026-11-18 14:33:00', 0.0),
            _tick('2026-11-19 14:31:00', 104.0),
            _tick('2026-11-18 14:34:00', math.nan),
            _tick('2026-11-18 14:36:00', math.inf),
            _tick('2026-11-18 14:30:00', 99.5),
            _tick('2026-11-18 21:00:00', 103.0),
            _tick('2026-11-17 20:59:00', 105.0),
        ]

        rows = page_to_rows(
            ticks, '2026-11-18 09:30:00', '2026-11-18 16:00:00'
        )

        self.assertEqual(
            rows,
            [
                ('2026-11-18 09:30:00', 99.5),
                ('2026-11-18 09:32:00', 101.0),
                ('2026-11-18 09:35:00', 102.0),
                ('2026-11-18 16:00:00', 103.0),
            ],
        )

    def test_page_to_rows_returns_no_rows_for_out_of_session_page(self):
        ticks = [
            _tick('2026-11-19 14:31:00', 100.0),
            _tick('2026-11-19 14:32:00', 101.0),
        ]

        self.assertEqual(
            page_to_rows(
                ticks, '2026-11-18 09:30:00', '2026-11-18 16:00:00'
            ),
            [],
        )


class TestErrorClassification(unittest.TestCase):
    def test_error_classification(self):
        assert classify_errors([]) is None
        assert classify_errors([(10187, 'permissions')]) == 'fatal'
        assert classify_errors([(1100, 'connectivity')]) == 'reconnect'
        assert classify_errors([(162, 'no data')]) == 'no_data'
        assert classify_errors([(10197, 'competing session')]) == 'failed'

    def test_error_precedence(self):
        self.assertEqual(
            classify_errors([(162, 'no data'), (10187, 'permissions')]),
            'fatal',
        )
        self.assertEqual(
            classify_errors([(162, 'no data'), (502, 'connection lost')]),
            'reconnect',
        )
        self.assertEqual(
            classify_errors([(162, 'no data'), (10197, 'competing')]),
            'failed',
        )
        self.assertEqual(classify_errors([(10314, 'bad time')]), 'fatal')
        self.assertEqual(classify_errors([(504, 'timeout')]), 'reconnect')
        self.assertEqual(classify_errors([(1101, 'lost')]), 'reconnect')


class TestRequestTickPage(TickCollectorTestCase):
    def test_informational_errors_are_ignored(self):
        tick = _et_tick('2026-11-18 10:00:00', 100.0)
        source = ChunkSource([
            (
                [tick],
                [
                    (2104, 'market data farm connection is OK'),
                    (2106, 'HMDS data farm connection is OK'),
                    (2158, 'sec-def data farm connection is OK'),
                ],
            )
        ])
        ibkr = FakeIBKR(source)

        ticks, errors = request_tick_page(
            ibkr, CONTRACT, '20261118-15:00:00'
        )

        self.assertEqual(ticks, [tick])
        self.assertEqual(errors, [])
        self.assertEqual(ibkr.errorEvent.handlers, [])

    def test_non_informational_errors_are_collected(self):
        source = ChunkSource([([], [(162, 'no data')])])
        ibkr = FakeIBKR(source)

        ticks, errors = request_tick_page(
            ibkr, CONTRACT, '20261118-15:00:00'
        )

        self.assertEqual(ticks, [])
        self.assertEqual(errors, [(162, 'no data')])
        self.assertEqual(ibkr.errorEvent.handlers, [])


class TestCollectSession(TickCollectorTestCase):
    def test_three_pages_to_close_complete(self):
        ticks = _ticks_to_close(2501)
        source = CursorSource(ticks)

        ibkr, state = self._collect(source)

        self.assertEqual(len(ibkr.starts), 3)
        self.assertEqual(state['status'], 'complete')
        self.assertEqual(state['tick_count'], 2501)
        self.assertEqual(state['first_tick'], '2026-11-18 09:30:01')
        self.assertEqual(state['last_tick'], '2026-11-18 15:59:59')
        self.assertEqual(state['requests'], 3)
        self.assertIsNotNone(state['fetched_at'])
        stored = self.conn.execute(
            'SELECT date FROM "TEST_index_ticks" ORDER BY date'
        ).fetchall()
        self.assertEqual(len(stored), 2501)
        self.assertEqual(stored, sorted(stored))

    def test_short_page_continues_while_data_remains(self):
        first_page = [
            _et_tick('2026-11-18 09:30:%02d' % second, 100.0 + second)
            for second in range(1, 11)
        ]
        later = _ticks_between(
            '2026-11-18 09:31:00',
            '2026-11-18 15:59:59',
            60,
            base_price=2000.0,
        )
        source = ChunkSource([(first_page, []), (later, [])])

        ibkr, state = self._collect(source)

        self.assertEqual(len(ibkr.starts), 2)
        self.assertEqual(state['status'], 'complete')
        self.assertEqual(state['tick_count'], 70)
        self.assertEqual(state['last_tick'], '2026-11-18 15:59:59')

    def test_empty_response_moves_retry_then_unavailable(self):
        source = CursorSource([])

        _, first = self._collect(source)
        self.assertEqual(first['status'], 'retry')
        self.assertEqual(first['attempts'], 1)
        self.assertEqual(first['tick_count'], 0)
        self.assertIsNone(first['first_tick'])
        self.assertIsNone(first['last_tick'])

        _, second = self._collect(source)
        self.assertEqual(second['status'], 'retry')
        self.assertEqual(second['attempts'], 2)

        _, third = self._collect(source)
        self.assertEqual(third['status'], 'unavailable')
        self.assertEqual(third['attempts'], 3)

        _, fourth = self._collect(source)
        self.assertEqual(fourth['status'], 'unavailable')
        self.assertEqual(fourth['attempts'], 3)
        self.assertEqual(
            self.conn.execute('SELECT COUNT(*) FROM "TEST_index_ticks"')
            .fetchone()[0],
            0,
        )

    def test_next_session_ticks_are_no_data_for_requested_session(self):
        next_session = [
            _et_tick('2026-11-19 09:30:01', 100.0),
            _et_tick('2026-11-19 09:30:02', 101.0),
        ]
        source = CursorSource(next_session)

        _, first = self._collect(source)
        self.assertEqual(first['status'], 'retry')
        self.assertEqual(first['attempts'], 1)
        self.assertEqual(first['tick_count'], 0)

        _, second = self._collect(source)
        _, third = self._collect(source)
        self.assertEqual(second['status'], 'retry')
        self.assertEqual(second['attempts'], 2)
        self.assertEqual(third['status'], 'unavailable')
        self.assertEqual(third['attempts'], 3)
        self.assertEqual(
            self.conn.execute('SELECT COUNT(*) FROM "TEST_index_ticks"')
            .fetchone()[0],
            0,
        )

    def test_early_end_is_partial(self):
        early = [
            _et_tick('2026-11-18 09:30:%02d' % second, 100.0 + second)
            for second in range(1, 11)
        ]
        source = ChunkSource([(early, []), ([], [])])

        ibkr, state = self._collect(source)

        self.assertEqual(len(ibkr.starts), 2)
        self.assertEqual(state['status'], 'partial')
        self.assertEqual(state['tick_count'], 10)
        self.assertEqual(state['last_tick'], '2026-11-18 09:30:10')

    def test_complete_session_is_skipped_unless_forced(self):
        ticks = _ticks_to_close(100)
        source = CursorSource(ticks)
        _, completed = self._collect(source)
        self.assertEqual(completed['status'], 'complete')
        rows_before = self.conn.execute(
            'SELECT date, price FROM "TEST_index_ticks" ORDER BY date'
        ).fetchall()

        skipped_ibkr, skipped = self._collect(source)
        self.assertEqual(skipped_ibkr.starts, [])
        self.assertEqual(skipped, completed)
        self.assertEqual(
            self.conn.execute(
                'SELECT date, price FROM "TEST_index_ticks" ORDER BY date'
            ).fetchall(),
            rows_before,
        )

        forced_ibkr, forced = self._collect(source, force=True)
        self.assertEqual(len(forced_ibkr.starts), 1)
        self.assertEqual(forced['status'], 'complete')
        self.assertEqual(forced['tick_count'], 100)

    def test_in_progress_session_resumes_from_last_tick_plus_one_second(self):
        save_session(
            self.conn,
            {
                **SESSION,
                'status': 'in_progress',
                'tick_count': 100,
                'first_tick': '2026-11-18 09:30:01',
                'last_tick': '2026-11-18 10:00:00',
            },
        )
        remaining = _ticks_to_close(20)

        ibkr, state = self._collect(CursorSource(remaining))

        self.assertEqual(
            ibkr.starts[0], et_to_utc_start('2026-11-18 10:00:01')
        )
        self.assertEqual(state['status'], 'complete')
        self.assertEqual(state['last_tick'], '2026-11-18 15:59:59')

    def test_forced_refetch_of_stored_rows_still_advances_and_completes(self):
        ticks = _ticks_to_close(2501)
        source = CursorSource(ticks)

        _, first = self._collect(source)
        self.assertEqual(first['status'], 'complete')

        ibkr, second = self._collect(source, force=True)
        self.assertEqual(len(ibkr.starts), 3)
        self.assertEqual(second['status'], 'complete')
        self.assertEqual(second['tick_count'], 2501)
        self.assertEqual(second['requests'], 3)

    def test_forced_refetch_revises_prices_without_duplication(self):
        ticks = _ticks_to_close(100)
        self._collect(CursorSource(ticks))
        revised = [
            Tick(tick.time, tick.price + 500.0)
            for tick in ticks
        ]

        _, state = self._collect(CursorSource(revised), force=True)

        self.assertEqual(state['tick_count'], 100)
        dates = self.conn.execute(
            'SELECT date FROM "TEST_index_ticks" ORDER BY date'
        ).fetchall()
        self.assertEqual(len(dates), 100)
        sample = ticks[50]
        stored_date = (
            sample.time.astimezone(EASTERN).strftime('%Y-%m-%d %H:%M:%S')
        )
        price = self.conn.execute(
            'SELECT price FROM "TEST_index_ticks" WHERE date = ?',
            (stored_date,),
        ).fetchone()
        self.assertEqual(price[0], sample.price + 500.0)

    def test_cursor_non_advance_terminates(self):
        tick = _et_tick('2026-11-18 10:00:00', 100.0)
        source = ChunkSource([([tick], []), ([tick], [])])

        ibkr, state = self._collect(source)

        self.assertEqual(len(ibkr.starts), 2)
        self.assertEqual(state['status'], 'partial')
        self.assertEqual(state['tick_count'], 1)

    def test_request_cap_terminates(self):
        def expanding_source(start_utc, size):
            start = _utc_from_request(start_utc)
            return [
                Tick(start + datetime.timedelta(seconds=offset), 100.0)
                for offset in range(size)
            ], []

        with patch.object(config, 'tick_max_requests_per_session', 2):
            ibkr, state = self._collect(expanding_source)

        self.assertEqual(len(ibkr.starts), 2)
        self.assertEqual(state['requests'], 2)
        self.assertEqual(state['status'], 'partial')


class TestCollectSessionErrors(TickCollectorTestCase):
    def test_fatal_error_saves_failed_and_raises(self):
        source = ChunkSource([([], [(10187, 'permissions')])])

        with self.assertRaises(FatalTickError):
            self._collect(source)

        state = get_session(self.conn, 'TEST', SESSION_DATE)
        self.assertEqual(state['status'], 'failed')
        self.assertIn('10187', state['last_error'])
        self.assertEqual(state['attempts'], 0)
        self.assertEqual(state['tick_count'], 0)

    def test_invalid_request_time_is_fatal(self):
        source = ChunkSource([([], [(10314, 'invalid time')])])

        with self.assertRaises(FatalTickError):
            self._collect(source)

        state = get_session(self.conn, 'TEST', SESSION_DATE)
        self.assertEqual(state['status'], 'failed')
        self.assertIn('10314', state['last_error'])

    def test_competing_session_error_saves_failed_without_raising(self):
        source = ChunkSource([([], [(10197, 'competing session')])])

        _, state = self._collect(source)

        self.assertEqual(state['status'], 'failed')
        self.assertIn('10197', state['last_error'])
        self.assertEqual(state['attempts'], 0)

    def test_no_data_error_marks_retry(self):
        source = ChunkSource([([], [(162, 'no data')])])

        _, state = self._collect(source)

        self.assertEqual(state['status'], 'retry')
        self.assertEqual(state['attempts'], 1)

    def test_reconnect_then_retry_succeeds(self):
        ticks = _ticks_to_close(100)
        calls = {'count': 0}

        def source(start_utc, size):
            calls['count'] += 1
            if calls['count'] == 1:
                return [], [(1100, 'connectivity lost')]
            return ticks, []

        reconnect = FakeReconnect()
        ibkr, state = self._collect(source, reconnect=reconnect)

        self.assertEqual(reconnect.calls, 1)
        self.assertEqual(len(ibkr.starts), 2)
        self.assertEqual(state['status'], 'complete')
        self.assertEqual(state['tick_count'], 100)

    def test_exhausted_reconnects_save_failed(self):
        def source(start_utc, size):
            return [], [(1100, 'connectivity lost')]

        reconnect = FakeReconnect()

        with patch.object(config, 'tick_connection_retries', 2):
            ibkr, state = self._collect(source, reconnect=reconnect)

        self.assertEqual(reconnect.calls, 2)
        self.assertEqual(len(ibkr.starts), 3)
        self.assertEqual(state['status'], 'failed')
        self.assertIn('1100', state['last_error'])
        self.assertEqual(state['attempts'], 0)


if __name__ == '__main__':
    unittest.main()
