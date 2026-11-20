import datetime
import logging
import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd

import config
import interface_ibkr
import maintainSPXTicks
from interface_ticks import (
    audit_sessions,
    connect_ticks,
    ensure_schema,
    get_session,
    save_session,
    session_schedule,
    upsert_ticks,
)
from maintainSPXTicks import (
    acquire_lock,
    main,
    release_lock,
    run_backfill,
    run_daily,
    run_repair,
    status_summary,
)

SYMBOL = config.tick_symbol


def _save_session(conn, session_date, status):
    save_session(
        conn,
        {
            'symbol': SYMBOL,
            'session_date': session_date,
            'session_open': '%s 09:30:00' % session_date,
            'session_close': '%s 16:00:00' % session_date,
            'status': status,
        },
    )


def _dates(sessions):
    return [session['session_date'] for session in sessions]


def _grid(session_date, step_seconds=300, start='09:30:00', end='16:00:00',
          price=100.0):
    """Return ``(date, price)`` rows every ``step_seconds`` in the window."""
    moment = pd.Timestamp('%s %s' % (session_date, start))
    last = pd.Timestamp('%s %s' % (session_date, end))
    rows = []
    while moment <= last:
        rows.append((moment.strftime('%Y-%m-%d %H:%M:%S'), price))
        moment += pd.Timedelta(seconds=step_seconds)
    return rows


def _findings(*rows):
    return pd.DataFrame(
        [
            {
                'session_date': session_date,
                'severity': severity,
                'finding': finding,
                'detail': '',
            }
            for session_date, severity, finding in rows
        ],
        columns=['session_date', 'severity', 'finding', 'detail'],
    )


class RecordingCollector:
    """Stand-in for ``collect_session`` that records calls and returns state."""

    def __init__(self, status='complete', interrupt_on=None, events=None,
                 probe=None):
        self.status = status
        self.interrupt_on = interrupt_on
        self.events = events if events is not None else []
        self.probe = probe
        self.calls = []

    def __call__(self, ibkr, conn, contract, session, force=False, resume=True):
        self.calls.append(
            {
                'session_date': session['session_date'],
                'force': force,
                'resume': resume,
            }
        )
        self.events.append(('collect', session['session_date']))
        if self.probe is not None:
            self.probe(session)
        if self.interrupt_on is not None and len(self.calls) == self.interrupt_on:
            raise KeyboardInterrupt
        result = dict(session)
        result['status'] = self.status
        return result

    @property
    def collected_dates(self):
        return [call['session_date'] for call in self.calls]


class JobsTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = connect_ticks(':memory:')
        ensure_schema(self.conn, symbol=SYMBOL)

    def tearDown(self):
        self.conn.close()

    def _run_backfill(self, collector, start_date, end_date, **kwargs):
        with patch.object(maintainSPXTicks, 'collect_session', collector):
            return run_backfill(
                object(), self.conn, object(), start_date, end_date, **kwargs
            )

    def _run_daily(self, collector, now_et):
        with patch.object(maintainSPXTicks, 'collect_session', collector):
            return run_daily(object(), self.conn, object(), now_et=now_et)


class TestRunBackfill(JobsTestCase):
    def test_processes_only_incomplete_sessions_oldest_first(self):
        start_date = '2026-11-09'
        end_date = '2026-11-20'
        scheduled = _dates(session_schedule(start_date, end_date))
        statuses = [
            'complete',
            'unavailable',
            'retry',
            'failed',
            'in_progress',
            'partial',
        ]
        for session_date, status in zip(scheduled, statuses):
            _save_session(self.conn, session_date, status)

        events = []
        real_ensure = maintainSPXTicks.ensure_sessions

        def ensure_spy(conn, sessions, symbol=config.tick_symbol):
            events.append(('ensure', len(sessions)))
            return real_ensure(conn, sessions, symbol=symbol)

        collector = RecordingCollector(events=events)
        with patch.object(maintainSPXTicks, 'ensure_sessions', ensure_spy):
            summary = self._run_backfill(collector, start_date, end_date)

        expected_dates = scheduled[2:]
        self.assertEqual(events[0][0], 'ensure')
        self.assertEqual(collector.collected_dates, expected_dates)
        for call in collector.calls:
            self.assertFalse(call['force'])
            self.assertTrue(call['resume'])
        self.assertEqual(summary['job'], 'backfill')
        self.assertEqual(summary['start_date'], start_date)
        self.assertEqual(summary['end_date'], end_date)
        self.assertEqual(summary['processed'], len(expected_dates))
        self.assertEqual(summary['skipped'], 2)
        self.assertFalse(summary['stopped_early'])
        self.assertEqual(
            summary['status_counts'], {'complete': len(expected_dates)}
        )

        self.assertEqual(get_session(self.conn, SYMBOL, scheduled[0])['status'],
                         'complete')
        self.assertEqual(get_session(self.conn, SYMBOL, scheduled[1])['status'],
                         'unavailable')

    def test_creates_expected_rows_without_resetting_existing_status(self):
        start_date = '2026-11-16'
        end_date = '2026-11-20'
        scheduled = _dates(session_schedule(start_date, end_date))
        _save_session(self.conn, scheduled[0], 'complete')

        collector = RecordingCollector()
        with patch.object(
            maintainSPXTicks, 'collect_session', collector
        ):
            summary = run_backfill(
                object(), self.conn, object(), start_date, end_date
            )

        self.assertEqual(collector.collected_dates, scheduled[1:])
        self.assertEqual(summary['processed'], len(scheduled) - 1)
        self.assertEqual(summary['skipped'], 1)
        self.assertEqual(get_session(self.conn, SYMBOL, scheduled[0])['status'],
                         'complete')
        for session_date in scheduled[1:]:
            self.assertIsNotNone(get_session(self.conn, SYMBOL, session_date))

    def test_complete_and_unavailable_sessions_are_skipped(self):
        start_date = '2026-11-09'
        end_date = '2026-11-20'
        scheduled = _dates(session_schedule(start_date, end_date))
        for index, session_date in enumerate(scheduled):
            _save_session(
                self.conn,
                session_date,
                'complete' if index % 2 == 0 else 'unavailable',
            )
        before = {
            session_date: get_session(self.conn, SYMBOL, session_date)
            for session_date in scheduled
        }

        collector = RecordingCollector()
        summary = self._run_backfill(collector, start_date, end_date)

        self.assertEqual(collector.calls, [])
        self.assertEqual(summary['processed'], 0)
        self.assertEqual(summary['skipped'], len(scheduled))
        self.assertFalse(summary['stopped_early'])
        self.assertEqual(summary['status_counts'], {})
        for session_date, state in before.items():
            self.assertEqual(
                get_session(self.conn, SYMBOL, session_date), state
            )

    def test_zero_runtime_stops_before_the_first_session(self):
        start_date = '2026-11-16'
        end_date = '2026-11-20'

        collector = RecordingCollector()
        summary = self._run_backfill(
            collector, start_date, end_date, max_runtime_minutes=0
        )

        self.assertEqual(collector.calls, [])
        self.assertEqual(summary['processed'], 0)
        self.assertTrue(summary['stopped_early'])
        self.assertEqual(summary['status_counts'], {})
        for session in session_schedule(start_date, end_date):
            self.assertEqual(
                get_session(self.conn, SYMBOL, session['session_date'])['status'],
                'pending',
            )

    def test_runtime_expiry_stops_before_the_next_session(self):
        start_date = '2026-11-16'
        end_date = '2026-11-20'
        collector = RecordingCollector()

        with patch(
            'maintainSPXTicks.time.monotonic', side_effect=[0.0, 0.0, 100.0]
        ):
            summary = self._run_backfill(
                collector, start_date, end_date, max_runtime_minutes=1
            )

        self.assertEqual(len(collector.calls), 1)
        self.assertEqual(summary['processed'], 1)
        self.assertTrue(summary['stopped_early'])

    def test_keyboard_interrupt_stops_cleanly(self):
        start_date = '2026-11-16'
        end_date = '2026-11-20'
        collector = RecordingCollector(interrupt_on=2)

        summary = self._run_backfill(collector, start_date, end_date)

        self.assertEqual(len(collector.calls), 2)
        self.assertEqual(summary['processed'], 1)
        self.assertTrue(summary['stopped_early'])
        self.assertEqual(summary['status_counts'], {'complete': 1})


class TestRunRepair(JobsTestCase):
    def _seed(self, session_date, rows, status='complete'):
        _save_session(self.conn, session_date, status)
        if rows:
            upsert_ticks(self.conn, rows, symbol=SYMBOL)

    def _repair(self, collector, findings, **kwargs):
        with patch.object(maintainSPXTicks, 'collect_session', collector):
            return run_repair(
                object(), self.conn, object(), findings, **kwargs
            )

    def _tick_dates(self):
        return [
            row[0]
            for row in self.conn.execute(
                'SELECT date FROM "SPX_index_ticks" ORDER BY date'
            )
        ]

    def _seed_week(self):
        self._seed('2026-11-16', _grid('2026-11-16'))
        self._seed('2026-11-17', _grid('2026-11-17'))
        self._seed('2026-11-18', _grid('2026-11-18'))
        self._seed(
            '2026-11-19',
            _grid('2026-11-19', step_seconds=240, start='09:36:00'),
        )
        self._seed('2026-11-20', [], status='unavailable')
        self.conn.execute(
            'INSERT INTO "SPX_index_ticks" (date, price) VALUES (?, ?)',
            ('2026-11-18 10:00:01', -1.0),
        )

    def test_error_sessions_are_repaired_oldest_first_with_force(self):
        self._seed_week()
        findings = audit_sessions(
            self.conn, '2026-11-16', '2026-11-20', symbol=SYMBOL
        )

        collector = RecordingCollector()
        summary = self._repair(collector, findings)

        self.assertEqual(collector.collected_dates, ['2026-11-18'])
        for call in collector.calls:
            self.assertTrue(call['force'])
            self.assertFalse(call['resume'])
        self.assertEqual(summary['job'], 'repair')
        self.assertEqual(summary['sessions'], ['2026-11-18'])
        self.assertEqual(summary['start_date'], '2026-11-18')
        self.assertEqual(summary['end_date'], '2026-11-18')
        self.assertEqual(summary['processed'], 1)
        self.assertEqual(summary['status_counts'], {'complete': 1})
        self.assertEqual(summary['cleanup'], {
            'invalid_rows': 1,
            'unexpected_rows': 0,
        })
        self.assertTrue(summary['findings'].empty)
        self.assertEqual(
            list(summary['findings'].columns),
            ['session_date', 'severity', 'finding', 'detail'],
        )

    def test_warnings_and_unavailable_are_opt_in(self):
        findings = _findings(
            ('2026-11-16', 'error', 'failed'),
            ('2026-11-17', 'warning', 'retry'),
            ('2026-11-18', 'warning', 'late_start'),
            ('2026-11-19', 'warning', 'gap'),
            ('2026-11-20', 'warning', 'low_density'),
            ('2026-11-23', 'info', 'unavailable'),
            ('2026-11-24', 'info', 'no_rows_unavailable'),
        )

        default = RecordingCollector()
        default_summary = self._repair(default, findings)
        warnings = RecordingCollector()
        warnings_summary = self._repair(
            warnings, findings, include_warnings=True
        )
        unavailable = RecordingCollector()
        unavailable_summary = self._repair(
            unavailable, findings, force_unavailable=True
        )

        self.assertEqual(default_summary['sessions'], ['2026-11-16'])
        self.assertEqual(
            warnings_summary['sessions'],
            [
                '2026-11-16',
                '2026-11-17',
                '2026-11-18',
                '2026-11-19',
                '2026-11-20',
            ],
        )
        self.assertEqual(
            unavailable_summary['sessions'], ['2026-11-16', '2026-11-23']
        )
        for collector in (default, warnings, unavailable):
            for call in collector.calls:
                self.assertTrue(call['force'])
                self.assertFalse(call['resume'])

    def test_cleanup_precedes_refetch_without_deleting_valid_rows(self):
        self._seed('2026-11-16', _grid('2026-11-16'))
        self._seed('2026-11-17', _grid('2026-11-17'))
        self._seed('2026-11-18', _grid('2026-11-18'))
        self._seed('2026-11-19', _grid('2026-11-19'))
        self._seed('2026-11-20', _grid('2026-11-20'))
        upsert_ticks(
            self.conn,
            [
                ('2026-11-18 07:00:00', 100.0),
                ('2026-11-21 12:00:00', 100.0),
            ],
            symbol=SYMBOL,
        )
        self.conn.execute(
            'INSERT INTO "SPX_index_ticks" (date, price) VALUES (?, ?)',
            ('2026-11-18 10:00:01', -1.0),
        )
        findings = audit_sessions(
            self.conn, '2026-11-16', '2026-11-21', symbol=SYMBOL
        )
        expected_rows = [
            row[0]
            for session_date in (
                '2026-11-16',
                '2026-11-17',
                '2026-11-18',
                '2026-11-19',
                '2026-11-20',
            )
            for row in _grid(session_date)
        ]

        seen = {}

        def probe(session):
            seen['dates'] = self._tick_dates()

        collector = RecordingCollector(probe=probe)
        summary = self._repair(collector, findings)

        self.assertEqual(collector.collected_dates, ['2026-11-18'])
        self.assertEqual(seen['dates'], expected_rows)
        self.assertEqual(
            summary['sessions'], ['2026-11-18', '2026-11-21']
        )
        self.assertEqual(summary['processed'], 1)
        self.assertEqual(summary['cleanup'], {
            'invalid_rows': 1,
            'unexpected_rows': 2,
        })
        self.assertEqual(self._tick_dates(), expected_rows)

    def test_weekend_only_selection_clears_the_range(self):
        upsert_ticks(
            self.conn, [('2026-11-21 12:00:00', 100.0)], symbol=SYMBOL
        )
        findings = _findings(('2026-11-21', 'error', 'unexpected_rows'))

        collector = RecordingCollector()
        summary = self._repair(collector, findings)

        self.assertEqual(summary['sessions'], ['2026-11-21'])
        self.assertEqual(summary['processed'], 0)
        self.assertEqual(summary['status_counts'], {})
        self.assertEqual(summary['cleanup'], {
            'invalid_rows': 0,
            'unexpected_rows': 1,
        })
        self.assertEqual(self._tick_dates(), [])
        self.assertTrue(summary['findings'].empty)

    def test_cleanup_stays_inside_the_selected_range(self):
        outside_rows = _grid('2026-11-13') + [('2026-11-14 12:00:00', 100.0)]
        upsert_ticks(self.conn, outside_rows, symbol=SYMBOL)
        upsert_ticks(
            self.conn, [('2026-11-18 07:00:00', 100.0)], symbol=SYMBOL
        )
        findings = _findings(('2026-11-18', 'error', 'unexpected_rows'))

        collector = RecordingCollector()
        summary = self._repair(collector, findings)

        self.assertEqual(summary['cleanup'], {
            'invalid_rows': 0,
            'unexpected_rows': 1,
        })
        self.assertEqual(self._tick_dates(), [row[0] for row in outside_rows])

    def test_no_findings_is_a_no_op(self):
        empty = pd.DataFrame(
            columns=['session_date', 'severity', 'finding', 'detail']
        )

        collector = RecordingCollector()
        summary = self._repair(collector, empty)

        self.assertEqual(collector.calls, [])
        self.assertEqual(summary['sessions'], [])
        self.assertEqual(summary['processed'], 0)
        self.assertEqual(summary['status_counts'], {})
        self.assertEqual(summary['start_date'], None)
        self.assertEqual(summary['end_date'], None)
        self.assertTrue(summary['findings'].empty)
        self.assertEqual(
            list(summary['findings'].columns),
            ['session_date', 'severity', 'finding', 'detail'],
        )


class TestRunDaily(JobsTestCase):
    def test_midday_skips_the_open_session_and_prior_sessions_run(self):
        now_et = datetime.datetime(2026, 11, 18, 12, 0)
        expected = _dates(session_schedule('2026-11-12', '2026-11-17'))

        collector = RecordingCollector()
        summary = self._run_daily(collector, now_et)

        self.assertEqual(collector.collected_dates, expected)
        self.assertEqual(summary['job'], 'daily')
        self.assertEqual(summary['start_date'], '2026-11-12')
        self.assertEqual(summary['end_date'], '2026-11-18')
        self.assertEqual(summary['processed'], len(expected))
        self.assertEqual(summary['skipped'], 0)
        self.assertFalse(summary['stopped_early'])
        self.assertEqual(summary['status_counts'], {'complete': len(expected)})
        self.assertIsNotNone(get_session(self.conn, SYMBOL, expected[-1]))

    def test_daily_discovers_fourteen_days_but_processes_only_seven(self):
        now_et = datetime.datetime(2026, 11, 18, 12, 0)
        discovery = _dates(session_schedule('2026-11-05', '2026-11-18'))
        older = [date for date in discovery if date < '2026-11-12']

        collector = RecordingCollector()
        self._run_daily(collector, now_et)

        self.assertTrue(older)
        for session_date in older:
            state = get_session(self.conn, SYMBOL, session_date)
            self.assertEqual(state['status'], 'pending')
            self.assertNotIn(session_date, collector.collected_dates)

    def test_after_close_includes_the_just_closed_session(self):
        now_et = datetime.datetime(2026, 11, 18, 16, 30)
        expected = _dates(session_schedule('2026-11-12', '2026-11-18'))

        collector = RecordingCollector()
        summary = self._run_daily(collector, now_et)

        self.assertEqual(collector.collected_dates, expected)
        self.assertEqual(collector.collected_dates[-1], '2026-11-18')
        self.assertEqual(summary['processed'], len(expected))

    def test_daily_skips_complete_and_unavailable_sessions(self):
        now_et = datetime.datetime(2026, 11, 18, 12, 0)
        _save_session(self.conn, '2026-11-16', 'complete')
        _save_session(self.conn, '2026-11-17', 'unavailable')

        collector = RecordingCollector()
        summary = self._run_daily(collector, now_et)

        self.assertEqual(
            collector.collected_dates, ['2026-11-12', '2026-11-13']
        )
        self.assertEqual(summary['skipped'], 2)
        self.assertEqual(
            get_session(self.conn, SYMBOL, '2026-11-16')['status'], 'complete'
        )
        self.assertEqual(
            get_session(self.conn, SYMBOL, '2026-11-17')['status'], 'unavailable'
        )

    def test_daily_accepts_a_naive_eastern_datetime(self):
        now_et = datetime.datetime(2026, 11, 18, 12, 0)
        collector = RecordingCollector()

        self._run_daily(collector, now_et)

        self.assertEqual(collector.collected_dates[-1], '2026-11-17')


def _job_fixture(job='backfill', start_date='2026-11-16',
                 end_date='2026-11-20'):
    return {
        'job': job,
        'start_date': start_date,
        'end_date': end_date,
        'processed': 0,
        'skipped': 0,
        'stopped_early': False,
        'status_counts': {},
    }


def _empty_findings():
    return pd.DataFrame(
        columns=['session_date', 'severity', 'finding', 'detail']
    )


class FakeIbkr:
    def __init__(self):
        self.disconnects = 0

    def disconnect(self):
        self.disconnects += 1


class TestWriteLock(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.path = os.path.join(self._dir.name, 'nested', 'ticks.lock')

    def test_lock_is_exclusive_and_reusable_after_release(self):
        handle = acquire_lock(self.path)
        try:
            self.assertTrue(os.path.exists(self.path))
            with self.assertRaises(RuntimeError):
                acquire_lock(self.path)
        finally:
            release_lock(handle, self.path)

        self.assertFalse(os.path.exists(self.path))
        again = acquire_lock(self.path)
        release_lock(again, self.path)
        self.assertFalse(os.path.exists(self.path))

    def test_stale_lock_file_does_not_block_a_new_run(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, 'w', encoding='utf-8'):
            pass

        handle = acquire_lock(self.path)
        release_lock(handle, self.path)

        self.assertFalse(os.path.exists(self.path))


class TestStatusSummary(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.db_path = os.path.join(self._dir.name, 'ticks.db')
        self.conn = connect_ticks(self.db_path)
        ensure_schema(self.conn, symbol=SYMBOL)
        self.addCleanup(self.conn.close)

    def test_reports_counts_bounds_rows_and_file_size(self):
        for session_date, status in (
            ('2026-11-16', 'complete'),
            ('2026-11-17', 'complete'),
            ('2026-11-18', 'partial'),
            ('2026-11-19', 'failed'),
            ('2026-11-20', 'pending'),
            ('2026-11-23', 'unavailable'),
        ):
            _save_session(self.conn, session_date, status)
        rows = (
            _grid('2026-11-16')[:3]
            + _grid('2026-11-17')[:2]
            + [('2026-11-18 10:00:00', 100.0)]
        )
        upsert_ticks(self.conn, rows, symbol=SYMBOL)

        summary = status_summary(self.conn)

        self.assertEqual(summary['symbol'], SYMBOL)
        self.assertEqual(
            summary['status_counts'],
            {
                'complete': 2,
                'partial': 1,
                'failed': 1,
                'pending': 1,
                'unavailable': 1,
            },
        )
        self.assertEqual(summary['sessions'], 6)
        self.assertEqual(summary['earliest_complete'], '2026-11-16')
        self.assertEqual(summary['latest_complete'], '2026-11-17')
        self.assertEqual(summary['tick_rows'], len(rows))
        self.assertGreater(summary['db_size_bytes'], 0)

    def test_empty_schema_reports_zeroes(self):
        summary = status_summary(self.conn)

        self.assertEqual(summary['status_counts'], {})
        self.assertEqual(summary['sessions'], 0)
        self.assertIsNone(summary['earliest_complete'])
        self.assertIsNone(summary['latest_complete'])
        self.assertEqual(summary['tick_rows'], 0)
        self.assertGreater(summary['db_size_bytes'], 0)

    def test_memory_database_has_no_file_size(self):
        conn = connect_ticks(':memory:')
        self.addCleanup(conn.close)
        ensure_schema(conn, symbol=SYMBOL)

        summary = status_summary(conn)

        self.assertIsNone(summary['db_size_bytes'])


class CliTestCase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.db_path = os.path.join(self._dir.name, 'ticks.db')
        conn = connect_ticks(self.db_path)
        try:
            ensure_schema(conn, symbol=SYMBOL)
        finally:
            conn.close()
        settings = patch.object(
            maintainSPXTicks,
            'configure_logging',
            side_effect=self._keep_logs_out_of_the_repository,
        )
        settings.start()
        self.addCleanup(settings.stop)

    @staticmethod
    def _keep_logs_out_of_the_repository(*args, **kwargs):
        logger = logging.getLogger(maintainSPXTicks.LOGGER_NAME)
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False
        return None

    def _seed_clean_session(self, session_date):
        conn = connect_ticks(self.db_path)
        try:
            _save_session(conn, session_date, 'complete')
            upsert_ticks(conn, _grid(session_date), symbol=SYMBOL)
        finally:
            conn.close()

    def _run_backfill(self, argv, fixture=None):
        fixture = fixture or _job_fixture()
        calls = []

        def fake_backfill(ibkr, conn, contract, start_date, end_date,
                          max_runtime_minutes=None):
            calls.append(
                {
                    'ibkr': ibkr,
                    'conn': conn,
                    'contract': contract,
                    'start_date': start_date,
                    'end_date': end_date,
                    'max_runtime_minutes': max_runtime_minutes,
                }
            )
            return dict(fixture, start_date=start_date, end_date=end_date)

        fake = FakeIbkr()
        with patch.object(
            interface_ibkr, 'setupConnection', return_value=fake
        ) as setup, patch.object(
            maintainSPXTicks, 'run_backfill', fake_backfill
        ), patch.object(maintainSPXTicks, 'Index') as index:
            self.code = main(argv)
        return calls, fake, setup, index


class TestCliBackfill(CliTestCase):
    def test_default_range_is_three_years_to_the_latest_completed_session(self):
        argv = ['--backfill', '--db-path', self.db_path]

        with patch.object(
            maintainSPXTicks,
            '_now_et',
            return_value=datetime.datetime(2026, 11, 19, 10, 0),
        ):
            calls, _, setup, _ = self._run_backfill(argv)

        self.assertEqual(self.code, 0)
        self.assertEqual(calls[0]['start_date'], '2023-11-19')
        self.assertEqual(calls[0]['end_date'], '2026-11-18')
        setup.assert_called_once_with(clientId=config.tick_client_id)

    def test_after_the_close_the_default_end_is_today(self):
        argv = ['--backfill', '--db-path', self.db_path]

        with patch.object(
            maintainSPXTicks,
            '_now_et',
            return_value=datetime.datetime(2026, 11, 19, 16, 30),
        ):
            calls, _, _, _ = self._run_backfill(argv)

        self.assertEqual(calls[0]['end_date'], '2026-11-19')

    def test_explicit_range_and_runtime_override_the_defaults(self):
        argv = [
            '--backfill',
            '--start', '2026-11-16',
            '--end', '2026-11-20',
            '--max-runtime-minutes', '1.5',
            '--db-path', self.db_path,
        ]

        with patch.object(
            maintainSPXTicks,
            '_now_et',
            return_value=datetime.datetime(2026, 11, 19, 10, 0),
        ):
            calls, _, _, _ = self._run_backfill(argv)

        self.assertEqual(self.code, 0)
        self.assertEqual(calls[0]['start_date'], '2026-11-16')
        self.assertEqual(calls[0]['end_date'], '2026-11-20')
        self.assertEqual(calls[0]['max_runtime_minutes'], 1.5)

    def test_write_run_uses_client_id_db_path_and_releases_the_lock(self):
        argv = [
            '--backfill',
            '--start', '2026-11-16',
            '--end', '2026-11-20',
            '--db-path', self.db_path,
        ]
        observed = {}

        def fake_backfill(ibkr, conn, contract, start_date, end_date,
                          max_runtime_minutes=None):
            observed['db_file'] = conn.execute(
                'PRAGMA database_list'
            ).fetchone()[2]
            observed['tables'] = [
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            ]
            observed['lock_exists'] = os.path.exists(self.db_path + '.lock')
            return _job_fixture(start_date=start_date, end_date=end_date)

        fake = FakeIbkr()
        with patch.object(
            interface_ibkr, 'setupConnection', return_value=fake
        ) as setup, patch.object(
            maintainSPXTicks, 'run_backfill', fake_backfill
        ), patch.object(maintainSPXTicks, 'Index') as index:
            code = main(argv)

        self.assertEqual(code, 0)
        setup.assert_called_once_with(clientId=11)
        index.assert_called_once_with(
            config.tick_symbol, config.tick_exchange, config.tick_currency
        )
        self.assertEqual(observed['db_file'], self.db_path)
        self.assertIn('SPX_index_ticks', observed['tables'])
        self.assertIn('00-tick_sessions', observed['tables'])
        self.assertTrue(observed['lock_exists'])
        self.assertFalse(os.path.exists(self.db_path + '.lock'))
        self.assertEqual(fake.disconnects, 1)

    def test_failure_releases_the_lock_and_disconnects(self):
        argv = [
            '--backfill',
            '--start', '2026-11-16',
            '--end', '2026-11-20',
            '--db-path', self.db_path,
        ]
        fake = FakeIbkr()

        with patch.object(
            interface_ibkr, 'setupConnection', return_value=fake
        ), patch.object(
            maintainSPXTicks,
            'run_backfill',
            side_effect=RuntimeError('request failed'),
        ):
            code = main(argv)

        self.assertEqual(code, 1)
        self.assertFalse(os.path.exists(self.db_path + '.lock'))
        self.assertEqual(fake.disconnects, 1)

    def test_a_held_lock_aborts_before_connecting(self):
        handle = acquire_lock(self.db_path + '.lock')
        self.addCleanup(release_lock, handle, self.db_path + '.lock')
        argv = [
            '--backfill',
            '--start', '2026-11-16',
            '--end', '2026-11-20',
            '--db-path', self.db_path,
        ]

        with patch.object(
            interface_ibkr, 'setupConnection', side_effect=AssertionError
        ) as setup:
            code = main(argv)

        self.assertEqual(code, 1)
        setup.assert_not_called()


class TestCliReadOnly(CliTestCase):
    def _spy_connect(self):
        real_connect = maintainSPXTicks.connect_ticks
        calls = {}

        def spy(db_path=config.dbname_ticks, readonly=False):
            calls['db_path'] = db_path
            calls['readonly'] = readonly
            return real_connect(db_path, readonly=readonly)

        return spy, calls

    def test_audit_reads_only_and_never_constructs_an_ibkr_connection(self):
        self._seed_clean_session('2026-11-16')
        spy, calls = self._spy_connect()
        argv = [
            '--audit',
            '--start', '2026-11-16',
            '--end', '2026-11-16',
            '--db-path', self.db_path,
        ]

        with patch.object(
            interface_ibkr, 'setupConnection', side_effect=AssertionError
        ) as setup, patch.object(
            maintainSPXTicks, 'Index', side_effect=AssertionError
        ), patch.object(maintainSPXTicks, 'connect_ticks', spy):
            code = main(argv)

        self.assertEqual(code, 0)
        setup.assert_not_called()
        self.assertTrue(calls['readonly'])
        self.assertEqual(calls['db_path'], self.db_path)

    def test_status_reads_only_and_never_constructs_an_ibkr_connection(self):
        spy, calls = self._spy_connect()
        argv = ['--status', '--db-path', self.db_path]

        with patch.object(
            interface_ibkr, 'setupConnection', side_effect=AssertionError
        ) as setup, patch.object(
            maintainSPXTicks, 'Index', side_effect=AssertionError
        ), patch.object(maintainSPXTicks, 'connect_ticks', spy):
            code = main(argv)

        self.assertEqual(code, 0)
        setup.assert_not_called()
        self.assertTrue(calls['readonly'])

    def test_read_only_modes_do_not_create_a_missing_database(self):
        missing = os.path.join(self._dir.name, 'missing.db')

        with patch.object(
            interface_ibkr, 'setupConnection', side_effect=AssertionError
        ) as setup:
            self.assertEqual(main(['--status', '--db-path', missing]), 1)
            self.assertEqual(
                main(
                    [
                        '--audit',
                        '--start', '2026-11-16',
                        '--end', '2026-11-16',
                        '--db-path', missing,
                    ]
                ),
                1,
            )

        setup.assert_not_called()
        self.assertFalse(os.path.exists(missing))


class TestCliAuditExitCode(CliTestCase):
    def _audit(self, argv, findings):
        with patch.object(
            interface_ibkr, 'setupConnection', side_effect=AssertionError
        ), patch.object(
            maintainSPXTicks, 'audit_sessions', return_value=findings
        ):
            return main(
                argv
                + [
                    '--start', '2026-11-16',
                    '--end', '2026-11-16',
                    '--db-path', self.db_path,
                ]
            )

    def test_errors_fail_the_audit(self):
        findings = _findings(('2026-11-16', 'error', 'failed'))

        self.assertEqual(self._audit(['--audit'], findings), 1)

    def test_warnings_fail_only_with_strict(self):
        findings = _findings(('2026-11-16', 'warning', 'gap'))

        self.assertEqual(self._audit(['--audit'], findings), 0)
        self.assertEqual(self._audit(['--audit', '--strict'], findings), 1)

    def test_informational_findings_never_fail_the_audit(self):
        findings = _findings(('2026-11-16', 'info', 'unavailable'))

        self.assertEqual(self._audit(['--audit'], findings), 0)
        self.assertEqual(self._audit(['--audit', '--strict'], findings), 0)


class TestCliRepair(CliTestCase):
    def test_repair_audits_then_repairs_with_the_selection_flags(self):
        findings = _findings(('2026-11-18', 'warning', 'low_density'))
        recorded = {}

        def fake_audit(conn, start_date, end_date,
                       symbol=config.tick_symbol):
            recorded['audit_dates'] = (start_date, end_date)
            return findings

        def fake_repair(ibkr, conn, contract, audit_findings,
                        include_warnings=False, force_unavailable=False):
            recorded['findings'] = audit_findings
            recorded['include_warnings'] = include_warnings
            recorded['force_unavailable'] = force_unavailable
            recorded['lock_exists'] = os.path.exists(self.db_path + '.lock')
            return {
                'job': 'repair',
                'start_date': '2026-11-18',
                'end_date': '2026-11-18',
                'sessions': ['2026-11-18'],
                'processed': 1,
                'status_counts': {'complete': 1},
                'cleanup': {'invalid_rows': 0, 'unexpected_rows': 0},
                'findings': _empty_findings(),
            }

        argv = [
            '--repair',
            '--start', '2026-11-16',
            '--end', '2026-11-20',
            '--include-warnings',
            '--force-unavailable',
            '--db-path', self.db_path,
        ]
        fake = FakeIbkr()
        with patch.object(
            interface_ibkr, 'setupConnection', return_value=fake
        ) as setup, patch.object(
            maintainSPXTicks, 'audit_sessions', fake_audit
        ), patch.object(
            maintainSPXTicks, 'run_repair', fake_repair
        ):
            code = main(argv)

        self.assertEqual(code, 0)
        setup.assert_called_once_with(clientId=config.tick_client_id)
        self.assertEqual(recorded['audit_dates'], ('2026-11-16', '2026-11-20'))
        self.assertIs(recorded['findings'], findings)
        self.assertTrue(recorded['include_warnings'])
        self.assertTrue(recorded['force_unavailable'])
        self.assertTrue(recorded['lock_exists'])
        self.assertFalse(os.path.exists(self.db_path + '.lock'))
        self.assertEqual(fake.disconnects, 1)

    def test_repair_returns_the_post_repair_audit_exit_code(self):
        findings = _findings(('2026-11-18', 'warning', 'low_density'))
        leftover = _findings(('2026-11-18', 'warning', 'gap'))
        argv = [
            '--repair',
            '--start', '2026-11-16',
            '--end', '2026-11-20',
            '--db-path', self.db_path,
        ]

        def fake_repair(ibkr, conn, contract, audit_findings,
                        include_warnings=False, force_unavailable=False):
            return {
                'job': 'repair',
                'start_date': '2026-11-18',
                'end_date': '2026-11-18',
                'sessions': ['2026-11-18'],
                'processed': 1,
                'status_counts': {'complete': 1},
                'cleanup': {'invalid_rows': 0, 'unexpected_rows': 0},
                'findings': leftover,
            }

        with patch.object(
            interface_ibkr, 'setupConnection', return_value=FakeIbkr()
        ), patch.object(
            maintainSPXTicks, 'audit_sessions', return_value=findings
        ), patch.object(maintainSPXTicks, 'run_repair', fake_repair):
            self.assertEqual(main(argv), 0)
            self.assertEqual(main(argv + ['--strict']), 1)


class TestCliLogging(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.db_path = os.path.join(self._dir.name, 'ticks.db')
        self.log_directory = os.path.join(self._dir.name, 'logs')
        conn = connect_ticks(self.db_path)
        try:
            ensure_schema(conn, symbol=SYMBOL)
        finally:
            conn.close()
        directory = patch.object(
            maintainSPXTicks, 'LOG_DIRECTORY', self.log_directory
        )
        directory.start()
        self.addCleanup(directory.stop)
        self.addCleanup(self._close_handlers)

    @staticmethod
    def _close_handlers():
        logger = logging.getLogger(maintainSPXTicks.LOGGER_NAME)
        for handler in list(logger.handlers):
            handler.close()
            logger.removeHandler(handler)

    def test_cli_runs_append_to_a_daily_log_file(self):
        argv = ['--status', '--db-path', self.db_path]
        log_path = os.path.join(self.log_directory, 'spx_ticks_20261119.log')

        with patch.object(
            maintainSPXTicks,
            '_now_et',
            return_value=datetime.datetime(2026, 11, 19, 10, 0),
        ):
            self.assertEqual(main(argv), 0)
            self.assertTrue(os.path.exists(log_path))
            self.assertEqual(main(argv), 0)

        with open(log_path, encoding='utf-8') as handle:
            contents = handle.read()
        self.assertEqual(contents.count('command=status'), 2)


if __name__ == '__main__':
    unittest.main()
