import datetime
import unittest
from unittest.mock import patch

import pandas as pd

import config
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
from maintainSPXTicks import run_backfill, run_daily, run_repair

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


if __name__ == '__main__':
    unittest.main()
