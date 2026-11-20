import datetime
import unittest
from unittest.mock import patch

import config
import maintainSPXTicks
from interface_ticks import (
    connect_ticks,
    ensure_schema,
    get_session,
    save_session,
    session_schedule,
)
from maintainSPXTicks import run_backfill, run_daily

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


class RecordingCollector:
    """Stand-in for ``collect_session`` that records calls and returns state."""

    def __init__(self, status='complete', interrupt_on=None, events=None):
        self.status = status
        self.interrupt_on = interrupt_on
        self.events = events if events is not None else []
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
