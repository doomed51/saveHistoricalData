import datetime
import math
import unittest

from interface_ticks import session_schedule
from maintainSPXTicks import et_to_utc_start, page_to_rows

UTC = datetime.timezone.utc


class Tick:
    """Minimal stand-in for an IBKR historical tick."""

    def __init__(self, time, price):
        self.time = time
        self.price = price


def _tick(time_text, price):
    time = datetime.datetime.strptime(time_text, '%Y-%m-%d %H:%M:%S')
    return Tick(time.replace(tzinfo=UTC), price)


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


if __name__ == '__main__':
    unittest.main()
