import datetime
import unittest
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import getHistoricalData as historical
import pandas as pd


EASTERN = ZoneInfo('America/New_York')


class TestAfterHoursUpdate(unittest.TestCase):
    def test_main_updates_at_startup_only_when_due(self):
        before_close = datetime.datetime(2026, 10, 21, 17, 14, tzinfo=EASTERN)
        after_close = datetime.datetime(2026, 10, 21, 17, 15, tzinfo=EASTERN)

        for now, update_expected in ((before_close, False), (after_close, True)):
            with self.subTest(now=now), \
                    patch.object(historical, '_eastern_now', return_value=now), \
                    patch.object(historical, 'updateRecords') as update, \
                    patch.object(historical, 'bulkUpdate') as bulk:
                historical.main([])
            self.assertEqual(update.called, update_expected)
            bulk.assert_called_once_with(last_current_update=now.date() if update_expected else None)

    def test_startup_is_not_due_before_five_on_a_trading_day(self):
        now = datetime.datetime(2026, 10, 21, 17, 14, tzinfo=EASTERN)
        self.assertFalse(historical._is_after_hours_update_due(now))

    def test_startup_is_due_at_five_on_a_trading_day(self):
        now = datetime.datetime(2026, 10, 21, 17, 15, tzinfo=EASTERN)
        self.assertTrue(historical._is_after_hours_update_due(now))

    def test_nyse_holiday_is_not_a_trading_day(self):
        now = datetime.datetime(2026, 11, 26, 17, 1, tzinfo=EASTERN)
        self.assertFalse(historical._is_after_hours_update_due(now))

    def test_eastern_timezone_observes_daylight_saving_time(self):
        winter = datetime.datetime(2026, 1, 12, 17, 15, tzinfo=EASTERN)
        summer = datetime.datetime(2026, 7, 13, 17, 15, tzinfo=EASTERN)
        self.assertTrue(historical._is_after_hours_update_due(winter))
        self.assertTrue(historical._is_after_hours_update_due(summer))
        self.assertEqual(winter.utcoffset(), datetime.timedelta(hours=-5))
        self.assertEqual(summer.utcoffset(), datetime.timedelta(hours=-4))

    def test_daily_update_runs_once_for_a_trading_date(self):
        now = datetime.datetime(2026, 10, 21, 17, 15, tzinfo=EASTERN)
        with patch.object(historical, '_is_after_hours_update_due', return_value=True), \
                patch.object(historical, 'updateRecords') as update:
            last_date = historical._maybe_update_current_records(None, now)
            last_date = historical._maybe_update_current_records(last_date, now)
        update.assert_called_once_with()
        self.assertEqual(last_date, now.date())

    def test_prehistory_stops_before_next_request_after_a_saved_chunk(self):
        lookup = pd.DataFrame([{
            'symbol': 'ABC',
            'interval': '1day',
            'numMissingBusinessDays': 10,
            'firstRecordDate': '2020-01-10',
        }])
        connection = MagicMock()
        connection.__enter__.return_value = connection
        bars = pd.DataFrame({'date': [pd.Timestamp('2020-01-05')]})

        with patch.object(historical.db, 'sqlite_connection', return_value=connection), \
                patch.object(historical.db, 'getLookup_symbolRecords', return_value=lookup), \
                patch.object(historical.db, 'getExhaustedPrehistoryRows', return_value=set()), \
                patch.object(historical.db, 'saveHistoryToDB') as save, \
                patch.object(historical.db, '_getFirstRecordDate', return_value='2020-01-05'), \
                patch.object(historical.db, '_updateLookup_symbolRecords'), \
                patch.object(historical.db, 'recordPrehistoryAttempt') as record_attempt, \
                patch.object(historical.ib, 'getEarliestTimeStamp', return_value=pd.Timestamp('2019-01-01')), \
                patch.object(historical, 'get_contract', return_value=object()), \
                patch.object(historical.ib, 'getBars', return_value=bars) as get_bars:
            should_yield = MagicMock(side_effect=[False, False, False, True])
            interrupted = historical.updatePreHistoricData(
                object(), should_yield=should_yield
            )

        self.assertTrue(interrupted)
        get_bars.assert_called_once()
        save.assert_called_once()
        record_attempt.assert_not_called()


if __name__ == '__main__':
    unittest.main()
