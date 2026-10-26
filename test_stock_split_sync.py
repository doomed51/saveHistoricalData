"""Split storage and scheduling tests without Yahoo, IBKR, or disk databases."""

import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, mock_open, patch

import interface_localDB as db
import getHistoricalData as collector
import stock_split_sync as sync
from yfinance.exceptions import YFRateLimitError


NOW = datetime(2026, 10, 26, 20, 0, tzinfo=timezone.utc)


class TestSplitStorage(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute(
            'CREATE TABLE "UVXY_stock_1day" (date TEXT, close REAL)'
        )
        self.conn.execute(
            'INSERT INTO "UVXY_stock_1day" VALUES (?, ?)', ("2020-01-02", 100.0)
        )
        db.create_split_tables(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_initial_repeat_and_corrected_ratio_preserve_price_rows(self):
        events = [("2020-08-31", 4.0), ("2023-06-01", 0.1)]
        db.record_split_success(self.conn, "UVXY", events, NOW)
        db.record_split_success(self.conn, "UVXY", events, NOW + timedelta(days=1))
        db.record_split_success(
            self.conn, "UVXY", [("2020-08-31", 5.0), events[1]],
            NOW + timedelta(days=2),
        )

        stored = db.get_split_events(self.conn, "UVXY")
        self.assertEqual(len(stored), 2)
        self.assertEqual(stored[0]["ratio"], 5.0)
        self.assertEqual(stored[0]["first_seen_utc"], NOW.isoformat())
        self.assertEqual(stored[0]["source_missing"], 0)
        self.assertEqual(
            self.conn.execute('SELECT date, close FROM "UVXY_stock_1day"').fetchall(),
            [("2020-01-02", 100.0)],
        )

    def test_missing_event_is_flagged_and_retained_then_cleared(self):
        db.record_split_success(self.conn, "UVXY", [("2020-08-31", 4.0)], NOW)
        db.record_split_success(self.conn, "UVXY", [], NOW + timedelta(days=1))
        self.assertEqual(db.get_split_events(self.conn, "UVXY")[0]["source_missing"], 1)
        self.assertEqual(
            db.get_split_sync_state(self.conn, "UVXY")["last_success_utc"],
            (NOW + timedelta(days=1)).isoformat(),
        )
        db.record_split_success(
            self.conn, "UVXY", [("2020-08-31", 4.0)], NOW + timedelta(days=2)
        )
        self.assertEqual(db.get_split_events(self.conn, "UVXY")[0]["source_missing"], 0)

    def test_failed_save_rolls_back_events_and_success_timestamp(self):
        db.record_split_success(self.conn, "UVXY", [("2020-08-31", 4.0)], NOW)
        with self.assertRaises(sqlite3.IntegrityError):
            db.record_split_success(
                self.conn, "UVXY",
                [("2020-08-31", 5.0), ("2023-06-01", -1.0)],
                NOW + timedelta(days=1),
            )
        self.assertEqual(db.get_split_events(self.conn, "UVXY")[0]["ratio"], 4.0)
        self.assertEqual(
            db.get_split_sync_state(self.conn, "UVXY")["last_success_utc"],
            NOW.isoformat(),
        )

    def test_split_tables_are_not_treated_as_price_tables(self):
        db.record_split_success(self.conn, "UVXY", [("2020-08-31", 4.0)], NOW)
        records = db.getRecords(self.conn)
        self.assertEqual(records["name"].tolist(), ["UVXY_stock_1day"])


class TestSplitSync(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        db.create_split_tables(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_candidates_union_watchlist_and_existing_stock_tables(self):
        self.conn.execute('CREATE TABLE "VXX_stock_1day" (date TEXT)')
        self.conn.execute('CREATE TABLE "SPX_index_1day" (date TEXT)')
        with patch("builtins.open", mock_open(read_data="UVXY, XFLT, SPX, BURU, UVXY\n")):
            self.assertEqual(
                sync.get_candidate_symbols(
                    self.conn, "tickerList.csv", indexes={"SPX"}, delisted={"BURU"}
                ),
                ["UVXY", "VXX", "XFLT"],
            )

    def test_daily_due_rule_and_empty_success(self):
        fetch = Mock(return_value=[])
        sync.sync_split_batch(self.conn, ["XFLT"], fetch=fetch, now=lambda: NOW,
                              sleep=Mock(), monotonic=lambda: 0)
        sync.sync_split_batch(self.conn, ["XFLT"], fetch=fetch,
                              now=lambda: NOW + timedelta(hours=23),
                              sleep=Mock(), monotonic=lambda: 0)
        self.assertEqual(fetch.call_count, 1)
        sync.sync_split_batch(self.conn, ["XFLT"], fetch=fetch,
                              now=lambda: NOW + timedelta(hours=24),
                              sleep=Mock(), monotonic=lambda: 0)
        self.assertEqual(fetch.call_count, 2)

    def test_batch_limit_resumes_on_next_call(self):
        fetch = Mock(return_value=[])
        symbols = ["XFLT", "UVXY", "VXX"]
        for _ in range(3):
            sync.sync_split_batch(self.conn, symbols, fetch=fetch, now=lambda: NOW,
                                  sleep=Mock(), monotonic=lambda: 0,
                                  max_symbols=1)
        self.assertEqual([call.args[0] for call in fetch.call_args_list], sorted(symbols))

    def test_other_error_preserves_events_and_continues(self):
        db.record_split_success(self.conn, "UVXY", [("2020-08-31", 4.0)],
                                NOW - timedelta(days=2))
        fetch = Mock(side_effect=[RuntimeError("network"), []])
        sync.sync_split_batch(self.conn, ["UVXY", "VXX"], fetch=fetch,
                              now=lambda: NOW, sleep=Mock(), monotonic=lambda: 0)
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(db.get_split_events(self.conn, "UVXY")[0]["ratio"], 4.0)
        self.assertEqual(
            db.get_split_sync_state(self.conn, "UVXY")["next_retry_utc"],
            (NOW + timedelta(hours=1)).isoformat(),
        )

    def test_rate_limit_stops_batch_and_persists_exponential_cooldown(self):
        fetch = Mock(side_effect=YFRateLimitError())
        for attempt, minutes in enumerate((1, 2, 4, 8, 16, 30), start=1):
            at = NOW + timedelta(hours=attempt - 1)
            sync.sync_split_batch(self.conn, ["UVXY", "VXX"], fetch=fetch,
                                  now=lambda at=at: at, sleep=Mock(),
                                  monotonic=lambda: 0)
            state = db.get_split_provider_state(self.conn)
            self.assertEqual(state["rate_limit_count"], attempt)
            self.assertEqual(state["next_allowed_utc"],
                             (at + timedelta(minutes=minutes)).isoformat())
            sync.sync_split_batch(self.conn, ["UVXY", "VXX"], fetch=fetch,
                                  now=lambda at=at: at + timedelta(seconds=30),
                                  sleep=Mock(), monotonic=lambda: 0)
            self.assertEqual(fetch.call_count, attempt)

    def test_elapsed_budget_stops_before_next_request(self):
        fetch = Mock(return_value=[])
        times = iter([0, 0, 121])
        sync.sync_split_batch(self.conn, ["UVXY", "VXX"], fetch=fetch,
                              now=lambda: NOW, sleep=Mock(),
                              monotonic=lambda: next(times), max_seconds=120)
        self.assertEqual(fetch.call_count, 1)

    def test_rate_limit_cooldown_survives_database_reopen(self):
        fetch = Mock(side_effect=YFRateLimitError())
        sync.sync_split_batch(self.conn, ["UVXY"], fetch=fetch, now=lambda: NOW,
                              sleep=Mock(), monotonic=lambda: 0)
        reopened = sqlite3.connect(":memory:")
        try:
            self.conn.backup(reopened)
            sync.sync_split_batch(reopened, ["VXX"], fetch=fetch,
                                  now=lambda: NOW + timedelta(seconds=30),
                                  sleep=Mock(), monotonic=lambda: 0)
            self.assertEqual(fetch.call_count, 1)
        finally:
            reopened.close()

    def test_rate_limit_sequence_resets_after_24_hours(self):
        fetch = Mock(side_effect=YFRateLimitError())
        sync.sync_split_batch(self.conn, ["UVXY"], fetch=fetch, now=lambda: NOW,
                              sleep=Mock(), monotonic=lambda: 0)
        later = NOW + timedelta(hours=25)
        sync.sync_split_batch(self.conn, ["UVXY"], fetch=fetch,
                              now=lambda: later, sleep=Mock(), monotonic=lambda: 0)
        self.assertEqual(db.get_split_provider_state(self.conn)["rate_limit_count"], 1)
        self.assertEqual(db.get_split_provider_state(self.conn)["next_allowed_utc"],
                         (later + timedelta(minutes=1)).isoformat())


class TestCollectorIntegration(unittest.TestCase):
    @patch("getHistoricalData.bulkUpdate")
    @patch("getHistoricalData.updateRecords")
    @patch("getHistoricalData.stock_split_sync.run_split_sync_batch")
    def test_main_checks_splits_before_price_flow(self, split_batch, update, bulk):
        split_batch.return_value = {"attempted": 0}
        with patch("getHistoricalData._eastern_now", return_value=NOW):
            collector.main(args=[])
        split_batch.assert_called_once()
        bulk.assert_called_once()

    @patch("getHistoricalData.ib.setupConnection")
    @patch("getHistoricalData.stock_split_sync.run_split_sync_batch")
    def test_bulk_cycle_checks_splits_before_ibkr_connection(self, split_batch, connect):
        split_batch.return_value = {"attempted": 0}
        connect.return_value.isConnected.return_value = False
        collector.bulkUpdate()
        split_batch.assert_called_once()

    @patch("getHistoricalData.bulkUpdate")
    @patch("getHistoricalData.updateRecords")
    @patch("getHistoricalData.stock_split_sync.run_split_sync_batch",
           side_effect=RuntimeError("Yahoo unavailable"))
    def test_split_sync_failure_does_not_stop_price_flow(self, split_batch, update, bulk):
        with patch("getHistoricalData._eastern_now", return_value=NOW):
            collector.main(args=[])
        bulk.assert_called_once()


if __name__ == "__main__":
    unittest.main()
