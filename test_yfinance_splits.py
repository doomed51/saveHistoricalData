"""Tests for the standalone Yahoo Finance split-history prototype."""

import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import pandas as pd

import yfinance_splits


class TestYfinanceSplits(unittest.TestCase):
    @patch("yfinance_splits.yf.Ticker")
    def test_fetch_normalizes_dates_and_retains_reported_ratios(self, ticker):
        ticker.return_value.get_splits.return_value = pd.Series(
            [0.1, 4.0],
            index=pd.to_datetime(["2023-06-01", "2020-08-31"]),
        )

        self.assertEqual(
            yfinance_splits.fetch_splits("UVXY"),
            [("2020-08-31", 4.0), ("2023-06-01", 0.1)],
        )

    @patch("yfinance_splits.yf.Ticker")
    def test_fetch_rejects_invalid_ratios(self, ticker):
        ticker.return_value.get_splits.return_value = pd.Series(
            [float("nan")], index=pd.to_datetime(["2023-06-01"])
        )

        with self.assertRaises(ValueError):
            yfinance_splits.fetch_splits("UVXY")

    @patch("yfinance_splits.yf.Ticker")
    def test_fetch_rejects_invalid_dates(self, ticker):
        ticker.return_value.get_splits.return_value = pd.Series(
            [2.0], index=pd.DatetimeIndex([pd.NaT])
        )

        with self.assertRaises(ValueError):
            yfinance_splits.fetch_splits("UVXY")

    @patch("yfinance_splits.yf.Ticker")
    def test_prints_split_dates_and_ratios(self, ticker):
        ticker.return_value.get_splits.return_value = pd.Series(
            [4.0, 0.1],
            index=pd.to_datetime(["2020-08-31", "2023-06-01"]),
        )

        output = io.StringIO()
        with redirect_stdout(output):
            yfinance_splits.print_splits(["UVXY"])

        ticker.assert_called_once_with("UVXY")
        ticker.return_value.get_splits.assert_called_once_with(period="max")
        self.assertIn("UVXY", output.getvalue())
        self.assertIn("2020-08-31: 4.0", output.getvalue())
        self.assertIn("2023-06-01: 0.1", output.getvalue())

    @patch("yfinance_splits.yf.Ticker")
    def test_reports_empty_split_history(self, ticker):
        ticker.return_value.get_splits.return_value = pd.Series(dtype=float)

        output = io.StringIO()
        with redirect_stdout(output):
            yfinance_splits.print_splits(["XFLT"])

        self.assertIn("XFLT: no splits returned", output.getvalue())

    @patch("yfinance_splits.yf.Ticker")
    def test_reports_error_and_continues(self, ticker):
        def get_splits(symbol):
            if symbol == "UVXY":
                raise RuntimeError("request failed")
            return pd.Series([2.0], index=pd.to_datetime(["2024-01-02"]))

        ticker.side_effect = lambda symbol: type(
            "TickerStub", (), {"get_splits": lambda self, period: get_splits(symbol)}
        )()

        output = io.StringIO()
        with redirect_stdout(output):
            yfinance_splits.print_splits(["UVXY", "VXX"])

        self.assertIn("UVXY: request error: request failed", output.getvalue())
        self.assertIn("VXX", output.getvalue())
        self.assertIn("2024-01-02: 2.0", output.getvalue())


if __name__ == "__main__":
    unittest.main()
