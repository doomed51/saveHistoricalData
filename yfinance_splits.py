"""Print Yahoo Finance split history for an editable list of symbols."""

import math

import pandas as pd
import yfinance as yf


SYMBOLS = ["XFLT", "UVXY", "VXX"]


def fetch_splits(symbol):
    """Return Yahoo's split dates and ratios, ordered by local event date."""
    splits = yf.Ticker(symbol).get_splits(period="max")
    events = []
    seen_dates = set()
    for date, ratio in splits.items():
        timestamp = pd.Timestamp(date)
        if pd.isna(timestamp):
            raise ValueError(f"Invalid split date for {symbol}: {date}")
        split_date = timestamp.date().isoformat()
        ratio = float(ratio)
        if not math.isfinite(ratio) or ratio <= 0:
            raise ValueError(f"Invalid split ratio for {symbol} on {split_date}: {ratio}")
        if split_date in seen_dates:
            raise ValueError(f"Duplicate split date for {symbol}: {split_date}")
        seen_dates.add(split_date)
        events.append((split_date, ratio))
    return sorted(events)


def print_splits(symbols):
    """Print all split dates and Yahoo's reported ratios for each symbol."""
    for symbol in symbols:
        try:
            splits = fetch_splits(symbol)
        except Exception as exc:
            print(f"{symbol}: request error: {exc}")
            continue

        if not splits:
            print(f"{symbol}: no splits returned")
            continue

        print(f"{symbol}:")
        for split_date, ratio in splits:
            print(f"  {split_date}: {ratio}")


if __name__ == "__main__":
    print_splits(SYMBOLS)
