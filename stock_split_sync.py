"""Resumable Yahoo split-history sync for the equities database."""

import csv
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone

from yfinance.exceptions import YFRateLimitError

import config
import interface_localDB as db
from yfinance_splits import fetch_splits


def get_candidate_symbols(conn, watchlist_path, indexes=None, delisted=None):
    """List tracked non-index symbols from the watchlist and stock tables."""
    indexes = set(config._index if indexes is None else indexes)
    delisted = set(config.delisted_symbols if delisted is None else delisted)
    with open(watchlist_path, newline="", encoding="utf-8-sig") as watchlist:
        symbols = {item.strip().upper() for item in next(csv.reader(watchlist), [])}

    table_names = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    for (name,) in table_names:
        parts = name.split("_")
        if len(parts) == 3 and parts[1] == "stock":
            symbols.add(parts[0].upper())

    return sorted(symbol for symbol in symbols if symbol and
                  symbol not in indexes and symbol not in delisted)


def _is_due(conn, symbol, at):
    state = db.get_split_sync_state(conn, symbol)
    if state is None:
        return True
    retry = state["next_retry_utc"]
    if retry and at < datetime.fromisoformat(retry):
        return False
    last_success = state["last_success_utc"]
    return last_success is None or at >= datetime.fromisoformat(last_success) + timedelta(hours=24)


def sync_split_batch(conn, symbols, fetch=fetch_splits, now=None, sleep=time.sleep,
                     monotonic=time.monotonic, max_symbols=50, max_seconds=120):
    """Check a bounded due-symbol batch; return counts without delaying on 429."""
    db.create_split_tables(conn)
    now = now or (lambda: datetime.now(timezone.utc))
    started = monotonic()
    counts = {"attempted": 0, "succeeded": 0, "errors": 0, "rate_limited": False}
    provider = db.get_split_provider_state(conn)
    retry = provider["next_allowed_utc"]
    if retry and now() < datetime.fromisoformat(retry):
        return counts

    for symbol in sorted(set(symbols)):
        if counts["attempted"] >= max_symbols or monotonic() - started >= max_seconds:
            break
        at = now()
        if not _is_due(conn, symbol, at):
            continue
        if counts["attempted"]:
            sleep(1)
            if monotonic() - started >= max_seconds:
                break
            at = now()
        counts["attempted"] += 1
        try:
            events = fetch(symbol)
        except YFRateLimitError:
            db.record_split_rate_limit(conn, symbol, at)
            counts["rate_limited"] = True
            break
        except Exception as exc:
            db.record_split_error(conn, symbol, at, exc, at + timedelta(hours=1))
            counts["errors"] += 1
            continue

        db.record_split_success(conn, symbol, events, at)
        counts["succeeded"] += 1
    return counts


def run_split_sync_batch(db_path=None, watchlist_path=None):
    """Sync one batch using the configured equity database and watchlist."""
    db_path = db_path or config.dbname_stock
    watchlist_path = watchlist_path or config.watchlist_main
    with closing(sqlite3.connect(db_path)) as conn:
        db.create_split_tables(conn)
        conn.commit()
        symbols = get_candidate_symbols(conn, watchlist_path)
        return sync_split_batch(conn, symbols)
