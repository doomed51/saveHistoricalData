# saveHistoricalData

A simple set of scripts to download and store historical timeseries data for equities and futures from ibkr

Requires ibkr account and appropriate setup for api access. 

Data is stored in SQLite3 database files. 

The equities updater also stores Yahoo Finance split dates and ratios in the
equities SQLite database for tracked non-index symbols. It checks each symbol
at most once per 24 hours while the updater runs, in bounded batches. Yahoo
rate limits pause split requests with a persisted exponential cooldown; IBKR
history work continues. Split history is stored separately and does not change
existing OHLCV rows or the IBKR price request.

To inspect stored splits in Python:

```python
import config
import interface_localDB as db

with db.sqlite_connection(config.dbname_stock) as conn:
    print(db.get_split_events(conn, "UVXY"))
```

`source_missing=1` means a previously stored event was omitted from Yahoo's
latest successful full response; the stored event is retained.

tickerList.csv list of symbols to be tracked


Basic structure:
 - db interface: manages connections and operations on the local store of symbol data 
 - ibkr interface: manages operations on the ibkr api 
 - getHistoricalData: manages equities data
 - maintainHistoricalData_futures: manages futures data
 - maintainTermStructure: builds term structure tables from locally stored futures contracts
 - maintainSPXTicks: collects SPX index ticks into a separate SQLite database

Futures configuration notes:
- `config.exchange_mapping` is the source of truth for futures exchange resolution.
- `config.futures_symbol_metadata` stores per-symbol futures settings used by downstream jobs.
- Supported futures symbols include: `CL`, `ES`, `ZB`, `ZN`, `GC`, `SI`, `NG`, `VIX`.

SPX tick history:
- `maintainSPXTicks.py` collects event-driven SPX ticks into `X:/data/ibkr/historicalData_ticks.db` (`config.dbname_ticks`), a separate WAL SQLite file. It does not write the equities or futures databases.
- `interface_ticks.py` owns the schema: `SPX_index_ticks` (`date`, `price`; one table per symbol) and `00-tick_sessions` (expected sessions and fetch state).
- Ticks use naive Eastern time text `YYYY-MM-DD HH:MM:SS`, matching the bar tables. IBKR requests use UTC strings; responses are converted to Eastern before storage.
- Commands:

```powershell
..\venv\Scripts\python.exe maintainSPXTicks.py --backfill [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--max-runtime-minutes N] [--db-path PATH]
..\venv\Scripts\python.exe maintainSPXTicks.py --daily
..\venv\Scripts\python.exe maintainSPXTicks.py --audit [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--strict]
..\venv\Scripts\python.exe maintainSPXTicks.py --repair [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--include-warnings] [--force-unavailable]
..\venv\Scripts\python.exe maintainSPXTicks.py --status
```

- `--backfill` is the initial multi-day job and resumes where a stopped run left off. `--max-runtime-minutes` stops it before the next session once the budget is exhausted.
- `--daily` uses its own 7-day processing window (it discovers 14 days); `--max-runtime-minutes` applies to backfill only.
- `--audit` is read-only and exits 1 on errors; warnings exit 0 unless `--strict`. `--repair` audits, cleans invalid or out-of-window rows in the selected range, and re-fetches the affected sessions.
- Run `--backfill`, `--daily`, or `--repair` only while the bar and futures collectors are idle. IBKR's historical-data pacing budget is account-wide, so a concurrent run can cause pacing errors for both. The `<db-path>.lock` file prevents two tick writers but does not coordinate with other scripts.
- Logs go to `logs/spx_ticks_YYYYMMDD.log` in Eastern time.
- Read access from Python:

```python
import interface_ticks

conn = interface_ticks.connect_ticks(readonly=True)
ticks = interface_ticks.get_ticks(
    conn, '2026-11-18 09:30:00', '2026-11-18 16:00:00'
)
conn.close()
```

R/RSQLite can query the file directly with bounded date filters; use `flags = RSQLite::SQLITE_RO` for read-only access. Avoid full-table pulls (about 17.7 million rows for three years). `--db-path` targets a temporary database for tests and smoke runs; do not point experiments at the production file.

Usage: 
- populate tickerlist.csv with the symbols you want to track
- Setup config.py (db location, watchlist locations, intervals to track)  
- open and login to IBKR TWS 
- run qt_getHistoricalData 
