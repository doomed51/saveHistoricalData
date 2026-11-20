# SPX tick history — design

Date: 2026-11-19
Status: approved 2026-11-19
Scope: add SPX-only historical tick collection to the existing IBKR/SQLite data system.

## 1. Intent

Add a research-grade, 1-second SPX index tick history covering the full IBKR retention window (about 3 years), stored locally and queryable from Python and R. The data will support historical research and provide bounded windows of recent ticks to a realtime monitor that computes z-scores.

The existing pipeline stores OHLCV bars. It has no tick store, no per-session fetch state for ticks, and no tick query helpers.

## 2. Verified provider behaviour

These facts were confirmed against the account's running TWS on 2026-11-19:

| Fact | Observation |
|---|---|
| Request | `reqHistoricalTicks(Index('SPX','CBOE','USD'), start, "", 1000, 'TRADES', True)` |
| What works | `TRADES` only. `MIDPOINT` and `BID_ASK` return error 10187. |
| Granularity | Event-driven, about 1 tick per second. In a 1,000-tick sample, 17 seconds had no tick. |
| Session | 09:30–16:00 ET. First tick observed 09:30:01, last 15:59:59. Early closes end earlier. |
| Outside session | A request starting pre-market, after-hours, or on a weekend silently jumps to the next session open. A request starting inside a session stops at that session's close and does not cross into the next session. |
| Per request | Maximum 1,000 ticks; data from one trading session only. |
| Retention | About 3 years. 2023-11-15 returned data on 2026-11-19; 2022-11-18 did not. |
| Payload | `price` only; `size=0`, empty `exchange`, no special conditions. This is the computed index level, not trade prints. |
| Intraday | Current-session ticks are available during the session. A 15:30 ET request succeeded at 15:34 ET. |
| No data | A request outside retention returns an empty list with no error event. A holiday or after-close request returns ticks from the next session open, not an empty list. |
| Errors | Permission errors (for example 10187) and invalid time strings (10314) are emitted as error events while the call returns an empty list. Connection errors surface separately. |
| Time strings | `20261118 10:00:00` (naive, TWS local time) works. `... US/Eastern` is rejected with error 10314. `... America/New_York` works. `20261118-14:30:00` (UTC notation) works and is unambiguous. |
| Live ticks | `reqTickByTickData` for indices is documented as CME-only, so SPX live tick-by-tick is not a usable path. Historical `TRADES` is. |

A full session is about 23,400 ticks, so about 24 requests per session. Three years is about 750 NYSE sessions, about 18,000 requests. At the existing cap of 55 requests per 10 minutes this is about 54.5 hours of continuous collection.

## 3. Goals, non-goals, anti-goals

### Goals

- Collect SPX `TRADES` ticks for every NYSE session in the last 3 years.
- Make ingestion idempotent and resumable across interruptions.
- Record per-session fetch state so completeness can be audited and repaired independently.
- Store raw tick events without forward-filling or resampling.
- Provide fast bounded reads for R research and for the realtime monitor.
- Keep the existing bar collector and its 152 GB database untouched.

### Non-goals

- Other symbols, including other indices.
- Live streaming or live tick-by-tick subscriptions.
- Bid, ask, midpoint, size, or exchange data for SPX.
- Derived 1-second or 1-minute bars.
- Parquet/DuckDB export, server databases, or dashboards.
- Changes to the existing OHLCV tables or collection flow.

### Anti-goals

- Treating every absent clock second as missing data.
- Inferring completeness from the latest stored timestamp.
- Blind appends without a uniqueness constraint.
- Coupling tick collection to the bar collector's table-discovery logic.
- Letting the backfill compete with the daily bar collector for IBKR's request budget.

## 4. Approaches considered

### A. Separate SQLite database — chosen

`X:/data/ibkr/historicalData_ticks.db` in WAL mode. Python writes; R and the monitor read. Bar joins use `ATTACH`.

Why: the main DB is 152 GB with a rollback journal, 2,964 tables, and no indexes on price tables. Tick writes there would intermittently block readers and would make `getRecords()` treat `SPX_index_ticks` as a new interval. A separate file isolates growth and locking, and keeps the collector's discovery logic unchanged.

### B. Table inside the existing database — rejected

One file and no `ATTACH`, but write/read contention during a multi-day backfill, table-discovery pollution, and larger backups/vacuums. The tick data is under 0.5% of the main file, so the only gain is one fewer path.

### C. DuckDB or Parquet — rejected for now

Columnar and fast for full-history scans, and `arrow` is already installed in R. But DuckDB is single-writer, so the monitor could not read during the backfill; `duckdb` is not installed; and neither provides the idempotent upsert and uniqueness constraints the ingestion needs. A Parquet export can be added later if whole-history R scans become a bottleneck.

## 5. Architecture

```text
IBKR reqHistoricalTicks
        |
        v
maintainSPXTicks.py  (backfill / daily / audit / repair)
        |
        +--> interface_ibkr.getHistoricalTicks()   pacing + request formatting
        |
        +--> interface_ticks.py                    schema, inserts, state, queries
                    |
                    v
        X:/data/ibkr/historicalData_ticks.db
          SPX_index_ticks          canonical tick rows
          00-tick_sessions         expected-session state and audit input
                    |
          +---------+----------+
          v                    v
    R / RSQLite          realtime_monitor.py
    research reads       bounded z-score windows
```

The design follows the Simple System: source → ingest → canonical store → audit → repair. No raw-payload layer, queue, or orchestration is added because IBKR data can be re-requested and the canonical store already preserves the provider values.

## 6. Storage design

### 6.1 Database

- Path: `X:/data/ibkr/historicalData_ticks.db` (new `config.dbname_ticks`).
- Journal mode: WAL. `X:` is a local NTFS volume, so WAL is safe. If the file is ever moved to a network share, switch to `DELETE`.
- Writer pragmas: `journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout=5000`.
- Reader connections: `mode=ro` URI plus `PRAGMA query_only=ON`.
- Estimated size: 17.7M rows at roughly 30–35 bytes per row, about 0.5–0.7 GB including the WAL during the backfill. The volume has 1.77 TB free.
- Backups: checkpoint the WAL (`PRAGMA wal_checkpoint(TRUNCATE)`) before copying the file. If copying while a writer is open, include the `-wal` and `-shm` files or use SQLite's backup API.

### 6.2 Canonical table

```sql
CREATE TABLE IF NOT EXISTS "SPX_index_ticks" (
    date  TEXT NOT NULL PRIMARY KEY,   -- 'YYYY-MM-DD HH:MM:SS', America/New_York, naive
    price REAL NOT NULL
) WITHOUT ROWID;
```

- `date` is stored in the same naive Eastern convention as the existing bar tables, so R joins and comparisons work without conversion. The ISO-8601 text format sorts chronologically under SQLite's default BINARY collation.
- `WITHOUT ROWID` makes the primary key the clustered storage order. Range scans by date read contiguous pages and include `price` in the same B-tree.
- One table per symbol, matching the repository's `<symbol>_<type>_<interval>` convention. Future symbols add tables; no generic symbol column is needed now.
- No `size`, `exchange`, or `symbol` columns: they are constant or empty for SPX and would add bytes without information.
- No derived bars. Consumers resample or forward-fill at query time.

### 6.3 Session state table

```sql
CREATE TABLE IF NOT EXISTS "00-tick_sessions" (
    symbol        TEXT NOT NULL,
    session_date  TEXT NOT NULL,       -- 'YYYY-MM-DD'
    session_open  TEXT NOT NULL,       -- 'YYYY-MM-DD HH:MM:SS' ET
    session_close TEXT NOT NULL,       -- 'YYYY-MM-DD HH:MM:SS' ET
    status        TEXT NOT NULL,       -- pending|in_progress|retry|complete|partial|unavailable|failed
    tick_count    INTEGER NOT NULL DEFAULT 0,
    first_tick    TEXT,
    last_tick     TEXT,
    requests      INTEGER NOT NULL DEFAULT 0,
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    fetched_at    TEXT,
    PRIMARY KEY (symbol, session_date)
) WITHOUT ROWID;
```

Status meanings:

- `pending`: expected session, never attempted.
- `in_progress`: fetching or interrupted mid-session. Resume from `last_tick + 1 second`.
- `retry`: a request returned no data, but `attempts < tick_max_attempts`. It will be retried.
- `complete`: reached the session close within tolerance with no error.
- `partial`: fetch ended before the close, or audit found a gap. Repair re-fetches the whole session.
- `unavailable`: provider returned no data after the configured attempts. This includes sessions outside retention and ad-hoc closures. It is an expected outcome at the retention boundary, so audit reports it as informational, not an error. `--force-unavailable` retries it.
- `failed`: request error. Retried on the next run.

`session_open` and `session_close` are stored so audit does not depend on a later calendar version.

### 6.4 Expected observations

Expected sessions are NYSE trading days from the backfill start through the last completed session, using `pandas_market_calendars.get_calendar('NYSE').schedule(...)`. This handles weekends, holidays, and early closes (13:00 ET).

Rules:

- A trading day with no ticks after 3 attempts is `unavailable`, not `complete`.
- A session is `complete` when `last_tick >= session_close - 60 seconds`.
- A session is `partial` when it ended earlier than that tolerance.
- A first tick later than `session_open + 5 minutes` is an audit warning, not a completeness failure; it can be legitimate.
- An internal gap is a stretch with no tick inside `[session_open, session_close)`. Gaps of 10 minutes or more are warnings; gaps of 30 minutes or more are errors. Natural one- or two-second absences are not gaps.
- Density is `tick_count / session_minutes`, which normalizes early closes. Audit compares it with the median density of the trailing 20 `complete` sessions and flags values below 50% of that median as a warning. The check is skipped until at least 5 baseline sessions exist. `partial`, `unavailable`, and `failed` sessions are excluded from the baseline.
- Rows outside an expected session window, on a non-trading day, or with invalid prices are audit errors.
- Freshness and historical completeness are separate: freshness means the latest completed NYSE session has a `complete` row; completeness means every expected session is `complete` or `unavailable`.

## 7. Ingestion design

### 7.1 Request formatting

- Contract: `Index('SPX', 'CBOE', 'USD')`, qualified once at startup.
- `whatToShow='TRADES'`, `useRth=True`, `numberOfTicks=1000`.
- Start strings use UTC notation: `YYYYMMDD-HH:MM:SS`. This avoids the rejected `US/Eastern` form and removes dependence on the TWS timezone setting.
- The response timestamps are UTC-aware. Convert with `astimezone(ZoneInfo('America/New_York')).replace(tzinfo=None)` before storage.
- Exactly one of `startDateTime` or `endDateTime` is set; this design always uses `startDateTime`.

### 7.2 Per-session algorithm

```text
ensure a status row exists for the session
if status in ('complete', 'unavailable') and not forced:
    return

cursor = session_open
if resuming an in_progress session and last_tick exists:
    cursor = last_tick + 1 second

requests = 0
while requests < max_requests_per_session:
    ticks = getHistoricalTicks(contract, cursor, 1000, 'TRADES', useRth=True)
    requests += 1
    if ticks is empty:
        break
    valid = [t for t in ticks if session_open <= t.local_et <= session_close]
    if valid is empty:
        # The provider jumped to another session (holiday, ad-hoc closure, or a
        # cursor that reached the close). Treat as no data for this session.
        break
    dedupe valid by timestamp
    upsert valid into SPX_index_ticks
    commit the batch
    update status: in_progress, requests, last_tick
    if last valid tick >= session_close - 1 second:
        break
    next_cursor = last valid tick + 1 second
    if next_cursor <= cursor:
        break        # no cursor progress; prevents an infinite loop
    cursor = next_cursor

at the end:
    tick_count = COUNT(*) for the session window
    first_tick, last_tick = MIN/MAX(date) for the session window
    if tick_count == 0:
        attempts += 1
        status = retry if attempts < tick_max_attempts else unavailable
    else:
        status = complete if last_tick >= session_close - 60 seconds else partial
    fetched_at = now
```

Notes:

- The API does not return ticks from multiple sessions in one request. Filtering to the session window is defensive; it protects against a future provider change and against a cursor that has crossed the close. It also handles the observed holiday behaviour, where the provider jumps forward and returns the next session's ticks.
- "No progress" means the cursor did not advance. It does not mean zero rows were inserted. Repair re-fetches pages that are already stored, so an inserted-row count would stop repair after the first page.
- A short page (fewer than 1,000 ticks) does not by itself mean the session is finished. Continue until an empty page, an all-out-of-window page, the close tolerance, no cursor progress, or the request cap.
- `max_requests_per_session` defaults to 40, comfortably above the ~24 needed for a normal session and enough for early-close sessions.
- Committing per 1,000-tick page keeps crash loss small and makes the 2.5-day backfill resumable.
- `INSERT ... ON CONFLICT(date) DO UPDATE SET price=excluded.price` makes repeated and overlapping fetches converge and lets a re-fetch correct a revised price. A duplicate timestamp within a page is removed before insert; the primary key is the final guard.

### 7.3 Validation

Reject a tick when any of these is true:

- `price` is not finite or is `<= 0`.
- `date` cannot be parsed, is not on the session date, or is outside `[session_open, session_close]`.
- The timestamp is duplicated within the page.

Do not reject a tick because the price is outside a historical range; SPX levels change over years. A broad sanity bound is optional and should be configurable.

Batch validity does not establish session completeness; the audit does that independently.

### 7.4 Idempotency and transactions

- Writer connection uses `isolation_level=None` and explicit `BEGIN IMMEDIATE` / `COMMIT` per page.
- Each page upserts with `ON CONFLICT(date) DO UPDATE SET price=excluded.price`.
- Status updates are upserts keyed by `(symbol, session_date)`.
- Re-running a session, resuming an interrupted one, or repairing a gap all use the same upsert path. There is no separate repair writer. Repair adds only a targeted cleanup for rows proven invalid or outside every expected session window; it does not delete a session before re-fetching it.

### 7.5 Pacing and concurrency

- Add `reqHistoricalTicks` to `interface_ibkr._IBKR_MIN_INTERVAL_BY_REQUEST` with a 3-second minimum, so it shares the existing per-process 55-requests-per-10-minutes cap and 3-second global interval.
- Add a `clientId` parameter to `interface_ibkr.setupConnection()` with a default of 10, preserving current behaviour. The tick job uses `config.tick_client_id = 11`, so it can stay connected while `realtime_monitor.py` holds clientId 10.
- The tick job acquires a single-instance lock file (`X:/data/ibkr/.spx_ticks.lock`) containing PID and start time. A stale lock is removed after checking that the PID is gone.
- The tick job must not run while `getHistoricalData.py` or the futures maintenance scripts are making IBKR requests. The IBKR pacing budget is account-level in practice, and two processes each capped at 55/10 minutes can exceed it. The monitor may stay connected on its own clientId because it does not issue historical tick requests.
- Recommended first backfill window: Friday after the daily collector finishes through Monday before the open (about 65 hours, enough for the ~54.5-hour backfill). If the job is started at another time, stop it during the daily collector's run.
- Daily updates are small (about 24 requests, 1–2 minutes) and should run after the collector, not concurrently with it.
- The job supports `--max-runtime-minutes` and graceful `Ctrl-C` handling. It checkpoints the current session as `in_progress` and exits cleanly.

### 7.6 Request outcomes and status mapping

| Outcome | Signal | Action |
|---|---|---|
| Normal page | Non-empty list, ticks inside the session | Upsert, advance cursor |
| End of session | Empty list after ticks near the close, or last tick at the close | Finalize |
| Provider jumped sessions | Non-empty list, but every tick is outside the requested window | Treat as no data for this session; stop |
| Outside retention | Empty list, no error event | Increment attempts; set `retry` until `tick_max_attempts`, then `unavailable` |
| Holiday or ad-hoc closure | Ticks from the next session | Same as "provider jumped sessions"; `retry` then `unavailable` |
| No data error 162 | Error event 162 | Treat as no data; increment attempts; `retry` then `unavailable` |
| No permissions | Error event 10187 | Mark `failed`, record the message, stop the run (configuration error) |
| Invalid request time | Error event 10314 | Mark `failed`, record the message, stop the run (code defect) |
| Competing live session | Error event 10197 | Mark `failed`, retry on the next run |
| Connection lost | 502/504/1100/1101 or socket error | Reconnect, retry the same page a bounded number of times |
| Unknown error | Error event not in the table | Mark `failed`, record the message, retry on the next run |

Implementation notes:

- `ib_insync` 0.9.86 returns an empty list for a no-data request and emits errors on `errorEvent`. Wrap each call with a small error collector that is cleared before the request and inspected afterwards. Do not infer success from an empty list when an error was emitted.
- `attempts` counts runs that finalized with zero rows, not individual requests. A session moves from `retry` to `unavailable` at `tick_max_attempts`.
- Permission and invalid-request errors are fatal for the run because retrying cannot fix them. Connection errors are retried with `refreshConnection()`.
- A `complete` session with no rows is an audit error; an `unavailable` session with no rows is expected.

## 8. Backfill and daily operation

### 8.1 Backfill

- Default range: `today - 3 years` through the last completed NYSE session. `--start` and `--end` override.
- Order: oldest-first. Retention is rolling, so the oldest sessions expire first. Fetching recent sessions first would lose the oldest sessions during the 2.5-day run. Oldest-first stays ahead of expiry.
- `ensure_sessions()` inserts `pending` rows for every expected session before processing, so the expected set is visible and auditable from the start. It inserts only missing rows (`ON CONFLICT(symbol, session_date) DO NOTHING`) and never resets an existing status.
- Process order: `pending`, `retry`, `failed`, `in_progress`, and `partial` rows by `session_date ASC`. `complete` and `unavailable` are skipped.
- Resume after interruption uses `last_tick + 1 second` for `in_progress`; `partial` and audit-driven repair re-fetch the whole session.
- After the run, checkpoint the WAL. `VACUUM` is optional; the file is expected to be compact because rows are inserted in key order.

### 8.2 Daily

- Run after 16:15 ET on trading days.
- Fetch the just-closed session and any `pending`/`failed`/`partial` sessions from the last 7 days.
- Skip today if the session is still open.
- The command is idempotent, so a missed or repeated run is safe.

### 8.3 First-run smoke test

Before the full backfill, fetch one known session into a temporary database and compare the row count, first tick, and last tick with a direct IBKR request. This validates timezone conversion, pagination, and the completeness rule without touching the production file.

## 9. Audit and repair

`audit(start, end)` compares expected sessions from the calendar with stored state and rows. Every finding has a `severity`: `error`, `warning`, or `info`.

| Finding | Severity | Meaning |
|---|---|---|
| `missing_status` | error | Expected session has no status row. |
| `not_started` | error | `pending` for a session that has closed. |
| `in_progress` | error | Fetch did not finish; resume or repair. |
| `partial` | error | Fetch ended before the close tolerance. |
| `failed` | error | Request error; retry. |
| `no_rows_complete` | error | `complete` session with no rows in its window. |
| `early_end` | error | Last tick earlier than close − 60 seconds. |
| `large_gap` | error | No tick for 30 minutes or more inside the session. |
| `invalid_price` | error | Stored price `<= 0` or non-finite. |
| `unexpected_rows` | error | Rows outside every expected session window or on a non-trading day. |
| `retry` | warning | No data on a prior attempt; will be retried. |
| `late_start` | warning | First tick later than open + 5 minutes. |
| `gap` | warning | No tick for 10–30 minutes inside the session. |
| `low_density` | warning | Density below 50% of the trailing 20-session median. |
| `unavailable` | info | No data after the configured attempts, including the retention boundary. |
| `no_rows_unavailable` | info | Expected: an unavailable session has no rows. |

The exit code is 1 when any `error` is present, 0 when only warnings or info are present. `--strict` also exits 1 on warnings. A "clean" audit means no errors; warnings are reviewed but do not block rollout.

Audit does not infer completeness from `MAX(date)`.

`repair` selects `error` findings by default; `--include-warnings` adds `retry`, `late_start`, `gap`, and `low_density`.

- `missing_status`, `retry`, `not_started`, `in_progress`, `partial`, `failed`, `no_rows_complete`, `early_end`, `gap`, `large_gap`: re-fetch the whole session through the normal upsert path with `force=True` and `resume=False`.
- `invalid_price`: delete rows with `price <= 0`, then re-fetch the affected session.
- `unexpected_rows`: delete rows outside every expected session window, then re-audit.
- `unavailable`: only with `--force-unavailable`; otherwise informational.

Repair does not delete a session's valid rows before re-fetching. If the re-fetch fails partway, the existing rows remain and the session stays `partial`. A re-fetch upserts prices, so a revised value replaces the stored one. If the provider removes a row that is still valid in the stored data, that row is not deleted; this is a documented limitation. If authoritative replacement becomes a requirement, add a staging-table swap.

After repair, re-run the audit for the repaired range. Repair is complete only when the errors are cleared or converted to warnings/info with a recorded reason.

Recommended cadence: run `--audit` after the backfill, after any interruption, and weekly thereafter. Run `--repair` when the audit reports errors.

## 10. R access

RSQLite and DBI are installed. Example:

```r
library(DBI)
library(RSQLite)

con <- dbConnect(SQLite(), "X:/data/ibkr/historicalData_ticks.db")
on.exit(dbDisconnect(con), add = TRUE)

ticks <- dbGetQuery(
  con,
  "SELECT date, price
     FROM SPX_index_ticks
    WHERE date >= ? AND date < ?
    ORDER BY date",
  params = list("2026-11-01 00:00:00", "2026-12-01 00:00:00")
)
ticks$date <- as.POSIXct(ticks$date, tz = "America/New_York")
```

Use `flags = RSQLite::SQLITE_RO` when you want the connection to enforce read-only access. The examples above open the file read-write but only read from it.

Joining to bar tables:

```r
dbExecute(con, "ATTACH DATABASE 'X:/data/ibkr/historicalData_index.db' AS px")
dbGetQuery(con, "
  SELECT t.date, t.price, b.close
    FROM SPX_index_ticks t
    LEFT JOIN px.SPX_index_1min b ON b.date = t.date
   WHERE t.date >= '2026-11-18 09:30:00'
     AND t.date <  '2026-11-18 16:00:00'
")
```

For large pulls, use `dbSendQuery()` plus `dbFetch(n = 100000)` rather than materialising the whole table. A whole-history pull is about 17.7M rows and should be avoided in the monitor.

## 11. Realtime monitor use

The monitor is Python and already opens the equities DB read-only. Add `interface_ticks.get_ticks(conn, start, end)` returning a sorted DataFrame with parsed `date`.

- WAL allows the monitor to read while the backfill writes.
- Load bounded windows only. Examples:
  - Last 20 sessions: about 460k rows.
  - Same 5-minute window across 252 sessions: about 75k rows.
  - Last 1,000 ticks: `ORDER BY date DESC LIMIT 1000`, a reverse scan on the primary key.
- If the monitor needs current-session ticks, it can request the delta from the last stored tick once per minute via `reqHistoricalTicks`; this is a separate live path, not part of the backfill. The historical request does return current-session data. This is out of scope for v1 but the schema supports it.
- Do not load the full 3-year table into memory for z-scores.

## 12. Configuration and CLI

New `config.py` values:

```python
dbname_ticks = 'X:/data/ibkr/historicalData_ticks.db'
tick_symbol = 'SPX'
tick_exchange = 'CBOE'
tick_currency = 'USD'
tick_client_id = 11
tick_backfill_years = 3
tick_request_size = 1000
tick_max_requests_per_session = 40
tick_complete_tolerance_seconds = 60
tick_start_tolerance_seconds = 300
tick_max_attempts = 3
tick_connection_retries = 3
tick_gap_warning_seconds = 600
tick_gap_error_seconds = 1800
tick_density_baseline_sessions = 20
tick_density_min_baseline = 5
tick_density_warning_ratio = 0.5
```

New entry point: `maintainSPXTicks.py`.

```text
--backfill [--start YYYY-MM-DD] [--end YYYY-MM-DD]
--daily
--audit [--start ...] [--end ...] [--strict]
--repair [--include-warnings] [--force-unavailable]
--status
--max-runtime-minutes N
```

`--status` prints coverage: earliest and latest complete session, counts by status, total rows, and database size. The script logs to the console and to `logs/` in the repository.

## 13. Testing

Tests use `unittest`, a temporary or in-memory SQLite database, and a fake IBKR client. They do not contact IBKR or touch the production database.

Required cases:

- Schema creation is idempotent.
- Inserting the same page twice produces the same row count.
- Overlapping pages converge.
- An upsert replaces a revised price for the same timestamp.
- Pagination advances by one second, stops at the session close, and never crosses into the next session.
- A short page followed by more data continues; an empty page stops.
- A holiday request that returns next-session ticks stores nothing and marks the session `unavailable` after the configured attempts.
- A re-fetch that inserts zero new rows still advances the cursor and completes; "no progress" is cursor-based, not insert-based.
- Invalid price, out-of-session timestamp, and duplicate timestamp are rejected or deduplicated.
- Status transitions: `pending` → `in_progress` → `complete`; interruption leaves `in_progress` and resumes from `last_tick + 1`; early end leaves `partial`.
- A no-data session moves to `retry` and then to `unavailable` at the configured number of attempts.
- Error mapping: 10187 and 10314 are fatal for the run; 10197 marks `failed` and retries; a connection error reconnects and retries the page.
- Audit detects missing, partial, failed, early-end, late-start, gap, large-gap, low-density, invalid-price, and unexpected-row cases.
- Audit severity and exit codes: `unavailable` is informational; errors exit 1; warnings exit 0 unless `--strict`.
- Density normalizes an early close and excludes `partial`/`unavailable` sessions from the baseline.
- Repair re-fetches through the normal upsert path, applies targeted cleanup, and the re-audit clears the errors.
- Calendar handles a holiday and an early close (for example, the day after Thanksgiving).
- Rows are stored in ET naive form from UTC input, including a DST boundary date.

Run the isolated suite with `..\venv\Scripts\python.exe -m unittest test_spx_ticks -v`. Do not add these tests to the unsafe `tests/` suite that calls `setupTestdata()`.

## 14. Rollout

1. Add config values, `interface_ibkr.getHistoricalTicks`, `interface_ticks.py`, `maintainSPXTicks.py`, and tests.
2. Run the one-session smoke test against a temporary database.
3. Create the production database and `ensure_sessions` for the full range.
4. Run the backfill in a weekend window; monitor `--status` and `--audit`.
5. Repair any flagged sessions and re-audit.
6. Schedule `--daily` after the bar collector on trading days.
7. Update `docs/architecture.md` and `README.md` with the new component, data path, database, and commands.

## 15. Risks and unknowns

- **Retention boundary.** IBKR's "3 years" is approximate; the oldest sessions may return no data. They are marked `unavailable`, not failed. `--start` can be moved earlier to probe.
- **Pacing contention.** A separate process can exceed the account-level budget if it runs with the bar collector. The design uses a single-instance lock and a scheduling rule; it does not add cross-process pacing coordination to the collector.
- **Provider gaps.** Event-driven ticks have natural gaps. Audit flags only gaps of 10 minutes or more; one- and two-second absences are not gaps. A large gap may be a provider limitation rather than a retrieval failure, so repair may leave it as a warning or error.
- **Ad-hoc market closures.** A closure not in the NYSE calendar looks like a missing session. A request for it returns the next session's ticks; the session filter rejects them, and the session becomes `unavailable` after retries. The audit reports it as informational.
- **Provider revisions.** Repair upserts prices but does not delete a stored row that the provider later omits. If authoritative replacement becomes necessary, add a staging-table swap.
- **Audit noise.** `late_start` and `low_density` are warnings by default so legitimate halts and early closes do not block a clean audit. `--strict` is available for a zero-warning check.
- **TWS timezone.** Requests use UTC notation, so the TWS setting does not affect the requested window. Stored timestamps are converted to ET.
- **WAL on a network share.** WAL is safe on `X:` today. If the file moves to a network share, switch to `DELETE` or keep the database local.
- **API change.** If IBKR changes index tick availability, the audit surfaces missing sessions; the collector does not silently claim success.

## 16. Decisions made

- Separate SQLite database (option A).
- Raw ticks only; no derived bars or forward-fill.
- ET naive timestamps for consistency with existing bar tables.
- UTC request strings; explicit timezone conversion on write.
- Oldest-first backfill to stay ahead of rolling retention.
- Standalone maintenance script, not integrated into the bar collector.
- Per-session state table as the source of truth for expected, present, and missing sessions.
- Audit findings carry a severity; errors block a clean audit, warnings and informational findings do not.
- Internal gaps use 10-minute warning and 30-minute error thresholds.
- Density is normalized per session minute and compared with a trailing 20-session median.
- Repair uses the normal upsert path plus targeted cleanup; it does not delete a session's valid rows before re-fetching.
- The tick job uses a dedicated clientId (11), so the realtime monitor can stay connected on clientId 10.
- A holiday or closure request that returns the next session's ticks is treated as no data for the requested session.
- No DuckDB/Parquet/server database in v1.

## 17. Left to the builder

- Exact function names and CLI output formatting.
- Exact implementation of the `errorEvent` collector and any additional IBKR error codes encountered during implementation.
- Whether to add a small index on `00-tick_sessions(status, session_date)`; the table is only about 750 rows.
- Fake-client design for pagination tests.
- Logging layout and rotation.
- Whether to add a `--mark-unavailable` convenience command for ad-hoc closures.
- Whether to add a Parquet export later, only if whole-history R scans become a bottleneck.
