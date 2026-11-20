# SPX Tick History Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an SPX-only IBKR historical-tick collector with a separate SQLite store, resumable session ingestion, independent audit/repair, daily updates, R/Python read helpers, and a one-session live smoke test.

**Architecture:** `maintainSPXTicks.py` handles IBKR pagination, status transitions, jobs, and CLI. `interface_ticks.py` owns the tick database, session state, calendar, queries, audit, and cleanup. `interface_ibkr.py` gains a paced tick-request helper and configurable client ID. Data is stored as raw `(date, price)` rows in ET-naive text form; no bar tables or existing collection behavior change.

**Tech Stack:** Python, `ib_insync` 0.9.86, stdlib `sqlite3`, pandas, `pandas_market_calendars`, `unittest`.

**Spec:** `docs/superpowers/specs/2026-11-19-spx-tick-history-design.md` (approved). The spec is normative for behavior, values, and operational rules.

## Global Constraints

- Work from the `saveHistoricalData` repository root. Use `..\venv\Scripts\python.exe` for every Python command.
- Use TDD: each behavior has a failing focused test before implementation. Tests use `unittest`, in-memory or temporary SQLite databases, and fake IBKR objects.
- Do not contact IBKR in unit tests, do not touch either existing production bar database, and do not run scripts under the unsafe `tests/` directory.
- New tests live at repository root as `test_spx_tick_*.py`. Do not add them to `tests/interface_ibkr_tests.py` or `tests/interface_localDb_tests.py`.
- Only changes to existing collection modules are: `setupConnection(clientId=10)`, a `reqHistoricalTicks` pacing entry, and the new request helper. Existing defaults must remain unchanged.
- Canonical DB path is `X:/data/ibkr/historicalData_ticks.db`; unit tests never use it. Live smoke test uses a temporary `--db-path`.
- Tick request: `TRADES`, `useRth=True`, max 1,000 ticks/page, UTC start strings in `YYYYMMDD-HH:MM:SS` format. Stored dates are naive America/New_York strings in `YYYY-MM-DD HH:MM:SS` format.
- Backfill order is oldest-first. Production backfill is not run as part of this plan; the live verification is one known session in a temporary database.
- Keep changes minimal and follow existing module style. During implementation, also follow `~/.codex/skills/implementation-discipline/SKILL.md` as required by repository guidance. Update `docs/architecture.md` and `README.md` when the new data path is added.

## Review Focus

- **Timezone conversion across DST or winter sessions:** an ET open must convert to the correct UTC request hour, not a fixed offset. Task 3 includes an exact winter/summer test.
- **A provider page is out of order or repeats a timestamp with a revised price:** ingestion must sort, dedupe deterministically (last page value wins), and never move the cursor backward. Tasks 3 and 4 include this test.
- **An empty response arrives with informational farm messages:** codes 2104/2106/2158 are ignored; real errors are classified separately. Task 4 tests both no-data and fatal/error paths.
- **A second run resets status or duplicates data:** `ensure_sessions`, page upserts, and backfill must be idempotent and preserve completed sessions. Tasks 2, 4, and 6 test this.
- **A read-only reader opens the store while a writer is active:** reader connections reject writes, and the writer/reader lock behavior is explicit. Task 2 tests read-only rejection.

---

### Task 1: Configuration and IBKR Request Plumbing

**Files:**
- Modify: `config.py` (append tick settings)
- Modify: `interface_ibkr.py` (pacing map near lines 34-38, `setupConnection()` near lines 98-116, new helper after `getBars`)
- Test: `test_spx_tick_ibkr.py` (create)

**Interfaces:**
- Consumes: existing `interface_ibkr._paceIbkrRequest`, `_exit_if_disconnected`, `IB` connection factory.
- Produces:
  - `config.tick_client_id = 11` and the other tick settings from spec section 12.
  - `setupConnection(clientId=10)` returning the connected IB object.
  - `getHistoricalTicks(ibkr, contract, startDateTime, numberOfTicks=1000, whatToShow='TRADES', useRth=True) -> list`.

- [ ] **Step 1: Write failing IBKR plumbing tests**

Create tests for:

```python
@patch('interface_ibkr.IB')
def test_setup_connection_uses_client_id(self, IB):
    interface_ibkr.setupConnection(clientId=11)
    IB.return_value.connect.assert_called_once_with('127.0.0.1', 7496, clientId=11)

def test_get_historical_ticks_request(self):
    # Fake IB has isConnected() -> True and reqHistoricalTicks recording kwargs.
    # Patch interface_ibkr._paceIbkrRequest and assert it is called with
    # (fake_ib, 'reqHistoricalTicks'). Assert response is returned unchanged and:
    kwargs = {
        'contract': contract,
        'startDateTime': '20261118-14:30:00',
        'endDateTime': '',
        'numberOfTicks': 1000,
        'whatToShow': 'TRADES',
        'useRth': True,
        'ignoreSize': False,
    }
```

Also assert `setupConnection()` without an argument still uses clientId 10.

- [ ] **Step 2: Run tests and confirm they fail**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_ibkr -v`
Expected: failures for missing `getHistoricalTicks` and unsupported `clientId` parameter.

- [ ] **Step 3: Add config values and IBKR helpers**

Append the exact tick settings from spec section 12 to `config.py`. Add `'reqHistoricalTicks': 3` to `_IBKR_MIN_INTERVAL_BY_REQUEST`. Change `setupConnection` to accept `clientId=10` and pass it to `connect`.

Implement `getHistoricalTicks` to call `_exit_if_disconnected`, pace with request name `reqHistoricalTicks`, then call `ibkr.reqHistoricalTicks` with keyword arguments matching the test and `miscOptions=[]` left at the library default. Do not catch request errors here.

- [ ] **Step 4: Run tests and confirm they pass**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_ibkr -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add config.py interface_ibkr.py test_spx_tick_ibkr.py
git commit -m "feat: add paced SPX tick request plumbing"
```

### Task 2: Tick Store Schema, Session State, and Queries

**Files:**
- Create: `interface_ticks.py`
- Test: `test_spx_tick_store.py` (create)

**Interfaces:**
- Consumes: `config.dbname_ticks`, `config.tick_symbol`.
- Produces:
  - `tick_table(symbol) -> str` returning `'<symbol>_index_ticks'`.
  - `connect_ticks(db_path=config.dbname_ticks, readonly=False) -> sqlite3.Connection` with `isolation_level=None`; accepts `':memory:'`, file paths, sets WAL/busy timeout on writers and `mode=ro`/`query_only=ON` on readers.
  - `ensure_schema(conn, symbol=config.tick_symbol) -> None` using spec section 6.2/6.3 SQL.
  - `ensure_sessions(conn, sessions, symbol=config.tick_symbol) -> None`; each session has `session_date`, `session_open`, `session_close`; inserts `pending` only when missing.
  - `get_session(conn, symbol, session_date) -> dict | None`.
  - `save_session(conn, state) -> None`; required keys `symbol`, `session_date`, `session_open`, `session_close`, `status`; remaining fields default to schema defaults and update on conflict.
  - `list_sessions(conn, symbol=config.tick_symbol, statuses=None, start_date=None, end_date=None) -> list[dict]` sorted by `session_date ASC`.
  - `upsert_ticks(conn, rows, symbol=config.tick_symbol) -> int` for `(date, price)` tuples using `ON CONFLICT(date) DO UPDATE SET price=excluded.price`; returns affected row count.
  - `session_metrics(conn, symbol, session_open, session_close) -> dict` with `tick_count`, `first_tick`, `last_tick`.
  - `get_ticks(conn, start, end, symbol=config.tick_symbol) -> pandas.DataFrame` with sorted `date, price`, start-inclusive/end-exclusive.

- [ ] **Step 1: Write failing store tests**

Cover:

```python
def test_schema_can_be_created_twice(self):
    ensure_schema(self.conn, symbol='TEST')
    ensure_schema(self.conn, symbol='TEST')

def test_upsert_is_idempotent_and_revises_price(self):
    upsert_ticks(self.conn, [('2026-11-18 09:30:01', 100.0)], symbol='TEST')
    upsert_ticks(self.conn, [('2026-11-18 09:30:01', 101.0)], symbol='TEST')
    # Exactly one row remains and price is 101.0.

def test_ensure_sessions_does_not_reset_complete(self):
    # Insert pending session, save it as complete, call ensure_sessions again;
    # status remains complete and attempts/tick_count are preserved.

def test_readonly_connection_rejects_writes(self):
    # Create file DB, close writer, open readonly=True, assert a write raises.
```

Add tests for `session_metrics` inclusive bounds, `list_sessions` status/date filtering and ordering, `get_ticks` bounds and parsed/sorted rows.

- [ ] **Step 2: Run tests and confirm they fail**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_store -v`
Expected: module or symbol import failures.

- [ ] **Step 3: Implement `interface_ticks.py`**

Implement the Interfaces block exactly. Use one table per supplied symbol, quoted identifiers, and bound values for dates/prices. `ensure_sessions` uses `ON CONFLICT(symbol, session_date) DO NOTHING`. `save_session` uses the matching `DO UPDATE` clause. Do not add commits inside store functions; `connect_ticks` uses autocommit, while task 4 explicitly wraps page + state in a transaction.

- [ ] **Step 4: Run tests and confirm they pass**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_store -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add interface_ticks.py test_spx_tick_store.py
git commit -m "feat: add idempotent SPX tick SQLite store"
```

### Task 3: Session Calendar and Tick Page Normalization

**Files:**
- Modify: `interface_ticks.py`
- Create: `maintainSPXTicks.py`
- Test: `test_spx_tick_ingest.py` (create)

**Interfaces:**
- Consumes: `pandas_market_calendars`, `zoneinfo.ZoneInfo`, store symbols.
- Produces from `interface_ticks.py`:
  - `session_schedule(start_date, end_date) -> list[dict]` inclusive date range, each dict containing `session_date`, `session_open`, `session_close` ET-naive strings.
- Produces from `maintainSPXTicks.py`:
  - `et_to_utc_start(value) -> str`, accepting an ET-naive datetime/string and returning `YYYYMMDD-HH:MM:SS` UTC.
  - `page_to_rows(ticks, session_open, session_close) -> list[tuple[str, float]]`; tick objects expose `.time` (UTC-aware datetime) and `.price`.

- [ ] **Step 1: Write failing calendar and normalization tests**

Use these exact fixtures:

```python
def test_schedule_excludes_holiday_and_uses_early_close(self):
    sessions = session_schedule('2025-11-27', '2025-11-28')
    # Only 2025-11-28; open is 09:30:00 and close is 13:00:00 ET.

def test_utc_request_uses_actual_dst_offset(self):
    assert et_to_utc_start('2026-11-18 09:30:00') == '20261118-14:30:00'
    assert et_to_utc_start('2026-06-18 09:30:00') == '20260618-13:30:00'

def test_page_to_rows_converts_filters_sorts_and_dedupes(self):
    # Give out-of-order ticks, a duplicate timestamp whose later value is 102.0,
    # price 0.0, and a next-session tick. Assert ascending unique rows, ET text,
    # duplicate price 102.0, and no invalid/next-session rows.
```

- [ ] **Step 2: Run tests and confirm they fail**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_ingest -v`
Expected: missing function failures.

- [ ] **Step 3: Implement calendar and page conversion**

Use NYSE calendar market open/close timestamps converted from their native timezone to `America/New_York`, then formatted as ET-naive text. `page_to_rows` filters non-finite/non-positive prices and out-of-session timestamps, converts UTC to ET, dedupes by timestamp with the last value in page order winning, and returns ascending rows.

- [ ] **Step 4: Run tests and confirm they pass**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_ingest -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add interface_ticks.py maintainSPXTicks.py test_spx_tick_ingest.py
git commit -m "feat: add SPX tick session and timestamp normalization"
```

### Task 4: Per-Session Pagination, Error Mapping, and Status Transitions

**Files:**
- Modify: `maintainSPXTicks.py`
- Modify: `test_spx_tick_ingest.py`

**Interfaces:**
- Consumes: `interface_ibkr.getHistoricalTicks`, `refreshConnection`, and all Task 2/3 interfaces.
- Produces:
  - `class FatalTickError(RuntimeError)`.
  - `request_tick_page(ibkr, contract, start_utc) -> tuple[list, list[tuple[int, str]]]` registering a temporary `errorEvent` collector; informational codes 2104/2106/2158 are ignored.
  - `classify_errors(errors) -> str | None` returning `fatal` (10187/10314), `reconnect` (502/504/1100/1101), `failed` (all others including 10197), `no_data` (162 only), or `None`. Precedence is `fatal` > `reconnect` > `failed` > `no_data`.
  - `collect_session(ibkr, conn, contract, session, force=False, resume=True, reconnect=interface_ibkr.refreshConnection) -> dict` implementing spec section 7.2 and returning the saved session state.

- [ ] **Step 1: Write failing pagination and status tests**

Use a fake page source driven by a cursor, with fake UTC tick objects, temp/in-memory store, and symbol `TEST`. Include:

- 2,501 ticks across three pages ending at the close: 3 requests, ascending rows, `complete`, correct first/last/count.
- A short page with later data still available: collection continues rather than assuming completion.
- Empty first response: no rows, `retry`, attempts 1. Two more no-data runs: `unavailable` at 3.
- Non-empty next-session-only response: stores nothing and follows the same no-data transition.
- Early-end response with some rows: `partial`; completed sessions are skipped unless `force=True`.
- Interrupted `in_progress` state with `last_tick`: first request starts at `last_tick + 1 second` in UTC.
- A forced re-fetch with every row already stored: cursor still advances and session completes.
- Re-running a completed session leaves rows and status unchanged; upsert revises a price on force without duplication.
- Cursor non-advance and request cap terminate safely (no infinite loop).

Add classification tests:

```python
def test_error_classification(self):
    assert classify_errors([]) is None
    assert classify_errors([(10187, 'permissions')]) == 'fatal'
    assert classify_errors([(1100, 'connectivity')]) == 'reconnect'
    assert classify_errors([(162, 'no data')]) == 'no_data'
    assert classify_errors([(10197, 'competing session')]) == 'failed'
```

Test informational error codes are ignored by `request_tick_page`, fatal errors save `failed` and raise `FatalTickError`, and reconnect is called before a successful retry; exhausted reconnects save `failed`.

- [ ] **Step 2: Run tests and confirm they fail**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_ingest -v`
Expected: missing function failures and collection behavior failures.

- [ ] **Step 3: Implement request handling and `collect_session`**

Follow spec sections 7.2/7.6 exactly. Wrap each page upsert and status update in `BEGIN IMMEDIATE`/`COMMIT`; rollback on failure. Advance by the last valid tick plus one second, never by inserted count. Finalize metrics with `session_metrics`; zero-row runs increment attempts and become `retry`/`unavailable`; non-empty runs become `complete` or `partial` using the 60-second close tolerance. Store `last_error` and `fetched_at`.

- [ ] **Step 4: Run tests and confirm they pass**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_ingest test_spx_tick_store -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add maintainSPXTicks.py test_spx_tick_ingest.py
git commit -m "feat: implement resumable SPX tick session ingestion"
```

### Task 5: Independent Audit and Severity Rules

**Files:**
- Modify: `interface_ticks.py`
- Create: `test_spx_tick_audit.py`

**Interfaces:**
- Consumes: Task 2 store/state/metrics and Task 3 calendar.
- Produces:
  - `session_max_gap_seconds(conn, symbol, session_open, session_close) -> float` using SQL window functions over ordered tick dates.
  - `audit_sessions(conn, start_date, end_date, symbol=config.tick_symbol) -> pandas.DataFrame` with columns `session_date`, `severity`, `finding`, `detail`.
  - `audit_exit_code(findings, strict=False) -> int`: 1 on any error, or on warnings when strict; otherwise 0.

- [ ] **Step 1: Write failing audit fixtures and tests**

Build a temporary store with explicit sessions and rows for:

- Clean complete session: no error finding.
- `missing_status`, `not_started`, `in_progress`, `partial`, `failed`: errors.
- `retry`: warning. `unavailable` is informational; an unavailable session with zero rows also yields the expected `no_rows_unavailable` informational finding.
- Complete status with zero rows: `no_rows_complete` error.
- Early end (< close−60s): error. Late start (> open+300s): warning.
- Internal no-tick intervals of 600 and 1,800 seconds: `gap` warning and only `large_gap` error respectively; one/two-second gaps produce neither.
- Stored non-positive price inserted directly: `invalid_price` error.
- A tick outside an expected session window and a tick on a weekend: `unexpected_rows` error.
- Five baseline complete sessions of comparable density: an early-close session with equal per-minute density is not flagged; a later session below half median is `low_density` warning. With fewer than five baselines, skip the check.
- `audit_exit_code`: errors return 1, warnings alone return 0, warnings return 1 with `strict=True`, informational `unavailable` alone returns 0.

- [ ] **Step 2: Run tests and confirm they fail**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_audit -v`
Expected: missing function failures.

- [ ] **Step 3: Implement audit**

Walk expected sessions oldest-first, independently compare state with stored rows, and emit only the highest-severity finding for a given gap. Calculate density as `tick_count / session_minutes`; maintain a rolling list of the previous 20 valid complete-session densities and evaluate only when at least five exist. Use SQL for counts/min/max/gaps rather than loading full sessions. Include the severity names and thresholds from spec section 9.

- [ ] **Step 4: Run tests and confirm they pass**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_audit -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add interface_ticks.py test_spx_tick_audit.py
git commit -m "feat: add independent SPX tick completeness audit"
```

### Task 6: Backfill and Daily Orchestration

**Files:**
- Modify: `maintainSPXTicks.py`
- Create: `test_spx_tick_jobs.py`

**Interfaces:**
- Consumes: store sessions, calendar, `collect_session`.
- Produces:
  - `run_backfill(ibkr, conn, contract, start_date, end_date, max_runtime_minutes=None) -> dict` summary with processed/status counts and `stopped_early`.
  - `run_daily(ibkr, conn, contract, now_et=None) -> dict`; `now_et` accepts a naive ET datetime for deterministic tests and defaults to current ET.

- [ ] **Step 1: Write failing job tests**

Patch `maintainSPXTicks.collect_session` to record session dates and return a completed state. Assert:

- `run_backfill` calls `ensure_sessions` first, then processes only `pending`, `retry`, `failed`, `in_progress`, `partial` oldest-first, with `force=False`, `resume=True`.
- Complete/unavailable sessions are skipped and `ensure_sessions` does not reset them.
- `max_runtime_minutes=0` stops before the next session and returns `stopped_early=True`.
- With `now_et='2026-11-18 12:00:00'` on a Wednesday, the open session is skipped and prior closed sessions are processed. With time after 16:00 ET, the just-closed session is included. Daily work is limited to the last seven days.

- [ ] **Step 2: Run tests and confirm they fail**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_jobs -v`
Expected: missing function failures.

- [ ] **Step 3: Implement jobs**

Backfill creates expected rows for the full range and processes the state list in ascending session date. Daily examines a 14-day calendar for session discovery, but only processes closed sessions in the last 7 days. Stop cleanly on runtime or `KeyboardInterrupt` and preserve `in_progress` state from the collector.

- [ ] **Step 4: Run tests and confirm they pass**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_jobs test_spx_tick_ingest -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add maintainSPXTicks.py test_spx_tick_jobs.py
git commit -m "feat: add SPX tick backfill and daily jobs"
```

### Task 7: Targeted Cleanup and Repair

**Files:**
- Modify: `interface_ticks.py`
- Modify: `maintainSPXTicks.py`
- Modify: `test_spx_tick_jobs.py`
- Modify: `test_spx_tick_audit.py`

**Interfaces:**
- Consumes: audit findings, calendar, collection.
- Produces from `interface_ticks.py`:
  - `delete_invalid_rows(conn, symbol=config.tick_symbol, start_date=None, end_date=None) -> int` deleting only rows with `price <= 0` in the optional inclusive date range.
  - `delete_unexpected_rows(conn, sessions, symbol=config.tick_symbol) -> int` deleting rows outside all supplied session windows, including non-trading dates.
- Produces from `maintainSPXTicks.py`:
  - `run_repair(ibkr, conn, contract, findings, include_warnings=False, force_unavailable=False) -> dict` returning the selected sessions and post-repair audit findings.

- [ ] **Step 1: Write failing cleanup and repair tests**

Cover:

- Invalid-row cleanup deletes only non-positive prices in range and leaves valid rows untouched.
- Unexpected-row cleanup removes weekend and out-of-window rows but preserves valid open/close-edge rows.
- Repair selects errors by default; `retry`/`late_start`/`gap`/`low_density` only when `include_warnings=True`; `unavailable` only with `force_unavailable=True`.
- Selected sessions are processed oldest-first with `force=True`, `resume=False`; no pre-delete of a session's valid rows occurs.
- Post-repair audit runs for the affected range and is included in the result.

- [ ] **Step 2: Run tests and confirm they fail**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_jobs test_spx_tick_audit -v`
Expected: missing function failures.

- [ ] **Step 3: Implement cleanup and repair**

Use the normal `collect_session` path for session findings. Apply targeted invalid/unexpected cleanup before refetch. Do not add a second tick writer. Reuse `session_schedule` to construct windows and re-audit the same range afterward.

- [ ] **Step 4: Run tests and confirm they pass**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_jobs test_spx_tick_audit test_spx_tick_ingest -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add interface_ticks.py maintainSPXTicks.py test_spx_tick_jobs.py test_spx_tick_audit.py
git commit -m "feat: add SPX tick repair and targeted cleanup"
```

### Task 8: CLI, Single-Instance Lock, and Status

**Files:**
- Modify: `maintainSPXTicks.py`
- Modify: `test_spx_tick_jobs.py`

**Interfaces:**
- Consumes: all earlier tasks, `config.tick_client_id`, `Index`.
- Produces:
  - `acquire_lock(path) -> file object` using an OS-level non-blocking file lock (`msvcrt` on Windows, `fcntl` elsewhere); raises `RuntimeError` if another live process holds it.
  - `release_lock(handle, path) -> None`.
  - `status_summary(conn, symbol=config.tick_symbol) -> dict` with status counts, earliest/latest complete session, total row count, and DB size where applicable.
  - `main(argv=None) -> int` supporting `--backfill`, `--daily`, `--audit`, `--repair`, `--status`, `--include-warnings`, `--force-unavailable`, `--strict`, `--start`, `--end`, `--max-runtime-minutes`, `--db-path`.

- [ ] **Step 1: Write failing lock/status/CLI tests**

Cover:

- Acquire a temp lock, assert a second acquisition fails, release it, assert acquisition succeeds again.
- Status counts and earliest/latest dates reflect fixture sessions and ticks.
- Default date range for backfill is three years; explicit `--start/--end` override it.
- `--audit` and `--status` do not construct an IBKR connection; write modes use clientId 11 and the supplied `--db-path`.
- Audit CLI propagates `audit_exit_code`, including `--strict`.

- [ ] **Step 2: Run tests and confirm they fail**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_jobs -v`
Expected: missing function failures.

- [ ] **Step 3: Implement lock, status, and CLI**

Use the OS lock handle rather than PID-kill liveness checks, so stale files do not block after process death. Only backfill/daily/repair acquire the write lock. Read-only audit/status open `mode=ro`. Default start is `today - 3 years`, default end is the latest completed NYSE session. Ensure schema and acquire lock before any write operation; release everything in `finally`. Configure console logging plus an append-only `logs/spx_ticks_YYYYMMDD.log` for CLI runs.

- [ ] **Step 4: Run tests and confirm they pass**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_jobs test_spx_tick_ibkr -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add maintainSPXTicks.py test_spx_tick_jobs.py
git commit -m "feat: add SPX tick maintenance CLI and status reporting"
```

### Task 9: Full Verification, One-Session Live Smoke Test, and Documentation

**Files:**
- Modify: `docs/architecture.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: completed CLI and all tests.
- Produces: verified one-session temporary database and updated operator documentation; no production DB writes.

- [ ] **Step 1: Run all new unit tests plus the isolated prehistory regression test**

Run: `..\venv\Scripts\python.exe -m unittest test_spx_tick_ibkr test_spx_tick_store test_spx_tick_ingest test_spx_tick_audit test_spx_tick_jobs test_prehistory_tracking -v`
Expected: all PASS; no IBKR calls in unit tests.

- [ ] **Step 2: Run the live smoke test against a temporary DB**

Only while TWS is available and the bar/futures collectors are idle, run from the repository root:

```powershell
..\venv\Scripts\python.exe maintainSPXTicks.py --backfill --start 2026-11-18 --end 2026-11-18 --db-path C:\Users\Frrlough.000\AppData\Local\Temp\opencode\spx_ticks_smoke.db
..\venv\Scripts\python.exe maintainSPXTicks.py --audit --start 2026-11-18 --end 2026-11-18 --db-path C:\Users\Frrlough.000\AppData\Local\Temp\opencode\spx_ticks_smoke.db
```

Expected: about 23k rows, first tick `2026-11-18 09:30:01`, last tick `2026-11-18 15:59:59`, session `complete`, audit exit 0. If the provider response differs, inspect the state/actual rows and fix behavior rather than relaxing assertions blindly. Delete only the temporary smoke files after recording results; leave `X:/data/ibkr/historicalData_ticks.db` untouched or nonexistent.

- [ ] **Step 3: Update architecture and usage documentation**

Add the tick maintenance component, separate SQLite path, schema roles, ET/UTC convention, read paths (R/RSQLite and monitor), and the backfill/daily/audit/repair commands. Include the rule not to run write jobs alongside the bar/futures collectors and that R should use bounded/optionally read-only queries.

- [ ] **Step 4: Run final tests and inspect the diff**

Run the Step 1 command again. Inspect `git diff --stat` and confirm only intended files changed; no production SQLite file, watchlist, token, or generated data file was modified.

- [ ] **Step 5: Commit**

```bash
git add docs/architecture.md README.md
git commit -m "docs: document SPX tick collection and operations"
```

## Execution Handoff

After implementation, production backfill remains a separately scheduled operation: about 54.5 hours of requests, oldest-first, run when the existing collectors are idle. The one-session smoke test does not start it.
