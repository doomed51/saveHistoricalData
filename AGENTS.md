# Repository guidance

This Python repository downloads and stores IBKR historical equities and futures data in SQLite. Work from this directory (`saveHistoricalData`); see `README.md` for the script roles and setup.

## Communication

- Address the current request without assuming where the conversation is going. Avoid unsolicited opinions about importance and platitudes; support conclusions with evidence.

## Development

- Use test-driven development for code changes: add or update a focused failing test, make it pass, then run relevant tests. Keep database tests on in-memory or temporary SQLite databases and mock IBKR calls.
- For implementation, modification, or refactoring of code, follow the `implementation-discipline` skill at `~/.codex/skills/implementation-discipline/SKILL.md`.
- Run the isolated prehistory tests from this directory with `..\venv\Scripts\python.exe -m unittest test_prehistory_tracking -v` on Windows. Other tests under `tests/` are not a safe default test suite: `tests/interface_ibkr_tests.py` calls `setupTestdata()` on import and can refresh data through IBKR; `tests/interface_localDb_tests.py` uses a SQLite file path.
- Keep equities history changes aligned with `getHistoricalData.py`, futures history changes with `maintainHistoricalData_futures.py`, and term-structure changes with `maintainTermStructure.py`. Database access is in `interface_localDB.py`, `interface_localDb_old.py`, and `db_utils.py`; IBKR access is in `interface_ibkr.py`.
- For futures symbol or exchange changes, update the relevant values in `config.exchange_mapping` and `config.futures_symbol_metadata` and check their consumers. See `README.md` for the supported symbols.

## Data and external systems

- `config.py` contains environment-specific database and watchlist paths, including databases under `X:/data/ibkr`. Inspect the resolved paths before running a script that writes data. Use a separate test database for development and verification.
- Scripts that download or maintain history may contact IBKR and write SQLite data. Do not run them as a routine test command. Preserve existing data and avoid changing local tokens, watchlists, database files, or generated output unless the task requires it.
