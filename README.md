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

Futures configuration notes:
- `config.exchange_mapping` is the source of truth for futures exchange resolution.
- `config.futures_symbol_metadata` stores per-symbol futures settings used by downstream jobs.
- Supported futures symbols include: `CL`, `ES`, `ZB`, `ZN`, `GC`, `SI`, `NG`, `VIX`.

Usage: 
- populate tickerlist.csv with the symbols you want to track
- Setup config.py (db location, watchlist locations, intervals to track)  
- open and login to IBKR TWS 
- run qt_getHistoricalData 
