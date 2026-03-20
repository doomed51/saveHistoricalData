# saveHistoricalData

A simple set of scripts to download and store historical timeseries data for equities and futures from ibkr

Requires ibkr account and appropriate setup for api access. 

Data is stored in SQLite3 database files. 

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
