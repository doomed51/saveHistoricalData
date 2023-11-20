# saveHistoricalData

A simple set of scripts to download and store historical data from ibkr

Requires ibkr account and appropriate setup for api access. 

Data is stored in SQLite. 

tickerList.csv -> list of symbols to store data for


Basic structure:
 - db interface: manages connections and operations on the local store of symbol data 
 - ibkr interface: manages operations on the ibkr api 
 - getHistoricalData: manages equities data
 - maintainHistoricalData_futures: manages futures data

Usage: 
- populate tickerlist.csv with the symbols you want to track
- Setup config.py (db location, watchlist locations, intervals to track)  
- open and login to IBKR TWS 
- run qt_getHistoricalData 
