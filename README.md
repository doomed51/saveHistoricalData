# saveHistoricalData

A simple set of scripts to download and store historical data from ibkr

Requires ibkr account and appropriate setup for api access. 

Data is stored in SQLite. 

tickerList.csv -> list of symbols to store data for


Basic structure:
 - localDbInterface: manages connections and operations on the local store of symbol data 
 - ibkr_gethistoricalData: manages operations on the ibkr api 
 - qt_getHistoricalData: core script that keeps locally available data up-to-date
 
 Functions in progress: 
 - analysis: seasonal analysis plots 
 - backtest: early experimentation of backtesting functionality 

How to use: 
- populate tickerlist.csv with the symbols you want to track
- open and login to IBKR TWS 
- run qt_getHistoricalData 
