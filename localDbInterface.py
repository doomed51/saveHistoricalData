"""
This module simplifies interacting with the local database of stock and ticker data. It consolidates functionality the previously existed across mutliple in the system. 

checklist: 
Create - 0
Read - 0
Update - 0
Delete - 0

"""

import sqlite3

import pandas as pd

""" Global vars """
dbname_stocks = 'historicalData_stock.db' ## vanilla stock data location 
dbname_index = 'historicalData_index.db' ## index data location

index_ = ['VIX', 'VIX3M', 'VVIX'] # global reference list of index symbols, this is some janky ass shit .... 

## lookup table for interval labels 
intervalMappings = pd.DataFrame(
    {
        'label': ['5m', '15m', '30m', '1h', '1d', '1m'],
        'stock': ['FiveMinutes', 'FifteenMinutes', 'HalfHour', 'OneHour', 'OneDay', 'OneMonth'],
        'index':['5 mins', '15 mins', '30 mins', '1 hour', '1 day', '1 month']
    }
)

"""
Returns dataframe of px from database 

Params
===========
symbol - [str]
interval - [str] 
lookback - [str] optional 

"""
def getPriceHistory(symbol, interval, lookback='10D'):
    print(intervalMappings)
    ## if index: 
        ## set lookbakc and interval strings
    ## else stock....
        ## set tablename
    
    ## create connection 
    ## get the data 
    ## return the data 

getPriceHistory('aapl', '5m')