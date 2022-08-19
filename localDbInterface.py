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

index_list = ['VIX', 'VIX3M', 'VVIX'] # global reference list of index symbols, this is some janky ass shit .... 

## lookup table for interval labels 
intervalMappings = pd.DataFrame(
    {
        'label': ['5m', '15m', '30m', '1h', '1d', '1m'],
        'stock': ['FiveMinutes', 'FifteenMinutes', 'HalfHour', 'OneHour', 'OneDay', 'OneMonth'],
        'index':['5mins', '15mins', '30mins', '1hour', '1day', '1month']
    }
)

## TODO test the below function, imported from another module...
"""
Save history to a sqlite3 database
###

Params
------------
history: [DataFrame]
    pandas dataframe with date and OHLC, volume, interval, and vwap  
conn: [Sqlite3 connection object]
    connection to the local db 
"""
def saveHistoryToDB(history, conn, type='stock'):

    ## Tablename convention: <symbol>_<stock/opt>_<interval>
    tableName = history['symbol'][0]+'_'+type+'_'+history['interval'][0]
    
    if type == 'stock':
        history.to_sql(f"{tableName}", conn, index=False, if_exists='append')
    
    elif type == 'index':
        history.to_sql(f"{tableName}", conn, index=False, if_exists='append')

    elif type == 'option':
        print(' saving options to the DB is not yet implemented')

"""
Returns dataframe of px from database 

Params
===========
symbol - [str]
interval - [str] 
lookback - [str] optional 

"""
def getPriceHistory(symbol, interval):
    tableName = _constructTableName(symbol, interval)
    conn = _connectToDb(symbol)
    sqlStatement = 'SELECT * FROM '+tableName
    pxHistory = pd.read_sql(sqlStatement, conn)
    conn.close()
    ## standardize col names
    if symbol in index_list:
        pxHistory.rename(columns={'date':'start'}, inplace=True)
    
    return pxHistory


"""
constructs the appropriate tablename to call local DB 

Params
===========
symbol - [str]
interval - [str] (must match with intervalMappings global var)

"""
def _constructTableName(symbol, interval):
    type_ = 'stock'
    if symbol.upper() in index_list:
        type_ = 'index'

    tableName = symbol+'_'+type_+'_'+intervalMappings.loc[intervalMappings['label'] == interval][type_]

    return tableName[0]

"""
establishes a connection to the appropriate DB based on type of symbol passed in. 

Returns sqlite connection object 

Params
========
symbol - [str] 
"""
def _connectToDb(symbol):
    if symbol in index_list:
        conn = sqlite3.connect(dbname_index)
    else:
        conn = sqlite3.connect(dbname_stocks)

    return conn

print(getPriceHistory('aapl', '5m'))

#print(_constructTableName('vix', '5m'))