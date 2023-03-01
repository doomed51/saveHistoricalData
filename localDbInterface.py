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
def saveHistoryToDB(history, conn):
    
    ## set type to index if the symbol is in the index list 
    if history['symbol'][0] in index_list:
        type = 'index'
    else: 
        type='stock'
    
    tableName = history['symbol'][0]+'_'+type+'_'+history['interval'][0]
    ## remove any duplicate records 
    # get 

    history.to_sql(f"{tableName}", conn, index=False, if_exists='append')
    
    ## Update the symbolRecord lookup table 
    # table name: 00-lookup_symbolRecords
    # query db for tablename 
    #   if no record
    #       add record
    #       add record.earliestTimestamp_ibkr
    #   if record is there 
    #       if record.startDate > history.min 
    #           update startDate -> history.min
    #           update numMissingBusDays to  

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
establishes a connection to the appropriate DB based on type of symbol passed in. 

Returns sqlite connection object 

Params
========
symbol - [str] 
"""
def _connectToDb():
    return sqlite3.connect(dbname_index)

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

    tableName = symbol+'_'+type_+'_'+intervalMappings.loc[intervalMappings['label'] == interval][type_].values[0]

    return tableName

"""
utility - remove duplicate records from ohlc table

Params
==========
tablename - [str]
"""
def _removeDuplicates(tablename):
    conn = _connectToDb() # connect to DB

    ## group on date & select the min row IDs; then delete all the ROWIDs not in the selected list
    sql_selectMinId = 'DELETE FROM %s WHERE ROWID NOT IN (SELECT MIN(ROWID) FROM %s GROUP BY date)'%(tablename, tablename)

    ## run the query 
    cursor = conn.cursor()
    cursor.execute(sql_selectMinId)


_removeDuplicates('VIX_index_5mins')


