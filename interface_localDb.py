"""
This module simplifies interacting with the local database of historical ohlc data.

    - connect to db
    - save historical data to local db 
    - retrieve historical data for symbol and interval 
    - automatically clears duplicates if any

"""

import sqlite3
import datetime
import re

import pandas as pd

from rich import print

""" Global vars """
dbname_stocks = 'historicalData_index.db' ## stock data location

index_list = ['VIX', 'VIX3M', 'VVIX', 'SPX', 'VIX1D'] # global reference list of index symbols, this is some janky ass shit .... 

"""
    A context manager to help manage connections with the sqlite database 
"""
class sqlite_connection(object):
    
    def __init__(self, db_name):
        self.db_name = db_name

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.commit()
        self.conn.close()

## adda space between num and alphabet
def _addspace(myStr): 
    return re.sub("[A-Za-z]+", lambda elm: " "+elm[0],myStr )

## removes spaces from the passed in string
def _removeSpaces(myStr):
    ## remove spaces from mystr
    return myStr.replace(" ", "")


"""
lambda function returns numbers of business days since a DBtable was updated
"""
def _getDaysSinceLastUpdated(row, conn):
    maxtime = pd.read_sql('SELECT MAX(date) FROM '+ row['name'], conn)
    mytime = datetime.datetime.strptime(maxtime['MAX(date)'][0][:10], '%Y-%m-%d')
    ## calculate business days since last update
    numDays = len( pd.bdate_range(mytime, datetime.datetime.now() )) - 1

    return numDays

def _getLastUpdateDate(row, conn):
    maxtime = pd.read_sql('SELECT MAX(date) FROM '+ row['name'], conn)
    maxtime = maxtime['MAX(date)'][0]
    
    if 19 >= len(maxtime) > 10:
        maxtime = datetime.datetime.strptime(maxtime, '%Y-%m-%d %H:%M:%S')
    elif len(maxtime) > 19:
        maxtime = datetime.datetime.strptime(maxtime[:19], '%Y-%m-%d %H:%M:%S')

    return maxtime

def _getFirstRecordDate(row, conn):
    mintime = pd.read_sql('SELECT MIN(date) FROM '+ row['name'], conn)
    mintime = mintime['MIN(date)'][0]

    ## convert to datetime handling cases where datetime is formatted as:
    #   1. yyyy-mm-dd
    #   2. yyyy-mm-dd hh:mm:ss 
    #   3. yyyy-mm-dd hh:mm:ss-##:##
    if  19 >= len(mintime) > 10: 
        mintime = datetime.datetime.strptime(mintime, '%Y-%m-%d %H:%M:%S')
    elif len(mintime) > 19:
        mintime = mintime[:19]
        mintime = datetime.datetime.strptime(mintime, '%Y-%m-%d %H:%M:%S')
    else:
        mintime = datetime.datetime.strptime(mintime, '%Y-%m-%d')

    return mintime

"""
constructs the appropriate tablename to call local DB 

Params
===========
symbol - [str]
interval - [str] 

"""
def _constructTableName(symbol, interval, lastTradeMonth=''):
    type_ = 'stock'
    tableName = symbol+'_'+type_+'_'+interval
    if symbol.upper() in index_list:
        type_ = 'index'
        tableName = symbol+'_'+type_+'_'+interval
    elif lastTradeMonth:
        tableName = symbol+'_'+str(lastTradeMonth)+'_'+interval


    

    return tableName

"""
utility - permanently remove duplicate records from ohlc table

Params
==========
tablename - [str]
"""
def _removeDuplicates(conn, tablename):
    ## construct SQL qeury that will group on 'date' column and
    ## select the min row ID of each group; then delete all the ROWIDs from 
    ## the table that not in this list
    sql_selectMinId = 'DELETE FROM %s WHERE ROWID NOT IN (SELECT MIN(ROWID) FROM %s GROUP BY date)'%(tablename, tablename)
    ## run the query 
    cursor = conn.cursor()
    cursor.execute(sql_selectMinId)

"""
Utility to update the symbol record lookup table
This should be called when local db records are updated 
This should not be run before security history is added to the db 

Params
----------
tablename: table that needs to be updated 
numMissingDays: number of days we do not have locally
earliestTimeStamp: earliest timestamp in ibkr
"""
def _updateLookup_symbolRecords(conn, tablename, type, earliestTimestamp, numMissingDays = 5):
    lookupTablename = '00-lookup_symbolRecords'

    ## get the earliest record date as per the db 
    if type == 'future':
        sql_minDate_symbolHistory = 'SELECT MIN(date), symbol, interval, lastTradeMonth FROM %s'%(tablename)
    else:
        sql_minDate_symbolHistory = 'SELECT MIN(date), symbol, interval FROM %s'%(tablename)
    minDate_symbolHistory = pd.read_sql(sql_minDate_symbolHistory, conn)

    # adjust endates caused by bad ibkr data
    if minDate_symbolHistory['symbol'][0] in ['VVIX','SPY']:
        if type == 'future':
            # update the record where name=tablename with nummissinbusinessdays = 
            return
        else:
            return

    #remove space from the interval column
    minDate_symbolHistory['interval'] = minDate_symbolHistory['interval'].apply(lambda x: _removeSpaces(x))
    
    ## get the earliest record date as per the lookup table
    if type == 'future': ## add lastTradeMonth to selection query 
        sql_minDate_recordsTable = 'SELECT firstRecordDate FROM \'%s\' WHERE symbol = \'%s\' and interval = \'%s\' and lastTradeMonth = \'%s\''%(lookupTablename, minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0], minDate_symbolHistory['lastTradeMonth'][0])
    else:
        sql_minDate_recordsTable = 'SELECT firstRecordDate FROM \'%s\' WHERE symbol = \'%s\' and interval = \'%s\''%(lookupTablename, minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0])
    minDate_recordsTable = pd.read_sql(sql_minDate_recordsTable, conn)
    
    ## if no entry is found in the lookup table, add one  
    if minDate_recordsTable.empty:
        print(' Adding new record to lookup table...')
        minDate_symbolHistory['name'] = tablename
        ## rename columns to match db table columns 
        minDate_symbolHistory.rename(columns={'MIN(date)':'firstRecordDate'}, inplace=True)
        if earliestTimestamp:
            ## set missing business days to the difference between the earliest available date in ibkr and the earliest date in the local db
    
            ## add missing columns 
            minDate_symbolHistory['numMissingBusinessDays'] = numMissingDays
        
            minDate_symbolHistory = minDate_symbolHistory.iloc[:,[4,1,2,0,3]]
        
        ## save record to db
        minDate_symbolHistory.to_sql(f"{lookupTablename}", conn, index=False, if_exists='append')
    
    ## otherwise update the existing record
    #elif minDate_symbolHistory['firstRecordDate'][0] < minDate_recordsTable['firstRecordDate'][0]:
    else:
        # if this is an empty string '', then we will use the min date from the record table instead of the lookup table 
        if minDate_recordsTable['firstRecordDate'][0] == '':
            minDate_recordsTable['firstRecordDate'][0] = minDate_symbolHistory['firstRecordDate'][0]

        ## rename columns to match db table columns 
        minDate_symbolHistory.rename(columns={'MIN(date)':'firstRecordDate'}, inplace=True)

        # calculate the number of missing business days between the earliest record date in ibkr, and the earliest record date as per the db
        if type == 'future':
            numMissingDays = len(pd.bdate_range(earliestTimestamp, minDate_symbolHistory.iloc[0]['firstRecordDate']))

        ## update lookuptable with the symbolhistory min date
        # if we are saving futures, we have to query on symbol, interval, AND lastTradeMonth
        print(' Updating lookup table...')
        sql_updateNumMissingDays=''
        if type == 'future':
            sql_update = 'UPDATE \'%s\' SET firstRecordDate = \'%s\' WHERE symbol = \'%s\' and interval = \'%s\' and lastTradeMonth = \'%s\''%(lookupTablename, minDate_symbolHistory['firstRecordDate'][0], minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0], minDate_symbolHistory['lastTradeMonth'][0])

            ## sql statement to update the numMissinbgBusinessDays column
            sql_updateNumMissingDays = 'UPDATE \'%s\' SET numMissingBusinessDays = \'%s\' WHERE symbol = \'%s\' and interval = \'%s\' and lastTradeMonth = \'%s\''%(lookupTablename, numMissingDays, minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0], minDate_symbolHistory['lastTradeMonth'][0])
        else:
            sql_update = 'UPDATE \'%s\' SET firstRecordDate = \'%s\' WHERE symbol = \'%s\' and interval = \'%s\''%(lookupTablename, minDate_symbolHistory['firstRecordDate'][0], minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0]) 
        cursor = conn.cursor()
        cursor.execute(sql_update)
        if sql_updateNumMissingDays:
            cursor.execute(sql_updateNumMissingDays)
        print('[green]  Done! [/green]')

""" ensures proper format of px history tables retrieved from db """
def _formatpxHistory(pxHistory):
    
    # Remove any errant timezone info:
    # get the rows that have timezone info in the date column
    # remove the timezone info from the date column
    # update pxhistory with the formatted date column
    pxHistory_hasTimezone = pxHistory[pxHistory['date'].str.len() > 19]
    if not pxHistory_hasTimezone.empty:
        # remove the timezone info from the date column
        pxHistory_hasTimezone['date'].str.slice(0,19)

        # update pxhistory with the formatted date column
        pxHistory.update(pxHistory_hasTimezone)

    # final formatting ... 
    pxHistory.rename(columns={'date':'Date'}, inplace=True)
    pxHistory.sort_values(by='Date', inplace=True) #sort by date
    
    # if interval is < 1 day, split the date and time column
    if pxHistory['interval'][0] in ['1min', '5mins', '15mins', '30mins', '1hour']:
        pxHistory[['Date', 'Time']] = pxHistory['Date'].str.split(' ', expand=True)
        # set format for Date and Time columns
        pxHistory['Date'] = pd.to_datetime(pxHistory['Date'], format='%Y-%m-%d')
        pxHistory['Time'] = pd.to_datetime(pxHistory['Time'], format='%H:%M:%S')
    return pxHistory
"""
Save history to a sqlite3 database
###

Params
------------
history: [DataFrame]
    pandas dataframe with security timeseries data
conn: [Sqlite3 connection object]
    connection to the local db 
"""
def saveHistoryToDB(history, conn, earliestTimestamp=''):
    ## set type to index if the symbol is in the index list 
    print(' Updating from %s to %s'%(history['date'].min(), history['date'].max()))

    if history['symbol'][0] in index_list:
        type = 'index'
    else: 
        type='stock'
    
    ## construct tablename
    if 'lastTradeMonth' in history.columns:
        # construct tablename as symbol_lastTradeMonth_interval
        tableName = history['symbol'][0]+'_'+history['lastTradeMonth'][0]+'_'+_removeSpaces(history['interval'][0])
        type = 'future'
    
    else:
        # construct tablename as symbol_type_interval
        tableName = history['symbol'][0]+'_'+type+'_'+history['interval'][0]
    
    # write history to db
    history.to_sql(f"{tableName}", conn, index=False, if_exists='append')

    #make sure there are no duplicates in the resulting table
    _removeDuplicates(conn, tableName)

    _updateLookup_symbolRecords(conn, tableName, type,earliestTimestamp=earliestTimestamp)

    ## print logging info
    if 'lastTradeMonth' in history.columns:
        print(' %s: %s-%s-%s[green]...Updated![/green]\n'%(datetime.datetime.now().strftime("%H:%M:%S"),history['symbol'][0], history['lastTradeMonth'][0], history['interval'][0]))
    else:
        print(' %s: %s-%s[green]...Updated![/green]\n'%(datetime.datetime.now().strftime("%H:%M:%S"), history['symbol'][0], history['interval'][0]))
    

"""
Returns a dataframe of all tables that currently exist in the db with some helpful stats
"""
def getRecords(conn):
    records = pd.DataFrame()
    try:
        tableNames = pd.read_sql('SELECT * FROM sqlite_master WHERE type=\'table\' AND NOT name LIKE \'00_%\'', conn)
        # remove tablename like _corrupt
        tableNames = tableNames[~tableNames['name'].str.contains('_corrupt')]            
    except:
        print('no tables!')
        exit()
    if not tableNames.empty:
        records[['symbol', 'type/expiry', 'interval']] = tableNames['name'].str.split('_',expand=True)     
        
        ## add tablename column
        records['name'] = tableNames['name']
        
        ## add record metadata
        records['lastUpdateDate'] = tableNames.apply(_getLastUpdateDate, axis=1, conn=conn)
        records['daysSinceLastUpdate'] = tableNames.apply(_getDaysSinceLastUpdated, axis=1, conn=conn)
        records['interval'] = records.apply(lambda x: _addspace(x['interval']), axis=1)
        records['firstRecordDate'] = tableNames.apply(_getFirstRecordDate, axis=1, conn=conn)
    
    return records


"""
Returns dataframe of px from database 

Params
===========
symbol - [str]
interval - [str] 
lookback - [str] optional 

"""
def getPriceHistory(conn, symbol, interval, withpctchange=False, lastTradeMonth=''):
    
    # construct sql statement and query the db 
    tableName = _constructTableName(symbol, interval, lastTradeMonth)
    sqlStatement = 'SELECT * FROM '+tableName
    pxHistory = pd.read_sql(sqlStatement, conn)
    #ensure formatting of retreived data
    pxHistory = _formatpxHistory(pxHistory)
   
    # calc pct change if requested
    if withpctchange:
        pxHistory['close_pctChange'] = pxHistory['close'].pct_change()
    
    return pxHistory

""" 
Returns the lookup table fo records history as df 
"""
def getLookup_symbolRecords(conn):
    sqlStatement_selectRecordsTable = 'SELECT * FROM \'00-lookup_symbolRecords\''
    symbolRecords = pd.read_sql(sqlStatement_selectRecordsTable, conn)
    # convert firstRecordDate column to datetime
    symbolRecords['firstRecordDate'] = pd.to_datetime(symbolRecords['firstRecordDate'], format='ISO8601')
    return symbolRecords