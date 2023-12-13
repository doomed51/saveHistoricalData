"""
This module simplifies interacting with the local database of historical ohlc data.

    - connect to db
    - save historical data to local db 
    - retrieve historical data for symbol and interval 
    - automatically clears duplicates if any

"""

import config
import datetime
import re
import sqlite3

import pandas as pd

from rich import print

""" Global vars """
dbname_stocks = 'historicalData_index.db' ## stock data location

index_list = config._index 

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
def _constructTableName(symbol, interval, lastTradeDate=''):
    type_ = 'stock'
    tableName = symbol+'_'+type_+'_'+interval
    if symbol.upper() in index_list:
        type_ = 'index'
        tableName = symbol+'_'+type_+'_'+interval
    elif lastTradeDate:
        tableName = symbol+'_'+str(lastTradeDate)+'_'+interval


    

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
        sql_minDate_symbolHistory = 'SELECT MIN(date), symbol, interval, lastTradeDate FROM %s'%(tablename)
    else:
        sql_minDate_symbolHistory = 'SELECT MIN(date), symbol, interval FROM %s'%(tablename)
    minDate_symbolHistory = pd.read_sql(sql_minDate_symbolHistory, conn)
    # convert date column to datetime
    minDate_symbolHistory['MIN(date)'] = pd.to_datetime(minDate_symbolHistory['MIN(date)'], format='ISO8601')

    # make sure date column is not tzaware
    minDate_symbolHistory['MIN(date)'] = minDate_symbolHistory['MIN(date)'].dt.tz_localize(None)

    #remove space from the interval column
    minDate_symbolHistory['interval'] = minDate_symbolHistory['interval'].apply(lambda x: _removeSpaces(x))
    
    ## get the earliest record date as per the lookup table
    if type == 'future': ## add lastTradeDate to selection query 
        sql_minDate_recordsTable = 'SELECT firstRecordDate FROM \'%s\' WHERE symbol = \'%s\' and interval = \'%s\' and lastTradeDate = \'%s\''%(lookupTablename, minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0], minDate_symbolHistory['lastTradeDate'][0])
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
            minDate_symbolHistory['numMissingBusinessDays'] = numMissingDays
            #minDate_symbolHistory = minDate_symbolHistory.iloc[:,[4,1,2,0,5,3]]

        ## save record to db
        minDate_symbolHistory.to_sql(f"{lookupTablename}", conn, index=False, if_exists='append')
    
    ## otherwise update the existing record in the lookup table 
    else:
        # if this is an empty string '', then we will use the min date from the record table instead of the lookup table 
        if minDate_recordsTable['firstRecordDate'][0] == '':
            minDate_recordsTable['firstRecordDate'][0] = minDate_symbolHistory['firstRecordDate'][0]

        ## rename columns to match db table columns 
        minDate_symbolHistory.rename(columns={'MIN(date)':'firstRecordDate'}, inplace=True)

        # calculate the number of missing business days between the earliest record date in ibkr, and the earliest record date as per the db
        if earliestTimestamp:
            numMissingDays = len(pd.bdate_range(earliestTimestamp, minDate_symbolHistory.iloc[0]['firstRecordDate']))

        ## update lookuptable with the symbolhistory min date
        # if we are saving futures, we have to query on symbol, interval, AND lastTradeDate
        print(' Updating lookup table...')
        sql_updateNumMissingDays=''
        if type == 'future':
            sql_update = 'UPDATE \'%s\' SET firstRecordDate = \'%s\' WHERE symbol = \'%s\' and interval = \'%s\' and lastTradeDate = \'%s\''%(lookupTablename, minDate_symbolHistory['firstRecordDate'][0], minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0], minDate_symbolHistory['lastTradeDate'][0])

            ## sql statement to update the numMissinbgBusinessDays column
            sql_updateNumMissingDays = 'UPDATE \'%s\' SET numMissingBusinessDays = \'%s\' WHERE symbol = \'%s\' and interval = \'%s\' and lastTradeDate = \'%s\''%(lookupTablename, numMissingDays, minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0], minDate_symbolHistory['lastTradeDate'][0])
        else:
            sql_update = 'UPDATE \'%s\' SET firstRecordDate = \'%s\' WHERE symbol = \'%s\' and interval = \'%s\''%(lookupTablename, minDate_symbolHistory['firstRecordDate'][0], minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0])

            if earliestTimestamp: 
                sql_updateNumMissingDays = 'UPDATE \'%s\' SET numMissingBusinessDays = \'%s\' WHERE symbol = \'%s\' and interval = \'%s\''%(lookupTablename, numMissingDays, minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0])

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
    #pxHistory_hasTimezone = pxHistory[pxHistory['date'].str.len() > 19]
    #if not pxHistory_hasTimezone.empty:
    #    # remove the timezone info from the date column
    #    pxHistory_hasTimezone['date'].str.slice(0,19)

        # update pxhistory with the formatted date column
    #    pxHistory.update(pxHistory_hasTimezone)

    # rename date column and sort 
    pxHistory.rename(columns={'date':'Date'}, inplace=True)
    pxHistory.sort_values(by='Date', inplace=True) #sort by date
    
    # make sure date column is datetime
    pxHistory['Date'] = pd.to_datetime(pxHistory['Date'], format='ISO8601', utc=True)
    pxHistory['Date'] = pxHistory['Date'].dt.tz_localize(None)

    # if interval is < 1 day, split the date and time column
    if pxHistory['interval'][0] in ['1min', '5mins', '15mins', '30mins', '1hour']:
        # select time component of date column
        pxHistory['Time'] = pxHistory['Date'].dt.time        
        # select date component of date column
        pxHistory['Date'] = pxHistory['Date'].dt.date

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
def saveHistoryToDB(history, conn, earliestTimestamp='', type=''):
    ## set type to index if the symbol is in the index list 
    print('%s: Adding %s-%s, dates %s to %s'%(datetime.datetime.now().strftime("%H:%M:%S") , history['symbol'][0], history['interval'][0], history['date'].min(), history['date'].max()))

    if type != 'future':
        if history['symbol'][0] in index_list:
            type = 'index'
        else: 
            type='stock'
    
    ## construct tablename
    if 'lastTradeDate' in history.columns:
        # construct tablename as symbol_lastTradeDate_interval
        tableName = history['symbol'][0]+'_'+history['lastTradeDate'][0]+'_'+_removeSpaces(history['interval'][0])
        type = 'future'
    
    else:
        # construct tablename as symbol_type_interval
        tableName = history['symbol'][0]+'_'+type+'_'+history['interval'][0]

    # write history to db
    history.to_sql(f"{tableName}", conn, index=False, if_exists='append')
    print('[green]  Done! [/green]')

    #make sure there are no duplicates in the resulting table
    _removeDuplicates(conn, tableName)

    _updateLookup_symbolRecords(conn, tableName, type, earliestTimestamp=earliestTimestamp)

    ## print logging info
    if 'lastTradeDate' in history.columns:
        print(' %s: %s-%s-%s[green]...Updated![/green]'%(datetime.datetime.now().strftime("%H:%M:%S"),history['symbol'][0], history['lastTradeDate'][0], history['interval'][0]))
    else:
        print(' %s: %s-%s[green]...Updated![/green]\n'%(datetime.datetime.now().strftime("%H:%M:%S"), history['symbol'][0], history['interval'][0]))
    
"""
Returns a dataframe of all tables that currently exist in the db with some helpful stats:
- lastUpdateDate: [datetime] last time the table was updated
- daysSinceLastUpdate: [int] number of business days since the table was last updated
- firstRecordDate: [datetime] first record date in the table

**note: this pulls directly from database tables and does not depend on the lookup table

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
def getPriceHistory(conn, symbol, interval, withpctchange=False, lastTradeDate=''):
    
    # construct sql statement and query the db 
    tableName = _constructTableName(symbol, interval, lastTradeDate)
    sqlStatement = 'SELECT * FROM '+tableName
    pxHistory = pd.read_sql(sqlStatement, conn)

    # standardize col formatting 
    pxHistory = _formatpxHistory(pxHistory)

    # calc pct change if requested
    if withpctchange:
        pxHistory['close_pctChange'] = pxHistory['close'].pct_change()
    
    return pxHistory

def getTable(conn, tablename):
    sqlStatement = 'SELECT * FROM '+tablename
    table = pd.read_sql(sqlStatement, conn)
    return table
""" 
Returns the lookup table fo records history as df 
"""
def getLookup_symbolRecords(conn):
    sqlStatement_selectRecordsTable = 'SELECT * FROM \'00-lookup_symbolRecords\''
    symbolRecords = pd.read_sql(sqlStatement_selectRecordsTable, conn)
    if not symbolRecords.empty:
        # convert firstRecordDate column to datetime
        symbolRecords['firstRecordDate'] = pd.to_datetime(symbolRecords['firstRecordDate'], format='ISO8601')

        # given that the 'name' column is in format symbol_type_interval, create a new column type that is just the type
        symbolRecords['type'] = symbolRecords['name'].str.split('_', expand=True)[1]

        # if type is a number, change type to 'future' 
        symbolRecords['type'] = symbolRecords['type'].apply(lambda x: 'future' if x.isdigit() else x)

    return symbolRecords

"""
    returns exchange:symbol in database lookup tablee
"""
def getLookup_exchange(conn, symbol):
    exchangeLookupTable = '00-lookup_exchangeMapping'
    sql = 'SELECT exchange FROM \'%s\' WHERE symbol=\'%s\'' %(exchangeLookupTable, symbol)
    exchange = pd.read_sql(sql, conn).values[0][0]

    return exchange
