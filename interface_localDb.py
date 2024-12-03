## Imported from: https://github.com/doomed51/saveHistoricalData.git

"""
This module simplifies interacting with the local database of historical ohlc data. 

    - connect to db
    - save historical data to local db 
    - retrieve historical data for symbol and interval 
    - automatically clears duplicates if any

"""

import sqlite3
import sys
import config
import datetime 
import re

import pandas as pd
sys.path.append('..')
from utils import utils as ut
from rich import print 
from functools import lru_cache
from typing import Optional

""" Global vars """
dbname_index = config.dbname_stock

index_list = config._indexList # global reference list of index symbols, this is some janky ass shit .... 

class sqlite_connection(object): 
    """ Context manager for connecting to sqlite db """
    
    def __init__(self, db_name):
        self.db_name = db_name
    
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.commit()
        self.conn.close()

def _constructTableName(symbol, interval):
    """
    constructs the appropriate tablename to call local DB 

    Params
    ===========
    symbol - [str]
    interval - [str] 

    """
    type_ = 'stock'
    if symbol.upper() in index_list:
        type_ = 'index'

    tableName = symbol+'_'+type_+'_'+interval

    return tableName

def _removeDuplicates(tablename, conn=None):
    """
    utility - permanently remove duplicate records from ohlc table

    Params
    ==========
    tablename - [str]
    """
    if conn is None:
        conn = _connectToDb() # connect to DB
    
    ## construct SQL qeury that will group on 'date' column and
    ## select the min row ID of each group; then delete all the ROWIDs from 
    ## the table that not in this list
    cursor = conn.cursor()
    # sql_selectMinId = 'DELETE FROM ? WHERE ROWID NOT IN (SELECT MIN(ROWID) FROM ? GROUP BY date)'
    # cursor.execute(sql_selectMinId, (tablename, tablename))
    # sql_selectMinId = 'SELECT FROM %s WHERE ROWID NOT IN (SELECT MIN(ROWID) FROM %s GROUP BY date)'%(tablename, tablename)
    sql_selectMinId = 'DELETE FROM %s WHERE ROWID NOT IN (SELECT MIN(ROWID) FROM %s GROUP BY date)'%(tablename, tablename)

    ## run the query 
    cursor.execute(sql_selectMinId)

def remove_duplicates_from_pxhistory_gaps_metadata(conn, tablename):
    """
    Remove duplicates from the pxhistory gaps metadata table
    """
    sqlStatement = 'DELETE FROM \'%s\' WHERE ROWID NOT IN (SELECT ROWID FROM \'%s\' AS sub WHERE (tablename, update_date) IN (SELECT tablename, MAX(update_date) FROM \'%s\' GROUP BY tablename))'%(tablename, tablename, tablename)
    cursor = conn.cursor()
    cursor.execute(sqlStatement)
    
## adda space between num and alphabet
def _addspace(myStr): 
    return re.sub("[A-Za-z]+", lambda elm: " "+elm[0],myStr )

## removes spaces from the passed in string
def _removeSpaces(myStr):
    ## remove spaces from mystr
    return myStr.replace(" ", "")

def _getDaysSinceLastUpdated(row, conn):
    maxtime = pd.read_sql('SELECT MAX(date) FROM '+ row['name'], conn)
    mytime = datetime.datetime.strptime(maxtime['MAX(date)'][0][:10], '%Y-%m-%d')
    ## calculate business days since last update
    numDays = len( pd.bdate_range(mytime, datetime.datetime.now())) - 1

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

def _updateLookup_symbolRecords(conn, tablename, earliestTimestamp, numMissingDays = 5, type =''):
    ## get the earliest record date as per the db 
    if type == 'future':
        sql_minDate_symbolHistory = 'SELECT MIN(date), symbol, interval, lastTradeDate FROM %s'%(tablename)
    else:
        sql_minDate_symbolHistory = 'SELECT MIN(date), symbol, interval FROM %s'%(tablename)
    minDate_symbolHistory = pd.read_sql(sql_minDate_symbolHistory, conn)
    
    # column formatting
    minDate_symbolHistory['MIN(date)'] = pd.to_datetime(minDate_symbolHistory['MIN(date)'], format='ISO8601')
    # make sure date column is not tzaware
    minDate_symbolHistory['MIN(date)'] = minDate_symbolHistory['MIN(date)'].dt.tz_localize(None)
    minDate_symbolHistory['interval'] = minDate_symbolHistory['interval'].apply(lambda x: _removeSpaces(x))
    
    ## get the earliest record date as per the lookup table
    if type == 'future': ## add lastTradeDate to selection query 
        sql_minDate_recordsTable = 'SELECT firstRecordDate FROM \'%s\' WHERE symbol = \'%s\' and interval = \'%s\' and lastTradeDate = \'%s\''%(config.lookupTableName, minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0], minDate_symbolHistory['lastTradeDate'][0])
    else:
        sql_minDate_recordsTable = 'SELECT firstRecordDate FROM \'%s\' WHERE symbol = \'%s\' and interval = \'%s\''%(config.lookupTableName, minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0])
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
        minDate_symbolHistory.to_sql(f"{config.lookupTableName}", conn, index=False, if_exists='append')
    
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
        print('%s: Updating lookup table...'%(datetime.datetime.now().strftime("%H:%M:%S")))
        sql_updateNumMissingDays=''
        if type == 'future':
            sql_update = 'UPDATE \'%s\' SET firstRecordDate = \'%s\' WHERE symbol = \'%s\' and interval = \'%s\' and lastTradeDate = \'%s\''%(config.lookupTableName, minDate_symbolHistory['firstRecordDate'][0], minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0], minDate_symbolHistory['lastTradeDate'][0])

            ## sql statement to update the numMissinbgBusinessDays column
            sql_updateNumMissingDays = 'UPDATE \'%s\' SET numMissingBusinessDays = \'%s\' WHERE symbol = \'%s\' and interval = \'%s\' and lastTradeDate = \'%s\''%(config.lookupTableName, numMissingDays, minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0], minDate_symbolHistory['lastTradeDate'][0])
        else:
            sql_update = 'UPDATE \'%s\' SET firstRecordDate = \'%s\' WHERE symbol = \'%s\' and interval = \'%s\''%(config.lookupTableName, minDate_symbolHistory['firstRecordDate'][0], minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0])

            if earliestTimestamp: 
                sql_updateNumMissingDays = 'UPDATE \'%s\' SET numMissingBusinessDays = \'%s\' WHERE symbol = \'%s\' and interval = \'%s\''%(config.lookupTableName, numMissingDays, minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0])
            
        cursor = conn.cursor()
        cursor.execute(sql_update)
        if sql_updateNumMissingDays:
            cursor.execute(sql_updateNumMissingDays)
        print('%s: [green]Done! [/green]'%(datetime.datetime.now().strftime("%H:%M:%S")))

def _updateLookup_symbolRecords_ORIGINAL(conn, tablename, earliestTimestamp, numMissingDays = 5, type =''):

    """
    sub to update the symbol record lookup table
    This should be called when local db records are updated 
    This should not be run before security history is added to the db 

    Params
    ----------
    tablename: table that needs to be updated 
    numMissingDays: number of days we do not have locally  
    """
    lookupTablename = '00-lookup_symbolRecords'
    
    ## get the earliest record date saved for the target symbol 
    sql_minDate_symbolHistory = 'SELECT MIN(date), symbol, interval FROM %s'%(tablename)
    minDate_symbolHistory = pd.read_sql(sql_minDate_symbolHistory, conn)
    
    ## get the earliest date from the lookup table for the matching symbol 
    sql_minDate_recordsTable = 'SELECT firstRecordDate FROM \'%s\' WHERE symbol = \'%s\' and interval = \'%s\''%(lookupTablename, minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0])
    minDate_recordsTable = pd.read_sql(sql_minDate_recordsTable, conn)
    
    ## add a new record entry in the lookup table since none are there 
    if minDate_recordsTable.empty:
        ## compute the number of missing business days 
        ## since this is a new record, we expect a timestamp to have been 
        ## passed on the call to write history to the db 
                
        if earliestTimestamp:
            ## set missing business days to the difference between the earliest available date in ibkr and the earliest date in the local db
            numMissingDays = len(pd.bdate_range(earliestTimestamp, minDate_symbolHistory.iloc[0]['MIN(date)']))

        ## add missing columns 
        minDate_symbolHistory['numMissingBusinessDays'] = numMissingDays
        minDate_symbolHistory['name'] = tablename
        
        ## rename columns to match db table columns 
        minDate_symbolHistory.rename(columns={'MIN(date)':'firstRecordDate'}, inplace=True)
        minDate_symbolHistory = minDate_symbolHistory.iloc[:,[4,1,2,0,3]]
      
        ## save record to db
        minDate_symbolHistory.to_sql(f"{lookupTablename}", conn, index=False, if_exists='append')
    
    ## otherwise update the existing record
    elif minDate_symbolHistory['MIN(date)'][0] < minDate_recordsTable['firstRecordDate'][0]:
        ## update lookuptable with the symbolhistory min date
        sql_update = 'UPDATE \'%s\' SET firstRecordDate = \'%s\' WHERE symbol = \'%s\' and interval = \'%s\''%(lookupTablename, minDate_symbolHistory['MIN(date)'][0], minDate_symbolHistory['symbol'][0], minDate_symbolHistory['interval'][0]) 
        cursor = conn.cursor()
        cursor.execute(sql_update)

def _formatpxHistory(pxHistory, type=None):
    """ 
        Handles formatting of px history tables retrieved from db
        type: {ohlc i.e. None, termstructure}
    """
    pxHistory.reset_index(drop=True, inplace=True) # reset index

    # if interval is 1day, make sure sure the date column only has 10chars 
    if pxHistory['interval'][1] == '1day':
        pxHistory['date'] = pxHistory['date'].str[:10]
    
    ##### Remove any errant timezone info:
    # get the rows that have timezone info in the date column
    # remove the timezone info from the date column
    # update pxhistory with the formatted date column
    pxHistory_hasTimezone = pxHistory[pxHistory['date'].str.len() > 19].copy()
    if not pxHistory_hasTimezone.empty:
        # remove the timezone info from the date column, while not triggering a settingwithcopy warning
        pxHistory_hasTimezone.loc[:, 'date'] = pxHistory_hasTimezone['date'].str[:19]
        # update pxhistory with the formatted date column
        pxHistory.update(pxHistory_hasTimezone)

    # final formatting ... 
    if pxHistory['interval'][1] == '1day':
        pxHistory['date'] = pd.to_datetime(pxHistory['date'], format='%Y-%m-%d')
    else:
        pxHistory['date'] = pd.to_datetime(pxHistory['date'], format='%Y-%m-%d %H:%M:%S')
    
    pxHistory.sort_values(by='date', inplace=True) #sort by date
    
    return pxHistory

def getRecords(conn):
    """
    Optimized version of getRecords that reduces database calls and improves performance
    """
    try:
        # Get all table names in a single query, excluding specific patterns
        query = '''
            SELECT name 
            FROM sqlite_master 
            WHERE type='table' 
                AND NOT name LIKE '00_%'
                AND NOT name LIKE '%_corrupt%'
        '''
        table_names = pd.read_sql(query, conn)
        
        if table_names.empty:
            return pd.DataFrame()

        # Split table names more efficiently using vectorized operations
        records = pd.DataFrame(
            table_names['name'].str.split('_', expand=True).values,
            columns=['symbol', 'type/expiry', 'interval']
        )
        records['name'] = table_names['name']

        # Convert list to tuple for caching and fetch metadata
        metadata = _batch_fetch_metadata(conn, tuple(table_names['name'].tolist()))
        
        # Update records with metadata
        records = pd.concat([records, metadata], axis=1)
        
        # Apply interval formatting
        records['interval'] = records['interval'].str.replace('(\\d+)([a-zA-Z])', r'\1 \2')
        
        return records
        
    except Exception as e:
        print(f'Error fetching records: {e}')
        return pd.DataFrame()

@lru_cache(maxsize=128)
def _batch_fetch_metadata(conn, table_names: list) -> pd.DataFrame:
    """
    Fetch metadata for all tables in a single operation
    Uses LRU cache to avoid repeated queries for the same tables
    """
    metadata_df = pd.DataFrame(index=table_names)
    
    # Create a single query for all tables
    union_queries = []
    for table in table_names:
        union_queries.append(f"""
            SELECT 
                '{table}' as table_name,
                MAX(date) as last_update,
                MIN(date) as first_record
            FROM "{table}"
        """)
    
    query = " UNION ALL ".join(union_queries)
    
    try:
        results = pd.read_sql(query, conn)
        
        metadata_df = pd.DataFrame({
            'table_name': results['table_name'],
            'lastUpdateDate': pd.to_datetime(results['last_update']),
            'firstRecordDate': pd.to_datetime(results['first_record'])
            # 'daysSinceLastUpdate': (current_date - results['last_update'].datetime.date).dt.days
        })
        metadata_df['daysSinceLastUpdate'] = results['last_update'].apply(
            lambda x: len(pd.bdate_range(x, datetime.datetime.now())) - 1
        )

        return metadata_df
        
    except Exception as e:
        print(f'Error fetching metadata: {e}')
        return pd.DataFrame()
    
def saveHistoryToDB(history, conn, earliestTimestamp='', type=''):
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
    if 'interval' in history.columns:
        history['interval'] = history['interval'].apply(lambda x: x.replace(' ', ''))

    ## set type to index if the symbol is in the index list 
    if 'lastTradeDate' in history.columns:
        type = 'future'
        tableName = history['symbol'][0]+'_'+history['lastTradeDate'][0]+'_'+history['interval'][0]
    else:
        if history['symbol'][0] in index_list:
            type = 'index'
        else: 
            type='stock'
        tableName = history['symbol'][0]+'_'+type+'_'+history['interval'][0]
    print('%s: Saving %s to db, Range: %s - %s...'%(datetime.datetime.now().strftime("%H:%M:%S"), tableName, history['date'].min(), history['date'].max()))
    history.to_sql(f"{tableName}", conn, index=False, if_exists='append')
    _removeDuplicates(tableName, conn)
    _updateLookup_symbolRecords(conn, tableName, earliestTimestamp=earliestTimestamp)

def save_table_to_db(conn, tablename, metadata_df, if_exists='append'):
    """
    Save metadata to the db
    """
    metadata_df.to_sql(tablename, conn, index=False, if_exists=if_exists)

def getPriceHistory(conn, symbol, interval, withpctChange=True, lastTradeMonth=''):
    """
    Returns dataframe of px from database 

    Params
    ===========
    symbol - [str]
    interval - [str] 
    lookback - [str] optional 

    """
    if lastTradeMonth:
        tableName = symbol+'_'+lastTradeMonth+'_'+interval
    else:
        tableName = _constructTableName(symbol, interval)
    sqlStatement = 'SELECT * FROM '+tableName
    pxHistory = pd.read_sql(sqlStatement, conn)
    
    if withpctChange:
        pxHistory['pctChange'] = pxHistory['close'].pct_change()

    pxHistory = _formatpxHistory(pxHistory)

    # caclulate log returns
    pxHistory = ut.calcLogReturns(pxHistory, 'close')
    
    return pxHistory

def getPriceHistoryWithTablename(conn, tablename):
    sqlStatement = 'SELECT * FROM '+tablename
    pxHistory = pd.read_sql(sqlStatement, conn)
    pxHistory = _formatpxHistory(pxHistory)
    pxHistory = ut.calcLogReturns(pxHistory, 'close')
    return pxHistory

def getTable(conn, tablename, is_pxhistory=False):
    sqlStatement = 'SELECT * FROM \'%s\''%(tablename)
    table_data = pd.read_sql(sqlStatement, conn)

    # handle termstrcuture case by adding interval and symbol to the df. first splot tablename by _ 
    if is_pxhistory:
        tableNameSplit = tablename.split('_')
        table_data['symbol'] = tableNameSplit[0]
        table_data['interval'] = tableNameSplit[1]
        table_data = _formatpxHistory(table_data)

    return table_data

def getLookup_symbolRecords(conn):
    """ 
    Returns the lookup table fo records history as df 
    """
    sqlStatement_selectRecordsTable = 'SELECT * FROM \'00-lookup_symbolRecords\''
    symbolRecords = pd.read_sql(sqlStatement_selectRecordsTable, conn)
    # convert firstRecordDate column to datetime
    symbolRecords['firstRecordDate'] = pd.to_datetime(symbolRecords['firstRecordDate'])
    return symbolRecords

def listSymbols(conn):
    """
    lists the unique symbols in the lookup table
    """
    sqlStatement_selectRecordsTable = 'SELECT DISTINCT symbol FROM \'00-lookup_symbolRecords\' ORDER BY symbol ASC'
    symbols = pd.read_sql(sqlStatement_selectRecordsTable, conn)
    return symbols

def futures_getCellValue(conn, symbol, interval='1day', lastTradeMonth='202308', targetColumn='close', targetDate='2023-07-21'):
    """
        Returns value of a specified cell for the target futures contract
        inputs:
            symbol: str
            interval: str
            expiryMonth: str as YYYYMM
            targetColumn: str, column we want from the db table 
            targetDate: str as YYYY-MM-DD, date of the column we want
        outputs:
            value of the target cell
    """
    # construct tablename
    tableName = symbol+'_'+str(lastTradeMonth)+'_'+interval
    targetDate = '2023-07-21 00:00:00'
    # run sql query to get cell value 
    sqlStatement = 'SELECT '+targetColumn+' FROM '+tableName+' WHERE date = \''+targetDate+'\''

    value = pd.read_sql(sqlStatement, conn)
    # return val 
    return value[targetColumn][0]

def getLookup_exchange(conn, symbol):
    exchangeLookupTable = '00-lookup_exchangeMapping'
    sql = 'SELECT exchange FROM \'%s\' WHERE symbol=\'%s\'' %(exchangeLookupTable, symbol)
    exchange = pd.read_sql(sql, conn).values[0][0]

    return exchange