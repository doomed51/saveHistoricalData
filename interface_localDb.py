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

import pandas as pd
sys.path.append('..')
from utils import utils as ut

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
    sql_selectMinId = 'DELETE FROM %s WHERE ROWID NOT IN (SELECT MIN(ROWID) FROM %s GROUP BY date)'%(tablename, tablename)

    ## run the query 
    cursor = conn.cursor()
    cursor.execute(sql_selectMinId)

def remove_duplicates_from_pxhistory_gaps_metadata(conn, tablename):
    """
    Remove duplicates from the pxhistory gaps metadata table
    """
    sqlStatement = 'DELETE FROM \'%s\' WHERE ROWID NOT IN (SELECT ROWID FROM \'%s\' AS sub WHERE (tablename, update_date) IN (SELECT tablename, MAX(update_date) FROM \'%s\' GROUP BY tablename))'%(tablename, tablename, tablename)
    cursor = conn.cursor()
    cursor.execute(sqlStatement)
    

def _updateLookup_symbolRecords(conn, tablename, earliestTimestamp, numMissingDays = 5):

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