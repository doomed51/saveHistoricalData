"""
    This module maintains term structure data for various futures contracts.
    Data is sourced from the local database with no connections required to the broker api.  
"""
import config 

import pandas as pd
import interface_localDb as db 
from rich import print
from sys import argv

dbanme_termstructure = config.dbname_termstructure
dbpath_futures = config.dbname_futures
trackedIntervals = config.intervals

"""
    Lambda function - Returns average volume for given tablename 
        - Used to determine which contract is most liquid for a given month, 
           and therefore worth tracking data for 
"""
def _averageContractVolume(conn, tablename):
    # get data from tablename 
    sql = "SELECT * FROM %s"%(tablename)
    df = pd.read_sql(sql, conn)
    
    # calculate average volume 
    averageVolume = df['volume'].mean()
    
    return averageVolume

"""
    Gets next n contracts in the db for a given symbol
"""
def _getNextContracts(conn, symbol, numContracts, interval='1day'):
        # get lookup table that is an index of all the tables in our db
        lookupTable = db.getLookup_symbolRecords(conn)

        # explicitly convert lastTradeDate to datetime
        lookupTable['lastTradeDate'] = pd.to_datetime(lookupTable['lastTradeDate'])
    
        # Slect contracts in our db expiring after today  
        lookupTable = lookupTable.loc[(
            lookupTable['symbol'] == symbol) & 
            (lookupTable['interval'] == interval) & 
            (lookupTable['lastTradeDate'] > pd.Timestamp.today())].sort_values(by='lastTradeDate').reset_index(drop=True)
        
        # select highest volume contracts for a given month since we care about the most 
        # liquid contracts which are generally the standard expiries 
        lookupTable['averageVolume'] = lookupTable.apply(lambda x: _averageContractVolume(conn, x['name']), axis=1)
        lookupTable['lastTradeDate_month'] = lookupTable['lastTradeDate'].dt.month
        lookupTable = lookupTable.groupby(['lastTradeDate_month']).apply(lambda x: x.sort_values(by='averageVolume', ascending=False).head(1)).reset_index(drop=True)

        # sort by lastTradeDate and select just the next n contracts
        lookupTable = lookupTable.sort_values(by='lastTradeDate').reset_index(drop=True)
        lookupTable = lookupTable.head(numContracts)

        return lookupTable

"""
    Gets term structure data for a given symbol
    @param symbol: symbol to get term structure for 
    @param interval: interval to get data for
    @param lookahead_months: number of months to look ahead
    @return: dataframe of term structure data: [date, close1, close2, ...]
"""
def getTermStructure(symbol:str, interval='1day', lookahead_months=8): 
    # convert to uppercase to follow db naming conventions 
    symbol = symbol.upper()
    # read in pxhistory for next n contracts 
    with db.sqlite_connection(dbpath_futures) as conn_futures:
        lookupTable = _getNextContracts(conn_futures, symbol, lookahead_months, interval)
        # create list of pxHistory dataframes
        termStructure_raw = []
        # get price history for each relevant contract 
        for index, row in lookupTable.iterrows():
            pxHistory = db.getTable(conn_futures, row['name'])
            # set index to date column
            pxHistory.set_index('date', inplace=True)
            # rename close column
            pxHistory.rename(columns={'close': 'month%s'%(index+1)}, inplace=True)
            termStructure_raw.append(pxHistory['month%s'%(index+1)])
    
    termStructure = pd.concat(termStructure_raw, axis=1).sort_values(by='date').reset_index()
    # drop records with NaN values
    termStructure.dropna(inplace=True)

    # sort termstructure by date 
    termStructure.sort_values(by='date', inplace=True)

    # add descriptive columns for later reference 
    termStructure['symbol'] = symbol
    termStructure['interval'] = interval

    return termStructure.reset_index(drop=True)

"""
    Save term structure data to local db
     - Handles duplicate records in input df <termStructure>
"""
def saveTermStructure(termStructure):
    # set the tablename for insertion 
    dbpath_termstructure = config.dbname_termstructure
    tablename = '%s_%s'%(termStructure['symbol'][0], termStructure['interval'][0])

    # filter out existing records if termstructure data already exists 
    with db.sqlite_connection(dbpath_termstructure) as conn:
        # get tablenames in db 
        sql_tableNames = "SELECT name FROM sqlite_master WHERE type='table'"
        tableNames = pd.read_sql(sql_tableNames, conn)['name'].tolist()
        # filter out duplicates  
        if tablename in tableNames:
            ts_db = db.getTable(conn, tablename)
            termStructure = termStructure.loc[~termStructure['date'].isin(ts_db['date'])].copy()
    
    # handle case where we don't have any new term structure data to update
    if termStructure.empty:
        print('%s: [green]Termstructure data up to date for %s [/green]'%(pd.Timestamp.today(), tablename))
        return
    
    # format termstructure dataframe for insertion 
    termStructure.drop(columns=['symbol', 'interval'], inplace=True)

    # save termstructure to db 
    with db.sqlite_connection(dbpath_termstructure) as conn:
         termStructure.to_sql(tablename, conn, if_exists='append', index=False)
         db._removeDuplicates(conn, tablename)
    print('%s: [green]Updated term structure data for %s[/green]'%(pd.Timestamp.today(), tablename))

""" 
    Updates term structure data for all symbols being tracked in the db 
"""
def updateAllTermstructureData():
    # get list of all symbols being tracked 
    with db.sqlite_connection(dbpath_futures) as conn_futures:
        lookupTable = db.getLookup_symbolRecords(conn_futures)
        symbols = lookupTable['symbol'].unique()

    # Update term structure data for each symbol and tracked interval
    for symbol in symbols:
        for interval in trackedIntervals:
            x = getTermStructure(symbol, interval=interval.replace(' ', ''))
            saveTermStructure(x)

""" 
    Returns nicely formatted vix termstrucuture data from csv 
    - assumes interval=1day 
"""
def getVixTermstructureFromCSV(path='vix.csv'): 
    # read in vix.csv termstructure data 
    vix_ts_raw = pd.read_csv(path)
    vix_ts_raw['date'] = pd.to_datetime(vix_ts_raw['date'], format='mixed', dayfirst=True)
    
    # drop the last column month8
    vix_ts_raw.drop(columns=['8'], inplace=True)
    # rename columns to month1, month2, etc.
    vix_ts_raw.rename(columns={'0': 'month1', '1': 'month2', '2': 'month3', '3': 'month4', '4': 'month5', '5': 'month6', '6': 'month7', '7':'month8'}, inplace=True)
    vix_ts_raw['symbol'] = 'VIX'
    vix_ts_raw['interval'] = '1day'
    
    return vix_ts_raw

if __name__ == '__main__':
    # if console arg = csvupdate then update vix termstructure data from csv
    if len(argv) > 1 and argv[1] == 'csvupdate':
        #print("THE PLART HAS STARTED")
        vix_ts_raw = getVixTermstructureFromCSV()
        saveTermStructure(vix_ts_raw)
    
    else:
        updateAllTermstructureData()

