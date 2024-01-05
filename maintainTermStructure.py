"""
    This module maintains term structure data for various futures contracts.
    Data is sourced from the local database with no connections required to the broker api.  
"""
import config 

import pandas as pd
import interface_localDb as db 

dbanme_termstructure = config.dbname_termstructure
dbpath_futures = config.dbname_futures

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
Used for when appending data to existing db tables
    Permanently removes duplicate records from table

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
def getTermStructure(symbol:str, interval='1day', lookahead_months=9): 
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

    # add descriptive columns for later reference 
    termStructure['symbol'] = symbol
    termStructure['interval'] = interval

    return termStructure.reset_index(drop=True)

"""
    Save term structure data to local db
"""
def saveTermStructure(termStructure):
    # set the tablename for insertion 
    dbpath_termstructure = config.dbname_termstructure
    tablename = '%s_%s'%(termStructure['symbol'][0], termStructure['interval'][0])

    # Only select dates that we don't already have ts data for
    with db.sqlite_connection(dbpath_termstructure) as conn:
        ts_db = db.getTable(conn, tablename)

    # select termstructure records not in ts_db 
    termStructure = termStructure.loc[~termStructure['date'].isin(ts_db['date'])].copy()
    
    # handle case wherewe don't have any new term structure data to update
    if termStructure.empty:
        print('Termstructure data up to date for %s'%(tablename))
        return
    # format termstructure dataframe for insertion 
    termStructure.drop(columns=['symbol', 'interval'], inplace=True)

    # save termstructure to db 
    with db.sqlite_connection(dbpath_termstructure) as conn:
         termStructure.to_sql(tablename, conn, if_exists='append', index=False)
         _removeDuplicates(conn, tablename)
        

