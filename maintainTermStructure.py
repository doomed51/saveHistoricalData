"""
    This module maintains term structure data for various futures contracts.
    Data is sourced from the local database with no connections required to the broker api.  
"""
import config 

import pandas as pd
import interface_localDb as db 

##################
########LAMBDA FUNCTIONS ***
##################

"""
    returns average volume for given tablename 
"""
def _averageContractVolume(conn, tablename):
    # get data from tablename 
    sql = "SELECT * FROM %s"%(tablename)
    df = pd.read_sql(sql, conn)
    #df = db.getSymbolRecords(conn, tablename)
    
    # calculate average volume 
    averageVolume = df['volume'].mean()
    
    return averageVolume

##################
### LAMBDA FUNCTIONS END ***
##################

"""
    Gets term structure data for a given symbol
    @param symbol: symbol to get term structure for 
    @param interval: interval to get data for
    @param lookahead_months: number of months to look ahead
    @return: dataframe of term structure data: [date, close1, close2, ...]
"""
def getTermStructure(symbol, interval='1day', lookahead_months=8): 
    # convert to uppercase to follow db naming conventions 
    symbol = symbol.upper()

    # read current ts data 
    dbPath_termstructure = config.dbname_termstructure
    dbpath_futures = config.dbname_future

    # get lookuptable from futures db 
    with db.sqlite_connection(dbpath_futures) as conn_futures:
        lookupTable = db.getLookup_symbolRecords(conn_futures)

        # explicitly convert lastTradeDate to datetime
        lookupTable['lastTradeDate'] = pd.to_datetime(lookupTable['lastTradeDate'])
    
        # Slect contracts expiring after today  
        lookupTable = lookupTable.loc[(
            lookupTable['symbol'] == symbol) & 
            (lookupTable['interval'] == interval) & 
            (lookupTable['lastTradeDate'] > pd.Timestamp.today())].sort_values(by='lastTradeDate').reset_index(drop=True)
        
        # select highest volume contracts for a given month 
        lookupTable['averageVolume'] = lookupTable.apply(lambda x: _averageContractVolume(conn_futures, x['name']), axis=1)
        lookupTable['lastTradeDate_month'] = lookupTable['lastTradeDate'].dt.month
        lookupTable = lookupTable.groupby(['lastTradeDate_month']).apply(lambda x: x.sort_values(by='averageVolume', ascending=False).head(1)).reset_index(drop=True)

        # sort by lastTradeDate
        lookupTable = lookupTable.sort_values(by='lastTradeDate').reset_index(drop=True)

        # select only the next 8 contracts
        lookupTable = lookupTable.head(lookahead_months)

        # create list of dataframes 
        month = []

        # get price history for each relevant contract 
        for index, row in lookupTable.iterrows():
            pxHistory = db.getTable(conn_futures, row['name'])
            # set index to date column
            pxHistory.set_index('date', inplace=True)
            # rename close column
            pxHistory.rename(columns={'close': 'close%s(%s)'%(index+1, pxHistory['lastTradeDate'][0])}, inplace=True)
            month.append(pxHistory['close%s(%s)'%(index+1, pxHistory['lastTradeDate'][0])])
    
    termStructure = pd.concat(month, axis=1).sort_values(by='date').reset_index()
    # drop records with NaN values
    termStructure.dropna(inplace=True)

    print( termStructure.reset_index(drop=True) )

    # print description of each dataframe in month list
    #for df in month:
    #    print(df.describe())
    

    # if latest record is more than 24 hours old, add new data
        #   get a list of available expiries in the db 

getTermStructure('ng')