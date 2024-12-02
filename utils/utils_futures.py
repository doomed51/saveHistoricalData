"""
Util functionsion specific to dealing with futures contracts 
"""

import interface_localDB as db
import pandas as pd

"""
    Function that returns the close price
"""

"""
    Function that returns term structure of futures contracts, 
        looks at the 1 day interval only 
    Inputs:
        conn: database connection object
        symbol: str of the symbol to get term structure for
        lookback: number of records to lookback, default is 100
        numMonths: number of future contract months to look forward, default 8
    Outputs:
        termStructure: pandas dataframe of term structure with columns (date, currentmonth_close, currentmonth+1_close, currentmonth+2_close, ...)
"""
def getRawTermstructure(conn, symbol, lookback=100, numMonths=8):
    # expiryString: current date in format YYYYMM + 1 month 
    expiryString = (pd.to_datetime('today') + pd.DateOffset(months=1)).strftime('%Y%m')

    # get the lookup table
    lookupTable = db.getLookup_symbolRecords(conn)

    # select just the records where lastTradeMonth is between currentdate in format YYYYMM + 1 month, and currentdate + numMonths months 
    lookupTable = lookupTable[(lookupTable['lastTradeMonth'] >= expiryString) & (lookupTable['lastTradeMonth'] <= (pd.to_datetime('today') + pd.DateOffset(months=numMonths)).strftime('%Y%m'))].reset_index(drop=True)

    ts = pd.DataFrame()

    ## iterate through the lookup table getting px history for each contract 
    for index, row in lookupTable.iterrows():
        if ts.empty:
            ts = db.getPriceHistoryWithTablename(conn, row['name'])
            # drop all columns other than date and close, rename the close column to close_lastTradeMonth
            ts = ts[['date', 'close']].rename(columns={'close': 'close_' + row['lastTradeMonth']})
        else:
            record = db.getPriceHistoryWithTablename(conn, row['name'])
            # join record with ts on 'date' and only include the 'close' column renaming it to close_lastTradeMonth 
            record = record[['date', 'close']].rename(columns={'close': 'close_' + row['lastTradeMonth']})
            ts = ts.merge(record, on='date', how='left')
    
    # drop rows in ts where any of the columns has a NaN value
    ts = ts.dropna(axis=0, how='any').reset_index(drop=True)

    # convert date column to datetime
    #ts['date'] = pd.to_datetime(ts['date'])

    # make sure tzinfo is set to US/Eastern
    #ts['date'] = ts['date'].dt.tz_localize('US/Eastern')

    return ts

"""
    Returns term structure contango for the passed in term structure dataframge and target period
    Inputs:
        ts: pandas dataframe of term structure with columns (date, currentmonth+1_close, currentmonth+2_close, currentmonth+3_close, ...)
        startMonth: int of the month to start the contango calculation from, default 1
        endMonth: int of the month to end the contango calculation at, default len(ts.columns)-1
"""
def getContango(ts, startMonth=1, endMonth=None):
    
    # adjust start/end columns 
    if endMonth == None:
        endMonth = len(ts.columns)-1
    else:
        endCol = endMonth + 1  
    startCol = startMonth + 1 

    # add new column named startMonth-endMonthContango to ts
    # calculated as ts.columns[endCol] - ts.columns[startCol]/ts.columns[startCol]
    ts[ts.columns[startCol] + '-' + ts.columns[endCol] + 'Contango'] = (ts[ts.columns[endCol]] - ts[ts.columns[startCol]])/ts[ts.columns[startCol]]


    
    return ts