from symtable import Symbol
from ib_insync import * 

import pandas as pd
import sqlite3

#global list of index symbols
_index = ['VIX', 'VIX3M', 'VVIX']

##
# IBKR API reference: https://interactivebrokers.github.io/tws-api/historical_bars.html
"""
    IBKR def'n for [lookback] = [Duration String] = [durationStr]
    
    Valid Duration String units
    Unit	Description
    S	    Seconds
    D	    Day
    W	    Week
    M	    Month
    Y	    Year

    if barsize = 5m -> durationStr max = 100
    if barsize >= 30m -> durationStr max = 365
"""

def _getHistoricalBars(ibkrObj, symbol, currency, endDate, lookback, interval):
    if symbol in _index:
        # set the contract to look for
        contract = Index(symbol, 'CBOE', currency)
    else:
        contract = Stock(symbol, 'SMART', currency) 
    
    # grab history from IBKR 
    contractHistory = ibkrObj.reqHistoricalData(
        contract, 
        endDateTime = endDate,
        durationStr=lookback,
        barSizeSetting=interval,
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1)

    if contractHistory: 
        # converting to dataframe for ease of use 
        contractHistory_df = util.df(contractHistory)
        contractHistory_df.drop(['average', 'barCount'], inplace=True, axis=1)
        contractHistory_df['symbol'] = symbol
        #contractHistory_df['interval'] = interval.replace(' ','')
        return contractHistory_df
    
    else: 
        print('\nNo history found for...%s!'%(symbol))

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
def _saveHistoryToDB(history, dbPath='historicalData_index.db', type='index'):
    conn = sqlite3.connect(dbPath)

    ## Tablename convention: <symbol>_<stock/opt>_<interval>
    tableName = history['symbol'][0]+'_'+type+'_'+history['interval'][0]
    
    if type == 'index':
        history.to_sql(f"{tableName}", conn, index=False, if_exists='append')
    
    elif type == 'option':
        print(' saving options to the DB is not yet implemented')

"""
Returns [DataFrame] of historical data with...
    [columns]: date | open | high | low | close | volume | symbol | interval 
"""
def getBars(ibkr, symbol='SPX', currency='USD', endDate='', lookback='10 D', interval='15 mins'):
    bars = _getHistoricalBars(ibkr, symbol, currency, endDate, lookback, interval)
    
    return bars
