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

def _getHistoricalBars(ibkrObj, symbol, currency, endDate, lookback, interval, whatToShow):
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
        whatToShow=whatToShow,
        useRTH=False,
        formatDate=1)
    
    ##ibkrObj.reqHistoricalData()

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
Returns [DataFrame] of historical data from IBKR with...
    inputs:
        ibkr connection object, ..,.., end date of lookup, nbr of days to look back, ..,..
    outputs:
        [columns]: date | open | high | low | close | volume | symbol | interval 
"""
def getBars(ibkr, symbol='SPX', currency='USD', endDate='', lookback='10 D', interval='15 mins', whatToShow='TRADES'):
    bars = _getHistoricalBars(ibkr, symbol, currency, endDate, lookback, interval, whatToShow)
    
    return bars

"""
Returns [datetime] of earliest datapoint available 
"""
def getEarliestTimeStamp(ibkr, symbol='SPX', currency='USD', exchange='SMART'):
    if symbol in _index:
        contract = Index(symbol, exchange, currency)
    else:
        contract = Stock(symbol, exchange, currency)
    earliestTS = ibkr.reqHeadTimeStamp(contract, useRTH=False, whatToShow='TRADES')
    return earliestTS