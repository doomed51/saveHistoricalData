from symtable import Symbol
from ib_insync import * 
from rich import print

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

"""
Setup connection to ibkr
###
--
Returns ibkr connection object 
"""
def setupConnection():
    ## connect with IBKR
    try:
        print('[yellow] Connecting with IBKR...[/yellow]')
        ibkr = IB() 
        ibkr.connect('127.0.0.1', 7496, clientId = 10)
        print('[green]  Success![/green]')
    except:
        print('[red]  Could not connect with IBKR![/red]\n')
        exit()

    return ibkr

""" formats the contract history returned from ibkr 
"""
def _formatContractHistory(contractHistory_df):
    contractHistory_df.drop(['average', 'barCount'], inplace=True, axis=1)
    # convert date column to datetime
    contractHistory_df['date'] = pd.to_datetime(contractHistory_df['date'])
    # trim the timezone info from the datetime
    contractHistory_df['date'] = contractHistory_df['date'].dt.tz_localize(None)
    return contractHistory_df

def _getHistoricalBars(ibkrObj, symbol, currency, endDate, lookback, interval, whatToShow):
    if symbol in _index:
        # set the contract to look for
        contract = Index(symbol, 'CBOE', currency)
    else:
        contract = Stock(symbol, 'SMART', currency) 
    
    # make sure endDate is tzaware
    if endDate:
        endDate = endDate.tz_localize('US/Eastern')
        
    # grab history from IBKR 
    contractHistory = ibkrObj.reqHistoricalData(
        contract, 
        endDateTime = endDate,
        durationStr=lookback,
        barSizeSetting=interval,
        whatToShow=whatToShow,
        useRTH=False,
        formatDate=1)
    
    if contractHistory: 
        # convert to dataframe & format for usage
        contractHistory_df = _formatContractHistory(util.df(contractHistory))
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
def getBars(ibkr, symbol='SPY', currency='USD', endDate='', lookback='10 D', interval='15 mins', whatToShow='TRADES'):
    # if an end date is specified, explicit convert to iso format 8601
    #if endDate:
        # convert enddate using tz_localize
        #endDate = endDate.tz_localize('US/Eastern')

    bars = _getHistoricalBars(ibkr, symbol, currency, endDate, lookback, interval, whatToShow)
    
    return bars

"""
Returns [datetime] of earliest datapoint available 
"""
def getEarliestTimeStamp(ibkr, symbol='SPY', currency='USD'):
    if symbol in _index:
        contract = Index(symbol, 'CBOE', currency)
    else:
        contract = Stock(symbol, 'SMART', currency)
    earliestTS = ibkr.reqHeadTimeStamp(contract, useRTH=False, whatToShow='TRADES')

    # return earliest timestamp in datetime format
    return pd.to_datetime(earliestTS)