from symtable import Symbol
from ib_insync import *
from rich import print

import pandas as pd

import ib_insync.wrapper
import datetime
import sqlite3
import sys
import time
import config

#global list of index symbols
_index = config._index

# load currency lookup table from config 
currency_mapping = config.currency_mapping

# load exchange lookup table from config
exchange_mapping = config.exchange_mapping

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
        print(' %s: [yellow]Connecting with IBKR...[/yellow]'%(datetime.datetime.now().strftime('%H:%M:%S')))
        ibkr = IB() 
        ibkr.connect('127.0.0.1', 7496, clientId = 10)
        print('[green]  Success![/green]\n')
    except Exception as e:
        print('[red]  Could not connect with IBKR![/red]\n')
        print(e)
        exit()
    
    return ibkr

""" 
    Formats the contract history returned from ibkr 
"""
def _formatContractHistory(contractHistory_df):
    contractHistory_df.drop(['average', 'barCount'], inplace=True, axis=1)
    # convert date column to datetime
    contractHistory_df['date'] = pd.to_datetime(contractHistory_df['date'])
    # trim the timezone info from the datetime
    contractHistory_df['date'] = contractHistory_df['date'].dt.tz_localize(None)
    return contractHistory_df

"""
Returns [DataFrame] of historical data from IBKR with...
    inputs:
        ibkr connection object, ..,.., end date of lookup, nbr of days to look back, ..,..
    outputs:
        [columns]: date | open | high | low | close | volume | symbol | interval 
"""
def getBars(ibkr, symbol='SPY', currency='USD', endDate='', lookback='10 D', interval='15 mins', whatToShow='TRADES'):
    # check if symbol is in currency mapping
    if symbol in currency_mapping:
        currency = currency_mapping[symbol]
    bars = _getHistoricalBars(ibkr, symbol, currency, endDate, lookback, interval, whatToShow)
    
    return bars

"""
Returns [DataFrame] of historical data for stocks and indexes from IBKR

"""
def _getHistoricalBars(ibkrObj, symbol, currency, endDate, lookback, interval, whatToShow):
    
    # set exchange
    if symbol in exchange_mapping:
        exchange = exchange_mapping[symbol]
    else:
        exchange = 'SMART'

    # define contract 
    if symbol in _index:
        contract = Index(symbol, exchange, currency)
    else:
        contract = Stock(symbol, exchange, currency) 
    
    # make sure endDate is tzaware
    if endDate:
        endDate = endDate.tz_localize('US/Eastern')
    
    # request history from ibkr 
    contractHistory = ibkrObj.reqHistoricalData(
            contract, 
            endDateTime = endDate,
            durationStr=lookback,
            barSizeSetting=interval,
            whatToShow=whatToShow,
            useRTH=False,
            formatDate=1)

    # convert retrieved data to dataframe    
    contractHistory_df = pd.DataFrame()
    if contractHistory: 
        # convert to dataframe & format for usage
        contractHistory_df = _formatContractHistory(util.df(contractHistory))
    
    else: 
        print('\nNo history found for...%s!'%(symbol))

    return contractHistory_df

"""
Returns dataframe of historical data for futures
    by default, returns data for NG futures
"""
def getBars_futures(ibkr, symbol, lastTradeDate, exchange, lookback, interval, endDate='', currency='USD', whatToShow='TRADES'):
    bars = _getHistoricalBars_futures(ibkr, symbol, exchange, lastTradeDate, currency, endDate, lookback, interval, whatToShow)
    
    return bars

"""
    Returns [DataFrame] of historical data for futures from IBKR
"""
def _getHistoricalBars_futures(ibkrObj, symbol, exchange, lastTradeDate, currency, endDate, lookback, interval, whatToShow):
    ## Future contract type definition: https://ib-insync.readthedocs.io/api.html#ib_insync.contract.Future
    ## contract month, or day format: YYYYMM or YYYYMMDD
    contract = Future(symbol=symbol, lastTradeDateOrContractMonth=lastTradeDate, exchange=exchange, currency=currency, includeExpired=True)
    # make sure endDate is tzaware
    if endDate:
        # convert to pd series
        endDate = pd.to_datetime(endDate)
        endDate = endDate.tz_localize('US/Eastern')
    print(' %s: [yellow]Calling ibkr for futures data[/yellow]'%(datetime.datetime.now().strftime('%H:%M:%S')))
    try:
        # grab history from IBKR 
        contractHistory = ibkrObj.reqHistoricalData(
            contract, 
            endDateTime = endDate,
            durationStr=lookback,
            barSizeSetting=interval,
            whatToShow=whatToShow,
            useRTH=False,
            formatDate=1)
        
    except Exception as e:
        print(e)
        print('\nCould not retrieve history for...%s!'%(symbol))
        return pd.DataFrame()
    
    contractHistory_df = pd.DataFrame()
    if contractHistory: 
        # convert to dataframe & format for usage
        contractHistory_df = _formatContractHistory(util.df(contractHistory))
    
    else:
        print('[red]No history found for...%s![/red]'%(symbol))
        return None

    return contractHistory_df

"""
    Returns [DataFrame] of historical data for futures from IBKR
    inputs:
        needs ibkr object and contract object
    returns dataframe of historical data
"""
def _getHistoricalBars_futures_withContract(ibkrObj, contract, endDate, lookback, interval, whatToShow):
    ## Future contract type definition: https://ib-insync.readthedocs.io/api.html#ib_insync.contract.Future
    
    # make sure endDate is tzaware
    if endDate:
        # convert to pd series
        endDate = pd.to_datetime(endDate)
        endDate = endDate.tz_localize('US/Eastern')
    try:
        # grab history from IBKR 
        contractHistory = ibkrObj.reqHistoricalData(
            contract, 
            endDateTime = endDate,
            durationStr=lookback,
            barSizeSetting=interval,
            whatToShow=whatToShow,
            useRTH=False,
            formatDate=1)
    except Exception as e:
        print(e)
        print('\nError42: Could not retrieve historys!')
        return pd.DataFrame()
    
    contractHistory_df = pd.DataFrame()
    if contractHistory: 
        # convert to dataframe & format for usage
        contractHistory_df = _formatContractHistory(util.df(contractHistory))
    
    else:
        print('[red]No history found for %s...%s!\n[/red]'%(interval, contract))
        return None

    return contractHistory_df

"""
Returns [datetime] of earliest datapoint available for index and stock 
"""
def getEarliestTimeStamp_m(ibkr, symbol='SPY', currency='USD', lastTradeDate='', exchange='SMART'):
    # set currency 
    if symbol in currency_mapping:
        currency = currency_mapping[symbol]
    
    # set exchange
    if symbol in exchange_mapping:
        exchange = exchange_mapping[symbol]
    
    # set the contract to look for
    if symbol in _index:
        contract = Index(symbol, exchange, currency)
    elif lastTradeDate:
        contract = Future(symbol=symbol, lastTradeDateOrContractMonth=lastTradeDate, exchange=exchange, currency=currency)
    else:
        contract = Stock(symbol, exchange, currency)
    earliestTS = ibkr.reqHeadTimeStamp(contract, useRTH=False, whatToShow='TRADES')
    return pd.to_datetime(earliestTS)

"""
Returns [datetime] of earliest datapoint available for index and stock, requires Contract object as input
"""
def getEarliestTimeStamp(ibkr, contract):
    # check if symbol is in currency mapping
    if contract.symbol in currency_mapping:
        contract.currency = currency_mapping[contract.symbol]

    earliestTS = ibkr.reqHeadTimeStamp(contract, useRTH=False, whatToShow='TRADES')
    timestamp = pd.to_datetime(earliestTS)
    # make sure timestamp is tzaware 
    timestamp = timestamp.tz_localize(None)

    # return earliest timestamp in datetime format
    return timestamp 

"""
Returns just the contract portion of contract details for a given symbol and type 
"""
def getContract(ibkr, symbol, type='stock', currency='USD'):
    conDetails = getContractDetails(ibkr, symbol, type, currency)
    if len(conDetails) == 0: # contract not found 
        return Contract()
    else:
        return conDetails[0].contract

"""
    Returns contract details for a given symbol, call must be type aware (stock, future, index)
    [inputs]
        ibkr connection object
        symbol
        [optional]
        type = 'stock' | 'future' | 'index'
        currency = 'USD' | 'CAD'
"""
def getContractDetails(ibkr, symbol, type = 'stock', currency='USD', delay=False):
    if delay:
        #print(' %s: [yellow]delaying request for 1 seconds...[/yellow]'%(datetime.datetime.now().strftime('%H:%M:%S')))
        time.sleep(1)
    # set currency 
    if symbol in currency_mapping:
        currency = currency_mapping[symbol]
    
    # change type to index if in index list
    if type != 'future': 
        if symbol in _index:
            type = 'index'
    # grab contract details from IBKR 
    try:
        if type == 'future':
            contracts = ibkr.reqContractDetails(Future(symbol))
        elif type == 'index':
            contracts = ibkr.reqContractDetails(Index(symbol, currency=currency))
        else: 
            contracts = ibkr.reqContractDetails(Stock(symbol, currency=currency))
    except Exception as e:
        print(e)
        print('\nCould not retrieve contract details for...%s!'%(symbol))
        return pd.DataFrame() 
            
    return contracts