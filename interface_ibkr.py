from symtable import Symbol
from ib_insync import *
from rich import print

import pandas as pd

import ib_insync.wrapper
import datetime
import sqlite3
import sys
import time
import threading
from collections import deque
import config
import re 

#global list of index symbols
_index = config._index

# load currency lookup table from config 
currency_mapping = config.currency_mapping

# load exchange lookup table from config
exchange_mapping = config.exchange_mapping_stocks

# Guard request cadence to reduce IBKR pacing violations.
_IBKR_REQUEST_LOCK = threading.Lock()
_IBKR_LAST_REQUEST_TIMES = {}
_IBKR_DEFAULT_MIN_INTERVAL_SECONDS = 3
_IBKR_GLOBAL_MIN_INTERVAL_SECONDS = 3
_IBKR_WINDOW_SECONDS = 600
_IBKR_MAX_REQUESTS_PER_WINDOW = 55
_IBKR_REQUEST_TIMESTAMPS = deque()
_IBKR_MIN_INTERVAL_BY_REQUEST = {
    'reqHistoricalData': 3,
    'reqHeadTimeStamp': 3,
    'reqContractDetails': 3
}


def _paceIbkrRequest(ibkr, request_name='general', min_interval_seconds=None):

    """
        Pacing detail: https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/#historical-pacing-limitations
    """
    if min_interval_seconds is None:
        min_interval_seconds = _IBKR_MIN_INTERVAL_BY_REQUEST.get(request_name, _IBKR_DEFAULT_MIN_INTERVAL_SECONDS)

    # with _IBKR_REQUEST_LOCK:
    while True:
        # print(f'{datetime.datetime.now().strftime("%H:%M:%S")}: [yellow]Pacing check: Number of IBKR requests in the last 10 minutes: {len(_IBKR_REQUEST_TIMESTAMPS)}[/yellow]')
        now = time.monotonic()

        # Keep only requests within the last 10 minutes.
        while _IBKR_REQUEST_TIMESTAMPS and (now - _IBKR_REQUEST_TIMESTAMPS[0]) >= _IBKR_WINDOW_SECONDS:
            _IBKR_REQUEST_TIMESTAMPS.popleft()

        last_method_ts = _IBKR_LAST_REQUEST_TIMES.get(request_name)
        last_global_ts = _IBKR_LAST_REQUEST_TIMES.get('__global__')

        sleep_for = 0.0
        if last_method_ts is not None:
            sleep_for = max(sleep_for, min_interval_seconds - (now - last_method_ts))
        if last_global_ts is not None:
            sleep_for = max(sleep_for, _IBKR_GLOBAL_MIN_INTERVAL_SECONDS - (now - last_global_ts))

        # Keep aggregate traffic below 60 requests in any rolling 10-minute window.
        if len(_IBKR_REQUEST_TIMESTAMPS) >= _IBKR_MAX_REQUESTS_PER_WINDOW:
            sleep_for = max(sleep_for, (_IBKR_REQUEST_TIMESTAMPS[0] + _IBKR_WINDOW_SECONDS) - now + 0.001)

        if sleep_for <= 0:
            break

        print(f'{datetime.datetime.now().strftime("%H:%M:%S")}: [yellow]Pacing IBKR request "{request_name}", sleeping for {sleep_for:.2f}[/yellow]')
        # time.sleep(sleep_for)
        ibkr.sleep(sleep_for)

    stamped_now = time.monotonic()
    _IBKR_LAST_REQUEST_TIMES[request_name] = stamped_now
    _IBKR_LAST_REQUEST_TIMES['__global__'] = stamped_now
    _IBKR_REQUEST_TIMESTAMPS.append(stamped_now)

def _exit_if_disconnected(ibkr, context):
    if ibkr is None or (hasattr(ibkr, "isConnected") and not ibkr.isConnected()):
        print(f"{datetime.datetime.now():%H:%M:%S}: [red]IBKR disconnected during {context}. Exiting.[/red]")
        exit() 

def _is_connection_error(exc):
    msg = str(exc).lower()
    return any(x in msg for x in ["not connected", "socket", "connection", "504", "1100", "1101"])

##
# IBKR API reference: https://interactivebrokers.github.io/tws-api/historical_bars.html
## add a space between num and alphabet
def _addspace(myStr): 
    return re.sub("[A-Za-z]+", lambda elm: " "+elm[0],myStr )

def setupConnection():
    """
    Setup connection to ibkr
    ###
    --
    Returns ibkr connection object 
    """
    ## connect with IBKR
    try:
        print('%s: [yellow]Connecting with IBKR...[/yellow]'%(datetime.datetime.now().strftime('%H:%M:%S')))
        ibkr = IB() 
        ibkr.connect('127.0.0.1', 7496, clientId = 10)
        print('%s: [green]  Success![/green]'%(datetime.datetime.now().strftime('%H:%M:%S')))
    except Exception as e:
        print('[red]  Could not connect with IBKR![/red]\n')
        print(e)
        exit()
    
    return ibkr

def refreshConnection(ibkr):
    print('%s: [yellow]Refreshing IBKR connection...[/yellow]'%(datetime.datetime.now().strftime('%H:%M:%S')))
    #if ibkr.isConnected():
    clientid = ibkr.client.clientId
    ibkr.disconnect()
    print('%s: [yellow] Connection terminated...[/yellow]'%(datetime.datetime.now().strftime('%H:%M:%S')))
    time.sleep(3) 
    print('%s: [yellow] Reconnecting...[/yellow]'%(datetime.datetime.now().strftime('%H:%M:%S')))
    ibkr = ibkr.connect('127.0.0.1', 7496, clientId = clientid)
    print('%s:[green]Success![/green]'%(datetime.datetime.now().strftime('%H:%M:%S')))
    return ibkr

def _formatContractHistory(contractHistory_df):
    """ 
        Formats the contract history returned from ibkr 
    """
    contractHistory_df.drop(['average', 'barCount'], inplace=True, axis=1)
    # convert date column to datetime
    contractHistory_df['date'] = pd.to_datetime(contractHistory_df['date'])
    # trim the timezone info from the datetime
    contractHistory_df['date'] = contractHistory_df['date'].dt.tz_localize(None)
    return contractHistory_df

def getBars(ibkr, symbol='SPY', currency='USD', endDate='', lookback='10 D', interval='15 mins', whatToShow='TRADES', **kwargs):
    """
    Returns [DataFrame] of historical data from IBKR with...
        inputs:
            ibkr connection object, ..,.., end date of lookup, nbr of days to look back, ..,..
        outputs:
            [columns]: date | open | high | low | close | volume | symbol | interval 
    """
    keepUpToDate = kwargs.get('keepUpToDate', False)
    # check if symbol is in currency mapping
    if symbol in currency_mapping:
        currency = currency_mapping[symbol]
    bars = _getHistoricalBars(ibkr, symbol, currency, endDate, lookback, interval, whatToShow, keepUpToDate=keepUpToDate)
    
    return bars

def _getHistoricalBars(ibkrObj, symbol, currency, endDate, lookback, interval, whatToShow, **kwargs):
    """
    Returns [DataFrame] of historical data for stocks and indexes from IBKR

    """
    _exit_if_disconnected(ibkrObj, 'requesting historical bars for symbol %s, interval %s'%(symbol, interval))
    keepUpToDate = kwargs.get('keepUpToDate', False)

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

    # make sure interval has a space in it
    if ' ' not in interval:
        interval = _addspace(interval)

    print('%s: Looking up history for %s, start: %s, end: %s, lookback: %s, interval: %s' % (
        datetime.datetime.now().strftime('%H:%M:%S'), symbol, endDate - pd.to_timedelta(lookback), endDate, lookback, interval))
    
    # request history from ibkr 
    _paceIbkrRequest(ibkrObj, 'reqHistoricalData')
    contractHistory = ibkrObj.reqHistoricalData(
            contract, 
            endDateTime = endDate,
            durationStr=lookback,
            barSizeSetting=interval,
            whatToShow=whatToShow,
            useRTH=False,
            formatDate=1, 
            keepUpToDate=keepUpToDate)

    # convert retrieved data to dataframe    
    contractHistory_df = pd.DataFrame()
    if contractHistory: 
        # convert to dataframe & format for usage
        contractHistory_df = _formatContractHistory(util.df(contractHistory))
    
    else: 
        print('%s: No history found for...%s!'%(datetime.datetime.now().strftime("%H:%M:%S"), symbol))

    return contractHistory_df

def getBars_futures(ibkr, contract, lookback, interval, endDate='', whatToShow='TRADES'):
    """
    Returns dataframe of historical data for futures
        by default, returns data for NG futures
    """
    # bars = _getHistoricalBars_futures(ibkr, symbol, exchange, lastTradeDate, currency, endDate, lookback, interval, whatToShow)
    bars = _getHistoricalBars_futures(ibkr, contract, endDate, lookback, interval, whatToShow)
    return bars

def _getHistoricalBars_futures(ibkrObj, contract, endDate, lookback, interval, whatToShow):
    """
        Returns [DataFrame] of historical data for futures from IBKR
    """
    ## Future contract definition: https://ib-insync.readthedocs.io/api.html#ib_insync.contract.Future
    _exit_if_disconnected(ibkrObj, 'requesting historical bars for futures contract %s'%contract)

    print('%s: [yellow]Requesting data for %s:%s-%s-%s, endDate: %s, lookback: %s[/yellow]'%(datetime.datetime.now().strftime('%H:%M:%S'), contract.exchange, contract.symbol, contract.lastTradeDateOrContractMonth, interval, endDate, lookback))

    # make sure endDate is tzaware
    if endDate:
        endDate = pd.to_datetime(endDate)
        endDate = endDate.tz_localize('US/Eastern')
    # check if interval has a space in it 
    if ' ' not in interval:
        interval = _addspace(interval)
    # handle expired contracts 
    if (contract.lastTradeDateOrContractMonth < datetime.datetime.now().strftime('%Y%m%d')) & (pd.to_datetime(endDate) > pd.to_datetime(contract.lastTradeDateOrContractMonth).tz_localize('US/Eastern')):
        print('%s: [yellow]Requesting invalid historical data for expired contract, resetting request end date...[/yellow]'%(datetime.datetime.now().strftime('%H:%M:%S')))
        # lastTradeDate = pd.to_datetime(lastTradeDate)
        lastTradeDate = pd.to_datetime(contract.lastTradeDateOrContractMonth)
        # lastTradeDate = lastTradeDate +  
        endDate = pd.to_datetime(lastTradeDate)
        endDate = endDate.tz_localize('US/Eastern')
    # subscribe to timeout event 
    
    try:
        
        ibkrObj.timeoutEvent += lambda x: print('%s: [red]Timeout event triggered![/red]'%(datetime.datetime.now().strftime('%H:%M:%S')))
        _paceIbkrRequest(ibkrObj, 'reqHistoricalData')
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
        print('\nCould not retrieve history for...%s!'%(contract.symbol))
        return pd.DataFrame()
    
    contractHistory_df = pd.DataFrame()
    if contractHistory: 
        # convert to dataframe & format for usage
        contractHistory_df = _formatContractHistory(util.df(contractHistory))
    
    else:
        print('%s: [red]No history found for...%s![/red]'%(datetime.datetime.now().strftime("%H:%M:%S"), contract.symbol))
        return None

    return contractHistory_df

def _getHistoricalBars_futures_withContract(ibkrObj, contract, endDate, lookback, interval, whatToShow):
    """
        Returns [DataFrame] of historical data for futures from IBKR
        inputs:
            needs ibkr object and contract object
        returns dataframe of historical data
    """
    ## Future contract type definition: https://ib-insync.readthedocs.io/api.html#ib_insync.contract.Future
    
    _exit_if_disconnected(ibkrObj, 'requesting historical bars for futures with contract %s'%contract)

    # make sure endDate is tzaware
    if endDate:
        # convert to pd series
        endDate = pd.to_datetime(endDate)
        endDate = endDate.tz_localize('US/Eastern')
    try:
        # grab history from IBKR 
        _paceIbkrRequest('reqHistoricalData')
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
        print('%s: [red]No history found for %s...%s![/red]'%(datetime.datetime.now().strftime("%H:%M:%S"), interval, contract))
        return None

    return contractHistory_df

def getEarliestTimeStamp_m(ibkr, symbol='SPY', currency='USD', lastTradeDate=None, exchange='SMART'):

    """
    Returns [datetime] of earliest datapoint available for index and stock 
    """
    _exit_if_disconnected(ibkr, 'getting earliest timestamp for symbol %s'%symbol)

    # set currency 
    if symbol in currency_mapping:
        currency = currency_mapping[symbol]
    
    # set exchange
    if symbol in exchange_mapping:
        exchange = exchange_mapping[symbol]
    
    # set the contract to look for
    if symbol in _index and not lastTradeDate:
        contract = Index(symbol, exchange, currency)
    elif lastTradeDate:
        contract = Future(symbol=symbol, lastTradeDateOrContractMonth=lastTradeDate, exchange=exchange, currency=currency)
    else:
        contract = Stock(symbol, exchange, currency)
    _paceIbkrRequest(ibkr, 'reqHeadTimeStamp')
    earliestTS = ibkr.reqHeadTimeStamp(contract, useRTH=False, whatToShow='TRADES')
    return pd.to_datetime(earliestTS)

def getEarliestTimeStamp(ibkr, contract):
    """
    Returns [datetime] of earliest datapoint available for index and stock, requires Contract object as input
    """
    _exit_if_disconnected(ibkr, 'getting earliest timestamp for contract %s'%contract)

    # check if symbol is in currency mapping
    if contract.symbol in currency_mapping:
        contract.currency = currency_mapping[contract.symbol]
    
    _paceIbkrRequest(ibkr, 'reqHeadTimeStamp')
    earliestTS = ibkr.reqHeadTimeStamp(contract, useRTH=False, whatToShow='TRADES')

    if not earliestTS:
        print('%s: [red]Earliest timestamp returned empty for contract...%s![/red]'%(datetime.datetime.now().strftime("%H:%M:%S"), contract))
        return None

    # cancel the request immediately after receiving the timestamp to avoid pacing issues
    # ibkr.cancelHeadTimeStamp(reqId=99)
    # ibkr.client.cancelHeadTimeStamp()
    
    timestamp = pd.to_datetime(earliestTS)
    # make sure timestamp is tzaware 
    timestamp = timestamp.tz_localize(None)

    # return earliest timestamp in datetime format
    return timestamp 

def getContract(ibkr, symbol, type='stock', currency='USD'):
    """
    Returns just the contract portion of contract details for a given symbol and type 
    """
    conDetails = getContractDetails(ibkr, symbol, type, currency)
    if len(conDetails) == 0: # contract not found 
        return Contract()
    else:
        return conDetails[0].contract

def getContractDetails(ibkr, symbol, type = 'stock', currency='USD', exchange=''):
    """
        Returns contract details for a given symbol, call must be type aware (stock, future, index)
        [inputs]
            ibkr connection object
            symbol
            [optional]
            type = 'stock' | 'future' | 'index'
            currency = 'USD' | 'CAD'
    """
    _exit_if_disconnected(ibkr, 'getting contract details for %s %s'%(exchange, symbol))
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
            if not exchange:
                exchange = exchange_mapping.get(symbol, '')
            _paceIbkrRequest(ibkr, 'reqContractDetails')
            contracts = ibkr.reqContractDetails(Future(symbol=symbol, exchange=exchange, currency=currency, includeExpired=True))
        elif type == 'index':
            _paceIbkrRequest(ibkr, 'reqContractDetails')
            contracts = ibkr.reqContractDetails(Index(symbol, currency=currency))
        else: 
            _paceIbkrRequest(ibkr, 'reqContractDetails')
            contracts = ibkr.reqContractDetails(Stock(symbol, currency=currency))
    except Exception as e:
        print('\nCould not retrieve contract details for...%s!'%(symbol))
        return pd.DataFrame() 
            
    return contracts