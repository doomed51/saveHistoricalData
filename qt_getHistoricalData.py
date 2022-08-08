"""
Author: Rachit Shankar 
Date: February, 2022

PURPOSE
----------
Build a local database of historical pricing data for a list of specified equities

VARIABLES
----------
# The LIST of equities to track history for 
tickerFilepath = 'tickerList.csv'

# The [database]] where history is saved
'historicalData_stock.db'
'historicalData_index.db'

# The data sources for historcal data 
Questrade 
IBKR

"""
from ib_insync import *
from matplotlib.pyplot import axis
from numpy import histogram, indices, true_divide

from pytz import timezone, utc

from pathlib import Path
from qtrade import Questrade 
from requests.exceptions import HTTPError
from rich import print
from urllib.error import HTTPError, URLError

import datetime
import sqlite3 
import pandas as pd
import re

import ibkr_getHistoricalData as ib

## Default DB names 
_dbName_stock = 'historicalData_stock.db'
_dbName_index = 'historicalData_index.db'

## default set of intervals to be saved for each ticker 
intervals_stock = ['FiveMinutes', 'FifteenMinutes', 'HalfHour', 'OneHour', 'OneDay', 'OneMonth']
intervals_index = ['5 mins', '15 mins', '30 mins', '1 day']

## global vars
_indexList = ['VIX']
_tickerFilepath = 'tickerList.csv'

"""
Lambda functions for dataframe processing
"""

## adda space between num and alphabet
def _addspace(myStr): 
    return re.sub("[A-Za-z]+", lambda elm: " "+elm[0],myStr )

## count weekdays between two dates
def _countWorkdays(startDate, endDate, excluded=(6,7)):
    days = []
    while startDate.date() < endDate.date():
        if startDate.isoweekday() not in excluded: 
            days.append(startDate)
        startDate += datetime.timedelta(days=1)
    return '%s D'%(len(days)-1)

"""
Setup connection to Questrade API
###
first try the yaml file
 if yaml fails, try a refresh
 if that fails try the token (i.e. a new token will 
   need to be manually updated from the qt API )
--
Returns Questrade object 
"""
def setupConnection():
    try:
        print("\n trying token yaml")
        qtrade = Questrade(token_yaml = "access_token.yml")
        w = qtrade.get_quote(['SPY'])
        #print (' Success! %s latest Price: %.2f \n'%(w['symbol'], w['lastTradePrice'])) 
        
    except:
        try: 
            print("\n Trying Refresh \n")
            qtrade = Questrade(token_yaml = "access_token.yml")
            qtrade.refresh_access_token(from_yaml = True, yaml_path="access_token.yml")
            #w = qtrade.get_quote(['SPY'])
            #print ('%s latest Price: %.2f'%(w['symbol'], w['lastTradePrice'])) 

        except:
            print("\n Trying Access Code \n")
            
            try:
                with open("token.txt") as f:
                    ac = f.read().strip()
                    qtrade = Questrade(access_code = ac)
                    w = qtrade.get_quote(['SPY'])
                    print ('%s latest Price: %.2f'%(w['symbol'], w['lastTradePrice'])) 
            
            except(HTTPError, FileNotFoundError) as err:
                print("\n Might neeed new access code from Questrade \n")
                print(err)
                quit() 
    return qtrade

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
def saveHistoryToDB(history, conn, type='stock'):

    ## Tablename convention: <symbol>_<stock/opt>_<interval>
    tableName = history['symbol'][0]+'_'+type+'_'+history['interval'][0]
    
    if type == 'stock':
        history.to_sql(f"{tableName}", conn, index=False, if_exists='append')
    
    elif type == 'index':
        history.to_sql(f"{tableName}", conn, index=False, if_exists='append')

    elif type == 'option':
        print(' saving options to the DB is not yet implemented')

"""
Save history to a CSV file 
### 

Params 
------------
history: [DataFrame]
    pandas dataframe with columns: date, OHLC, volume, interval, vwap, symbol, and interval
"""
def saveHistoryToCSV(history, type='stock'):
    if not history.empty:
        filepath = Path('output/'+history['symbol'][0]+'_'+history['interval'][0]+'_'+history['end'][0]+'.csv')
        print('Saving %s interval data for %s'%(interval, ticker))
        filepath.parent.mkdir(parents=True, exist_ok=True)
        history.to_csv(filepath, index=False)


"""
Retrieves history from questrade and 
saves it to a local database 
###

Params
-----------
qtrade: [Questrade] - active questrade connection obj
ticker: [str] - symbol / tickers
startDate/endDate: [datetime] - period to look up 
interval: [str]: time granularity i.e. oneDay, oneHour, etc. 
"""
def getLatestHistory(qtrade, ticker, startDate, endDate, interval):
    ## Retrieve historical data  
    history = pd.DataFrame()
    try:
        ## Retrieve data from questrade
        history = pd.DataFrame(qtrade.get_historical_data(ticker, startDate, endDate, interval))

        ## cleanup timestamp formatting
        history['start'] = history['start'].astype(str).str[:-6]
        
        ## add some columns for easier reference later
        history['interval'] = interval  
        history['symbol'] = ticker

    except(HTTPError, FileNotFoundError) as err:
        print ('ERROR History not retrieved\n')
        print(err)
        print('\n')

    #saveHistoryToDB(history)
    return history

"""
 Returns the last recorded date with price history 
###

Params
---------
sqlConn: [sqlite3.connection]
symbol: [str] lookup symbol
interval: [str] oneday, hour, etc
type: [str] stock, or option
---------
Returns [dateTime] or [str]:n/a if table is not found in the DB 

"""
def getLastUpdateDate(sqlConn, symbol, interval, type):
    lastDate = 'n/a'
    sql = 'SELECT * FROM ' + symbol + '_' + type + '_' + interval
    history = pd.DataFrame()

    try:
        history = pd.read_sql(sql, sqlConn)
        history['end'] = pd.to_datetime(history['end'], dayfirst=False)
        lastDate = history['end'].max()
    except :
        next

    return lastDate

def updateStockHistory(stocksToUpdate, stocksToAdd):

    # check for missing intervals 
    missingIntervals = getMissingIntervals(stocksToUpdate)
    
    ## establish connections if updating is needed 
    if (stocksToUpdate['daysSinceLastUpdate'].count() > 0) or (stocksToAdd['ticker'].count() > 0) or (len(missingIntervals) > 0):
        print(' Connecting with [red]Qtrade & DB [/red]')
        qtrade = setupConnection()
        conn = sqlite3.connect(_dbName_stock)
            
    ## update missing intervals
    for item in missingIntervals:
        [_tkr, _intvl] = item.split('-')
        startDate = datetime.datetime.now(tz=None) - datetime.timedelta(30000)
        history = getLatestHistory(qtrade, _tkr, startDate.date(), datetime.datetime.now(tz=None).date(), _intvl)
        saveHistoryToDB(history, conn)
        print('[red]Missing interval[/red] %s-%s...[red]updated![/red]\n'%(_tkr, _intvl))

    ## update existing records that are more than 5 days old 
    if not stocksToUpdate.empty:
        print('\n[blue]Some records are more than 5 days old. Updating...[/blue]')
        pd.to_datetime(stocksToUpdate['lastUpdateDate'])

        for index, row in stocksToUpdate.iterrows():            
            startDate = datetime.datetime.strptime(row['lastUpdateDate'][:10], '%Y-%m-%d') #+ datetime.timedelta(hours=9)
            history = getLatestHistory(qtrade, row['ticker'], startDate.date(), datetime.datetime.now(tz=None).date(), row['interval'])
            saveHistoryToDB(history, conn)
            print('%s-%s...[red]updated![/red]\n'%(row['ticker'], row['interval']))
        #conn.close()
    else: 
        print('\n[green]Existing records are up to date...[/green]')

    # Add new records added to the watchlist 
    if stocksToAdd['ticker'].count() > 0: 
        print('\n[blue]%s new ticker(s) found[/blue], adding to db...'%(stocksToAdd['ticker'].count()))

        # loop through each new ticker
        for ticker in stocksToAdd['ticker']:
            startDate = datetime.datetime.now(tz=None) - datetime.timedelta(30000)
            print('\nAdding [underline]%s[/underline] to the database...'%(ticker))

            # we will update every interval for the ticker             
            for intvl in intervals_stock:
                # get the latest history from questrade 
                history = getLatestHistory(qtrade, ticker, startDate.date(), datetime.datetime.now(tz=None).date(), intvl)
                # save the history to DB 
                saveHistoryToDB(history, conn)
                print(' %s...[green]added![/green] (%s records)'%(intvl, history['end'].count()))
    else:
        print('[green]No new tickers added to watchlist...[/green]')
    
    print('\n[green]All Done![/green]')

"""
update history for indices
"""
def updateIndexHistory(indicesToUpdate= pd.DataFrame(), indicesToAdd  = pd.DataFrame()):
    print('checking if index records need updating')
    
    ## check for missing intervals and update them 
    missingIntervals = getMissingIntervals(indicesToUpdate, type='index')
    
    ## establish connections if updating is needed 
    if (indicesToUpdate['daysSinceLastUpdate'].count() > 0) or (indicesToAdd['ticker'].count() > 0) or (len(missingIntervals) > 0):
        
        ## add lookback column, formatted string for ibkr call 
        indicesToUpdate['lookback'] =  indicesToUpdate.apply(lambda x: _countWorkdays(datetime.datetime.strptime(x['lastUpdateDate'][:10], '%Y-%m-%d'), datetime.datetime.now()), axis=1) 

        try:
            print('[red]Connecting with IBKR[/red]\n')
            ibkr = IB() 
            ibkr.connect('127.0.0.1', 7496, clientId = 10)
            conn = sqlite3.connect(_dbName_index)
        except:
            print('[red]Could not connect with IBKR![/red]\n')
    
    ## update missing intervals
    for item in missingIntervals:
        [_tkr, _intvl] = item.split('-')
        print('Updating [red]missing intervals[/red]...')
        if ( _intvl in ['5 mins', '15 mins']):
            history = ib.getBars(ibkr, symbol=_tkr,lookback='100 D', interval=_intvl )
        
        elif (_intvl in ['30 mins', '1 day']):
            history = ib.getBars(ibkr, symbol=_tkr,lookback='365 D', interval=_intvl )

        saveHistoryToDB(history, conn, 'index')

        print('[red]Missing interval[/red] %s-%s...[red]updated![/red]\n'%(_tkr, _intvl))

    ## update existing records that are more than 5 days old 
    if not indicesToUpdate.empty:
        print('\n[blue]Some records are more than 5 days old. Updating...[/blue]')
        pd.to_datetime(indicesToUpdate['lastUpdateDate'])
        
        print('Updating [red]>5 day old data[/red]...')
        for index, row in indicesToUpdate.iterrows():            
            history = ib.getBars(ibkr, symbol=row['ticker'], lookback=row['lookback'], interval=row['interval']) 
                        
            saveHistoryToDB(history, conn, 'index')
            print('%s-%s...[red]updated![/red]\n'%(row['ticker'], row['interval']))
        #conn.close()
    else: 
        print('\n[green]Existing records are up to date...[/green]')

    if not indicesToAdd.empty:
        print('[red][bold] 404: Adding indicies not implemented[/bold][/red]')
    
def getQuote():
    try:
        qtrade = setupConnection()
    except : 
        print('could not connect to DB/Qtrade!')

    quote = qtrade.get_quote('AAPL')
    print(quote['symbolId'])

def getDaysSinceLastUpdated(row):
    if row['ticker'] in _indexList:
        conn = sqlite3.connect(_dbName_index)
        maxtime = pd.read_sql('SELECT MAX(date) FROM '+ row['name'], conn)
        mytime = datetime.datetime.strptime(maxtime['MAX(date)'][0][:10], '%Y-%m-%d')
    else:
        conn = sqlite3.connect(_dbName_stock)
        maxtime = pd.read_sql('SELECT MAX(end) FROM '+ row['name'], conn)
        mytime = datetime.datetime.strptime(maxtime['MAX(end)'][0][:10], '%Y-%m-%d')
   # conn.close()
    
    delta = datetime.datetime.now() - mytime
    return delta.days

def getLastUpdateDate(row):
    if row['ticker'] in _indexList:
        conn = sqlite3.connect(_dbName_index)
        maxtime = pd.read_sql('SELECT MAX(date) FROM '+ row['name'], conn)
        maxtime = maxtime['MAX(date)']
    else:
        conn = sqlite3.connect(_dbName_stock)
        maxtime = pd.read_sql('SELECT MAX(end) FROM '+ row['name'], conn)
        maxtime = maxtime['MAX(end)']

    return maxtime
    

"""
Returns a list of symbol-interval combos that are missing from the local database 
----------
Params: 
records: [Dataframe] of getRecords() 
"""
def getMissingIntervals(records, type = 'stock'):
    
    if type == 'stock':
        #records = getRecords()
        myIntervals = intervals_stock
    elif type == 'index':
        myIntervals=intervals_index
    numRecordsPerSymbol = records.groupby(by='ticker').count()

    # each symbol where count < interval.count
    symbolsWithMissingIntervals = numRecordsPerSymbol.loc[
        numRecordsPerSymbol['name'] < len(intervals_stock)].reset_index()['ticker'].unique()

    ## find missing symbol-interval combos and put them in a DF
    missingCombos = []
    for symbol in symbolsWithMissingIntervals:
        for interval in myIntervals:
            myRecord = records.loc[
                (records['ticker'] == symbol) & (records['interval'] == interval)
            ]
            if myRecord.empty:
                missingCombos.append(symbol+'-'+interval)
    
    return missingCombos


"""
Returns a DF with all saved ticker data and last update date 
"""
def getRecords(type = 'stock'):
    if type == 'index':
        conn = sqlite3.connect(_dbName_index)
    else: 
        conn = sqlite3.connect(_dbName_stock)
    
    try:
        tables = pd.read_sql('SELECT name FROM sqlite_master WHERE type=\'table\'', conn)
       # conn.close()
    except:
        print('no tables!')
    
    if not tables.empty:
        tables[['ticker', 'type', 'interval']] = tables['name'].str.split('_',expand=True)     
        tables['lastUpdateDate'] = tables.apply(getLastUpdateDate, axis=1)
        tables['daysSinceLastUpdate'] = tables.apply(getDaysSinceLastUpdated, axis=1)
        if type == 'index': 
            tables['interval'] = tables.apply(lambda x: _addspace(x['interval']), axis=1)
        
    return tables

"""
Root function to update all records 
"""
def updateRecords():
    print('checking for updates')

    #### check for new tickers 
    # read in tickerlist.txt
    tickerList = pd.read_csv(_tickerFilepath)
    myList = pd.DataFrame(tickerList.columns).reset_index(drop=True)
    myList.rename(columns={0:'ticker'}, inplace=True)
    myList['ticker'] = myList['ticker'].str.strip(' ').str.upper()
    myList.sort_values(by=['ticker'], inplace=True)
    
    # get existing tickers and when they were last updated
    stocks = getRecords()
    stocks['type'] = 'stock'
    index = getRecords(type='index')
    index['type'] = 'index'
    records = pd.concat([stocks, index])

    # filter out records that are older than 5 days 
    recordsToUpdate = records.loc[records['daysSinceLastUpdate'] >= 5]

    # build a list of stocks and indices that need to be updated
    stocksToUpdate = stocks.loc[stocks['daysSinceLastUpdate'] >= 5]
    indicesToUpdate = index.loc[stocks['daysSinceLastUpdate'] >= 5]
    
    # build a list of stocks and indices that have been added to the watchlist 
    newList = myList[~myList['ticker'].isin(stocks['ticker'])].reset_index(drop=True)
    newList_stocks = newList[~newList['ticker'].isin(_indexList)].reset_index(drop=True)
    newList_indices = newList[~newList['ticker'].isin(_indexList)].reset_index(drop=True)
    
    # filter out records that are new (i.e. no records exist)
    newList = myList[~myList['ticker'].isin(records['ticker'])].reset_index(drop=True)
    #print(newList)
    
    ## update stocks
    updateStockHistory(stocksToUpdate, newList_stocks) # function will handle empty dataframes if there is nothing to update
    updateIndexHistory(indicesToUpdate, newList_indices)

"""
Get options historical data

## !!!!!     !!!!!!!
## NOT FUNCTIONAL !! 
## !!!!!     !!!!!!!

Params
-----------
symbol: [string] - e.g. 'AAPL' 
strike: [int] - strike price 
date: [str] - YYYY-MM-DD

Returns
-------
optionIDs - [List] with strikePrice, callSymbolID, putSymbolID
    e.g. {'strikePrice': 125, 'callSymbolId': 40444547, 'putSymbolId': 40444581}

Empty list is returned if the option chain does not exist

"""
def updateOptionHistory(symbol, strike, date):
    print('\n Getting Options History! \n')

    try:
        qtrade = setupConnection()
    except : 
        print('could not connect to DB/Qtrade!')

    symbolId = qtrade.get_quote('AAPL')['symbolId']
    filter =  [
                    {
                     "optionType": "Call",
                     "underlyingId": symbolId,
                     "expiryDate": date+"T00:00:00.000000-05:00",
                     "minstrikePrice": strike,
                     "maxstrikePrice": strike
                     }
                 ]


    ## get available chains for symbol 
    optionChains = qtrade.get_option_chain(symbol)
    """"
    The structure of optionChains is whacky, heres a reference: 

    > optionChain is a LIST of:
        > Expiry Dates, which is a list of:
            > 'Chain Roots' i.e. symbols, which is a list of 
                > Strike prices, which is a list of:
                    > [strike, callsymbolID, putSymbolID]

    Shit is whack. Hence the nested for loops 

    """
    allChains = optionChains['optionChain']
    optionIDs = list() # return var
    
    for chain in allChains:
        if chain['expiryDate'][:-22] == date:
            for strikes in chain['chainPerRoot'][0]['chainPerStrikePrice']:
                if strikes['strikePrice'] == strike:
                    optionIDs = strikes
    
    optionQuote = qtrade.get_option_quotes(option_ids=optionIDs['callSymbolId'], filters=filter)
    return optionIDs

updateRecords()