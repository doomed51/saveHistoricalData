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
import time

import ibkr_getHistoricalData as ib
import localDbInterface as db

## Default DB names 
_dbName_stock = 'historicalData_stock.db'
_dbName_index = 'historicalData_index.db'

"""Tracked intervals for stocks
    Note: String format is specific to questrade""" 
intervals_stock = ['FiveMinutes', 'FifteenMinutes', 'HalfHour', 'OneHour', 'OneDay', 'OneMonth']

"""Tracked intervals for indices
    Note: String format is specific to questrade"""
intervals_index = ['5 mins', '15 mins', '30 mins', '1 day']

## global vars
_indexList = ['VIX', 'VIX3M', 'VVIX']
_tickerFilepath = 'tickerList.csv'

"""
######################################################

##################  Lambda functions for dataframe processing 

######################################################
"""

## adda space between num and alphabet
def _addspace(myStr): 
    return re.sub("[A-Za-z]+", lambda elm: " "+elm[0],myStr )

""" returns number of work/business days 
    between two provided datetimes 
""" 
def _countWorkdays(startDate, endDate, excluded=(6,7)):
    return len(pd.bdate_range(startDate, endDate))

"""
lambda function returns numbers of business days since a DBtable was updated
"""
def _getDaysSinceLastUpdated(row):
    #if row['ticker'] in _indexList:
    conn = sqlite3.connect(_dbName_index)
    maxtime = pd.read_sql('SELECT MAX(date) FROM '+ row['name'], conn)
    mytime = datetime.datetime.strptime(maxtime['MAX(date)'][0][:10], '%Y-%m-%d')
    """    else:
    conn = sqlite3.connect(_dbName_stock)
    maxtime = pd.read_sql('SELECT MAX(end) FROM '+ row['name'], conn)
    mytime = datetime.datetime.strptime(maxtime['MAX(end)'][0][:10], '%Y-%m-%d')"""
    
    ## calculate business days since last update
    numDays = len( pd.bdate_range(mytime, datetime.datetime.now() )) - 1

    return numDays

def _getLastUpdateDate(row):
    #if row['ticker'] in _indexList:
    conn = sqlite3.connect(_dbName_index)
    maxtime = pd.read_sql('SELECT MAX(date) FROM '+ row['name'], conn)
    maxtime = maxtime['MAX(date)']
    """else:
    conn = sqlite3.connect(_dbName_stock)
    maxtime = pd.read_sql('SELECT MAX(end) FROM '+ row['name'], conn)
    maxtime = maxtime['MAX(end)']"""

    return maxtime

def _getFirstRecordDate(row):
    #if row['ticker'] in _indexList:
    conn = sqlite3.connect(_dbName_index)
    mintime = pd.read_sql('SELECT MIN(date) FROM '+ row['name'], conn)
    mintime = mintime['MIN(date)']
    """else:
    conn = sqlite3.connect(_dbName_stock)
    mintime = pd.read_sql('SELECT MAX(end) FROM '+ row['name'], conn)
    mintime = mintime['MAX(end)']"""
    return mintime

def _getEarliestAvailableRecordDate(row):
    if row['type'] == 'index':
        earliestTimeStamp = ib.getEarliestTimeStamp(ibkr, symbol=row['ticker'], currency='USD', exchange='CBOE')
    elif row['type'] == 'stock':
        print('na')##earliestTimeStamp = ib.getEarliestTimeStamp(ibkrObj, row['ticker'], 'USD', 'SMART')
    return earliestTimeStamp
"""
################################################################
########################### END ################################
################################################################
"""

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
        print("\nTrying Token YAML")
        qtrade = Questrade(token_yaml = "access_token.yml")
        sw = qtrade.get_quote(['SPY'])
        
    except:
        print("[red] FAILED![/red]\n")
        try: 
            print("Trying Refresh")
            qtrade = Questrade(token_yaml = "access_token.yml")
            qtrade.refresh_access_token(from_yaml = True, yaml_path="access_token.yml")

        except:
            print("[red] FAILED![/red]\n")
            print("Trying Access Code")
            
            try:
                with open("token.txt") as f:
                    ac = f.read().strip()
                    qtrade = Questrade(access_code = ac)
                    w = qtrade.get_quote(['SPY'])
                    print ('%s latest Price: %.2f'%(w['symbol'], w['lastTradePrice'])) 
            
            except:
                print("[red] FAILED![/red]")
                print("[yellow] Might neeed new access code from Questrade[/yellow] \n")
                ##print(err)
                quit() 
    return qtrade

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
        filepath = Path('output/'+history['symbol'][0]+'_'+history['interval'][0]+'.csv')
        print('Saving %s interval data for %s'%(history['interval'], history['symbol']))
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
 Returns the ealiest stored record for a particular symbol-interval combo
    INPUTS
    ---------
    sqlConn: [sqlite3.connection]
    symbol: [str] lookup symbol
    interval: [str] oneday, hour, etc
    type: [str] stock, or option
    ---------
    RETURNS [dateTime] or [str]:empty string if table is not found in the DB 
"""
def getLastUpdateDate(sqlConn, symbol, interval, type):
    lastDate = ''
    sql = 'SELECT * FROM ' + symbol + '_' + type + '_' + interval
    history = pd.DataFrame()

    try:
        history = pd.read_sql(sql, sqlConn)
        history['end'] = pd.to_datetime(history['end'], dayfirst=False)
        lastDate = history['end'].max()
    except:
        next

    return lastDate

def updateStockHistory(stocks, stocksToUpdate, stocksToAdd):

    # check for missing intervals 
    missingIntervals = getMissingIntervals(stocksToUpdate)
    
    ## establish connections if updating is needed 
    if (stocksToUpdate['daysSinceLastUpdate'].count() > 0) or (stocksToAdd['ticker'].count() > 0) or (len(missingIntervals) > 0):
        print(' Connecting with [red]Qtrade & DB [/red]')
        qtrade = setupConnection()
        conn = sqlite3.connect(_dbName_stock)
            
    ## update missing intervals if we have any
    if len(missingIntervals)>0:
        for item in missingIntervals:
            [_tkr, _intvl] = item.split('-')
            startDate = datetime.datetime.now(tz=None) - datetime.timedelta(30000)
            history = getLatestHistory(qtrade, _tkr, startDate.date(), datetime.datetime.now(tz=None).date(), _intvl)
            saveHistoryToDB(history, conn)
            print('[red]Missing interval[/red] %s-%s...[green]updated![/green]\n'%(_tkr, _intvl))

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
    if not stocksToAdd.empty: 
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
    
    print('\n[green]All Done![/green]\n')

"""
update history for indices
 updateIndexHistory(index, indicesWithOutdatedData, newlyAddedIndices)
"""
def updateIndexHistory(index, indicesWithOutdatedData= pd.DataFrame(), newlyAddedIndices  = pd.DataFrame()):
    print('checking if index records need updating')

    ## get a list of missing intervals if any 
    missingIntervals = pd.DataFrame()
    missingIntervals = getMissingIntervals(index, type='index')
 
    ## if we have any missing data to update, establish connection with IBKR  
    if (not indicesWithOutdatedData.empty) or ( not newlyAddedIndices.empty) or (len(missingIntervals) > 0):
        
        ## connect with IBKR
        try:
            print('[yellow]Records need updating...[/yellow]\n')
            print('[yellow] Connecting with IBKR...[/yellow]\n')
            ibkr = IB() 
            ibkr.connect('127.0.0.1', 7496, clientId = 10)
            print('[green]  Success![/green]\n')
        except:
            print('[red]  Could not connect with IBKR![/red]\n')
        
        ## connect with local DB 
        try:
            print('[yellow] Connecting with the local DB...[/yellow]')
            conn = sqlite3.connect(_dbName_index)
            print('[green]  Success![/green]\n')
        except:
            print('[red]  Could not connect with local DB![/red]\n')
    
    ## update missing intervals if we have any 
    if len(missingIntervals) > 0: 
        print('[yellow]Some records are missing intervals, Updating...[/yellow]')
        for item in missingIntervals:
            [_tkr, _intvl] = item.split('-')
            if ( _intvl in ['5 mins', '15 mins']):
                history = ib.getBars(ibkr, symbol=_tkr,lookback='100 D', interval=_intvl )
            
            elif (_intvl in ['30 mins', '1 day']):
                history = ib.getBars(ibkr, symbol=_tkr,lookback='365 D', interval=_intvl )

            history['interval'] = _intvl.replace(' ', '')
            db.saveHistoryToDB(history, conn)

            print('[red]Missing interval[/red] %s-%s...[red]updated![/red]\n'%(_tkr, _intvl))

    ## update the existing records that are outdated 
    if not indicesWithOutdatedData.empty:
        print('\n[yellow]Outdated records found. Updating...[/yellow]')
        pd.to_datetime(indicesWithOutdatedData['lastUpdateDate'])

        ## regex to add a space between any non-digit and digit (adds a space to interval column)
        indicesWithOutdatedData['interval'] = indicesWithOutdatedData['interval'].apply(lambda x: re.sub(r'(?<=\d)(?=[a-z])', ' ', x))

        print(indicesWithOutdatedData)
        # Iterate through records with missing data and update the local 
        # database with the latest available data from ibkr
        for index, row in indicesWithOutdatedData.iterrows():            
            ## Add column with number of business days that need updating (curr date - last record)
            
            ## get history from ibkr 
            history = ib.getBars(ibkr, symbol=row['ticker'], lookback='%s D'%(row['daysSinceLastUpdate']), interval=row['interval']) 
            
            ## add interval column for easier lookup 
            history['interval'] = row['interval'].replace(' ', '')
            
            ## save history to db 
            #db.saveHistoryToDB(history, conn)
            print(history['date'].min())
            print(history['date'].max())
            print(row['daysSinceLastUpdate'])
            print(row['lastUpdateDate'])
            print(history)
            print('%s-%s...[red]updated![/red]\n'%(row['ticker'], row['interval']))
                
    else: 
        print('\n[green]Existing records are up to date...[/green]')

    if not newlyAddedIndices.empty:
        print('\n[blue]%s new indicies found[/blue], adding to db...'%(newlyAddedIndices['ticker'].count()))

        # update each symbol w/ list of tracked intervals
        for newIndex in newlyAddedIndices['ticker']:
            for _intvl in intervals_index:
                
                ## set lookback based on interval
                if ( _intvl in ['5 mins', '15 mins']):
                    lookback = 80
            
                elif (_intvl in ['30 mins', '1 day']):
                    lookback = 300
                
                ## lookup history 
                print('%s - %s - %s'%(newIndex, _intvl, lookback))
                history = ib.getBars(ibkr, symbol=newIndex,lookback='%s D'%(lookback), interval=_intvl )
                
                ## add interval column for easier lookup 
                history['interval'] = _intvl.replace(' ', '')
                db.saveHistoryToDB(history, conn)

                print(' New record Added for %s-%s..from %s to %s'%(newIndex, _intvl, history['date'].min(), history['date'].max()))
                

"""
get quote from questrade
"""
def getQuote(smbl='AAPL'):
    try:
        qtrade = setupConnection()
    except : 
        print('could not connect to DB/Qtrade!')

    quote = qtrade.get_quote(smbl)
    print(quote['symbolId'])
    

"""
Returns a list of symbol-interval combos that are missing from the local database 
----------
Params: 
records: [Dataframe] of getRecords() 
"""
def getMissingIntervals(records, type = 'stock'):
    
    if type == 'stock':
        myIntervals = intervals_stock
    elif type == 'index':
        myIntervals=intervals_index
    numRecordsPerSymbol = records.groupby(by='ticker').count()

    # each symbol where count < interval.count
    symbolsWithMissingIntervals = numRecordsPerSymbol.loc[
        numRecordsPerSymbol['name'] < len(myIntervals)].reset_index()['ticker'].unique()

    ## find missing symbol-interval combos
    missingCombos = []
    for symbol in symbolsWithMissingIntervals:
        for interval in myIntervals:
            myRecord = records.loc[
                (records['ticker'] == symbol) & (records['interval'] == interval)
            ]
            if myRecord.empty:
                missingCombos.append(symbol+'-'+interval)
    
    ## return the missing symbol-interval combos
    return missingCombos


"""
Returns a DF with all saved ticker data and last update date 
"""
def getRecords():
    conn = sqlite3.connect(_dbName_index)
    
    try:
        tables = pd.read_sql('SELECT name FROM sqlite_master WHERE type=\'table\' AND NOT name LIKE \'00_%\'', conn)
       # conn.close()
    except:
        print('no tables!')

    if not tables.empty:
        tables[['ticker', 'type', 'interval']] = tables['name'].str.split('_',expand=True)     
        tables['lastUpdateDate'] = tables.apply(_getLastUpdateDate, axis=1)
        tables['daysSinceLastUpdate'] = tables.apply(_getDaysSinceLastUpdated, axis=1)
        if type == 'index': 
            tables['interval'] = tables.apply(lambda x: _addspace(x['interval']), axis=1)
            tables['firstRecordDate'] = tables.apply(_getFirstRecordDate, axis=1)
    return tables

""""
Saves <lookback> number of days of historical data to local db 
__
function is kinda half assed, but it does the job of grabbing missing older data 
"""
def update200():
    print('updating 200!\n')
    
    ## get local index records DF 
    index = getRecords(type='index')
    lookback = 30

    try:
            ibkr = IB() 
            ibkr.connect('127.0.0.1', 7496, clientId = 10)
            conn = sqlite3.connect(_dbName_index)
    except:
        print('[red]Could not connect with IBKR![/red]\n')

    ## get earliest available dates for list of indices 
    pf = pd.DataFrame({'ticker':_indexList}) ## convert series into DF 
    pf['firstAvailableRecord'] = pf.apply(lambda x: ib.getEarliestTimeStamp(ibkr, x['ticker'], 'USD', 'CBOE'), axis=1)

    ## add the earliest record dates to the records DF  
    merged = pd.merge(index, pf, how='left', on = 'ticker')
    merged['workdays'] = merged.apply(lambda x: _countWorkdays(x['firstAvailableRecord'], x['firstRecordDate']), axis=1)
    index = merged

    for index, row in index.iterrows():

        ## skip if theres enough local history already 
        if row['workdays'] < lookback:
            continue

        ## manual throttling of api requests 
        if index > 0:
            print('Pausing before next round....%s-%s\n'%(row['ticker'], row['interval']))
            time.sleep(45)
        
        print('%s-%s[yellow]....Updating[/yellow]'%(row['ticker'], row['interval']))

        # initiate 'enddate from the last time history was updated
        endDate = datetime.datetime.strptime(row['firstRecordDate'][:10], '%Y-%m-%d')-pd.offsets.BDay(1)
        endDate = endDate.replace(hour = 20)
        i=0

        ## initiate the history datafram that will hold the retrieved bars 
        history = pd.DataFrame()

        ## append records to history dataframe while days to update remain
        while i < 4:
            i+=1
            ## concatenate history retrieved from ibkr 
            history = pd.concat([history, ib.getBars(ibkr, symbol=row['ticker'], lookback='%s D'%lookback, interval=row['interval'], endDate=endDate)], ignore_index=True)
            
            ## update enddate for the next iteration
            endDate = endDate - pd.offsets.BDay(lookback - 1)
            endDate = endDate.replace(hour = 20)
            
            ## manual throttling of api requests 
            time.sleep(5)
        
        ## add interval column for easier lookup 
        history['interval'] = row['interval'].replace(' ', '')

        ## save history to db 
        saveHistoryToDB(history, conn, 'index')
        
        ## print logging info & reset history df 
        print(' %s-%s[green]...Updated![/green]'%(row['ticker'], row['interval']))
        print(' from %s to %s\n'%(history['date'].min(), history['date'].max()))
        history = pd.DataFrame()
        

    ibkr.disconnect()


"""
Core function that will update all records in the local db
--
inputs: 
    tickerlist.csv -> list of tickers to keep track of(Note: only for adding new symbols 
        to keep track of. existing records will be kept up to date by default) 
    updateThresholdDays.int -> number of days before records are updated
"""
def updateRecords(updateThresholdDays = 5):

    #### check for new tickers 
    # read in tickerlist.txt
    symbolList = pd.read_csv(_tickerFilepath)

    ## convert to dataframe, and cleanup  
    symbolList = pd.DataFrame(symbolList.columns).reset_index(drop=True)
    symbolList.rename(columns={0:'ticker'}, inplace=True)
    symbolList['ticker'] = symbolList['ticker'].str.strip(' ').str.upper()
    symbolList.sort_values(by=['ticker'], inplace=True)

    # merge into master records list 
    records = getRecords()

    if not records.empty: ## if database contains some records, check if any need to be updated
        symbolsWithOutdatedData = records.loc[records['daysSinceLastUpdate'] >= updateThresholdDays]
        newlyAddedSymbols = symbolList[~symbolList['ticker'].isin(records['ticker'])]

    # update history in local DB 
    updateIndexHistory(records, symbolsWithOutdatedData, newlyAddedSymbols)

    print(records)

"""    if not stocks.empty:
        stocksWithOutdatedData = stocks.loc[stocks['daysSinceLastUpdate'] >= updateThresholdDays]
        newlyAddedSymbols = symbolList[~symbolList['ticker'].isin(stocks['ticker'])].reset_index(drop=True)
        newlyAddedStocks = newlyAddedSymbols[~newlyAddedSymbols['ticker'].isin(_indexList)].reset_index(drop=True)
    elif stocks.empty: ## database is empty 
        newlyAddedStocks = symbolList

    if not index.empty:
        indicesWithOutdatedData = index.loc[index['daysSinceLastUpdate'] >= updateThresholdDays]
    
        # Build a list of indices that have been newly added to the watchlist csv 
        newlyAddedSymbols = symbolList[~symbolList['ticker'].isin(index['ticker'])].reset_index(drop=True)
        newlyAddedIndices = newlyAddedSymbols[newlyAddedSymbols['ticker'].isin(_indexList)].reset_index(drop=True)
        newlyAddedIndices = newlyAddedSymbols[newlyAddedSymbols['ticker'].isin(index['ticker'])].reset_index(drop=True)
    
    elif index.empty: ##no indices stored in the db
        newlyAddedIndices = symbolList"""

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

"""updateRecords()
try:
        ibkr = IB() 
        ibkr.connect('127.0.0.1', 7496, clientId = 10)
        conn = sqlite3.connect(_dbName_index)
except:
    print('[red]Could not connect with IBKR![/red]\n')

tlt = ib.getBars(ibkr=ibkr, symbol='SPY', interval = '5 mins')
print(tlt)
print(ib.getEarliestTimeStamp(ibkr=ibkr, symbol='SPY'))"""

## section below is to update older data and view index records + date range available 
"""masterloop = 0

while masterloop < 2:
    print('[green]Sstarting iteration...%s[/green]\n'%(masterloop))
    #update200()
    masterloop += 1
    #time.sleep(300)

try:
        ibkr = IB() 
        ibkr.connect('127.0.0.1', 7496, clientId = 10)
        conn = sqlite3.connect(_dbName_index)
except:
    print('[red]Could not connect with IBKR![/red]\n')

index = getRecords(type='index')

# get earliest available dates for list of indices 
pf = pd.DataFrame({'ticker':_indexList})
pf['firstAvailableRecord'] = pf.apply(lambda x: ib.getEarliestTimeStamp(ibkr, x['ticker'], 'USD', 'CBOE'), axis=1)
#pf['firstAvailableRecord'] = pf.apply(lambda x: x)

merged = pd.merge(index, pf, how='left', on = 'ticker')
merged['workdays'] = merged.apply(lambda x: _countWorkdays(x['firstAvailableRecord'], x['firstRecordDate']), axis=1)

print(index)
print(pf)
print(merged)"""