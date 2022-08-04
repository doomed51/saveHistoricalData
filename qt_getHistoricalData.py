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

# The DATABSE where history is saved
conn = sqlite3.connect('historicalData_stock.db')

# The data SOURCE for historcal data 
Questrade (requires account)

"""
from ib_insync import *
from matplotlib.pyplot import axis
from numpy import histogram, true_divide

from pytz import timezone, utc

from pathlib import Path
from qtrade import Questrade 
from requests.exceptions import HTTPError
from rich import print
from urllib.error import HTTPError, URLError

import datetime
import sqlite3 
import pandas as pd

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


"""
read in list of symbol names 
check which ones had an update > 5 days ago 
get history of old ones from questrade 
update history in the local db 

"""
def updateSymbolHistory2():
    # check our DB records that haven't been updated for more than 5 days
    dbTables = getRecords() # get all saved tickers and when the they were last updated 
    outdatedTickerRecords = dbTables.loc[(dbTables['daysSinceLastUpdate'] >= 5)]
    
    # in dbtables -> make sure each symbol has all intervals in intervals_stock 


    # check if there have been any new tickers added to tickerlist.txt
    tickerList = pd.read_csv(_tickerFilepath)
    myList = pd.DataFrame(tickerList.columns).reset_index(drop=True)
    myList.rename(columns={0:'ticker'}, inplace=True)
    myList['ticker'] = myList['ticker'].str.strip(' ').str.upper()
    myList.sort_values(by=['ticker'], inplace=True)
    
    # Get a list of all tickers currently being tracked  
    trackedTickers = dbTables['ticker'].drop_duplicates().reset_index()
    trackedTickers.drop(['index'], inplace=True, axis=1)
    trackedTickers.sort_values(by=['ticker'], inplace=True)
    
    # compare dbTables_3 with tickerlist.txt to identify new entries
    newList = myList[~myList['ticker'].isin(trackedTickers['ticker'])]
    
    ## establish connections if updating is needed 
    if (outdatedTickerRecords['daysSinceLastUpdate'].count() > 0) or (newList['ticker'].count() > 0):
        print(' Connecting with [red]Qtrade & DB [/red]')
        qtrade = setupConnection()
        conn = sqlite3.connect(_dbName_stock)
            
    ## check for missing intervals and update them 
    missingIntervals = getMissingIntervals()
    for item in missingIntervals:
        [_tkr, _intvl] = item.split('-')
        startDate = datetime.datetime.now(tz=None) - datetime.timedelta(30000)
        history = getLatestHistory(qtrade, _tkr, startDate.date(), datetime.datetime.now(tz=None).date(), _intvl)
        saveHistoryToDB(history, conn)
        print('[red]Missing interval[/red] %s-%s...[red]updated![/red]\n'%(_tkr, _intvl))

    ## update existing records that are more than 5 days old 
    if outdatedTickerRecords['daysSinceLastUpdate'].count() > 0:
        print('\n[blue]Some records are more than 5 days old. Updating...[/blue]')
        pd.to_datetime(outdatedTickerRecords['lastUpdateDate'])

        for index, row in outdatedTickerRecords.iterrows():            
            startDate = datetime.datetime.strptime(row['lastUpdateDate'][:10], '%Y-%m-%d') #+ datetime.timedelta(hours=9)
            history = getLatestHistory(qtrade, row['ticker'], startDate.date(), datetime.datetime.now(tz=None).date(), row['interval'])
            saveHistoryToDB(history, conn)
            print('%s-%s...[red]updated![/red]\n'%(row['ticker'], row['interval']))
        #conn.close()
    else: 
        print('\n[green]Existing records are up to date...[/green]')

    # Add new records added to the watchlist 
    if newList['ticker'].count() > 0: 
        print('\n[blue]%s new ticker(s) found[/blue], adding to db...'%(newList['ticker'].count()))

        # loop through each new ticker
        for ticker in newList['ticker']:
            startDate = datetime.datetime.now(tz=None) - datetime.timedelta(30000)
            print('\nAdding [underline]%s[/underline] to the database...'%(ticker))

            # we will update every interval for the ticker             
            for intvl in intervals_stock:
                # get the latest history from questrade 
                history = getLatestHistory(qtrade, ticker, startDate.date(), datetime.datetime.now(tz=None).date(), intvl)
                # save the history to DB 
                saveHistoryToDB(history, conn)
                print(' %s...[green]added![/green] (%s records)'%(intvl, history['end'].count()))
        
        # close the DB connection when done
       # conn.close()
    else:
        print('[green]No new tickers to update...[/green]')
    
    print('\n[green]All Done.[/green]')

"""
update history for indexes 
"""
def updateIndexHistory():
    print('checking if index records need updating')

    # get saved records w/ last update date
    dbTables = getRecords(type='index')
    if not dbTables.empty:
        outdatedTickerRecords = dbTables.loc[(dbTables['daysSinceLastUpdate'] >= 5)]
        print(outdatedTickerRecords)
        for index, row in outdatedTickerRecords.iterrows():            
            #startDate = datetime.datetime.strptime(row['lastUpdateDate'][:10], '%Y-%m-%d')
            startDate = row['lastUpdateDate']#+ datetime.timedelta(minutes=9))
            print(startDate) 

        #history = ib.getBars(outdatedTickerRecords['ticker'][0], '30 D')
        
    #print(dbTables)


"""
Get options historical data

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
## !!!!!     !!!!!!!
## NOT FUNCTIONAL !! 
## !!!!!     !!!!!!!
def updateOptionHistory(symbol, strike, date):
    print('\n Getting Options History! \n')

    try:
        qtrade = setupConnection()
    except : 
        print('could not connect to DB/Qtrade!')

    symbolId = qtrade.get_quote('AAPL')['symbolId']
    print(symbolId)
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
    print(optionQuote)
    
    #print(optionIDs['callSymbolId'])
    return optionIDs
    
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
    
   # conn.close()
    
    return maxtime
    #return delta.days

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
        
    return tables

# Global function to check entire DB  
# if index 
#   call index-specific function
# if stock
#   call stock-specific funtion 
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
    index = getRecords(type='index')
    records = pd.concat([stocks, index])

    # filter out records that are older than 5 days 
    recordsToUpdate = records.loc[records['daysSinceLastUpdate'] >= 5]
    
    # filter out records that are new (i.e. no records exist)
    newList = myList[~myList['ticker'].isin(records['ticker'])].reset_index(drop=True)
    #print(newList)
    
    ## establish connections if updating is needed 
    if (recordsToUpdate['daysSinceLastUpdate'].count() > 0) or (newList['ticker'].count() > 0):
        try:
            print(' Connecting with [red]Qtrade & DB [/red]')
            #qtrade = setupConnection()
            #conn = sqlite3.connect(_dbName_stock)
            ibkr = IB() 
            ibkr.connect('127.0.0.1', 7496, clientId = 10)
        except : 
            print('[red]could not connect to DB/Qtrade![/red]')
    
        ## Initiate data for newly added tickers 
        for row in newList['ticker']:
            
            ## use IBKR if index 
            if row in _indexList:
                conn = sqlite3.connect(_dbName_index)
                for intvl in intervals_index:
                    #contract = Index(row, 'CBOE', 'USD')
                    bars = pd.DataFrame()
                    
                    if ( intvl == '5 mins'):
                        bars = ib.getBars(ibkr, symbol=row,lookback='100 D', interval=intvl )
                    
                    elif (intvl == '30 mins' or intvl == '1 day'):
                        bars = ib.getBars(ibkr, symbol=row,lookback='365 D', interval=intvl )
                    
                    #bars.drop(columns=['volume'], inplace=True)
                    if not bars.empty:
                        saveHistoryToDB(bars, conn,'index')
                
            ## if stock then use questrade
            else: 
                conn = sqlite3.connect(_dbName_stock)
                    

    # Get a list of all tickers currently being tracked  
    #trackedTickers = dbTables['ticker'].drop_duplicates().reset_index()
    #trackedTickers.drop(['index'], inplace=True, axis=1)
    #trackedTickers.sort_values(by=['ticker'], inplace=True)
    
    # compare dbTables_3 with tickerlist.txt to identify new entries
    #newList = myList[~myList['ticker'].isin(trackedTickers['ticker'])]

"""
    Makes sure the target Symbol-Interval combo
        1. Exists in the DB 
        2. Contains the most up-to-date data  
Params
-----------
invtlToUpdate: [array] of intervals
symbolToUpdate: [array] of Symbols (don't mix indices and stock)
"""
def updateSingleTF(intervalsToUpdate=['FiveMinutes'], symbolsToUpdate=['AAPL']):
    isIndex = False
    if symbolsToUpdate[0] in _indexList:
        isIndex = True 
    
    # get the available records, check if index or symbol passed in 
    if isIndex:
        records = getRecords(type='index')
    else:
        records = getRecords()
    
    for symbol in symbolsToUpdate:
        for interval in intervalsToUpdate:
            print('%s interval for %s'%(interval, symbol))
            
            # try to locate the record 
            myRecord = records.loc[
                (records['ticker'] == symbol) & 
                (records['interval'] == interval)]
            
            if not myRecord.empty: # if records exist then check if the data is old enough to update 
                if myRecord['daysSinceLastUpdate'] >= 5:
                    records = getLatestHistory
                    print('\n')
                    print(myRecord)
            else: # if no records exist then update the entire dataset 
                print('Error1337: no records!!!!!!!!!!!!!!!!!!!')
    
    # immediately update if table doesn't exist 
    # escape if table is already up to date

"""
Returns a list of symbol-interval combos that are missing from the local database 
"""
def getMissingIntervals(type = 'stock'):
    
    if type == 'stock':
        records = getRecords()
        myIntervals = intervals_stock
    numRecordsPerSymbol = records.groupby(by='ticker').count()

    # each symbol where count < interval.count
    symbolsWithMissingIntervals = numRecordsPerSymbol.loc[
        numRecordsPerSymbol['name'] < len(intervals_stock)].reset_index()['ticker'].unique()

    ## find missing symbol-interval combos and put them in a DF...or update them!  
    missingCombos = []
    for symbol in symbolsWithMissingIntervals:
        for interval in myIntervals:
            myRecord = records.loc[
                (records['ticker'] == symbol) & (records['interval'] == interval)
            ]
            if myRecord.empty:
                missingCombos.append(symbol+'-'+interval)
    
    return missingCombos



updateSymbolHistory2()
#updateIndexHistory()

#updateRecords()

#missingIntervals = getMissingIntervals()
#for item in missingIntervals:
#    [_tkr, _intvl] = item.split('-')
#    print(_intvl)