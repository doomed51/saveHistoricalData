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
conn = sqlite3.connect('historicalData.db')

# The data SOURCE for historcal data 
Questrade (requires account)

"""
from urllib.error import HTTPError, URLError

from pytz import timezone, utc

from qtrade import Questrade 
from pathlib import Path
from requests.exceptions import HTTPError
from rich import print

import datetime
import sqlite3 
import pandas as pd

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
        print (' Success! %s latest Price: %.2f \n'%(w['symbol'], w['lastTradePrice'])) 
        
    except(HTTPError, FileNotFoundError, URLError) as err:
        try: 
            print("\n Trying Refresh \n")
            qtrade = Questrade(token_yaml = "access_token.yml")
            qtrade.refresh_access_token(from_yaml = True, yaml_path="access_token.yml")
            w = qtrade.get_quote(['SPY'])
            print ('%s latest Price: %.2f'%(w['symbol'], w['lastTradePrice'])) 

        except(HTTPError, FileNotFoundError, URLError):
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
    #conn = sqlite3.connect('historicalData.db')

    ## Tablename convention: <symbol>_<stock/opt>_<interval>
    tableName = history['symbol'][0]+'_'+type+'_'+history['interval'][0]
    
    if type == 'stock':
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
def updateSymbolHistory(tickerFilepath = 'tickerList.csv'):
    ## setup lookup var defaults
    type = 'stock'
    interval = ['FiveMinutes', 'OneHour', 'OneDay', 'OneMonth', 'FifteenMinutes'] #OneHour
    endDate = datetime.datetime.now(tz=None).date()
    startDate = datetime.datetime.now(tz=None) - datetime.timedelta(10000)

    ## read in list of tickers
    tickerList = pd.read_csv(tickerFilepath)
    
    ## initiate connection to the DB & Questrade
    try:
        conn = sqlite3.connect('historicalData.db')
        qtrade = setupConnection()
    except : 
        print('could not connect to DB/Qtrade!')

    ## Update saved data
    for ticker in tickerList.columns: 
        ticker = ticker.strip(' ').upper()
        print('\nChecking...%s'%(ticker))
        
        for intvl in interval:
            ## if not history set start date far back and update
            lastUpdateDate = getLastUpdateDate(conn, ticker, intvl, type) 
            if lastUpdateDate == 'n/a': #table doesnt exist 
                ## get latest history
                
                startDate = datetime.datetime.now(tz=None) - datetime.timedelta(10000)
                history = getLatestHistory(qtrade, ticker, startDate.date(), endDate, intvl)
                saveHistoryToDB(history, conn)
                print('%s...[red]updated![/red]\n'%(intvl))

            ## if saved date < 5 days old, skip 
            elif (lastUpdateDate.replace(tzinfo=None) > datetime.datetime.now(tz=None) - datetime.timedelta(5)):
                print('%s...No update needed'%(intvl))
                next
            
            ## if saved data exists, but is > 5 days old, 
            # set start date to last saved date and update
            else:
                startDate = lastUpdateDate + datetime.timedelta(hours=9)
                history = getLatestHistory(qtrade, ticker, startDate.date(), endDate, intvl)
                saveHistoryToDB(history, conn)
                print('%s...[red]updated![/red]\n'%(intvl))

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

updateSymbolHistory()


