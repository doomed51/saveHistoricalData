"""
Author: Rachit Shankar 
Date of Origination: February, 2022

Purpose: Build a local database of historical pricing data for a list of specified equities

----
# the list of equities
tickerFilepath = 'tickerList.csv'

# The database
conn = sqlite3.connect('historicalData.db')

# Historical data source
Questrade (requires account)

"""
from math import fabs
from operator import index
from time import strftime
from urllib.error import HTTPError, URLError
from qtrade import Questrade 
from pathlib import Path
from requests.exceptions import HTTPError

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
        print("\n trying token yaml \n")
        qtrade = Questrade(token_yaml = "access_token.yml")
        w = qtrade.get_quote(['SPY'])
        print ('%s latest Price: %.2f'%(w['symbol'], w['lastTradePrice'])) 
        
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
    return qtrade

"""
Save history to a sqlite3 database
###
uses sqlite3
connects to db in local directory 

Params
------------
history: [DataFrame]
    pandas dataframe with date and OHLC, volume, interval, and vwap  
"""
def saveHistoryToDB(history, conn, type='stock'):
    #conn = sqlite3.connect('historicalData.db')

    ## Tablename convention: <symbol>_<stock/opt>_<interval>
    tableName = history['symbol'][0]+'_'+type+'_'+history['interval'][0]
    
    if type == 'stock':
        print('saving %s - %s data!'%(history['symbol'][0], (history['interval'][0])))
        history.to_sql(f"{tableName}", conn, index=False, if_exists='append')
    
    elif type == 'option':
        print('saving options to the DB is not yet implemented')

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
qtrade: [Questrade] - active questrade object
ticker: [str] - symbol / ticker
startDate/endDate: [datetime] - period to look up 
interval: [str]: time granularity i.e. oneDay, oneHour, etc. 
"""
def getLatestHistory(qtrade, ticker, startDate, endDate, interval):
    ## Retrieve historical data  
    history = pd.DataFrame()
    print('looking up history for: %s, %s, %s, %s'%(ticker, startDate, endDate, interval))
    try:
        ## Retrieve data from questrade
        history = pd.DataFrame(qtrade.get_historical_data(ticker, startDate, endDate, interval))
        print('\n history retrieved! \n')

        #res = history[ ::len(history)-1 ]
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
#####
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
        history['start'] = pd.to_datetime(history['start'], dayfirst=False)
        lastDate = history['start'].max()
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
    interval = 'OneHour'
    endDate = datetime.datetime.now(tz=None).date()
    startDate = datetime.datetime.now(tz=None) - datetime.timedelta(5000)

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
        print('\n Looking up: %s'%(ticker))
        lastUpdateDate = getLastUpdateDate(conn, ticker, interval, type) 
        
        ## if not history set start date far back and update
        if lastUpdateDate == 'n/a':
            ## get latest history
            print('Updating history...\n')
            history = getLatestHistory(qtrade, ticker, startDate.date(), endDate, interval)
            saveHistoryToDB(history, conn)
        
        ## if saved date < 5 days old, skip 
        elif (lastUpdateDate > datetime.datetime.now(tz=None) - datetime.timedelta(5)):
            print('No update needed...\n')
            next
        
        ## if saved data exists, but is > 5 days old, set start date 
        ## to last saved date and update
        else:
            print('Updating history...\n')
            startDate = lastUpdateDate + datetime.timedelta(hours=9)
            history = getLatestHistory(qtrade, ticker, startDate.date(), endDate, interval)
            saveHistoryToDB(history, conn)

updateSymbolHistory()
