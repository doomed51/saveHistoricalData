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
from tracemalloc import start
from urllib.error import HTTPError, URLError
from matplotlib.pyplot import axis

from pytz import timezone, utc

from qtrade import Questrade 
from pathlib import Path
from requests.exceptions import HTTPError
from rich import print

import datetime
import sqlite3 
import pandas as pd

## Default DB names 
_dbName_stock = 'historicalData_stock.db'
_dbName_index = 'historicalData_index.db'
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
def updateSymbolHistory2(tickerFilepath = 'tickerList.csv'):
    ## vars for when updating is needed 
    type = 'stock'
    interval = ['FiveMinutes', 'FifteenMinutes', 'OneHour', 'OneDay', 'OneMonth']
    
    # check our DB records that haven't been updated for more than 5 days
    dbTables = checkRecords()
    dbTables_ = dbTables.loc[(dbTables['daysSinceLastUpdate'] >= 5)]
    
    if dbTables_['daysSinceLastUpdate'].count() > 0: ## we have records thats need updating
        pd.to_datetime(dbTables_['lastUpdateDate'])
        print('\n[red]Some records are more than 5 days old. Updating...[/red]')
        try:
            print(' Connecting with [red]Qtrade & DB [/red]')
            qtrade = setupConnection()
            conn = sqlite3.connect(_dbName_stock)
        except : 
            print('[red]could not connect to DB/Qtrade![/red]')
        
        for index, row in dbTables_.iterrows():            
            startDate = datetime.datetime.strptime(row['lastUpdateDate'][:10], '%Y-%m-%d') #+ datetime.timedelta(hours=9)
            history = getLatestHistory(qtrade, row['ticker'], startDate.date(), datetime.datetime.now(tz=None).date(), row['interval'])
            saveHistoryToDB(history, conn)
            print('%s-%s...[red]updated![/red]\n'%(row['ticker'], row['interval']))
          #  print(ticker)
        conn.close()
    else: 
        print('\n[green]Existing records are up to date...[/green]')

    # check if there have been any new tickers added to tickerlist.txt
    tickerList = pd.read_csv(tickerFilepath)
    myList = pd.DataFrame(tickerList.columns).reset_index(drop=True)
    myList.rename(columns={0:'ticker'}, inplace=True)
    myList['ticker'] = myList['ticker'].str.strip(' ')
    myList['ticker'] = myList['ticker'].str.upper()
    myList.sort_values(by=['ticker'], inplace=True)
    
    # select all unique tickers in the DB 
    dbTables_3 = dbTables['ticker'].drop_duplicates().reset_index()
    dbTables_3.drop(['index'], inplace=True, axis=1)
    dbTables_3.sort_values(by=['ticker'], inplace=True)
    
    # compare dbTables_3 with tickerlist.txt to identify new entries
    newList = myList[~myList['ticker'].isin(dbTables_3['ticker'])]
    
    # if there are new entries, update the DB 
    if newList['ticker'].count() > 0: 
        print('\n[red]%s new ticker(s) found[/red], adding to db...'%(newList['ticker'].count()))
        try:
            print('Connecting with [red]Qtrade & DB [/red]')
            qtrade = setupConnection()
            conn = sqlite3.connect(_dbName_stock)
        except : 
            print('could not connect to DB/Qtrade!')

        # loop through each new entry (i.e. ticker)
        for ticker in newList['ticker']:
            startDate = datetime.datetime.now(tz=None) - datetime.timedelta(30000)
            print('\nAdding [underline]%s[/underline] to the database...'%(ticker))

            # we will update every interval for the ticker             
            for intvl in interval:
                # get the latest history from questrade 
                history = getLatestHistory(qtrade, ticker, startDate.date(), datetime.datetime.now(tz=None).date(), intvl)
                # save the history to DB 
                saveHistoryToDB(history, conn)
                print(' %s...[green]added![/green] (%s records)'%(intvl, history['end'].count()))
        
        # close the DB connection when done
        conn.close()
    else:
        print('[green]No new tickers to update...[/green]')
    
    print('\n[green]All Done.[/green]')

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

def getDaysSinceLastUpdated(row):
    conn = sqlite3.connect(_dbName_stock)
    maxtime = pd.read_sql('SELECT MAX(end) FROM '+ row['name'], conn)
    conn.close()
    mytime = datetime.datetime.strptime(maxtime['MAX(end)'][0][:10], '%Y-%m-%d')
    delta = datetime.datetime.now() - mytime
    #return maxtime['MAX(end)']
    return delta.days

def getLastUpdateDate(row):
    conn = sqlite3.connect(_dbName_stock)
    maxtime = pd.read_sql('SELECT MAX(end) FROM '+ row['name'], conn)
    conn.close()
    
    return maxtime['MAX(end)']
    #return delta.days

def checkRecords():
    conn = sqlite3.connect(_dbName_stock)
    tables = pd.read_sql('SELECT name FROM sqlite_master WHERE type=\'table\'', conn)
    conn.close()
    
    tables[['ticker', 'type', 'interval']] = tables['name'].str.split('_',expand=True)
    tables['lastUpdateDate'] = tables.apply(getLastUpdateDate, axis=1)
    tables['daysSinceLastUpdate'] = tables.apply(getDaysSinceLastUpdated, axis=1)

    return tables

print(updateSymbolHistory2())
