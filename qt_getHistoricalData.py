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
    ## handle negatives when endDate > startDate 
    if startDate > endDate:
        return (len(pd.bdate_range(endDate, startDate)) * -1)
    else:
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

    ## convert to datetime handling cases where there is 
    ## only a date and no time (e.g. 1day interval data)
    if len(mintime.iloc[0]) > 10: 
        mintime = datetime.datetime.strptime(mintime.iloc[0], '%Y-%m-%d %H:%M:%S')
    else:
        mintime = datetime.datetime.strptime(mintime.iloc[0], '%Y-%m-%d')
    
    return mintime

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
    ## connect with IBKR
    try:
        print('[yellow] Connecting with IBKR...[/yellow]\n')
        ibkr = IB() 
        ibkr.connect('127.0.0.1', 7496, clientId = 10)
        print('[green]  Success![/green]\n')
    except:
        print('[red]  Could not connect with IBKR![/red]\n')

    return ibkr

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
 Returns the ealiest stored record for the specified symbol-interval combo
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
        print('[yellow]Updates pending...[/yellow]\n')

        ## connect with IBKR
        ibkr = setupConnection()

        ## connect with local DB 
        try:
            print('[yellow] Connecting with the local DB..."%s"[/yellow]'%(_dbName_index))
            conn = sqlite3.connect(_dbName_index)
            print('[green]  Success![/green]\n')
        except:
            print('[red]  Could not connect with local DB "%s"![/red]\n'%(_dbName_index))
    
    ## update missing intervals if we have any 
    if len(missingIntervals) > 0: 
        print('[yellow]Some records have missing intervals, updating...[/yellow]')
        for item in missingIntervals:
            [_tkr, _intvl] = item.split('-')
            if ( _intvl in ['5 mins', '15 mins']):
                history = ib.getBars(ibkr, symbol=_tkr,lookback='100 D', interval=_intvl )
            
            elif (_intvl in ['30 mins', '1 day']):
                history = ib.getBars(ibkr, symbol=_tkr,lookback='365 D', interval=_intvl )

            history['interval'] = _intvl.replace(' ', '')

            ## get earliest record available froms ibkr
            earliestTimestamp = ib.getEarliestTimeStamp(ibkr, symbol=newIndex)
            
            db.saveHistoryToDB(history, conn, earliestTimestamp=earliestTimestamp)

            print('[red]Missing interval[/red] %s-%s...[red]updated![/red]\n'%(_tkr, _intvl))

    ## update symbols with outdated records 
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

    ## add records for symbols newly added to the watchlist 
    if not newlyAddedIndices.empty:
        print('\n[blue]%s new indicies found[/blue], adding to db...'%(newlyAddedIndices['ticker'].count()))

        for newIndex in newlyAddedIndices['ticker']:
            ## add records for each tracked interval 
            for _intvl in intervals_index:
                
                ## set lookback based on interval
                if ( _intvl in ['5 mins', '15 mins']):
                    lookback = 80
            
                elif (_intvl in ['30 mins', '1 day']):
                    lookback = 300
                
                ## get history from ibkr 
                print('%s - %s - %s'%(newIndex, _intvl, lookback))
                history = ib.getBars(ibkr, symbol=newIndex,lookback='%s D'%(lookback), interval=_intvl)
                
                ## get earliest record available for ibkr
                earliestTimestamp = ib.getEarliestTimeStamp(ibkr, symbol=newIndex)

                ## add interval column for easier lookup 
                history['interval'] = _intvl.replace(' ', '')
                db.saveHistoryToDB(history, conn, earliestTimestamp=earliestTimestamp)

                print(' New record Added for %s-%s..from %s to %s'%(newIndex, _intvl, history['date'].min(), history['date'].max()))
                
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
    ibkr = setupConnection()
    try:
            conn = sqlite3.connect(_dbName_index)
    except:
        print('[red]Could not connect to DB![/red]\n')

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

"""
Refreshes the lookup_symbolRecords table with the latest data 
uses live api connection 
"""
def refreshLookupTable(): 
    print('\n[yellow]Refreshing lookup table[/yellow]')
    
    ibkr = setupConnection() ## to get earliest available timestamt
    lookupTableName = '00-lookup_symbolRecords' ##lookup table name in db 
    records = getRecords() ## latest local records to compare against 
    
    ## select unique symbols and get the earliest available timestamp from ibkr 
    uniqueRecordSymbols = pd.DataFrame({'ticker':records['ticker'].unique()})
    uniqueRecordSymbols['earliestAvailableTimestamp'] = uniqueRecordSymbols['ticker'].apply(lambda x: ib.getEarliestTimeStamp(ibkr, x))

    ## merge with records table... 
    records_withEarliestAvailableDate = pd.merge(records, uniqueRecordSymbols, how='left', on='ticker')
    
    ## compute the number of missing records in num days 
    records_withEarliestAvailableDate['numMissingBusinessDays'] =  records_withEarliestAvailableDate.apply(lambda x: _countWorkdays(x['earliestAvailableTimestamp'], x['firstRecordDate']), axis=1)
    
    ## select columns to match the records table in the db 
    records_forInput = records_withEarliestAvailableDate[['name', 'ticker', 'interval', 'firstRecordDate', 'numMissingBusinessDays']]

    ## format col names etc to abide by db naming & interval formatting conventions
    records_forInput = records_forInput.rename({'ticker':'symbol'}, axis=1)
    records_forInput['interval'] = records_forInput['interval'].str.replace(' ', '')

    symbolRecords = db.getLookup_symbolRecords()

    ## merge them 
    merged = pd.merge(records_forInput, symbolRecords[['name', 'numMissingBusinessDays']], how='outer', on='name')
    
    ## select just the records that don't match
    merged = merged.loc[merged['numMissingBusinessDays_x'] != merged['numMissingBusinessDays_y']].reset_index(drop=True)  

    if not merged.empty:
        ## drop unneeded col's and rename cols following db table conventions 
        merged.drop(inplace=True, columns=['numMissingBusinessDays_y'])
        merged.rename(columns={'numMissingBusinessDays_x':'numMissingBusinessDays'}, inplace=True)
        
        ## save to db replacing existing (outdated) records 
        merged.to_sql(f"{lookupTableName}", db._connectToDb(), index=False, if_exists='replace')
    

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

#updateRecords()
refreshLookupTable()

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