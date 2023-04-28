"""
Author: Rachit Shankar 
Date: February, 2022

PURPOSE
----------
Maintains a local database of historical pricing data for a list of specified symbols

VARIABLES
----------
# The LIST of symbols to be tracked 
tickerFilepath = 'tickerList.csv'

# The [database]] where pricing data is stored
'historicalData_index.db'

# The data sources for historcal data 
IBKR

"""
from ib_insync import *
from matplotlib.pyplot import axis
from numpy import histogram, indices, true_divide

from pytz import timezone, utc

from pathlib import Path
from requests.exceptions import HTTPError
from rich import print
from urllib.error import HTTPError, URLError

import datetime
import sqlite3 
import pandas as pd
import re
import time

import interface_ibkr as ib
import interface_localDb as db

## Default DB names 
_dbName_index = 'historicalData_index.db'

"""Tracked intervals for indices
    Note: String format is specific to ibkr"""
intervals_index = ['1 min', '5 mins', '15 mins', '30 mins', '1 day']

## global vars
_indexList = ['VIX', 'VIX3M', 'VVIX']
_tickerFilepath = 'tickerList.csv'

"""
######################################################

##################  Lambda functions for dataframe processing 

######################################################
"""
## add a space between num and alphabet
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
Root function that will check the the watchlist and update all records in the local db
--
inputs: 
    tickerlist.csv -> list of tickers to keep track of(Note: only for adding new symbols 
        to keep track of. existing records will be kept up to date by default) 
    updateThresholdDays.int -> only updates records that are older than this number of days
"""
def updateRecords(updateThresholdDays = 5):

    #### check for new tickers 
    # read in tickerlist.txt
    symbolList = pd.read_csv(_tickerFilepath)

    ## convert to dataframe, and cleanup  
    symbolList = pd.DataFrame(symbolList.columns).reset_index(drop=True)
    symbolList.rename(columns={0:'symbol'}, inplace=True)
    symbolList['symbol'] = symbolList['symbol'].str.strip(' ').str.upper()
    symbolList.sort_values(by=['symbol'], inplace=True)

    with db.sqlite_connection(_dbName_index) as conn:
        # merge into master records list 
        records = db.getRecords(conn)

        if not records.empty: ## if database contains some records, check if any need to be updated
            symbolsWithOutdatedData = records.loc[records['daysSinceLastUpdate'] >= updateThresholdDays]
            newlyAddedSymbols = symbolList[~symbolList['symbol'].isin(records['symbol'])]

        try:
            ibkr = ib.setupConnection()
        except:
            print('[red] Could not connect to IBKR[/red]')
            return
        
        # update history in local DB 
        updateRecordHistory(ibkr, records, symbolsWithOutdatedData, newlyAddedSymbols)
        
        updatePreHistoricData(ibkr)

        if ibkr: ibkr.disconnect()

        # print updated records
        updatedRecords = db.getRecords(conn)

    updatedRecords['numYearsOfHistory'] = updatedRecords.apply(lambda x: _countWorkdays(pd.to_datetime(x['firstRecordDate']), pd.to_datetime(x['lastUpdateDate']))/260, axis=1)
    updatedRecords.drop(columns=['firstRecordDate', 'name'], inplace=True)

    print('[green]---------------------------------- CURRENT RECORDS ----------------------------------[/green]')
    print(updatedRecords)
    print('[green]-------------------------------------------------------------------------------------[/green]')

"""
Updates record history handling the following scenarios:
    1. New symbols added to tickerlist.csv
    2. Existing symbols in tickerlist.csv that have not been updated in a while
    3. Existing symbols in tickerlist.csv that have missing intervals
 
"""
def updateRecordHistory(ibkr, records, indicesWithOutdatedData= pd.DataFrame(), newlyAddedIndices  = pd.DataFrame()):
    print('checking if records records need updating')
    # initialize connection object as empty until we need it
    conn = ''

    ## get a list of missing intervals if any 
    missingIntervals = pd.DataFrame()
    missingIntervals = getMissingIntervals(records, type='index')
    
    ## if we have any missing data to update, establish connection with IBKR and local db
    if (not indicesWithOutdatedData.empty) or ( not newlyAddedIndices.empty) or (len(missingIntervals) > 0):
        print('[yellow]Updates pending...[/yellow]\n')

        ## connect with local DB 
        try:
            print('[yellow] Connecting with the local DB..."%s"[/yellow]'%(_dbName_index))
            conn = sqlite3.connect(_dbName_index)
            print('[green]  Success![/green]\n')
        except:
            print('[red]  Could not connect with local DB "%s"![/red]\n'%(_dbName_index))
    
    ##
    ## add records for symbols newly added to the watchlist 
    ##
    if not newlyAddedIndices.empty:
        print('\n[blue]%s new indicies found[/blue], adding to db...'%(newlyAddedIndices['symbol'].count()))

        for newIndex in newlyAddedIndices['symbol']:
            ## add records for each tracked interval 
            for _intvl in intervals_index:
                
                ## set lookback based on interval
                if ( _intvl in ['5 mins', '15 mins']):
                    lookback = 80
            
                elif (_intvl in ['30 mins', '1 day']):
                    lookback = 300
                
                elif (_intvl in ['1 min']):
                    lookback = 15
                
                ## get history from ibkr 
                print('Adding %s - %s interval - %s day lookback'%(newIndex, _intvl, lookback))
                history = ib.getBars(ibkr, symbol=newIndex,lookback='%s D'%(lookback), interval=_intvl)
                
                ## get earliest record available for ibkr
                earliestTimestamp = ib.getEarliestTimeStamp(ibkr, symbol=newIndex)

                ## add interval column for easier lookup 
                history['interval'] = _intvl.replace(' ', '')
                db.saveHistoryToDB(history, conn, earliestTimestamp=earliestTimestamp)

                print(' [green]Success![/green] New record Added for %s-%s..from %s to %s'%(newIndex, _intvl, history['date'].min(), history['date'].max()))

    ##
    ## update symbols with outdated records 
    ##
    if not indicesWithOutdatedData.empty:
        print('\n[yellow]Outdated records found. Updating...[/yellow]')
        pd.to_datetime(indicesWithOutdatedData['lastUpdateDate'], format='ISO8601')

        ## regex to add a space between any non-digit and digit (adds a space to interval column)
        indicesWithOutdatedData['interval'].apply(lambda x: re.sub(r'(?<=\d)(?=[a-z])', ' ', x))

        # Iterate through records with missing data and update the local 
        # database with the latest available data from ibkr
        for index, row in indicesWithOutdatedData.iterrows():            
            ## Add column with number of business days that need updating (curr date - last record)
            
            ## get history from ibkr 
            history = ib.getBars(ibkr, symbol=row['symbol'], lookback='%s D'%(row['daysSinceLastUpdate']), interval=row['interval']) 
            
            ## add interval column for easier lookup 
            history['interval'] = row['interval'].replace(' ', '')
            history['symbol'] = row['symbol']
            
            ## save history to db 
            db.saveHistoryToDB(history, conn)
            print('%s-%s...[green]updated![/green]\n'%(row['symbol'], row['interval']))

    ##
    ## update missing intervals if we have any 
    ##
    if len(missingIntervals) > 0: 
        print('[yellow]Some records have missing intervals, updating...[/yellow]')
        for item in missingIntervals:
            [_tkr, _intvl] = item.split('-')
            if ( _intvl in ['5 mins', '15 mins']):
                history = ib.getBars(ibkr, symbol=_tkr,lookback='100 D', interval=_intvl )
            
            elif ( _intvl in ['1 min']):
                history = ib.getBars(ibkr, symbol=_tkr,lookback='10 D', interval=_intvl )
            
            elif (_intvl in ['30 mins', '1 day']):
                history = ib.getBars(ibkr, symbol=_tkr,lookback='365 D', interval=_intvl )

            history['interval'] = _intvl.replace(' ', '')
            history['symbol'] = _tkr
            ## get earliest record available froms ibkr
            earliestTimestamp = ib.getEarliestTimeStamp(ibkr, symbol=_tkr)
            db.saveHistoryToDB(history, conn, earliestTimestamp=earliestTimestamp)

            print('[red]Missing interval[/red] %s-%s...[red]updated![/red]\n'%(_tkr, _intvl))
                
    else: 
        print('\n[green]Existing records are up to date...[/green]')
    
    # if a connection to the db is open, close it
    if conn:
        conn.close()
    
                
"""
Returns a list of symbol-interval combos that are missing from the local database 
----------
Params: 
records: [Dataframe] of getRecords() 
"""
def getMissingIntervals(records, type = 'stock'):
    
    numRecordsPerSymbol = records.groupby(by='symbol').count()

    # each symbol where count < interval.count
    symbolsWithMissingIntervals = numRecordsPerSymbol.loc[
        numRecordsPerSymbol['name'] < len(intervals_index)].reset_index()['symbol'].unique()

    ## find missing symbol-interval combos
    missingCombos = []
    for symbol in symbolsWithMissingIntervals:
        for interval in intervals_index:
            myRecord = records.loc[
                (records['symbol'] == symbol) & (records['interval'] == interval)
            ]
            if myRecord.empty:
                missingCombos.append(symbol+'-'+interval)
    
    ## return the missing symbol-interval combos
    return missingCombos


"""
Updates a chunk of pre-histric data for existing records  
__
Logic:
0. get records from the lookup table
1. Select records with numMissingBusinessDays > 5
2. Get history from ibkr
3. Save history to db
"""
def updatePreHistoricData(ibkr):
    print('')
    print('[yellow]Updating pre-history...[/yellow]')
    
    lookback = 30 # number of days to look back

    with db.sqlite_connection(_dbName_index) as conn:
        ## get the lookup table
        lookupTable = db.getLookup_symbolRecords(conn)

    # select records that have numMissingBusinessDays > 5 
    lookupTable = lookupTable.loc[lookupTable['numMissingBusinessDays'] > 1].reset_index(drop=True)
    
    lookupTable = lookupTable.loc[lookupTable['symbol'] != 'SPY'].reset_index(drop=True)
    
    if lookupTable.empty:
        print('No records to update')
        return
    #drop any rows where symbol='SPY'
    
    ## add a space in the interval column 
    lookupTable['interval'] = lookupTable.apply(lambda x: _addspace(x['interval']), axis=1)

    # loop thr each reccord in the lookup table
    for index, row in lookupTable.iterrows():
        ## get earliest timestamp from ibkr
        earliestAvailableTimestamp = ib.getEarliestTimeStamp(ibkr, row['symbol'])
        numIterations = 4 #number of subsequent calls to ibkr for the same sybol-interval combo

        ## set the lookback based on the history left in ibkr or the interval,
        ## whichever is the more limiting factor
        if (lookback > (row['firstRecordDate'] - earliestAvailableTimestamp).days):
            # set lookback to the number of days left in ibkr
            lookback = (row['firstRecordDate'] - earliestAvailableTimestamp).days
            numIterations = 1 # only need the one iteration
        elif row['interval'] == '1 min':
            lookback = 10
        else:
            lookback = 30

        ## manual throttling of api requests after the first record (index 0)
        if index > 0:
            print('Pausing before next round....%s-%s\n'%(row['symbol'], row['interval']))
            time.sleep(45)
        
        print('%s-%s[yellow]....Updating[/yellow]'%(row['symbol'], row['interval']))

        # initiate 'enddate from the last time history was updated, manually set hour 
        # to end of day so no data is missed (duplicates are handled later)
        endDate = row['firstRecordDate']-pd.offsets.BDay(1)
        endDate = endDate.replace(hour = 20)
        
        ## initiate the history datafram that will hold the retrieved bars 
        history = pd.DataFrame()

        i=0 # good ol' loop counter 
        while i < numIterations:
            i+=1
            ##exit while loop when lookback is larger than the avilable days in ibkr 
            if 0 > (endDate - earliestAvailableTimestamp).days:
                print('No more data available for %s-%s'%(row['symbol'], row['interval']))
                break
            ## concatenate history retrieved from ibkr 
            history = pd.concat([history, ib.getBars(ibkr, symbol=row['symbol'], lookback='%s D'%lookback, interval=row['interval'], endDate=endDate)], ignore_index=True)
            
            ## update enddate for the next iteration
            endDate = endDate - pd.offsets.BDay(lookback - 1)
            endDate = endDate.replace(hour = 20)
            
            ## manual throttling of api requests 
            #time.sleep(5)

        ## add interval column for easier lookup 
        history['interval'] = row['interval'].replace(' ', '')
        history['symbol'] = row['symbol']

        ## save history to db 
        with db.sqlite_connection(_dbName_index) as conn:
            db.saveHistoryToDB(history, conn)
        
        ## print logging info & reset history df 
        print(' %s-%s[green]...Updated![/green]'%(row['symbol'], row['interval']))
        print(' from %s to %s\n'%(history['date'].min(), history['date'].max()))
        history = pd.DataFrame()
        

"""
Refreshes the lookup_symbolRecords table with the latest data 
uses live api connection 
"""
def refreshLookupTable():
    print('\n[red]Refreshing lookup table...[/red]')
    
    ibkr = setupConnection() ## to get earliest available timestamt
    if not ibkr.isConnected():
        exit()
    lookupTableName = '00-lookup_symbolRecords' ##lookup table name in db 
    
    with db.sqlite_connection(_dbName_index) as conn:
        records = db.getRecords(conn) ## latest local records to compare against 

        ## select unique symbols and get the earliest available timestamp from ibkr 
        uniqueRecordSymbols = pd.DataFrame({'symbol':records['symbol'].unique()})
        uniqueRecordSymbols['earliestAvailableTimestamp'] = uniqueRecordSymbols['symbol'].apply(lambda x: ib.getEarliestTimeStamp(ibkr, x))

        ## merge with records table... 
        records_withEarliestAvailableDate = pd.merge(records, uniqueRecordSymbols, how='left', on='symbol')
        
        ## compute the number of missing business days
        records_withEarliestAvailableDate['numMissingBusinessDays'] =  records_withEarliestAvailableDate.apply(lambda x: _countWorkdays(x['earliestAvailableTimestamp'], x['firstRecordDate']), axis=1)
        
        ## select columns to match the records table in the db 
        records_forInput = records_withEarliestAvailableDate[['name', 'symbol', 'interval', 'firstRecordDate', 'numMissingBusinessDays']]

        ## format col names etc to abide by db naming & interval formatting conventions
        records_forInput = records_forInput.rename({'symbol':'symbol'}, axis=1)
        records_forInput['interval'] = records_forInput['interval'].str.replace(' ', '')

        # get the lookup table from db
        symbolRecords = db.getLookup_symbolRecords(conn)

        ## merge them 
        merged = pd.merge(records_forInput, symbolRecords[['name', 'numMissingBusinessDays']], how='outer', on='name')
        
        if not merged.empty:
            ## drop unneeded col's and rename cols following db table conventions 
            merged.drop(inplace=True, columns=['numMissingBusinessDays_y'])
            merged.rename(columns={'numMissingBusinessDays_x':'numMissingBusinessDays'}, inplace=True)
            
            ## save to db replacing existing (outdated) records 
            merged.to_sql(f"{lookupTableName}", conn, index=False, if_exists='replace')
        
    if ibkr: ibkr.disconnect()
    print('\n[green] Done![/green]')

"""
function that calls updatePreHistoricData over the course of a night, pausing for 5 minutes between each iteration 
"""
def bulkUpdate():
    # check if the time is between 10pm and 4am eastern time
    #if datetime.datetime.now().hour < 22 or datetime.datetime.now().hour > 4:
    #    print('[red]Not between 10pm and 4am eastern time. Exiting...[/red]')
    #    return
    #print('[yellow]Starting overnight update...[/yellow]')
    
    #while datetime.datetime.now().hour > 22 and datetime.datetime.now().hour < 4:

    ## connect to ibkr
    ibkr = setupConnection()

    if not ibkr.isConnected():
        print('[red]  Exiting!\n[/red]')
        return
    
    i=0
    while i < 5:
        i=i+1
        updatePreHistoricData(ibkr)
        refreshLookupTable()
        time.sleep(300)

refreshLookupTable()
updateRecords()
#bulkUpdate()