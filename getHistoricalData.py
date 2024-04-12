"""
Author: Rachit Shankar 
Date: February, 2022

PURPOSE
----------
Maintains a local database of historical ohlc data for a list of specified symbols

VARIABLES
----------
# The LIST of symbols to be tracked 
tickerFilepath = 'tickerList.csv'

# The database where pricing data is stored
'historicalData_index.db'

# The data sources for historcal data 
IBKR

"""
from ib_insync import *
from matplotlib.pyplot import axis
from sys import argv
from numpy import histogram, indices, true_divide

from pytz import timezone, utc

from pathlib import Path
from requests.exceptions import HTTPError
from rich import print
from urllib.error import HTTPError, URLError
from ib_insync import util as ibkrUtil

import config 
import datetime
import sqlite3 
import pandas as pd
import re
import time

import interface_ibkr as ib
import interface_localDb as db

######### SET GLOBAL VARS #########

_tickerFilepath = config.watchlist_main ## List of symbols to keep track of
_dbName_index = config.dbname_stock ## Default DB names 

intervals_index = config.intervals
_indexList = config._index

ibkrThrottleTime = 10 # minimum seconds to wait between api requests to ibkr

"""
######################################################

#### Lambda functions for dataframe cleanup 

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
    Returns dataframe of historical data read from given csv file
"""
def updateHistoryFromCSV(filepath, symbol, interval):
    pxHistory_raw = db.getHistoryFromCSV(filepath, symbol, interval)
    # convert date column to datetime
    pxHistory_raw['date'] = pd.to_datetime(pxHistory_raw['date'])
    with db.sqlite_connection(_dbName_index) as conn:
        db.saveHistoryToDB(pxHistory_raw, conn)

"""
     Loads vix futures data from futures data retreived from rwTools 
"""
def update_vix_futures_history_from_rwtools(filepath):
    raw_csv = pd.read_csv(filepath)
    # explicitly convert date column to datetime
    raw_csv['date'] = pd.to_datetime(raw_csv['date'], format='%Y-%m-%d')
    
    #get recs in db
    with db.sqlite_connection(config.dbname_futures) as conn:
        db_recs = db.getRecords(conn)
        # select only 1day vix 
        db_recs = db_recs.loc[(db_recs['interval'] == '1 day') & (db_recs['symbol'] == 'VIX')].copy()
        # convert to datetime
        db_recs['firstRecordDate'] = pd.to_datetime(db_recs['lastUpdateDate'], format='%Y-%m-%d')


    # for each unique expiry in the raw csv, construct tablename, and insert into the db
    for expiry in raw_csv['expiry'].unique():
        tablename = 'VIX_%s_1day'%(expiry.replace('-', ''))
        # check if table exists in db
        if tablename in db_recs['name'].tolist():
            print('%s [yellow]already exists in db, appending any missing data...[/yellow]'%(tablename))
            additionalHistory = raw_csv.loc[raw_csv['expiry'] == expiry].copy()
            additionalHistory['symbol'] = 'VIX'
            additionalHistory['interval'] = '1day'
            additionalHistory.rename(columns={'expiry':'lastTradeDate'}, inplace=True)
            additionalHistory['lastTradeDate'] = additionalHistory['lastTradeDate'].str.replace('-', '')
            # drop margin, type, exchange, currency, tick_size, contract_name, ticker
            additionalHistory.drop(columns=['margin', 'type', 'exchange', 'currency', 'tick_size', 'contract_name', 'ticker', 'point_value', 'open_interest' ], inplace=True)

            # for existing contracts, check if there is new data to add and add it 
            with db.sqlite_connection(config.dbname_futures) as conn:
                # get the earliest date saved in the db 
                lastDateSaved = db_recs.loc[db_recs['name'] == tablename]['firstRecordDate'].values[0]
                # get the earliest date in the raw csv 
                lastDateInRawCSV = raw_csv.loc[raw_csv['expiry'] == expiry]['date'].min()
                # convert 
                print(lastDateSaved)
                print(lastDateInRawCSV)
                print('\n')
                # if the earliest date in the raw csv is less than the earliest date saved in the db, add the missing data
                if lastDateInRawCSV < lastDateSaved:
                    # get the missing data 
                    #new_data = raw_csv.loc[(raw_csv['expiry'] == expiry) & (raw_csv['date'] < lastDateSaved)].copy()
                    # add the new data to the db 
                    db.saveHistoryToDB(additionalHistory.loc[(additionalHistory['date'] < lastDateSaved)].reset_index(drop=True)
                                       , conn
                                       , type='future'
                                       )
                    print('%s: [green]Added missing data to %s[/green]'%(datetime.datetime.now().strftime("%H:%M:%S"), tablename))
                    exit()
        else:
            history = raw_csv.loc[raw_csv['expiry'] == expiry].copy()
            # rename the expiry column to lastTradeDate
            history['symbol'] = 'VIX'
            history['interval'] = '1day'
            history.rename(columns={'expiry':'lastTradeDate'}, inplace=True)
            history['lastTradeDate'] = history['lastTradeDate'].str.replace('-', '')
            # drop margin, type, exchange, currency, tick_size, contract_name, ticker
            history.drop(columns=['margin', 'type', 'exchange', 'currency', 'tick_size', 'contract_name', 'ticker', 'point_value', 'open_interest' ], inplace=True)
            #db.saveHistoryToDB(history, conn, tablename=tablename)
            print('%s:[yellow] Adding %s to db[/yellow]'%( datetime.datetime.now().strftime("%H:%M:%S") ,tablename))
            print(history.tail(10))
            exit()
        # get the history for the current expiry
        # add symbol, interval

        # save to db
        #with db.sqlite_connection(_dbName_index) as conn:
        #    db.saveHistoryToDB(history, conn, tablename=tablename)
        #    print('Saved %s to db'%(tablename)) 
    pass

"""
Root function that will check the the watchlist and make sure all records are up to date
--
inputs: 
    tickerlist.csv -> list of tickers to keep track of(Note: only for adding new symbols 
        to keep track of. existing records will be kept up to date by default) 
    updateThresholdDays.int -> only updates records that are older than this number of days
"""
def updateRecords(updateThresholdDays = 1):

    #### check for new tickers 
    # read in tickerlist.txt
    symbolList = pd.read_csv(_tickerFilepath)

    ## convert to dataframe, and cleanup  
    symbolList = pd.DataFrame(symbolList.columns).reset_index(drop=True)
    symbolList.rename(columns={0:'symbol'}, inplace=True)
    symbolList['symbol'] = symbolList['symbol'].str.strip(' ').str.upper()
    symbolList.sort_values(by=['symbol'], inplace=True)

    # get record metadata from db
    with db.sqlite_connection(_dbName_index) as conn:
        records = db.getRecords(conn)

    if not records.empty: ## if database contains some records, check if any need to be updated
        symbolsWithOutdatedData = records.loc[records['daysSinceLastUpdate'] >= updateThresholdDays]
        newlyAddedSymbols = symbolList[~symbolList['symbol'].isin(records['symbol'])]

    if not (symbolsWithOutdatedData.empty or newlyAddedSymbols.empty):
        try:
            ibkr = ib.setupConnection()
        except:
            print('[red] Could not connect to IBKR[/red]')
            return

        # update history in local DB 
        updateRecordHistory(ibkr, records, symbolsWithOutdatedData, newlyAddedSymbols)

        # disconnect from ibkr
        if ibkr: ibkr.disconnect()
        
        # get updated records from db 
        with db.sqlite_connection(_dbName_index) as conn:
            updatedRecords = db.getRecords(conn)

        updatedRecords['numYearsOfHistory'] = updatedRecords.apply(lambda x: _countWorkdays(pd.to_datetime(x['firstRecordDate']), pd.to_datetime(x['lastUpdateDate']))/260, axis=1)
        updatedRecords.drop(columns=['firstRecordDate', 'name'], inplace=True)

"""
Updates record history handling the following scenarios:
    1. New symbols added to tickerlist.csv
    2. Existing symbols in tickerlist.csv that have not been updated in a while
    3. Existing symbols in tickerlist.csv that have missing intervals
 
"""
def updateRecordHistory(ibkr, records, indicesWithOutdatedData= pd.DataFrame(), newlyAddedIndices  = pd.DataFrame()):
    print('checking if records need updating...')
    # initialize connection object as empty until we need it

    ## get a list of missing intervals if any 
    missingIntervals = pd.DataFrame()
    missingIntervals = getMissingIntervals(records, type='index')
    
    ## add records for symbols newly added to the watchlist 
    if not newlyAddedIndices.empty:
        print('\n[blue]%s New symbols found in watchlist[/blue], adding to db...'%(newlyAddedIndices['symbol'].count()))

        for newIndex in newlyAddedIndices['symbol']:
            # set type 
            if newIndex in _indexList:
                type = 'index'
            else:
                type = 'stock'

            # get earliest datapoint available so we can save it to db 
            earliestTimestamp = ib.getEarliestTimeStamp(ibkr, ib.getContract(ibkr, newIndex, type))

            ## add records for each tracked interval 
            for _intvl in intervals_index:
                
                ## set lookback based on interval
                if ( _intvl in ['5 mins', '15 mins']):
                    lookback = 80
            
                elif (_intvl in ['30 mins', '1 day']):
                    lookback = 300
                
                elif (_intvl in ['1 min']):
                    lookback = 15
                
                else:
                    print('[red]Interval not supported![/red]')
                
                ## get history from ibkr 
                print('Adding %s - %s interval - %s day lookback'%(newIndex, _intvl, lookback))
                history = ib.getBars(ibkr, symbol=newIndex,lookback='%s D'%(lookback), interval=_intvl)

                ## add interval column for easier lookup 
                history['interval'] = _intvl.replace(' ', '')
                history['symbol'] = newIndex

                with db.sqlite_connection(_dbName_index) as conn:
                    db.saveHistoryToDB(history, conn, earliestTimestamp=earliestTimestamp)

                print(' [green]Success![/green] New record Added for %s-%s..from %s to %s\n'%(newIndex, _intvl, history['date'].min(), history['date'].max()))
            
                print('%s: [yellow]Pausing %ss before next record...[/yellow]\n'%(datetime.datetime.now().strftime("%H:%M:%S"), ibkrThrottleTime/5))
                time.sleep(ibkrThrottleTime/5)

    ## update symbols with outdated records 
    if not indicesWithOutdatedData.empty:
        print('\n[blue]Outdated records found. Updating...[/blue]\n')
        pd.to_datetime(indicesWithOutdatedData['lastUpdateDate'], format='ISO8601')
        indicesWithOutdatedData = indicesWithOutdatedData.sort_values(by=['symbol', 'interval']).reset_index(drop=True)

        ## regex to add a space between any non-digit and digit (adds a space to interval column)
        indicesWithOutdatedData['interval'].apply(lambda x: re.sub(r'(?<=\d)(?=[a-z])', ' ', x))

        # Iterate through records with missing data and update the local 
        # database with the latest available data from ibkr
        count = 1
        for index, row in indicesWithOutdatedData.iterrows():            
            # every multiple of 50 refresh the connection
            if count % config.ibkr_max_consecutive_calls == 0:
                ibkr = ib.refreshConnection(ibkr)
            ## get history from ibkr 
            history = ib.getBars(ibkr, symbol=row['symbol'], lookback='%s D'%(row['daysSinceLastUpdate']), interval=row['interval']) 
            ## add interval & symbol columns for easier lookup 
            history['interval'] = row['interval'].replace(' ', '')
            history['symbol'] = row['symbol']
            
            # get earliestTimeStamp 
            earliestTimestamp = ib.getEarliestTimeStamp_m(ibkr, symbol=row['symbol'])

            ## save history to db 
            with db.sqlite_connection(_dbName_index) as conn:
                db.saveHistoryToDB(history, conn, earliestTimestamp)
            
            count+=1
            print('%s: [yellow]Pausing %ss before next record...[/yellow]'%(datetime.datetime.now().strftime("%H:%M:%S"), ibkrThrottleTime/4))
            time.sleep(ibkrThrottleTime/4)

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
            
            elif (_intvl in ['30 mins', '1 day', '1 hour']):
                history = ib.getBars(ibkr, symbol=_tkr,lookback='365 D', interval=_intvl )

            elif (_intvl in ['1 month']):
                history = ib.getBars(ibkr, symbol=_tkr,lookback='2 Y', interval=_intvl )
                
            history['interval'] = _intvl.replace(' ', '')
            history['symbol'] = _tkr
            ## get earliest record available froms ibkr
            earliestTimestamp = ib.getEarliestTimeStamp(ibkr, symbol=_tkr)
            with db.sqlite_connection(_dbName_index) as conn:
                db.saveHistoryToDB(history, conn, earliestTimestamp=earliestTimestamp)

            print('[green]Missing interval[/red] %s-%s...[red]updated![/green]\n'%(_tkr, _intvl))

            print('%s: [yellow]\nPausing %ss before next record...[/yellow]\n'%(datetime.datetime.now().strftime("%H:%M:%S"), ibkrThrottleTime))
            time.sleep(ibkrThrottleTime)
                
    else: 
        print('\n[green]Existing records are up to date![/green]')
                
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
    print('%s: [yellow]Updating pre-history...\n[/yellow]'%(datetime.datetime.now().strftime("%H:%M:%S")))
    print('[green]----------------------------------------[/green]')
    
    lookback = 30 # number of days to look back

    # read in the lookup table 
    with db.sqlite_connection(_dbName_index) as conn:
        lookupTable = db.getLookup_symbolRecords(conn)
    # select records that are missing more than 2 business days of data
    lookupTable = lookupTable.loc[lookupTable['numMissingBusinessDays'] > 2].reset_index(drop=True)
    # exit if nothing to update
    if lookupTable.empty:
        print('[green]All historic data has been loaded![/green]')
        exit()
    # format lookupTable
    lookupTable['interval'] = lookupTable['interval'].apply(lambda x: _addspace(x))
    lookupTable.sort_values(by=['symbol'], inplace=True)
    # get earliest availeble timestamp for each symbol in the lookup table 
    uniqueSymbolsInLookupTable = pd.DataFrame({'symbol':lookupTable['symbol'].unique()})
    # set type column 
    uniqueSymbolsInLookupTable['type'] = uniqueSymbolsInLookupTable['symbol'].apply(
        lambda x: 'index' if x in _indexList else 'stock')
    
    uniqueSymbolsInLookupTable['earliestAvailableTimestamp'] = uniqueSymbolsInLookupTable.apply(lambda x: ib.getEarliestTimeStamp(ibkr, ib.getContractDetails(ibkr, x['symbol'], type = x['type'], delay=True)[0].contract), axis=1)

    # loop through each record in the lookup table
    for index, row in lookupTable.iterrows():
        ## set earliestAvailableTimestamp to the matching symbol in uniqueSymbolsInLookupTable
        earliestAvailableTimestamp = uniqueSymbolsInLookupTable.loc[uniqueSymbolsInLookupTable['symbol'] == row['symbol']]['earliestAvailableTimestamp'].values[0]
        
        # convert to datetime
        earliestAvailableTimestamp = pd.to_datetime(earliestAvailableTimestamp)
        numIterations = 4 #number of subsequent calls to ibkr for the same sybol-interval combo

        ## set the lookback based on the history left in ibkr or the interval,
        ## whichever is the more limiting factor
        if (lookback > (row['firstRecordDate'] - earliestAvailableTimestamp).days):
            # set lookback to the number of days left in ibkr
            lookback = (row['firstRecordDate'] - earliestAvailableTimestamp).days
            numIterations = 1 # only need the one iteration
        elif row['interval'] == '1 min':
            lookback = 3
        elif row['interval'] == '1 day':
            lookback = 100
        else:
            lookback = 30

        # initiate 'enddate from the last time history was updated, manually set hour 
        # to end of day so no data is missed (duplicates are handled later)
        endDate = row['firstRecordDate']#-pd.offsets.BDay(1)
        
        ##exit while loop when lookback is larger than the avilable days in ibkr 
        if 0 > (endDate - earliestAvailableTimestamp).days:
            print('[red]No more data available[/red] for %s-%s'%(row['symbol'], row['interval']))
            # update the lookup table 
            with db.sqlite_connection(_dbName_index) as conn:
                # set type 
                if row['symbol'] in _indexList:
                    type = 'index'
                else:
                    type = 'stock'
                tablename = row['symbol']+'_'+type+'_'+row['interval'].replace(' ', '')
                db._updateLookup_symbolRecords(conn, tablename, type, earliestAvailableTimestamp)
                print('\n')
            continue
        
        print('%s: Updating %s-%s, %s days from %s'%(datetime.datetime.now().strftime("%H:%M:%S"), row['symbol'], row['interval'], lookback*numIterations, endDate))
        print('%s, Earliest available datapoint: %s'%(row['symbol'], earliestAvailableTimestamp))

        ## initiate the history datafram that will hold the retrieved bars 
        history = pd.DataFrame()

        i=0 # call ibkr numIterations times 
        while i < numIterations:
            i+=1
            
            print('%s: [yellow]Pausing %ss before next ibkr call...[/yellow]'%(datetime.datetime.now().strftime("%H:%M:%S"), ibkrThrottleTime/30))
            ## manual throttling of api requests 
            time.sleep(ibkrThrottleTime/6)
            
            # handle error on ib.getbars()
            try:
                currentIterationHistoricalBars = ib.getBars(ibkr, symbol=row['symbol'], lookback='%s D'%lookback, interval=row['interval'], endDate=endDate)
            except:
                print('[red]  Error retrieving data from IBKR![/red]\n')
                continue

            # skip to next if history is empty
            if currentIterationHistoricalBars.empty:
                i=numIterations ## quit out of the while loop since there is no data left
                continue
                        
            ## concatenate history retrieved from ibkr 
            history = pd.concat([history, currentIterationHistoricalBars], ignore_index=True)
            
            ## update enddate for the next iteration
            endDate = endDate - pd.offsets.BDay(lookback - 1)
            endDate = endDate.replace(hour = 20)
            


        # stop updating the symbol as all history has been saved
        if history.empty:

            # update the lookup table for the last time 
            with db.sqlite_connection(_dbName_index) as conn:
                # set type 
                if row['symbol'] in _indexList:
                    type = 'index'
                else:
                    type = 'stock'
                tablename = row['symbol']+'_'+type+'_'+row['interval'].replace(' ', '')

                # set the earliest available timestamp to the min(date) saved in the db
                history = db.getPriceHistory(conn, row['symbol'], row['interval'].replace(' ', ''))

                # set earliest timestamp to the min(date) record in the db 
                earliestAvailableTimestamp = db._getFirstRecordDate(row, conn)

                db._updateLookup_symbolRecords(conn, tablename, type, earliestAvailableTimestamp)
                print('\n')
            continue

        ## add interval column for easier lookup 
        history['interval'] = row['interval'].replace(' ', '')
        history['symbol'] = row['symbol']

        ## save history to db 
        with db.sqlite_connection(_dbName_index) as conn:
            db.saveHistoryToDB(history, conn, earliestAvailableTimestamp)

        ## manual throttling: pause before requesting next set of data
        print('%s: [yellow]Pausing %ss before next record...[/yellow]\n'%(datetime.datetime.now().strftime("%H:%M:%S"), ibkrThrottleTime))
        time.sleep(ibkrThrottleTime)

def refreshLookupTable(ibkr, dbname):
    print('%s: [red] Refreshing lookup table...[/red]'%(datetime.datetime.now().strftime("%H:%M:%S")))
    
    lookupTableName = config.lookupTableName ##lookup table name in db
    
    with db.sqlite_connection(dbname) as conn:
        ## get a fresh read of local db records & clean them up
        records = db.getRecords(conn) 

        records['interval'] = records['interval'].str.replace(' ', '')
        records.rename(columns={'type/expiry':'type'}, inplace=True)

        # get the lookup table that is currently saved in the db 
        lookupTableRecords = db.getLookup_symbolRecords(conn)

        # manage case where lookup table is empty
        if lookupTableRecords.empty:
            allRecords = records[['name', 'symbol', 'type', 'interval', 'lastUpdateDate', 'daysSinceLastUpdate', 'firstRecordDate']].copy()
            allRecords['numMissingBusinessDays'] = 10
        else:
            allRecords = pd.merge(lookupTableRecords, records, how='right', on=['name', 'symbol', 'interval', 'type'])
            allRecords.drop(columns=['firstRecordDate_x'], inplace=True)
            allRecords.rename(columns={'firstRecordDate_y':'firstRecordDate'}, inplace=True)

        # select records from allRecords where (numMissingBusinessData > 1 or NaN) 
        lookupRecords = allRecords.loc[(allRecords['numMissingBusinessDays'] > 1) | (allRecords['numMissingBusinessDays'].isnull())].reset_index(drop=True)  

        ## get earliestTimeStamp for each unique symbol in the lookup list
        lookupRecords_uniqueSymbols = pd.DataFrame({'symbol':lookupRecords['symbol'].unique()})
        lookupRecords_uniqueSymbols['earliestAvailableTimestamp'] = lookupRecords_uniqueSymbols['symbol'].apply(
            lambda x: ib.getEarliestTimeStamp(ibkr, 
                                              ib.getContractDetails(ibkr,x, delay=True)[0].contract))
        ## merge the earliest availabe timestamp from ibkr with the records table
        records_withEarliestAvailableDate = pd.merge(lookupRecords, lookupRecords_uniqueSymbols, how='left', on='symbol')
        
        # newdr = records_withEarliestAvailableDate where earliestAvailableTimestamp is not null
        records_withNewEarliestAvailableDate = records_withEarliestAvailableDate.loc[records_withEarliestAvailableDate['earliestAvailableTimestamp'].notnull()]
        
        ## compute the number of missing business days
        records_withNewEarliestAvailableDate['numMissingBusinessDays'] = records_withNewEarliestAvailableDate.apply(lambda x: _countWorkdays(x['earliestAvailableTimestamp'], x['firstRecordDate']), axis=1)
        
        # limit just to columns name, numMissingBusinessDays
        records_withNewEarliestAvailableDate = records_withNewEarliestAvailableDate[['name', 'numMissingBusinessDays']]
        # drop numMissingBusinessDays from allRecords
        allRecords.drop(columns=['numMissingBusinessDays'], inplace=True)
        
        
        # merge allRecords and records_withNewEarliestAvailableDate 
        updatedRecords = pd.merge(allRecords, records_withNewEarliestAvailableDate, how='left', on='name')

        ## prepare data for insertion into db  
        records_forInput = updatedRecords[['name', 'symbol', 'type', 'interval', 'numMissingBusinessDays', 'firstRecordDate']].copy()
        
        # drop rows with nan in numMissingBusinessDays
        records_forInput.dropna(subset=['numMissingBusinessDays'], inplace=True)

        if not lookupTableRecords.empty:
            # update the lookuptable with input records 
            lookupTableRecords.set_index('name', inplace=True)
            records_forInput.set_index('name', inplace=True)

            lookupTableRecords.update(records_forInput)

            # add records from records_forInput that are not in lookupTableRecords
            lookupTableRecords = pd.concat([lookupTableRecords, records_forInput.loc[~records_forInput.index.isin(lookupTableRecords.index)]])
            
            ## save to db replacing existing (outdated) records 
            lookupTableRecords.to_sql(f"{lookupTableName}", conn, index=True, if_exists='replace')
            print('\n[green] Done![/green]')
        
        else: # update db lookup table with input records
            if records_forInput.empty:
                print('No new records to add to lookup table')
                return
            print(' Lookup table is empty, initializing with db records...')
            # split name column into symbol, type, interval
            records_forInput[['symbol', 'type', 'interval']] = records_forInput['name'].str.split('_', expand=True)
            # switch columns 5 and 6 
            records_forInput = records_forInput[['name', 'symbol', 'type', 'interval', 'numMissingBusinessDays', 'firstRecordDate']]
            # save to db
            records_forInput.to_sql(f"{lookupTableName}", conn, index=False, if_exists='replace')
            print('%s: [green] Done![/green]%(datetime.datetime.now().strftime("%H:%M:%S"))')
"""
function that calls updatePreHistoricData over the course of a night, pausing for 5 minutes between each iteration 
"""
def bulkUpdate():    
    i = 0
    numcycles=15
    cycletime_= [0] * (numcycles+1)
    while i < numcycles:
        ## connect to ibkr
        ibkr = ib.setupConnection()

        if not ibkr.isConnected():  
            print('[red]  Exiting!\n[/red]')
            return
        starttime = datetime.datetime.now()
        refreshLookupTable(ibkr, _dbName_index)
        
        ibkr = ib.refreshConnection(ibkr)        
        updatePreHistoricData(ibkr)
        ibkr.disconnect()

        cycletime = (datetime.datetime.now() - starttime).seconds
        cycletime_[i+1] = cycletime
        print('Bulk update #%s with cycle time: %.2f'%(i+1, cycletime))
        print('Average cycle time: %s'%(sum(cycletime_)/len(cycletime_) ))
        print('[green]----------------------------------------[/green]')
        if i != 14:
            print('%s: Pausing 5 mins before next round\n'%(datetime.datetime.now().strftime("%H:%M:%S")))
            time.sleep(ibkrThrottleTime * 10)
        i=i+1

# if more than 0 args
if len(argv) > 1:
    if argv[1] == 'csv':
        #update_vix_futures_history_from_rwtools(config.dbname_rwtools_futures_vix_csv)
        # if no arg 2 , print 
        updateHistoryFromCSV('data/FXB.csv', 'FXB', '1day')
        #print('error 11 - no csv file specified')
else:
    ## update existing records after EST market close or on weekends
    if (datetime.datetime.today().weekday() < 5 and datetime.datetime.now().hour >= 17) or (datetime.datetime.today().weekday() >= 5): #or (datetime.datetime.today().weekday() < 5 and datetime.datetime.now().hour<= 9):
        updateRecords()
    bulkUpdate()
