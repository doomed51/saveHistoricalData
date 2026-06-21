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
import numpy as np
import re
import time

import interface_ibkr as ib
import interface_localDb_old as db
import interface_localDB as db_new

######### SET GLOBAL VARS #########

_tickerFilepath = config.watchlist_main ## List of symbols to keep track of
_dbName_index = config.dbname_stock ## Default DB names 

intervals_index = config.intervals
_index = config._index

# load currency lookup table from config 
currency_mapping = config.currency_mapping

# load exchange lookup table from config
exchange_mapping = config.exchange_mapping_stocks

ibkrThrottleTime = 10 # minimum seconds to wait between api requests to ibkr

records = pd.DataFrame()

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
def _countWorkdays_old(startDate, endDate, excluded=(6,7)):
    # convert to pd.datetime if not already 
    if not isinstance(startDate, pd.Timestamp):
        startDate = pd.to_datetime(startDate)
    if not isinstance(endDate, pd.Timestamp):
        endDate = pd.to_datetime(endDate)

    ## handle negatives when endDate > startDate 
    if startDate > endDate:
        return (len(pd.bdate_range(endDate, startDate)) * -1)
    else:
        return len(pd.bdate_range(startDate, endDate))

def _countWorkdays(startDate, endDate, excluded=(6,7)):
    # normalize array-like inputs: accept scalar, Series, DatetimeIndex, list, ndarray
    def _scalar_or_none(v):
        if isinstance(v, (pd.DatetimeIndex, pd.Series, list, tuple, np.ndarray)):
            if len(v) == 0:
                return None
            return v[0]
        return v

    startDate = _scalar_or_none(startDate)
    endDate = _scalar_or_none(endDate)

    # coerce to Timestamp (returns NaT on failure)
    start = pd.to_datetime(startDate, errors='coerce')
    end = pd.to_datetime(endDate, errors='coerce')

    if pd.isna(start) or pd.isna(end):
        return 0  # or return np.nan if you prefer

    # convert to date-only numpy datetime64[D]
    s = np.datetime64(start.date())
    e = np.datetime64(end.date())

    # numpy.busday_count counts business days in [start, end) so add 1 day to end to make it inclusive
    if s <= e:
        num = np.busday_count(s, e + np.timedelta64(1, 'D'))
    else:
        # mirror your original behavior that returned negative counts when startDate > endDate
        num = -int(np.busday_count(e, s + np.timedelta64(1, 'D')))

    return int(num)

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
    # cleanup  
    symbolList = pd.DataFrame(symbolList.columns).reset_index(drop=True)
    symbolList.rename(columns={0:'symbol'}, inplace=True)
    symbolList['symbol'] = symbolList['symbol'].str.strip(' ').str.upper()
    symbolList.sort_values(by=['symbol'], inplace=True)

    # get record metadata from db
    with db.sqlite_connection(_dbName_index) as conn:
        # records = db.getRecords(conn)
        records = db_new.getRecords(conn)
    
    if not records.empty: ## if database contains some records, check if any need to be updated
        symbolsWithOutdatedData = records.loc[records['daysSinceLastUpdate'] >= updateThresholdDays]
        newlyAddedSymbols = symbolList[~symbolList['symbol'].isin(records['symbol'])]
        
        # remove delisted symbols
        newlyAddedSymbols = newlyAddedSymbols.loc[~newlyAddedSymbols['symbol'].isin(config.delisted_symbols)] 
        symbolsWithOutdatedData = symbolsWithOutdatedData.loc[~symbolsWithOutdatedData['symbol'].isin(config.delisted_symbols)] 

    
    if (not symbolsWithOutdatedData.empty or not newlyAddedSymbols.empty):
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
        # with db.sqlite_connection(_dbName_index) as conn:
        #     updatedRecords = db.getRecords(conn)

        # updatedRecords['numYearsOfHistory'] = updatedRecords.apply(lambda x: _countWorkdays(pd.to_datetime(x['firstRecordDate']), pd.to_datetime(x['lastUpdateDate']))/260, axis=1)
        # updatedRecords.drop(columns=['firstRecordDate', 'name'], inplace=True)

def updateRecordHistory(ibkr, records, indicesWithOutdatedData= pd.DataFrame(), newlyAddedIndices  = pd.DataFrame()):
    """
    Updates record history handling the following scenarios:
        1. New symbols added to tickerlist.csv
        2. Existing symbols in tickerlist.csv that have not been updated in a while
        3. Existing symbols in tickerlist.csv that have missing intervals
    
    """
    print('%s: Checking if records need updating...'%(datetime.datetime.now().strftime("%H:%M:%S")))
    # initialize connection object as empty until we need it

    ## get a list of missing intervals if any 
    missingIntervals = pd.DataFrame()
    missingIntervals = getMissingIntervals(records, type='index')

    # build a cache for earliest available timestamps to avoid redundant calls to ibkr for duplicate symbols across intervals
    earliestAvailableTimestamps_cache = {}
    
    ## add records for symbols newly added to the watchlist 
    if not newlyAddedIndices.empty:
        print('\n[blue]%s New symbols found in watchlist[/blue], adding to db...'%(newlyAddedIndices['symbol'].count()))

        for newIndex in newlyAddedIndices['symbol']:
            # set type 
            if newIndex in _index:
                type = 'index'
            else:
                type = 'stock'

            # get earliest datapoint available so we can save it to db 
            earliestTimestamp = ib.getEarliestTimeStamp(ibkr, get_contract(newIndex, type))
            if not earliestTimestamp: 
                continue 

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

                # if history is empty, skip to next interval
                if history.empty:
                    print('%s: No data available for %s-%s'%(datetime.datetime.today().strftime('%H:%M:%S'),newIndex, _intvl))
                    continue

                ## add interval column for easier lookup 
                history['interval'] = _intvl.replace(' ', '')
                history['symbol'] = newIndex
                with db.sqlite_connection(_dbName_index) as conn:
                    db.saveHistoryToDB(history, conn, earliestTimestamp=earliestTimestamp)

                print(' [green]Success![/green] New record Added for %s-%s..from %s to %s\n'%(newIndex, _intvl, history['date'].min(), history['date'].max()))
            
                # print('%s: [yellow]Pausing %.2fs before next record...[/yellow]\n'%(datetime.datetime.now().strftime("%H:%M:%S"), ibkrThrottleTime/2))
                # time.sleep(ibkrThrottleTime/2)

    ## update symbols with outdated records 
    if not indicesWithOutdatedData.empty:
        print('%s: [yellow]Outdated records found. Updating...[/yellow]'%(datetime.datetime.now().strftime("%H:%M:%S")))
        pd.to_datetime(indicesWithOutdatedData['lastUpdateDate'], format='ISO8601')
        indicesWithOutdatedData = indicesWithOutdatedData.sort_values(by=['symbol', 'interval']).reset_index(drop=True)

        ## regex to add a space between any non-digit and digit (adds a space to interval column)
        indicesWithOutdatedData['interval'].apply(lambda x: re.sub(r'(?<=\d)(?=[a-z])', ' ', x))

        # Iterate through records with missing data and update the local 
        # database with the latest available data from ibkr
        count = 1
        for index, row in indicesWithOutdatedData.iterrows():
            # normalize interval formatting (e.g., "1day" -> "1 day")
            interval = str(row['interval']).strip()

            # choose max lookback per interval
            if interval in ['1 min', '5 mins', '15 mins']:
                max_lb_days = 15
            elif interval in ['30 mins', '1 day']:
                max_lb_days = 300
            else:
            # default to original behavior for other intervals
                max_lb_days = int(row['daysSinceLastUpdate'])

            total_days = int(row['daysSinceLastUpdate']) if pd.notna(row['daysSinceLastUpdate']) else 0
            if total_days <= 0:
                continue

            # gather all chunks before saving
            all_chunks = []
            remaining = total_days
            endDate = None

            # earliestTimeStamp for DB save
            if row['symbol'] in earliestAvailableTimestamps_cache:
                earliestTimestamp = earliestAvailableTimestamps_cache[row['symbol']]
            else:
                earliestTimestamp = ib.getEarliestTimeStamp(ibkr, get_contract(symbol=row['symbol'], type='index' if row['symbol'] in _index else 'stock'))
                earliestAvailableTimestamps_cache[row['symbol']] = earliestTimestamp
            if not earliestTimestamp: 
                continue 

            while remaining > 0:
            # refresh connection every N calls
                if count % config.ibkr_max_consecutive_calls == 0:
                    ibkr = ib.refreshConnection(ibkr)

                chunk_days = min(max_lb_days, remaining)

                try:
                    history_chunk = ib.getBars(
                        ibkr,
                        symbol=row['symbol'],
                        lookback=f'{chunk_days} D',
                        interval=interval,
                        endDate=endDate
                    )
                except Exception:
                    history_chunk = pd.DataFrame()

                if history_chunk.empty:
                    print('%s: No data available for %s-%s' % (datetime.datetime.today().strftime('%H:%M:%S'), row['symbol'], interval))
                    break
                all_chunks.append(history_chunk)

                # prepare next chunk endDate to go further back without gaps
                earliest_in_chunk = pd.to_datetime(history_chunk['date'].min(), errors='coerce')
                if pd.isna(earliest_in_chunk):
                    break
                endDate = earliest_in_chunk #- pd.Timedelta(minutes=1)

                remaining -= chunk_days
                count += 1

                # if remaining > 0:
                #     print('%s: [yellow]Pausing %.2fs before next record...[/yellow]' % (datetime.datetime.now().strftime("%H:%M:%S"), ibkrThrottleTime/4))
                #     time.sleep(ibkrThrottleTime/4)

            if not all_chunks:
                continue

            history = pd.concat(all_chunks, ignore_index=True)

            # add interval & symbol columns for easier lookup
            history['interval'] = interval.replace(' ', '')
            history['symbol'] = row['symbol']
            history.sort_values(by='date', inplace=True)
            # save history to db
            with db.sqlite_connection(_dbName_index) as conn:
                db.saveHistoryToDB(history, conn, earliestTimestamp)
            
            # sleep before next record
            # print('%s: [yellow]Pausing %.2fs before next record...[/yellow]\n'%(datetime.datetime.now().strftime("%H:%M:%S"), ibkrThrottleTime/2))
            # time.sleep(ibkrThrottleTime/2)

    ##
    ## update missing intervals if we have any 
    ##
    if len(missingIntervals) > 0: 
        print('%s: [yellow]Some records have missing intervals, updating...[/yellow]'%(datetime.datetime.now().strftime("%H:%M:%S")))
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
            
            if history.empty:
                print('%s: [yellow]No data available for %s-%s[/yellow]'%(datetime.datetime.now().strftime("%H:%M:%S"), _tkr, _intvl))
                continue

            history['interval'] = _intvl.replace(' ', '')
            history['symbol'] = _tkr
            ## get earliest record available froms ibkr
            earliestTimestamp = ib.getEarliestTimeStamp(ibkr, ib.getContract(ibkr, symbol=_tkr))
            if not earliestTimestamp: 
                continue 
            with db.sqlite_connection(_dbName_index) as conn:
                db.saveHistoryToDB(history, conn, earliestTimestamp=earliestTimestamp)

            print('%s: [green]Missing interval %s-%s...updated![/green]'%(datetime.datetime.now().strftime("%H:%M:%S"), _tkr, _intvl))

            # print('%s: [yellow]Pausing %.2fs before next record...[/yellow]\n'%(datetime.datetime.now().strftime("%H:%M:%S"), ibkrThrottleTime))
            # time.sleep(ibkrThrottleTime)
                
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

def updatePreHistoricData(ibkr):
    """
    Updates a chunk of pre-histric data for existing records  
    __
    Logic:
    0. get records from the lookup table
    1. Select records with numMissingBusinessDays > 5
    2. Get history from ibkr
    3. Save history to db
    """
    print('%s: [yellow]Updating pre-history...\n[/yellow]'%(datetime.datetime.now().strftime("%H:%M:%S")))
    print('[green]----------------------------------------[/green]')

    # read in the lookup table
    with db.sqlite_connection(_dbName_index) as conn:
        lookupTable = db.getLookup_symbolRecords(conn)

    # select records still missing history; include null metadata for newly discovered rows
    lookupTable = lookupTable.loc[
        (lookupTable['numMissingBusinessDays'] > 2) | (lookupTable['numMissingBusinessDays'].isnull())
    ].reset_index(drop=True)
    lookupTable = lookupTable.loc[~lookupTable['symbol'].isin(config.delisted_symbols)]

    if lookupTable.empty:
        print('[green]All historic data has been loaded![/green]')
        return

    lookupTable['interval'] = lookupTable['interval'].apply(lambda x: _addspace(x))
    lookupTable['firstRecordDate'] = pd.to_datetime(lookupTable['firstRecordDate'], errors='coerce')
    lookupTable = lookupTable.loc[lookupTable['firstRecordDate'].notnull()].copy()
    lookupTable.sort_values(by=['symbol', 'interval'], inplace=True)

    if lookupTable.empty:
        print('[yellow]No valid firstRecordDate values found in lookup table.[/yellow]')
        return

    # cache earliest-available timestamps so duplicate symbols across intervals call IBKR once
    symbol_earliest_ts_cache = {}
    ibkr_call_count = 0
    max_consecutive_calls = getattr(config, 'ibkr_max_consecutive_calls', 20)

    # process one symbol at a time; exhaust all intervals before moving to the next symbol
    for symbol, symbol_rows in lookupTable.groupby('symbol', sort=True):
        if symbol not in symbol_earliest_ts_cache:
            try:
                symbol_earliest_ts_cache[symbol] = ib.getEarliestTimeStamp(
                    ibkr,
                    get_contract(symbol=symbol, type='index' if symbol in _index else 'stock')
                )
            except Exception:
                symbol_earliest_ts_cache[symbol] = pd.NaT

        earliestAvailableTimestamp = pd.to_datetime(symbol_earliest_ts_cache[symbol], errors='coerce')
        if pd.isna(earliestAvailableTimestamp):
            print('[yellow]Could not determine earliest timestamp for %s. Skipping symbol.[/yellow]' % (symbol))
            continue

        print('%s: [cyan]Exhausting history for %s across %s interval(s)...[/cyan]' % (
            datetime.datetime.now().strftime("%H:%M:%S"),
            symbol,
            len(symbol_rows)
        ))

        for _, row in symbol_rows.iterrows():
            interval = row['interval']
            interval_no_space = interval.replace(' ', '')
            endDate = pd.to_datetime(row['firstRecordDate'], errors='coerce')

            if pd.isna(endDate):
                print('[yellow]Skipping %s-%s due to invalid firstRecordDate.[/yellow]' % (symbol, interval))
                continue

            if interval == '1 min':
                base_lookback_days = 5
            elif interval == '1 day':
                base_lookback_days = 100
            else:
                base_lookback_days = 30

            empty_retry_count = 0
            max_empty_retries = 3
            had_backward_progress = False

            print('%s: Updating %s-%s from %s back to %s' % (
                datetime.datetime.now().strftime("%H:%M:%S"),
                symbol,
                interval,
                endDate,
                earliestAvailableTimestamp
            ))

            while True:
                remaining_days = (endDate - earliestAvailableTimestamp).days
                if remaining_days <= 0:
                    print('[green]Exhausted available history for %s-%s[/green]' % (symbol, interval))
                    break

                lookback_days = min(base_lookback_days, remaining_days)
                if lookback_days <= 0:
                    break

                if ibkr_call_count and ibkr_call_count % max_consecutive_calls == 0:
                    ibkr = ib.refreshConnection(ibkr)

                try:
                    currentIterationHistoricalBars = ib.getBars(
                        ibkr,
                        symbol=symbol,
                        lookback='%s D' % lookback_days,
                        interval=interval,
                        endDate=endDate
                    )
                    ibkr_call_count += 1
                except Exception:
                    currentIterationHistoricalBars = pd.DataFrame()

                if currentIterationHistoricalBars.empty:
                    empty_retry_count += 1
                    if empty_retry_count <= max_empty_retries:
                        print('[yellow]Empty history for %s-%s. Retry %s/%s...[/yellow]' % (
                            symbol,
                            interval,
                            empty_retry_count,
                            max_empty_retries
                        ))
                        continue

                    print('[yellow]No data returned after retries for %s-%s. Moving to next interval.[/yellow]' % (
                        symbol,
                        interval
                    ))
                    break

                empty_retry_count = 0
                chunk_min_date = pd.to_datetime(currentIterationHistoricalBars['date'].min(), errors='coerce')
                if pd.isna(chunk_min_date):
                    print('[yellow]Invalid date values returned for %s-%s. Moving to next interval.[/yellow]' % (
                        symbol,
                        interval
                    ))
                    break

                # guard against non-progressing windows that can cause infinite loops
                if chunk_min_date >= endDate:
                    print('[yellow]No backward progress for %s-%s (chunk min: %s, endDate: %s). Moving to next interval.[/yellow]' % (
                        symbol,
                        interval,
                        chunk_min_date,
                        endDate
                    ))
                    break

                history_chunk = currentIterationHistoricalBars.copy()
                history_chunk['interval'] = interval_no_space
                history_chunk['symbol'] = symbol

                with db.sqlite_connection(_dbName_index) as conn:
                    db.saveHistoryToDB(history_chunk, conn, earliestAvailableTimestamp)

                had_backward_progress = True
                endDate = chunk_min_date

            # update lookup metadata with the actual first record date currently in DB
            with db.sqlite_connection(_dbName_index) as conn:
                type = 'index' if symbol in _index else 'stock'
                tablename = symbol+'_'+type+'_'+interval_no_space
                db_first_record_date = db._getFirstRecordDate(row, conn)
                if pd.isna(pd.to_datetime(db_first_record_date, errors='coerce')):
                    db_first_record_date = earliestAvailableTimestamp
                db._updateLookup_symbolRecords(conn, tablename, type, db_first_record_date)

            if had_backward_progress:
                print('[green]Completed backfill pass for %s-%s[/green]' % (symbol, interval))
            else:
                print('[yellow]No new history saved for %s-%s in this pass.[/yellow]' % (symbol, interval))

def _load_lookup_source_records(dbname):
    lookupTableName = config.lookupTableName
    with db.sqlite_connection(dbname) as conn:
        records = db_new.getRecords(conn)
        records['interval'] = records['interval'].str.replace(' ', '')
        records.rename(columns={'type/expiry': 'type'}, inplace=True)
        lookupTableRecords = db.getLookup_symbolRecords(conn)
    return lookupTableName, records, lookupTableRecords


def _exclude_delisted_symbols(records, lookupTableRecords):
    records = records.loc[~records['symbol'].isin(config.delisted_symbols)]
    lookupTableRecords = lookupTableRecords.loc[~lookupTableRecords['symbol'].isin(config.delisted_symbols)]
    return records, lookupTableRecords


def _build_all_lookup_records(records, lookupTableRecords):
    if lookupTableRecords.empty:
        print('%s: [yellow]Lookup table is empty, initializing with db records...[/yellow]' % (datetime.datetime.now().strftime("%H:%M:%S")))
        allRecords = records[['name', 'symbol', 'type', 'interval', 'lastUpdateDate', 'daysSinceLastUpdate', 'firstRecordDate']].copy()
        allRecords['numMissingBusinessDays'] = 10
    else:
        print('%s: [yellow]Lookup table exists, updating...[/yellow]' % (datetime.datetime.now().strftime("%H:%M:%S")))
        allRecords = pd.merge(lookupTableRecords, records, how='right', on=['name', 'symbol', 'interval', 'type'])
        allRecords.drop(columns=['firstRecordDate_x'], inplace=True)
        allRecords.rename(columns={'firstRecordDate_y': 'firstRecordDate'}, inplace=True)
    return allRecords


def _get_lookup_records_needing_update(allRecords):
    return allRecords.loc[
        (allRecords['numMissingBusinessDays'] > 1) | (allRecords['numMissingBusinessDays'].isnull())
    ].reset_index(drop=True)


def _fetch_earliest_available_timestamps(ibkr, lookupRecords):
    print('%s: [yellow]looking up records for %s symbols[/yellow]' % (datetime.datetime.now().strftime("%H:%M:%S"), len(lookupRecords['symbol'].unique())))
    lookupRecords_uniqueSymbols = pd.DataFrame({'symbol': lookupRecords['symbol'].unique()})
    print('%s: [yellow]Retrieving earliest available timestamps from IBKR...[/yellow]' % (datetime.datetime.now().strftime("%H:%M:%S")))

    call_count = 0
    for i, sym in enumerate(lookupRecords_uniqueSymbols['symbol']):
        print('%s: Looking up %s of %s: %s' % (datetime.datetime.now().strftime("%H:%M:%S"), i + 1, len(lookupRecords_uniqueSymbols), sym))
        if call_count and call_count % 20 == 0:
            ibkr = ib.refreshConnection(ibkr)

        try:
            contract = get_contract(symbol=sym)
            ts = ib.getEarliestTimeStamp(ibkr, contract)
            if not ts:
                continue
        except Exception:
            ts = pd.NaT

        lookupRecords_uniqueSymbols.loc[i, 'earliestAvailableTimestamp'] = ts
        call_count += 1
        # time.sleep(ibkrThrottleTime / 2)

    return lookupRecords_uniqueSymbols


def _apply_earliest_timestamp_updates(allRecords, lookupRecords, lookupRecords_uniqueSymbols):
    records_withEarliestAvailableDate = pd.merge(lookupRecords, lookupRecords_uniqueSymbols, how='left', on='symbol')
    records_withNewEarliestAvailableDate = records_withEarliestAvailableDate.loc[
        records_withEarliestAvailableDate['earliestAvailableTimestamp'].notnull()
    ]

    records_withNewEarliestAvailableDate['numMissingBusinessDays'] = records_withNewEarliestAvailableDate.apply(
        lambda x: _countWorkdays(x['earliestAvailableTimestamp'], x['firstRecordDate']), axis=1
    )
    records_withNewEarliestAvailableDate.loc[
        records_withNewEarliestAvailableDate['numMissingBusinessDays'] < 0,
        'numMissingBusinessDays'
    ] = -1

    records_withNewEarliestAvailableDate = records_withNewEarliestAvailableDate[['name', 'numMissingBusinessDays']]
    allRecords_withoutMissingDays = allRecords.drop(columns=['numMissingBusinessDays'])
    updatedRecords = pd.merge(allRecords_withoutMissingDays, records_withNewEarliestAvailableDate, how='left', on='name')
    return updatedRecords


def _prepare_lookup_records_for_input(updatedRecords):
    records_forInput = updatedRecords[['name', 'symbol', 'type', 'interval', 'numMissingBusinessDays', 'firstRecordDate']].copy()
    records_forInput.dropna(subset=['numMissingBusinessDays'], inplace=True)
    return records_forInput


def _prepare_lookup_records_for_input_without_ibkr(allRecords):
    records_forInput = allRecords[['name', 'symbol', 'type', 'interval', 'numMissingBusinessDays', 'firstRecordDate']].copy()
    # New lookup rows can have null missing-day metadata before IBKR sync; keep them update-eligible.
    records_forInput['numMissingBusinessDays'] = records_forInput['numMissingBusinessDays'].fillna(10)
    return records_forInput


def _save_lookup_table_records(dbname, lookupTableName, lookupTableRecords, records_forInput):
    if not lookupTableRecords.empty:
        lookupTableRecords.set_index('name', inplace=True)
        records_forInput.set_index('name', inplace=True)

        lookupTableRecords.update(records_forInput)
        lookupTableRecords = pd.concat([
            lookupTableRecords,
            records_forInput.loc[~records_forInput.index.isin(lookupTableRecords.index)]
        ])

        with db.sqlite_connection(dbname) as conn:
            lookupTableRecords.to_sql(f"{lookupTableName}", conn, index=True, if_exists='replace')
            print('%s:[green]  Done![/green]' % (datetime.datetime.now().strftime("%H:%M:%S")))
        return

    if records_forInput.empty:
        print('No new records to add to lookup table')
        return

    print(' Lookup table is empty, initializing with db records...')
    records_forInput[['symbol', 'type', 'interval']] = records_forInput['name'].str.split('_', expand=True)
    records_forInput = records_forInput[['name', 'symbol', 'type', 'interval', 'numMissingBusinessDays', 'firstRecordDate']]

    with db.sqlite_connection(dbname) as conn:
        records_forInput.to_sql(f"{lookupTableName}", conn, index=False, if_exists='replace')
        print('%s: [green]  Done![/green]' % (datetime.datetime.now().strftime("%H:%M:%S")))


def refreshLookupTable(ibkr, dbname, fetchEarliestTimestamps=True):
    print('%s: [red]Refreshing lookup table...[/red]' % (datetime.datetime.now().strftime("%H:%M:%S")))

    lookupTableName, records, lookupTableRecords = _load_lookup_source_records(dbname)
    
    records, lookupTableRecords = _exclude_delisted_symbols(records, lookupTableRecords)
    
    allRecords = _build_all_lookup_records(records, lookupTableRecords)

    if not fetchEarliestTimestamps:
        records_forInput = _prepare_lookup_records_for_input_without_ibkr(allRecords)
        _save_lookup_table_records(dbname, lookupTableName, lookupTableRecords, records_forInput)
        print('%s: [green]Lookup table refreshed without IBKR timestamp fetch.[/green]' % (datetime.datetime.now().strftime("%H:%M:%S")))
        return

    lookupRecords = _get_lookup_records_needing_update(allRecords)
    if lookupRecords.empty:
        print('%s: [green]Lookup table is up to date! No records to update.[/green]' % (datetime.datetime.now().strftime("%H:%M:%S")))
        return

    lookupRecords_uniqueSymbols = _fetch_earliest_available_timestamps(ibkr, lookupRecords)
    
    updatedRecords = _apply_earliest_timestamp_updates(allRecords, lookupRecords, lookupRecords_uniqueSymbols)
    
    records_forInput = _prepare_lookup_records_for_input(updatedRecords)
    
    _save_lookup_table_records(dbname, lookupTableName, lookupTableRecords, records_forInput)

def get_contract(symbol, type='stock', currency='USD', exchange=None):
    if symbol in currency_mapping:
        currency = currency_mapping[symbol]
    
    # change type to index if in index list
    if type != 'future': 
        if symbol in _index:
            type = 'index'
    
    # construct contract 
    try:
        if type == 'future':
            if not exchange:
                exchange = exchange_mapping.get(symbol, '')
            contract = Future(symbol=symbol, exchange=exchange, currency=currency, includeExpired=True)
        elif type == 'index':
            if not exchange:
                exchange = exchange_mapping.get(symbol, '')
            contract = Index(symbol, currency=currency, exchange=exchange)
        else: 
            contract = Stock(symbol, currency=currency, exchange= exchange_mapping.get(symbol, 'SMART'))
    except Exception as e:
        print('\nCould not retrieve contract details for...%s!'%(symbol))
        return pd.DataFrame() 
            
    return contract
"""
function that calls updatePreHistoricData over the course of a night, pausing for 5 minutes between each iteration 
"""
def bulkUpdate():    
    i = 0
    numcycles=15
    cycletime_= []
    print('\n[green]Starting bulk update of pre-historic data...[/green]')
    while i < numcycles:
        ## connect to ibkr
        ibkr = ib.setupConnection()

        if not ibkr.isConnected():  
            print('[red]  Exiting!\n[/red]')
            return
        starttime = datetime.datetime.now()
        
        # refreshLookupTable(ibkr, _dbName_index, fetchEarliestTimestamps=False)
        # ibkr = ib.refreshConnection(ibkr)        
        updatePreHistoricData(ibkr)
        ibkr.disconnect()

        cycletime = (datetime.datetime.now() - starttime).seconds
        # add cycle time to list
        cycletime_.append(cycletime)        
        
        print('Bulk update #%s with cycle time: %.2f'%(i+1, cycletime))
        print('Average cycle time: %s'%(sum(cycletime_)/len(cycletime_) ))
        print(cycletime_)
        print('[green]----------------------------------------[/green]')
        if i != 14:
            print('%s: Pausing %.2f mins before next round\n'%(datetime.datetime.now().strftime("%H:%M:%S"), ibkrThrottleTime*10/60))
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

    # print('\n\n*****!!!!!!!!!*****!!!!    SLEEPING FOR 2 HOURS BEFORE STARTING UPDATE...    !!!!*****!!!!!!!!!*****\n\n')
    # time.sleep(7200)

    ## update existing records after EST market close or on weekends
    if (datetime.datetime.today().weekday() < 5 and datetime.datetime.now().hour >= 17) or (datetime.datetime.today().weekday() >= 5): #or (datetime.datetime.today().weekday() < 5 and datetime.datetime.now().hour<= 9):
        print('[green]Updating existing records...[/green]')
        updateRecords()
    
    bulkUpdate()
