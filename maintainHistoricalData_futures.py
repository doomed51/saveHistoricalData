"""
This module maintains futures data. 

General logic: 
    1. Read watchlist csv file
    2. Check db for latest available data
    3. If latest data is not up to date, grab new data from IBKR, and update the db 
    4. if there are any new contracts in the watchlist, grab new data from IBKR, and update the db
    
"""
import time
import re 
import config
import math

import pandas as pd
import numpy as np
# import interface_localDb_old as db
import interface_localDB as db
import interface_ibkr as ibkr
import checkDataIntegrity as cdi

from datetime import datetime
from dateutil.relativedelta import relativedelta
from rich import print

# set pands to print all rows in df 
# pd.set_option('display.max_rows', None)
# set pandas to print entire col
pd.set_option('display.max_colwidth', None)

"""
Config vars 
"""
_defaultSleepTime = 30 #seconds, wait time between ibkr api calls 

"""
    global variables
"""
filename_futuresWatchlist = 'futuresWatchlist.csv'
dbName_futures = config.dbname_futures
trackedIntervals = config.intervals
numExpiryMonths = 14 # number of future expiries we want to track at any given time 

def _addspace(myStr): 
    """
        adds a space between num and alphabet
    """
    # check if there is a space in the string 
    if ' ' in myStr:
        return myStr
    return re.sub("[A-Za-z]+", lambda elm: " "+elm[0],myStr )

def _setLookback(interval):
    """
        lambda function to set lookback based on interval
    """
    lookback = '10 D'
    if interval in ['1 day']:
        lookback = '300 D'
    elif interval in ['30 mins', '5 mins']:
        lookback = '20 D'
    elif interval in ['1 min']:
        lookback = '5 D'
    return lookback

def _getWatchlist(filename):
    """
        returns [DataFrame] of watchlist (csv of futures)
    """
    # read watchlist csv file
    watchlist = pd.read_csv(filename)

    #cleanup watchlist
    watchlist = pd.DataFrame(watchlist.columns).reset_index(drop=True)
    watchlist.rename(columns={0:'symbol'}, inplace=True)
    watchlist['symbol'] = watchlist['symbol'].str.strip(' ').str.upper()
    watchlist.drop_duplicates(inplace=True)
    watchlist.sort_values(by=['symbol'], inplace=True)

    return watchlist

def _getLatestRecords():
    """
        Returns dataframe of records in the db  
    """
    # get latest data from db
    with db.sqlite_connection(dbName_futures) as conn:
        latestData = db.getRecords(conn)
    return latestData

def _getMinRecordDate(conn, tablename):
    """
        returns the minimum record date from target table
    """
    sqlStatement = 'SELECT MIN(date) FROM \'%s\''%(tablename)
    minDate = pd.read_sql(sqlStatement, conn)

    return minDate.iloc[0]['MIN(date)']

def _countWorkdays(startDate, endDate, excluded=(6,7)):
    """ returns number of business days 
        between two provided datetimes 
    """ 
    #make sure startDate and endDate are datetime objects
    if not isinstance(startDate, datetime):
        startDate = datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')
    if not isinstance(endDate, datetime):
        endDate = datetime.strptime(endDate, '%Y-%m-%d %H:%M:%S')
    ## handle negatives when endDate > startDate 
    if startDate > endDate:
        return (len(pd.bdate_range(endDate, startDate)) * -1)
    else:
        return len(pd.bdate_range(startDate, endDate))

def _updateSingleRecord(ib, symbol, expiry, interval, lookback, endDate=''):
    # get exchange from lookup table 
    with db.sqlite_connection(dbName_futures) as conn:
        exchange = db.getLookup_exchange(conn, symbol)
    
    # Get futures history from ibkr
    # Split into multiple calls for shorter intervals so we can get more data in 1 go   
    if (interval in ['1 min', '5 mins']) and int(lookback.strip(' D')) > 12:
        # calculate number of calls needed
        numCalls = math.ceil(int(lookback.strip(' D'))/12)#int(int(int(lookback.strip(' D'))/12))
        record=pd.DataFrame()
        # loop for numCalls appending records and reducing endDate by lookback each time
        for i in range(0, numCalls):
            bars = ibkr.getBars_futures(ib, symbol=symbol, lastTradeDate=expiry, interval=interval, endDate=endDate, lookback='12 D', exchange=exchange)
            if (bars is None) or (bars.empty):
                break
            else:
                record = record._append(bars)   
                endDate = record['date'].min() # update endDate for next loop 
                if i < numCalls-1:
                    print('%s: [orange]sleeping for %ss...[/orange]'%(datetime.now().strftime('%H:%M:%S'), _defaultSleepTime/30))
                    time.sleep(_defaultSleepTime/30)
        record.reset_index(drop=True, inplace=True)
    else:
        # query ibkr for futures history 
        record = ibkr.getBars_futures(ib, symbol=symbol, lastTradeDate=expiry, interval=interval, endDate=endDate, lookback=lookback, exchange=exchange)

    # handle case where no records are returned
    if (record is None) or (record.empty):
        print('[green]End of history![/green]')
        # update the lookup table to reflect no records left 
        with db.sqlite_connection(dbName_futures) as conn:
            # set tablename
            tablename = symbol+'_'+expiry+'_'+interval.replace(' ', '')
            # get lookuptable
            lookupTable = db.getLookup_symbolRecords(conn)
            if tablename in lookupTable['name'].values:
                if interval in ['1 day', '1 month']:
                    _earliestTimeStamp = datetime.today().strftime('%Y%m%d')
                else:
                    _earliestTimeStamp = datetime.today().strftime('%Y%m%d %H:%M:%S')
                db._updateLookup_symbolRecords(conn, tablename, type = 'future', earliestTimestamp = _earliestTimeStamp, numMissingDays = 0)
    else:
        record['symbol'] = symbol
        record['interval'] = interval.replace(' ', '')
        record['lastTradeDate'] = expiry
        earlistTimestamp = ibkr.getEarliestTimeStamp_m(ib, symbol=symbol, lastTradeDate=expiry, exchange=exchange)
        with db.sqlite_connection(dbName_futures) as conn:
            db.saveHistoryToDB(record, conn, earlistTimestamp, type='future')
    
    # sleep
    print('%s: [green]Record updated, sleeping for %ss...[/green]\n'%(datetime.now().strftime('%H:%M:%S'), _defaultSleepTime/30))
    time.sleep(_defaultSleepTime/30)
        
def _getMissingContracts(ib, symbol, numMonths = numExpiryMonths):
    """
        returns contracts missing from the db for given symbol 
        inputs: 
            latestRectords: [DataFrame] of latest records from the db (limited to 1 symbol)
            numMonths: [int] num months into future to look for available contracts
        returns:
            [DataFrame] of missing contracts informat symbol_expiry_interval 
    """
    print('%s:[yellow] Checking missing contracts for %s...[/yellow]'%(datetime.now().strftime('%H:%M:%S'), symbol))
    # get latest records from db 
    with db.sqlite_connection(dbName_futures) as conn:
        latestRecords = db.getRecords(conn)
        # select only records with symbol = symbol
        latestRecords = latestRecords.loc[latestRecords['symbol'] == symbol].reset_index(drop=True)
    # get contracts from ibkr 
    contracts = ibkr.getContractDetails(ib, symbol, type='future')
    contracts = ibkr.util.df(contracts)
    
    # if symbol = VIX, drop the weekly contracts 
    if symbol == 'VIX':
        contracts = contracts.loc[contracts['marketName'] == 'VX'].reset_index(drop=True)
    
    # if symbol = NG, limit contracts to nymex and 2 years out
    if symbol == 'NG': 
        contracts['exchange'] = contracts['contract'].apply(lambda x: x.exchange)

        # select only contracts where contract.exchange = nymex
        contracts = contracts.loc[contracts['exchange'] == 'NYMEX'].reset_index(drop=True)

        # limit contracts to 2 years out 
        maxDate = datetime.today() + relativedelta(months=24)
        contracts = contracts.loc[contracts['realExpirationDate'] <= maxDate.strftime('%Y%m%d')].reset_index(drop=True)

    missingContracts = pd.DataFrame(columns=['interval', 'realExpirationDate', 'contract'])
    
    # append missing contracts for each tracked interval 
    for interval in trackedIntervals:
        # select latestRecords for interval 
        latestRecords_interval = latestRecords.loc[latestRecords['interval'] == interval]
        
        # handle case where entire interval data is missing 
        if latestRecords_interval.empty:
            # add all contracts for the interval
            contracts['interval'] = interval
            missingContracts = missingContracts._append(contracts[['interval', 'realExpirationDate', 'contract']])
        
        else: # otherwise select just the contracts not in our db 
            contracts['interval'] = interval
            missingContracts = missingContracts._append(contracts.loc[~contracts['realExpirationDate'].isin(latestRecords_interval['type/expiry'])][['interval', 'realExpirationDate', 'contract']])
    
    if missingContracts.empty:
        print('%s: [green]No missing contracts found![/green]'%(datetime.now().strftime('%H:%M:%S')))
    else:
        print('%s:[yellow] Found %s missing contracts[/yellow]\n'%(datetime.now().strftime('%H:%M:%S'), str(len(missingContracts))))
    return missingContracts.reset_index(drop=True)

def uniqueIDMapper(ib, symbol, expiry): 
    """ 
        maps future expiry, symbol combo to ibkr unique id 
        TODO not implemented fully
    """
    # get contract details 
    contractDetails = ibkr.getContractDetails(ib, symbol=symbol, type='future')
    # convert to dataframe 
    contractDetails = ibkr.util.df(contractDetails)
    # set pands to print all columns
    pd.set_option('display.max_columns', None)
    contractDetails['localSymbol'] = contractDetails['contract'].apply(lambda x: x.localSymbol)
    contractDetails['exchange'] = contractDetails['contract'].apply(lambda x: x.exchange)

    # select only contracts where contract.exchange = db.getexchange
    with db.sqlite_connection(dbName_futures) as conn:
        exchange = db.getExchange(conn, symbol)
        print(exchange)
    print(expiry)
    contractDetails = contractDetails.loc[contractDetails['exchange'] == exchange].reset_index(drop=True)
    print(contractDetails[['localSymbol', 'realExpirationDate']])
    # select only contracts where contract.realExpirationDate = expiry
    contractDetails = contractDetails.loc[contractDetails['realExpirationDate'] == expiry].reset_index(drop=True)

    print(contractDetails[['localSymbol', 'realExpirationDate']])
    exit()

def updateRecords(ib_):     
    """
        Updates existing, and adds missing records to the db
        logic: 
        - update existing records
        - add new contracts, if needed, to maintain numContract number of forward contracts being tracked 
        - finally add new contracts from the watchlist with numConctract number of forward contract 
    """
    
    # get watchlist
    watchlist = pd.read_csv(filename_futuresWatchlist)
    ## format watchlist  
    watchlist = pd.DataFrame(watchlist.columns).reset_index(drop=True)
    watchlist.rename(columns={0:'symbol'}, inplace=True)
    watchlist['symbol'] = watchlist['symbol'].str.strip(' ').str.upper()
    watchlist.sort_values(by=['symbol'], inplace=True)
    
    # get latest data from db
    with db.sqlite_connection(dbName_futures) as conn:
        latestRecords = db.getRecords(conn)

    # drop latestRecords where expiry is before current date
    latestRecords = latestRecords.loc[latestRecords['type/expiry'] > datetime.today().strftime('%Y%m%d')].sort_values(by=['interval']).reset_index(drop=True)

    # find contracts missing from db 
    missingContracts = pd.DataFrame()
    for symbol in watchlist['symbol']:
        missingContracts = missingContracts._append(_getMissingContracts(ib_, symbol))

    print('%s: Total missing contracts: %s'%(datetime.now().strftime("HH:MM:SS"),str(len(missingContracts))))
    missingContracts['symbol'] = missingContracts['contract'].apply(lambda x: x.symbol)
    missingContracts['realExpirationDate'] = missingContracts['contract'].apply(lambda x: x.lastTradeDateOrContractMonth)
    print(missingContracts[['symbol', 'realExpirationDate', 'interval']])
    print('\n')
    
    # add lookback columnbased on interval
    missingContracts['lookback'] = missingContracts.apply(
        lambda row: _setLookback(row['interval']), axis=1)
    # remove spac efrom the interval column
    # missingContracts['interval'] = missingContracts['interval'].apply(lambda x: db._removeSpaces(x))
    
    # add missing contracts to our db
    if not missingContracts.empty:
        print('[green]----------------------------------------------[/green]')
        print('[yellow]---------- Adding missing contracts ---------[/yellow]')
        print('[green]----------------------------------------------[/green]\n')
    for missingContract in missingContracts.iterrows():
        print('Adding contract %s %s %s'%(missingContract[1]['contract'].symbol, missingContract[1]['realExpirationDate'], missingContract[1]['interval']))
        _updateSingleRecord(ib_, missingContract[1]['contract'].symbol, missingContract[1]['realExpirationDate'], missingContract[1]['interval'], missingContract[1]['lookback'])
        # sleep for defaulttime
        print('%s: sleeping for %ss...\n'%(datetime.now().strftime('%H:%M:%S'), _defaultSleepTime/30))
        time.sleep(_defaultSleepTime/30)

    print('[green]----------------------------------------------[/green]')
    print('[green]----- Completed adding missing contracts -----[/green]')
    print('[green]----------------------------------------------[/green]\n')

    # update records in our db that have not been updated in the last 24 hours 
    if not latestRecords.loc[latestRecords['daysSinceLastUpdate'] > 1].empty:
        print('[green]----------------------------------------------[/green]')
        print('[yellow]--------- Updating outdated records ----------[/yellow]')
        print('[green]----------------------------------------------[/green]\n')

        # print recodrs where type/expiry is before current date
        i=1
        for row in (latestRecords.loc[latestRecords['daysSinceLastUpdate'] >= 2]).iterrows():
            print('%s: (%s/%s) Updating contract %s %s %s'%(datetime.now().strftime('%H:%M:%S'), i,latestRecords.loc[latestRecords['daysSinceLastUpdate']>=1]['symbol'].count(), row[1]['symbol'], row[1]['type/expiry'], row[1]['interval'].replace(' ', '')) )
            _updateSingleRecord(ib_, row[1]['symbol'], row[1]['type/expiry'], row[1]['interval'], str(row[1]['daysSinceLastUpdate']+1)+' D')
            i+=1
    
    print('[green]----------------------------------------------[/green]')
    print('[green]---- Completed updating outdated records ----[/green]')
    print('[green]----------------------------------------------[/green]\n')
        
def DELETE_loadExpiredContracts(ib, symbol, lastTradeDate, interval):
    """
        This function updates the past two years of futures data.
        use this when a symbol is first added from the watchlist  
    """
    ###############
    ## placeholder!!! needs to be implemented
    ###############
    
    ## manually setting contract expiry example
    conDetails = ibkr.getContractDetails(ib, symbol=symbol, type='future')
    
    conDetails[2].contract.lastTradeDateOrContractMonth = '20230820'
    record2 = ibkr._getHistoricalBars_futures(ib, conDetails[2].contract, interval=interval, endDate=datetime.today(), lookback='300 D', whatToShow='BID')

def calculate_datetime_counts(pxhistory):
    """
        Calculates the number of unique datetime counts per date in pxHistory
        Returns df['date_only', 'count']
    """
    pxhistory['date'] = pd.to_datetime(pxhistory['date'], format='%Y-%m-%d %H:%M:%S' )
    pxhistory['date_only'] = (pxhistory['date'].dt.date)
    pxhistory['date_only'] = pd.to_datetime(pxhistory['date_only']).dt.strftime('%Y-%m-%d')
    pxhistory.sort_values(by=['date'], inplace=True)
    pxhistory.set_index('date', inplace=True)

    return pxhistory[['date_only', 'open']].groupby('date_only').count().reset_index().rename(columns={'open':'count'})

def check_gaps_in_pxhistory_metadata_up_to_date(conn, threshold_days=10):
    """
        checks if metadata is up to date
         Returns true if data is up to date
    """
    pxhistory_metada = db.getTable(conn, config.table_name_futures_pxhistory_metadata)

    if pxhistory_metada.empty:
        print('[yellow]No records found in table %s[/yellow]'%(config.table_name_futures_pxhistory_metadata))
        return False
    else: 
        latestDate = pd.to_datetime(pxhistory_metada['update_date'].max())
        if (datetime.now() - latestDate).days <= threshold_days:
            print('%s: [green]pxhistory_metadata is up to date! Last updated %s[/green]'%(datetime.now().strftime('%H:%M:%S'), latestDate.strftime('%Y-%m-%d %H:%M:%S')))
            return True
        else: 
            print('%s: [yellow]pxhistory_metadata is outdated![/yellow]'%(datetime.now().strftime('%H:%M:%S')))
            return False

def generate_pxhistory_metadata_master_table(conn):
    """
        Generates a master table of tablename, # of unique gap counts, and datetime recorded
        Includes dates missing between current data and expiry date
    """
    lookupTable = db.getLookup_symbolRecords(conn)

    print('%s: [yellow]Generating pxhistory_metadata master table...[/yellow]'%(datetime.now().strftime('%H:%M:%S')))

    futures_pxhistory_metadata_table = db.getTable(conn, config.table_name_futures_pxhistory_metadata)
    futures_pxhistory_metadata_current_db_snapshot = pd.DataFrame(columns=['tablename', 'num_unique_gaps', 'update_date','date_of_last_gap_date_polled'])
    for idx, row in lookupTable.iterrows():
        print('%s: [yellow]Scanning gaps for %s...[/yellow]'%(datetime.now().strftime('%H:%M:%S'), row['name']))
        tablename = row['name']
        pxHistory = db.getTable(conn, tablename)
        number_of_datetime_in_each_date = calculate_datetime_counts(pxHistory)
        # if contract is expired, make sure the expiry date is in pxhistory, if not add a dummy row with the expiry date
        pxHistory.reset_index(inplace=True)
        if pd.to_datetime(row['lastTradeDate']) not in pxHistory.index.to_list():
            pxHistory = pd.concat([pxHistory, pd.DataFrame([{'date': (pd.to_datetime(row['lastTradeDate'])), 'date_only': (pd.to_datetime(row['lastTradeDate']).date()), 'open': 0, 'high': 0, 'low': 0, 'close': 0, 'volume': 0, 'symbol':row['symbol'], 'interval':row['interval'], 'lastTradeDate':row['lastTradeDate']}])], ignore_index=True)
        missing_dates = cdi._check_for_missing_dates_in_timeseries(pxHistory, date_col_name='index')
        
        # drop the dummy row if it exists
        if pd.to_datetime(row['lastTradeDate']) not in pxHistory.index.to_list():
            pxHistory.drop(pxHistory.index[-1], inplace=True)

        date_of_last_gap = max(pd.to_datetime(missing_dates.max()), pd.to_datetime(number_of_datetime_in_each_date['date_only'].max()))
       
        number_of_datetime_in_each_date = number_of_datetime_in_each_date.groupby('count').count().reset_index().rename(columns={'date_only':'frequency'})
        futures_pxhistory_metadata_current_db_snapshot = pd.concat([futures_pxhistory_metadata_current_db_snapshot, pd.DataFrame({'tablename':tablename, 'num_unique_gaps':number_of_datetime_in_each_date['frequency'].count(), 'update_date':datetime.now(), 'date_of_last_gap_date_polled':date_of_last_gap}, index=[0])], ignore_index=True)
    
    # create master dataframe of tablename, gap, datetime recorded 
    futures_pxhistory_metadata_current_db_snapshot['update_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    futures_pxhistory_metadata_current_db_snapshot['date_of_last_gap_date_polled'] = pd.to_datetime(futures_pxhistory_metadata_current_db_snapshot['date_of_last_gap_date_polled']).dt.date

    # merge db snapshot with metadata table    
    if futures_pxhistory_metadata_table.empty: # init metadata table with db snapshot 
        updated = futures_pxhistory_metadata_current_db_snapshot
    else:
        updated = pd.merge(futures_pxhistory_metadata_current_db_snapshot, futures_pxhistory_metadata_table, on='tablename', how='inner', suffixes=('', '_metadata'))
        # in updated, set date_of_last_gap_date_polled to metadata if it is not None 
        updated.loc[updated['date_of_last_gap_date_polled_metadata'].notnull(), 'date_of_last_gap_date_polled'] = updated['date_of_last_gap_date_polled_metadata']
        # drop _metadata columns
        updated.drop([col for col in updated.columns if '_metadata' in col], axis=1, inplace=True)
    
    return updated

def update_gaps_in_pxhistory_metadata(conn):
    """
        updates pxhistory_metadata table from db records 
    """
    if check_gaps_in_pxhistory_metadata_up_to_date(conn):
        return
    else: 
        record_unique_datetime_count = generate_pxhistory_metadata_master_table(conn)
        db.save_table_to_db(conn = conn, tablename=config.table_name_futures_pxhistory_metadata, metadata_df = record_unique_datetime_count, if_exists='replace')

def update_gaps_in_pxhistory(conn, ib, ibkr_lookback_period = '5 D'): 
    """
        updates gaps in pxhistory, usees the pxhistory_metadata table to determine which tables need to be updated. 
    """
    DEFAULT_DATE_IF_NO_GAPS = pd.to_datetime('1989-12-30')
    # get table metadata, and filter out tables that no longer have gaps 
    pxhistory_metadata = db.getTable(conn, config.table_name_futures_pxhistory_metadata)
    pxhistory_metadata = pxhistory_metadata.loc[pxhistory_metadata['date_of_last_gap_date_polled'] != 1989-12-30]

    # update gaps for each table in the db 
    for idx, row in pxhistory_metadata.iterrows():
        tablename = row['tablename']
        pxHistory = db.getTable(conn, tablename)
        symbol, expiry, interval = tablename.split('_')
        
        # Determine the gap date that should be updated 
        last_gap_polled = pd.to_datetime(row['date_of_last_gap_date_polled'])
        number_of_datetime_in_each_date = calculate_datetime_counts(pxHistory)
        if not pd.isna(last_gap_polled):
            number_of_datetime_in_each_date = number_of_datetime_in_each_date.loc[pd.to_datetime(number_of_datetime_in_each_date['date_only']) < last_gap_polled]
            # if this returns empty, 
            # that means there are no more gaps left to update. Update metadata to reflect this 
            if number_of_datetime_in_each_date.empty:
                # pxhistory_metadata.loc[pxhistory_metadata['tablename'] == tablename, 'date_of_last_gap_date_polled'] = pd.to_datetime('1989-12-30').date()
                # pxhistory_metadata.loc[pxhistory_metadata['tablename'] == tablename, 'update_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                update_metadata(pxhistory_metadata, tablename, DEFAULT_DATE_IF_NO_GAPS)
                continue
        
        date_to_update = pd.to_datetime(number_of_datetime_in_each_date['date_only'].max()) + pd.to_timedelta(1, unit='D')

        # get gap data from ibkr 
        print('%s: [yellow]Updating gap history for %s, gap date: %s...[/yellow]'%(datetime.now().strftime('%H:%M:%S'), tablename, date_to_update.strftime('%Y-%m-%d')))     
        exchange = config.exchange_mapping[tablename.split('_')[0]]
        ibkr_pxhistory = ibkr.getBars_futures(ib, symbol=symbol, lastTradeDate=expiry, interval=_addspace(interval), endDate=date_to_update, lookback=ibkr_lookback_period, exchange=exchange)
        if ibkr_pxhistory is None:
            print('%s: [green]No records found for %s, with endDate: %s [/green]'%(datetime.now().strftime('%H:%M:%S'), tablename, date_to_update.strftime('%Y-%m-%d')))
            # pxhistory_metadata.loc[pxhistory_metadata['tablename'] == tablename, 'date_of_last_gap_date_polled'] = pd.to_datetime('1989-12-30').date()
            # pxhistory_metadata.loc[pxhistory_metadata['tablename'] == tablename, 'update_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            update_metadata(pxhistory_metadata, tablename, DEFAULT_DATE_IF_NO_GAPS)
            continue        
        ibkr_pxhistory['symbol'] = symbol
        ibkr_pxhistory['interval'] = interval
        ibkr_pxhistory['lastTradeDate'] = expiry                 

        # save it to db 
        db.saveHistoryToDB(ibkr_pxhistory, conn, type='future')
        print('%s: [green]Record %s of %s updated for %s, sleeping for %ss...[/green]\n'%(datetime.now().strftime('%H:%M:%S'),idx,len(pxhistory_metadata), tablename, _defaultSleepTime/30))
        time.sleep(_defaultSleepTime/30)
        
        # Update metadata 
        # pxhistory_metadata.loc[pxhistory_metadata['tablename'] == tablename, 'date_of_last_gap_date_polled'] = ibkr_pxhistory['date'].min().date()
        # pxhistory_metadata.loc[pxhistory_metadata['tablename'] == tablename, 'update_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_metadata(pxhistory_metadata, tablename, ibkr_pxhistory['date'].min().date())

    print('%s: [green]DONE! Completed updating gaps in pxhistory_metadata, cleaning up metadata[/green]\n'%(datetime.now().strftime('%H:%M:%S')))
    db.save_table_to_db(conn = conn, tablename=config.table_name_futures_pxhistory_metadata, metadata_df = pxhistory_metadata)
    db.remove_duplicates_from_pxhistory_gaps_metadata(conn, config.table_name_futures_pxhistory_metadata)

def update_metadata(pxhistory_metadata: pd.DataFrame, tablename: str, date: pd.Timestamp) -> None:
    """
        Updates metadata table with the date of the last gap polled
    """
    pxhistory_metadata.loc[pxhistory_metadata['tablename'] == tablename, 'date_of_last_gap_date_polled'] = date
    pxhistory_metadata.loc[pxhistory_metadata['tablename'] == tablename, 'update_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def _dirtyRefreshLookupTable(ib, mode): 
    """
        Gross. but use if necessary. 
        Use when lookup table hasnt been updated after a new contract is added 
    """

    mode = 'SET_FIRST_MISSING_RECORD_DATE'
    if mode == 'SET_FIRST_MISSING_RECORD_DATE': 
        tablename = 'VIX_20240117_1min'

        # get table from db 
        with db.sqlite_connection(dbName_futures) as conn:
            lookupTable = db.getLookup_symbolRecords(conn)
            pxHistory = db.getTable(conn, tablename)
        # remove rows with interval = 1day
        lookupTable = lookupTable.loc[lookupTable['interval'] != '1day']
        print(lookupTable)

        # create empty dict of tablename, and count 
        record_unique_datetime_count = {}
        for idx, row in lookupTable.iterrows():
            tablename = row['name']
            with db.sqlite_connection(dbName_futures) as conn:
                pxHistory = db.getTable(conn, tablename)
            
            number_of_datetime_in_each_date = calculate_datetime_counts(pxHistory)
            number_of_datetime_in_each_date = number_of_datetime_in_each_date.groupby('count').count().reset_index().rename(columns={'date_only':'frequency'})
            # add tablename and count to dict 
            record_unique_datetime_count[tablename] = number_of_datetime_in_each_date['frequency'].count()
            
        # create master dataframe of tablename, gap, datetime recorded 
        record_unique_datetime_count = pd.DataFrame.from_dict(record_unique_datetime_count, orient='index').reset_index().rename(columns={0:'num_unique_gaps', 'index':'tablename'})
        record_unique_datetime_count['datetime'] = datetime.now()
        record_unique_datetime_count = record_unique_datetime_count.loc[record_unique_datetime_count['num_unique_gaps'] > 1]

        print(record_unique_datetime_count)
        record_unique_datetime_count.sort_values(by=['num_unique_gaps'], inplace=True)
        # plot grouped 
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots()
        record_unique_datetime_count.plot(y='num_unique_gaps', x='tablename', kind='bar', ax=ax)
        plt.show()

        # print(pxHistory)
        # print(grouped)
        exit()

        # # selected.plot(y='close', kind='line', ax=ax)
        # grouped.plot(y='numDates',x='count', kind='bar', ax=ax)
        # # ax.xaxis.set_major_formatter(MyFormatter(selected.index, '%Y-%m-%d %H:%M:%S'))
        # plt.show()
    
    elif mode == 'ADD_MISSING_RECORDS_TO_LOOKUP_TABLE':
        with db.sqlite_connection(dbName_futures) as conn:
            lookupTable = db.getLookup_symbolRecords(conn)

        trackedSymbols = lookupTable.drop_duplicates(subset=['symbol'])
        
        trackedSymbols['earliestTimestamp'] = trackedSymbols.apply(lambda row: ibkr.getEarliestTimeStamp(ib, symbol=row['symbol'], lastTradeDate=row['lastTradingDate']), axis=1)

        # get list of all tablenames in the db
        with db.sqlite_connection(dbName_futures) as conn:
            sqlStatement = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '00-%'"
            tablenames = pd.read_sql(sqlStatement, conn)
        
        # select only tablesnames that are not in the lookup table
        tablenames = tablenames.loc[~tablenames['name'].isin(lookupTable['name'])]
        tablenames[['symbol', 'lastTradingDate', 'interval']] = tablenames['name'].str.split('_', expand=True)

        # add new column firstRecordDate by applying lambda function _getMinRecordDate(tableName)
        with db.sqlite_connection(dbName_futures) as conn:
            tablenames['firstRecordDate'] = tablenames.apply(lambda row: _getMinRecordDate(conn, row['name']), axis=1)

        tablenames = tablenames.assign(numMissingBusinessDays=tablenames.apply(lambda row: _countWorkdays(row['firstRecordDate'], trackedSymbols.loc[trackedSymbols['symbol'] == row['symbol']]['earliestTimestamp'].iloc[0]), axis=1))

        # reorder columns as: 0, 1, 3, 4, 5, 2
        tablenames = tablenames.iloc[:,[0,1,3,4,5,2]]

        with db.sqlite_connection(dbName_futures) as conn:
            tablenames.to_sql('00-lookup_symbolRecords', conn, index=False, if_exists='append')

    return

def _updatePreHistory(lookupTable, ib):
    """
        update pre-history for records in the db 
            inputs: 
                pd.lookuptable of records that need to be updated
                ibkr object 
            algo:
                1. set interval
                2. set lookback to 60
                3. set end date
                4. set earliestTimeStamp
                5. iterate through each records:
                    a. set the endDate to the firstRecordDate
                    b. query ibkr for history 
                    c. skip to next if no data is returned
                    d. append history to the db
    """
    print('[green]----------------------------------------------[/green]')
    print('[yellow]--------- Updating prehistorica data ---------[/yellow]')
    print('[green]----------------------------------------------[/green]\n')    
    # make sure interval formatting matches ibkr rqmts e.g. 5 mins, 1 day 
    lookupTable['interval'] = lookupTable.apply(lambda row: _addspace(row['interval']), axis=1)
    
    # drop records where lastTradeDate <= todays date in format YYYYMM
    # lookupTable = lookupTable.loc[lookupTable['lastTradeDate'] > datetime.today().strftime('%Y%m')].reset_index(drop=True)

    lookupTable = lookupTable.loc[lookupTable['numMissingBusinessDays'] > 0].reset_index(drop=True)
    # set Exchange lookup 
    uniqueSymbol = lookupTable.drop_duplicates(subset=['symbol'])
    with db.sqlite_connection(dbName_futures) as conn:
        uniqueSymbol = uniqueSymbol.assign(exchange=uniqueSymbol.apply(lambda row: db.getLookup_exchange(conn, row['symbol']), axis=1))
    
    uniqueSymbol['earliestTimeStamp'] = (datetime.today() - relativedelta(years=2)).strftime('%Y%m%d %H:%M:%S')
    lookupTable.sort_values(by=['interval'], inplace=True)
    lookupTable.reset_index(drop=True, inplace=True)
    i=0
    for index, record in lookupTable.iterrows():  
        lookback = 100
        i+=1
        print('%s: [yellow]Record [/yellow]%s of %s: %s-%s-%s'%(datetime.now().strftime("%H:%M:%S"),i, len(lookupTable), record.symbol, record['lastTradeDate'], record['interval']))
        
        # set end date 
        endDate = (record['firstRecordDate'] + relativedelta(days=1)).strftime('%Y%m%d %H:%M:%S')
        # if record['interval'] == '1 day':
        # else:
            # endDate = record['firstRecordDate'] - relativedelta(minutes=1)

        # set earliestTimeStamp
        earliestAvailableTimestamp = pd.to_datetime(uniqueSymbol.loc[uniqueSymbol['symbol'] == record['symbol']]['earliestTimeStamp'].iloc[0])
        exchange = uniqueSymbol.loc[uniqueSymbol['symbol'] == record['symbol']]['exchange'].iloc[0]
    
        # set lookback
        if lookback >= (record['firstRecordDate'] - earliestAvailableTimestamp).days:
            lookback = (record['firstRecordDate'] - earliestAvailableTimestamp).days
        elif record['interval'] in ['1 day', '1 month']:
            lookback = 100
        elif record['interval'] in ['1 min']:
            lookback = 5
        else:
            lookback = 30

        history = pd.DataFrame()
        if lookback < 0:
            print(' [green]No data left [/green]for %s %s %s!'%(record['symbol'], record['lastTradeDate'], record['interval']))
            with db.sqlite_connection(dbName_futures) as conn:
                earliestAvailableTimestamp = db._getFirstRecordDate(record, conn)
                db._updateLookup_symbolRecords(conn, record['name'], earliestTimestamp=earliestAvailableTimestamp, numMissingDays=0, type='future')
            print('\n')
            continue
        else: 
            if record['interval'] in ['1 min', '5 mins']: # Make multiple calls for ltf data
                for i in range(10): 
                    currentIterationBars = ibkr.getBars_futures(ib, symbol=record['symbol'], lastTradeDate=record['lastTradeDate'], interval=record['interval'], endDate=endDate, lookback=(str(lookback) + ' D'), exchange=exchange)
                    if (currentIterationBars is None): 
                        break
                    else: 
                        history = pd.concat([history, currentIterationBars], ignore_index=True)
                        endDate = history['date'].min()
                        
            else:
                history = ibkr.getBars_futures(ib, symbol=record['symbol'], lastTradeDate=record['lastTradeDate'], interval=record['interval'], endDate=endDate, lookback=(str(lookback) + ' D'), exchange=exchange)
        
        # skip to next if no data is returned
        if history is None or history.empty:
            print(' [green]No data left [/green]for %s %s %s!'%(record['symbol'], record['lastTradeDate'], record['interval']))
            with db.sqlite_connection(dbName_futures) as conn:
                earliestAvailableTimestamp = db._getFirstRecordDate(record, conn)
                db._updateLookup_symbolRecords(conn, record['name'], earliestTimestamp=earliestAvailableTimestamp, numMissingDays=0, type='future')
            print('\n')
            continue
        
        # update history to the db
        history['symbol'] = record['symbol']
        history['interval'] = record['interval'].replace(' ', '')
        history['lastTradeDate'] = record['lastTradeDate']        
        with db.sqlite_connection(dbName_futures) as conn:
            db.saveHistoryToDB(history, conn, earliestAvailableTimestamp)
        print('\n')
        
        if index != len(lookupTable)-1:
            print('%s: [yellow]Sleeping for %ss...[/yellow]\n'%(datetime.now().strftime('%H:%M:%S'), str(_defaultSleepTime/30)))
            time.sleep(_defaultSleepTime/30)

        # update metadata 
    with db.sqlite_connection(dbName_futures) as conn:
        update_gaps_in_pxhistory_metadata(conn)
    
    print('[green]----------------------------------------------[/green]')
    print('[green]---- Completed updating prehistoric data -----[/green]')
    print('[green]----------------------------------------------[/green]\n')
    return

def initializeRecords(ib, watchlist,  updateThresholdDays=1):
    """
        Run this to initialize records in the db based on the watchlist 
    """
    # get watchlist
    watchlist = _getWatchlist(filename_futuresWatchlist)
    # get latest data from db
    latestData = _getLatestRecords()

    if latestData.empty: ## db is empty, get all data for contracts in the watchlist

        ## use todays date as the starting contract expiry date 
        expiryStr = datetime.strptime(datetime.today().strftime('%Y%m'), '%Y%m')
        # add 1 month to expiryStr
        expiryStr += relativedelta(months=1)
        

        # iterate through each symbol in the watchlist 
        for numWatchlist in range(len(watchlist)):
            earlistTimestamp = ibkr.getEarliestTimeStamp(ib, symbol=watchlist['symbol'][numWatchlist], lastTradeDate=expiryStr.strftime('%Y%m'))
            
            # get contracts for the next 45 months 
            for i in range(1, numExpiryMonths):
                for interval in trackedIntervals: # iterate through each interval
                    # get data for the contract, and interval 
                    print('looking up %s %s'%(watchlist['symbol'][numWatchlist], expiryStr.strftime('%Y%m')))
                    data = ibkr.getBars_futures(ib, symbol=watchlist['symbol'][numWatchlist], lastTradeDate=expiryStr.strftime('%Y%m'), interval=interval)
                    
                    # skip if no data is returned
                    if data.empty:
                        print(' No data found, skipping to next contract...\n')
                        continue

                    # add columns to simplify life 
                    data['symbol'] = watchlist['symbol'][numWatchlist]
                    data['interval'] = interval.replace(' ', '')
                    data['lastTradeDate'] = expiryStr.strftime('%Y%m')

                    # update local records
                    with db.sqlite_connection(dbName_futures) as conn:
                        print(' saving to db...\n')
                        db.saveHistoryToDB( data, conn, earlistTimestamp)
                    
                    # sleep for 40s
                    print('sleeping for %ss...'%(str(_defaultSleepTime/30)))
                    time.sleep(_defaultSleepTime/30)
                # increment expiry date
                expiryStr += relativedelta(months=1) 
    exit()

    # get latest date from db
    latestDate = _getLatestDate()
    # get latest date from IBKR
    latestDate_ibkr = _getLatestDate_ibkr()
    
    # if latest data is not up to date, grab new data from IBKR, and update the db 
    if latestDate_ibkr > latestDate:
        # grab new data from IBKR
        newData = _getNewData(latestDate_ibkr)
        # update the db
        _updateDb(newData)
    
    # if there are any new contracts in the watchlist, grab new data from IBKR, and update the db
    if watchlist:
        # get new contracts
        newContracts = _getNewContracts(watchlist, latestData)
        # grab new data from IBKR
        newData = _getNewData(newContracts)
        # update the db
        _updateDb(newData)
    
    return

def _check_missing_dates(record):
        with db.sqlite_connection(dbName_futures) as conn:
            data = db.getTable(conn, record['name'])
        # Check for missing dates in the data
        missingDates = cdi._check_for_missing_dates_in_timeseries(data)
        return missingDates

def check_futures_data_integrity():
    """
        Check, and plugs gaps in data for active futures contracts 
        Note: This will not work for expired contracts (due to IBKR data limitations)
    """
    ## get latest data records in db.
    with db.sqlite_connection(dbName_futures) as conn:
        latestData = db.getRecords(conn)
    active_contracts = latestData.loc[latestData['type/expiry'] > datetime.today().strftime('%Y%m%d')]
    active_contracts = active_contracts.loc[active_contracts['interval'] == '1 day']

    for index, record in active_contracts.iterrows():
        with db.sqlite_connection(dbName_futures) as conn:
            data = db.getTable(conn, record['name'])
        missingDates = cdi._check_for_missing_dates_in_timeseries(data)

        if not missingDates.empty:
            print('[yellow]Warning: Missing dates found in %s[/yellow]'%(record['name']))
            print('\n')

if __name__ == '__main__':
    ib = ibkr.setupConnection()
    updateRecords(ib)       
    for i in range(15):
        with db.sqlite_connection(dbName_futures) as conn:
            lookupTable = db.getLookup_symbolRecords(conn)
        _updatePreHistory(lookupTable, ib)

        with db.sqlite_connection(dbName_futures) as conn:
            # generate_pxhistory_metadata_master_table(conn)
            update_gaps_in_pxhistory_metadata(conn)
            update_gaps_in_pxhistory(conn, ib)
        # Refresh ib connection 
        if i%3 == 0:
            ib = ibkr.refreshConnection(ib)
    pass