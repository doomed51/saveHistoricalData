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

from ib_insync import Future
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

def _get_exchange_for_symbol(symbol):
    symbol = symbol.upper()
    exchange = config.exchange_mapping.get(symbol)
    if exchange is None:
        raise KeyError(f'No exchange mapping found for symbol: {symbol}')
    return exchange

def _updateSingleRecord(ib, symbol, expiry, interval, lookback, endDate='', currency='USD'):
    exchange = _get_exchange_for_symbol(symbol)
    contract = Future(symbol=symbol, lastTradeDateOrContractMonth=expiry, exchange=exchange, currency=currency, includeExpired=True)
    
    # Get futures history from ibkr
    # Split into multiple calls for shorter intervals so we can get more data in 1 go   
    if (interval in ['1 min', '5 mins', '1min', '5mins']) and int(lookback.strip(' D')) > 3:
        # calculate number of calls needed
        numCalls = math.ceil(int(lookback.strip(' D'))/3)
        record=pd.DataFrame()
        # loop for numCalls appending records and reducing endDate by lookback each time
        for i in range(0, numCalls):
            bars = ibkr.getBars_futures(ib, symbol=symbol, lastTradeDate=expiry, interval=interval, endDate=endDate, lookback='3 D', exchange=exchange)
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
        print('%s: [green]No more history![/green]'%(datetime.now().strftime('%H:%M:%S')))
        # update the lookup table to reflect no records left 
        with db.sqlite_connection(dbName_futures) as conn:
            # set tablename
            tablename = symbol+'_'+expiry+'_'+interval.replace(' ', '')
        
            # get lookuptable
            lookupTable = db.getLookup_symbolRecords(conn)
            if interval in ['1 day', '1 month']:
                _earliestTimeStamp = datetime.today().strftime('%Y-%m-%d')
            else:
                _earliestTimeStamp = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            if tablename in lookupTable['name'].values:
                db._update_symbol_metadata(conn, tablename, type = 'future', earliestTimestamp = _earliestTimeStamp, numMissingDays = 0)
            else:
                db.insert_new_record_metadata(conn, tablename, type='future', earliestTimestamp= _earliestTimeStamp, numMissingDays=0)
    else:
        record['symbol'] = symbol
        record['interval'] = interval.replace(' ', '')
        record['lastTradeDate'] = expiry
        earlistTimestamp = ibkr.getEarliestTimeStamp(ib, contract)
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
        # latestRecords = db.getRecords(conn)
        latestRecords = db.getLookup_symbolRecords(conn)
        # select only records with symbol = symbol
        latestRecords = latestRecords.loc[latestRecords['symbol'] == symbol].reset_index(drop=True)
    
    # get contracts from ibkr 
    exchange = _get_exchange_for_symbol(symbol)
    contracts = ibkr.getContractDetails(ib, symbol, type='future', exchange=exchange)
    contracts = ibkr.util.df(contracts)
    
    if symbol == 'VIX':
        contracts = contracts.loc[contracts['marketName'] == 'VX'].reset_index(drop=True) # we only want the monthly cons 
    
    contracts['exchange'] = contracts['contract'].apply(lambda x: x.exchange)
    contracts = contracts.loc[contracts['exchange'] == exchange].reset_index(drop=True)

    maxDate = datetime.today() + relativedelta(months=numMonths)
    contracts = contracts.loc[contracts['realExpirationDate'] <= maxDate.strftime('%Y%m%d')].reset_index(drop=True)

    missingContracts = pd.DataFrame(columns=['interval', 'realExpirationDate', 'contract'])
    
    # append missing contracts for each tracked interval 
    for interval in trackedIntervals:
        # select latestRecords for interval 
        latestRecords_interval = latestRecords.loc[latestRecords['interval'] == interval.replace(' ','')]
        latestRecords_interval['type/expiry'] = latestRecords_interval['name'].apply(lambda x: '_'.join(x.split('_')[1:2]))
        
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
        missingContracts['symbol'] = symbol
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

    print('%s: Total missing contracts: %s'%(datetime.now().strftime('HH:MM:SS'),str(len(missingContracts))))
    missingContracts['realExpirationDate'] = missingContracts['contract'].apply(lambda x: x.lastTradeDateOrContractMonth)
    
    # add lookback columnbased on interval
    missingContracts['lookback'] = missingContracts.apply(
        lambda row: _setLookback(row['interval']), axis=1)

    # add missing contracts to our db
    if not missingContracts.empty:
        print('[green]----------------------------------------------[/green]')
        print('[yellow]---------- Adding missing contracts ---------[/yellow]')
        print('[green]----------------------------------------------[/green]')
    for missingContract in missingContracts.iterrows():
        print('Adding contract %s %s %s'%(missingContract[1]['contract'].symbol, missingContract[1]['realExpirationDate'], missingContract[1]['interval']))
        _updateSingleRecord(ib_, missingContract[1]['contract'].symbol, missingContract[1]['realExpirationDate'], missingContract[1]['interval'], missingContract[1]['lookback'])
        # sleep for defaulttime
        print('%s: sleeping for %ss...\n'%(datetime.now().strftime('%H:%M:%S'), _defaultSleepTime/30))
        time.sleep(_defaultSleepTime/30)
    print('[green]----------------------------------------------[/green]')
    print('[green]----- Completed adding missing contracts -----[/green]')
    print('[green]----------------------------------------------[/green]\n')

    # update records in our db that have not been updated in in over 24 hours 
    if not latestRecords.loc[latestRecords['daysSinceLastUpdate'] > 1].empty:
        print('[green]----------------------------------------------[/green]')
        print('[yellow]--------- Updating outdated records ----------[/yellow]')
        print('[green]----------------------------------------------[/green]')
        i=1
        for row in (latestRecords.loc[latestRecords['daysSinceLastUpdate'] >= 1]).iterrows():
            print('%s: (%s/%s) Updating contract %s %s %s'%(datetime.now().strftime('%H:%M:%S'), i,latestRecords.loc[latestRecords['daysSinceLastUpdate']>=1]['symbol'].count(), row[1]['symbol'], row[1]['type/expiry'], row[1]['interval']) )
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

def _quote_sqlite_identifier(identifier: str) -> str:
    """
        Safely quote SQLite identifiers such as table names.
    """
    return '"%s"' % str(identifier).replace('"', '""')

def _build_expected_trading_days(start_date, end_date, last_trade_date, exchange, country='US'):
    """
        Builds expected trading days list between two dates excluding weekends and holidays.
    """
    if (start_date is None) or (end_date is None):
        return []
    start_date = pd.to_datetime(start_date).date()
    end_date = pd.to_datetime(end_date).date()

    # if end_date is in the future, set it to today to avoid generating expected dates in the future
    if end_date > datetime.today().date():
        end_date = datetime.today().date()

    if end_date < start_date:
        return []

    all_dates = pd.date_range(start=start_date, end=end_date)
    years = list(range(start_date.year, end_date.year + 1))
    holidays = cdi._get_holidays_for_exchange(exchange=exchange, years=years, country=country)
    holidays = set(holidays)

    return [
        d.strftime('%Y-%m-%d') for d in all_dates
        if d.weekday() < 5 and d.date() not in holidays and d.date() < last_trade_date
    ]

def _is_intraday_interval(interval_value):
    """
        Returns True when interval is intraday (minute/hour granularity).
    """
    interval_str = str(interval_value).lower().replace(' ', '')
    return ('min' in interval_str) or ('hour' in interval_str)

def generate_pxhistory_metadata_master_table(conn):
    """
        Generates a master table of tablename, # of unique gap counts, and datetime recorded
        Includes dates missing between current data and expiry date
    """
    lookupTable = db.getLookup_symbolRecords(conn)

    print('%s: [yellow]Generating pxhistory_metadata master table...[/yellow]'%(datetime.now().strftime('%H:%M:%S')))

    futures_pxhistory_metadata_table = db.getTable(conn, config.table_name_futures_pxhistory_metadata)
    metadata_rows = []
    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    conn.execute('DROP TABLE IF EXISTS temp_expected_trading_days')
    conn.execute('CREATE TEMP TABLE temp_expected_trading_days (trade_date TEXT PRIMARY KEY)')

    for row in lookupTable.itertuples(index=False):
        print('%s: [yellow]Scanning gaps for %s...[/yellow]'%(datetime.now().strftime('%H:%M:%S'), row.name))
        tablename = row.name

        # if lastTradeDate is empty, infer it from the name given we follow convention of symbol_expiry_interval
        inferred_last_trade_date = row.lastTradeDate
        if pd.isna(inferred_last_trade_date):
            try:
                inferred_last_trade_date = datetime.strptime(row.name.split('_')[1], '%Y%m%d').strftime('%Y-%m-%d')
            except Exception:
                print('%s: [red]Error inferring lastTradeDate for %s, skipping...[/red]'%(datetime.now().strftime('%H:%M:%S'), tablename))
                continue

        quoted_tablename = _quote_sqlite_identifier(tablename)
        try:
            bounds = conn.execute(
                'SELECT MIN(DATE(date)) AS min_date, MAX(DATE(date)) AS max_date FROM %s' % quoted_tablename
            ).fetchone()
        except Exception:
            print('%s: [red]Error fetching data for %s, skipping...[/red]'%(datetime.now().strftime('%H:%M:%S'), tablename))
            continue

        if (bounds is None) or (bounds[0] is None):
            continue

        min_date = pd.to_datetime(bounds[0]).date()
        max_date = pd.to_datetime(bounds[1]).date() if bounds[1] is not None else min_date
        last_trade_date = pd.to_datetime(inferred_last_trade_date).date()
        end_date = max(max_date, last_trade_date)

        try:
            exchange = _get_exchange_for_symbol(row.symbol)
        except Exception:
            print('%s: [red]Error resolving exchange for %s, skipping...[/red]'%(datetime.now().strftime('%H:%M:%S'), tablename))
            continue

        expected_days = _build_expected_trading_days(min_date, end_date, last_trade_date, exchange=exchange)
        conn.execute('DELETE FROM temp_expected_trading_days')
        if expected_days:
            conn.executemany(
                'INSERT INTO temp_expected_trading_days(trade_date) VALUES (?)',
                [(d,) for d in expected_days]
            )

        num_unique_gaps_row = conn.execute(
            'SELECT COUNT(DISTINCT daily_count) FROM '
            '(SELECT COUNT(*) AS daily_count FROM %s GROUP BY DATE(date))' % quoted_tablename
        ).fetchone()
        num_unique_gaps = int(num_unique_gaps_row[0]) if num_unique_gaps_row and num_unique_gaps_row[0] is not None else 0

        last_missing_row = conn.execute(
            'SELECT MAX(e.trade_date) '
            'FROM temp_expected_trading_days e '
            'LEFT JOIN (SELECT DISTINCT DATE(date) AS trade_date FROM %s) a '
            'ON a.trade_date = e.trade_date '
            'WHERE a.trade_date IS NULL' % quoted_tablename
        ).fetchone()
        last_missing_date = pd.to_datetime(last_missing_row[0]) if last_missing_row and last_missing_row[0] else pd.NaT

        # For intraday tables, treat partial sessions as gaps by flagging days below the observed full-session count.
        last_incomplete_intraday_date = pd.NaT
        if _is_intraday_interval(row.interval):
            expected_intraday_count_row = conn.execute(
                'SELECT MAX(daily_count) FROM '
                '(SELECT COUNT(*) AS daily_count FROM %s GROUP BY DATE(date))' % quoted_tablename
            ).fetchone()
            expected_intraday_count = int(expected_intraday_count_row[0]) if expected_intraday_count_row and expected_intraday_count_row[0] is not None else 0
            if expected_intraday_count > 0:
                last_incomplete_intraday_row = conn.execute(
                    'SELECT MAX(trade_date) FROM '
                    '(SELECT DATE(date) AS trade_date, COUNT(*) AS daily_count FROM %s GROUP BY DATE(date)) '
                    'WHERE daily_count < ? '
                    'AND CAST(STRFTIME("%%w", trade_date) AS INTEGER) NOT IN (0, 6)' % quoted_tablename,
                    (expected_intraday_count,)
                ).fetchone()
                last_incomplete_intraday_date = pd.to_datetime(last_incomplete_intraday_row[0]) if last_incomplete_intraday_row and last_incomplete_intraday_row[0] else pd.NaT

        if pd.isna(last_missing_date):
            last_gap_date = last_incomplete_intraday_date
        elif pd.isna(last_incomplete_intraday_date):
            last_gap_date = last_missing_date
        else:
            last_gap_date = max(last_missing_date, last_incomplete_intraday_date)

        latest_observed_date = pd.to_datetime(max_date)

        if pd.isna(last_gap_date):
            date_of_last_gap = latest_observed_date
        else:
            date_of_last_gap = max(last_gap_date, latest_observed_date)

        metadata_rows.append({
            'tablename': tablename,
            'num_unique_gaps': num_unique_gaps,
            'update_date': now_ts,
            'date_of_last_gap_date_polled': date_of_last_gap,
        })

    futures_pxhistory_metadata_current_db_snapshot = pd.DataFrame(
        metadata_rows,
        columns=['tablename', 'num_unique_gaps', 'update_date', 'date_of_last_gap_date_polled']
    )
    conn.execute('DROP TABLE IF EXISTS temp_expected_trading_days')
    
    # create master dataframe of tablename, gap, datetime recorded 
    futures_pxhistory_metadata_current_db_snapshot['update_date'] = now_ts
    if not futures_pxhistory_metadata_current_db_snapshot.empty:
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

def find_next_gap_date_in_table(conn, tablename, interval, exchange, start_after_date=None, country='US'):
    """
        Returns the next gap date (pd.Timestamp) in a table after start_after_date.
        Gap definitions:
        1) Missing expected trading day (weekdays minus exchange holidays)
        2) Incomplete intraday weekday session (daily bar count below expected full-session count)
           - weekend sessions (including Sunday open) are excluded from incomplete-session checks

        Returns pd.NaT when no gap is found.
    """
    quoted_tablename = _quote_sqlite_identifier(tablename)

    try:
        bounds = conn.execute(
            'SELECT MIN(DATE(date)) AS min_date, MAX(DATE(date)) AS max_date FROM %s' % quoted_tablename
        ).fetchone()
    except Exception:
        return pd.NaT

    if (bounds is None) or (bounds[0] is None):
        return pd.NaT

    min_date = pd.to_datetime(bounds[0]).date()
    max_date = pd.to_datetime(bounds[1]).date()

    if start_after_date is None:
        start_after_date = min_date - pd.to_timedelta(1, unit='D')
    else:
        start_after_date = pd.to_datetime(start_after_date).date()

    last_trade_date = tablename.split('_')[1] if len(tablename.split('_')) > 2 else max_date.strftime('%Y-%m-%d')
    last_trade_date = pd.to_datetime(last_trade_date).date()

    expected_days = _build_expected_trading_days(min_date, max_date, last_trade_date, exchange=exchange, country=country)
    if not expected_days:
        return pd.NaT

    daily_counts = pd.read_sql(
        'SELECT DATE(date) AS trade_date, COUNT(*) AS daily_count FROM %s GROUP BY DATE(date)' % quoted_tablename,
        conn
    )
    if daily_counts.empty:
        return pd.NaT

    daily_counts['trade_date'] = pd.to_datetime(daily_counts['trade_date']).dt.date
    actual_dates = set(daily_counts['trade_date'].astype(str).tolist())

    # Missing expected trading days
    missing_dates = [
        pd.to_datetime(d).date()
        for d in expected_days
        if d not in actual_dates
    ]

    # Incomplete intraday weekdays (exclude weekend sessions like Sunday open)
    incomplete_intraday_dates = []
    if _is_intraday_interval(interval):
        weekday_rows = daily_counts[
            pd.to_datetime(daily_counts['trade_date']).dt.weekday < 5
        ]
        if not weekday_rows.empty:
            expected_intraday_count = int(weekday_rows['daily_count'].max())
            if expected_intraday_count > 0:
                incomplete_intraday_dates = weekday_rows.loc[
                    weekday_rows['daily_count'] < expected_intraday_count, 'trade_date'
                ].tolist()

    gap_dates = sorted(set(missing_dates + incomplete_intraday_dates))
    next_gaps = [d for d in gap_dates if d > start_after_date]

    if not next_gaps:
        return pd.NaT
    return pd.to_datetime(next_gaps[-1])

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
        
        # if expiry is more than 2 years ago, update metadata to reflect no gaps and continue
        if pd.to_datetime(expiry) < datetime.today() - relativedelta(years=2):
            print('%s: [yellow]Expiry for %s is more than 2 years ago, marking as no gaps and skipping...[/yellow]'%(datetime.now().strftime('%H:%M:%S'), tablename))
            update_metadata(pxhistory_metadata, tablename, DEFAULT_DATE_IF_NO_GAPS, num_unique_gaps=0)
            continue

        # Determine the gap date that should be updated 
        last_gap_polled = pd.to_datetime(row['date_of_last_gap_date_polled'])

        # if last_gap_polled is same as expiry, get the next gap date 
        if last_gap_polled.date() == pd.to_datetime(expiry).date():
            last_gap_polled = find_next_gap_date_in_table(conn, tablename, interval, _get_exchange_for_symbol(symbol))

        number_of_datetime_in_each_date = calculate_datetime_counts(pxHistory)
        if not pd.isna(last_gap_polled):
            number_of_datetime_in_each_date = number_of_datetime_in_each_date.loc[pd.to_datetime(number_of_datetime_in_each_date['date_only']) < last_gap_polled]
            # if this returns empty, 
            # that means there are no more gaps left to update. Update metadata to reflect this 
            if number_of_datetime_in_each_date.empty:
                update_metadata(pxhistory_metadata, tablename, DEFAULT_DATE_IF_NO_GAPS, num_unique_gaps=0)
                continue
        
        date_to_update = pd.to_datetime(number_of_datetime_in_each_date['date_only'].max()) + pd.to_timedelta(1, unit='D')

        # get gap data from ibkr 
        print('%s: [yellow]Updating gap history for %s, gap date: %s...[/yellow]'%(datetime.now().strftime('%H:%M:%S'), tablename, date_to_update.strftime('%Y-%m-%d')))     
        exchange = _get_exchange_for_symbol(tablename.split('_')[0])
        ibkr_pxhistory = ibkr.getBars_futures(ib, symbol=symbol, lastTradeDate=expiry, interval=_addspace(interval), endDate=date_to_update, lookback=ibkr_lookback_period, exchange=exchange)
        if ibkr_pxhistory is None:
            print('%s: [green]No records found for %s, with endDate: %s [/green]'%(datetime.now().strftime('%H:%M:%S'), tablename, date_to_update.strftime('%Y-%m-%d')))
            update_metadata(pxhistory_metadata, tablename, DEFAULT_DATE_IF_NO_GAPS)
            continue        
        ibkr_pxhistory['symbol'] = symbol
        ibkr_pxhistory['interval'] = interval
        ibkr_pxhistory['lastTradeDate'] = expiry                 

        # print(ibkr_pxhistory)

        # get earliest timestamp from ibkr if contract is not expired 
        if expiry > datetime.today().strftime('%Y%m%d'):
            earliestTimestamp = ibkr.getEarliestTimeStamp_m(ib, symbol=symbol, lastTradeDate=expiry, exchange=exchange)
        else:
            # set to 2 years ago 
            earliestTimestamp = pd.to_datetime((datetime.today() - relativedelta(years=2)).strftime('%Y-%m-%d'))
        # print(earliestTimestamp)
        # exit() 

        # save it to db 
        db.saveHistoryToDB(ibkr_pxhistory, conn, earliestTimestamp=earliestTimestamp, type='future')
        # exit() 
        print('%s: [green]Record %s of %s updated for %s, sleeping for %ss...[/green]'%(datetime.now().strftime('%H:%M:%S'),idx,len(pxhistory_metadata), tablename, _defaultSleepTime/30))
        time.sleep(_defaultSleepTime/30)
        
        # Update metadata 
        # pxhistory_metadata.loc[pxhistory_metadata['tablename'] == tablename, 'date_of_last_gap_date_polled'] = ibkr_pxhistory['date'].min().date()
        # pxhistory_metadata.loc[pxhistory_metadata['tablename'] == tablename, 'update_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_metadata(pxhistory_metadata, tablename, ibkr_pxhistory['date'].min().date())

    print('%s: [green]DONE! Completed updating gaps in pxhistory_metadata, cleaning up metadata[/green]\n'%(datetime.now().strftime('%H:%M:%S')))
    # make sure updatde_date and date_og_last_gap_date_polled are datetime 
    pxhistory_metadata['update_date'] = pd.to_datetime(pxhistory_metadata['update_date'])
    pxhistory_metadata['date_of_last_gap_date_polled'] = pd.to_datetime(pxhistory_metadata['date_of_last_gap_date_polled'])

    print(pxhistory_metadata)
    print(pxhistory_metadata.dtypes)
    db.save_table_to_db(conn = conn, tablename=config.table_name_futures_pxhistory_metadata, metadata_df = pxhistory_metadata)
    db.remove_duplicates_from_pxhistory_gaps_metadata(conn, config.table_name_futures_pxhistory_metadata)

def update_metadata(pxhistory_metadata: pd.DataFrame, tablename: str, date: pd.Timestamp, num_unique_gaps=None) -> None:
    """
        Updates metadata table with the date of the last gap polled
    """
    pxhistory_metadata.loc[pxhistory_metadata['tablename'] == tablename, 'date_of_last_gap_date_polled'] = date
    pxhistory_metadata.loc[pxhistory_metadata['tablename'] == tablename, 'update_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if num_unique_gaps is not None:
        pxhistory_metadata.loc[pxhistory_metadata['tablename'] == tablename, 'num_unique_gaps'] = num_unique_gaps

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

def _updatePreHistory(lookupTable: pd.DataFrame, ib: 'IBKRConnection'):
    """
        update pre-history for records in the db 
            inputs: 
                lookupTable: pd.DataFrame - DataFrame of records that need to be updated. Expected columns: ['symbol', 'lastTradeDate', 'interval', 'firstRecordDate', 'numMissingBusinessDays', 'name']
                ib: IBKRConnection - IBKR connection object 
            pseudo:
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
    print('[yellow]----- Updating missing historical data ------[/yellow]')
    print('[green]----------------------------------------------[/green]\n')    
    # make sure interval formatting matches ibkr rqmts e.g. 5 mins, 1 day 
    lookupTable['interval'] = lookupTable.apply(lambda row: _addspace(row['interval']), axis=1)
    
    # lookupTable = lookupTable.loc[lookupTable['lastTradeDate'] > datetime.today().strftime('%Y%m')].reset_index(drop=True)

    lookupTable = lookupTable.loc[lookupTable['numMissingBusinessDays'] > 0].reset_index(drop=True)
    # set Exchange lookup 
    uniqueSymbol = lookupTable.drop_duplicates(subset=['symbol'])
    uniqueSymbol = uniqueSymbol.assign(exchange=uniqueSymbol.apply(lambda row: _get_exchange_for_symbol(row['symbol']), axis=1))
    
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
        if lookback <= 0:
            print(' [green]No data left [/green]for %s %s %s!'%(record['symbol'], record['lastTradeDate'], record['interval']))
            with db.sqlite_connection(dbName_futures) as conn:
                earliestAvailableTimestamp = db._getFirstRecordDate(record, conn)
                db._update_symbol_metadata(conn, record['name'], earliestTimestamp=earliestAvailableTimestamp, numMissingDays=0, type='future')
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
                db._update_symbol_metadata(conn, record['name'], earliestTimestamp=earliestAvailableTimestamp, numMissingDays=0, type='future')
            continue
        
        # update history to the db
        history['symbol'] = record['symbol']
        history['interval'] = record['interval'].replace(' ', '')
        history['lastTradeDate'] = record['lastTradeDate']        
        with db.sqlite_connection(dbName_futures) as conn:
            db.saveHistoryToDB(history, conn, earliestAvailableTimestamp)
        
        if i != len(lookupTable)-1:
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
    # updateRecords(ib)       
    for i in range(15):
        with db.sqlite_connection(dbName_futures) as conn:
            lookupTable = db.getLookup_symbolRecords(conn)
        # _updatePreHistory(lookupTable, ib)

        with db.sqlite_connection(dbName_futures) as conn:
            # generate_pxhistory_metadata_master_table(conn)
            update_gaps_in_pxhistory_metadata(conn)
            update_gaps_in_pxhistory(conn, ib)
        # Refresh ib connection 
        if i%3 == 0:
            ib = ibkr.refreshConnection(ib)
    