"""
This module maintains futures data. 

General logic: 
    1. Read watchlist csv file
    2. Check db for latest available data
    3. If latest data is not up to date, grab new data from IBKR, and update the db 
    4. if there are any new contracts in the watchlist, grab new data from IBKR, and update the db
    
"""
from locale import currency
import time
import re 
import config
import math

from ib_insync import Future
import pandas as pd
import numpy as np
# import exchange_calendars as xcals
import pandas_market_calendars as xcals
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
_calendar_cache = {}

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
    # contract = Future(symbol=symbol, lastTradeDateOrContractMonth=expiry, exchange=exchange, currency=currency, includeExpired=True)
    if symbol == 'SI':
        contract = Future(symbol=symbol, lastTradeDateOrContractMonth=expiry, exchange=exchange, currency=currency, includeExpired=True, multiplier="5000")
    else:
        contract = Future(symbol=symbol, lastTradeDateOrContractMonth=expiry, exchange=exchange, currency=currency, includeExpired=True)
    
    # Get futures history from ibkr
    # Split into multiple calls for shorter intervals so we can get more data in 1 go   
    if (interval in ['1 min', '5 mins', '1min', '5mins']) and int(lookback.strip(' D')) > 3:
        # calculate number of calls needed
        numCalls = math.ceil(int(lookback.strip(' D'))/3)
        record=pd.DataFrame()
        # loop for numCalls appending records and reducing endDate by lookback each time
        for i in range(0, numCalls):
            # bars = ibkr.getBars_futures(ib, symbol=symbol, lastTradeDate=expiry, interval=interval, endDate=endDate, lookback='3 D', exchange=exchange)
            bars = ibkr.getBars_futures(ib, contract, interval=interval, endDate=endDate, lookback='3 D')
            # drop the last bar as its likely incomplete 
            if (bars is None) or (bars.empty):
                break
            else:
                # bars = bars.iloc[:-1,:]
                record = record._append(bars)   
                endDate = record['date'].min() # update endDate for next loop 
                if i < numCalls-1:
                    print('%s: [orange]sleeping for %ss...[/orange]'%(datetime.now().strftime('%H:%M:%S'), _defaultSleepTime/30))
                    time.sleep(_defaultSleepTime/30)
        record.reset_index(drop=True, inplace=True)
    else:
        # query ibkr for futures history 
        record = ibkr.getBars_futures(ib, contract, interval=interval, endDate=endDate, lookback=lookback)
    
    # print(record)
    # exit() 

    # handle case where no records are returned
    if (record is None) or (record.empty) or len(record) == 1:
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
                db.insert_new_record_metadata(conn, tablename, type='future', earliestTimestamp= _earliestTimeStamp, numMissingDays = 0)
    else:
        record = record.iloc[:-1, :] # drop last bar as its likely incomplete
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
    # filter for relevant symbol
    latestRecords = latestRecords.loc[latestRecords['symbol'] == symbol].reset_index(drop=True)

    # add lookup column 
    latestRecords['type/expiry'] = latestRecords['name'].apply(lambda x: '_'.join(x.split('_')[1:2]))
    
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



    ######################################## add missing contracts to our db
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

    
    
    
    ######################################### update records in our db that have not been updated in in over 24 hours 
    if not latestRecords.loc[latestRecords['daysSinceLastUpdate'] > 1].empty:
        print('[green]----------------------------------------------[/green]')
        print('[yellow]--------- Updating outdated records ----------[/yellow]')
        print('[green]----------------------------------------------[/green]')
        i=1
        latestRecords = latestRecords.loc[(latestRecords['symbol'] == 'SI') & (latestRecords['interval'] == '1min')].reset_index(drop=True) # for testing only, filter for specific symbol and interval
        # print(latestRecords)
        # exit() 
        for row in (latestRecords.loc[latestRecords['daysSinceLastUpdate'] >= 1]).iterrows():
            print('%s: (%s/%s) Updating contract %s %s %s'%(datetime.now().strftime('%H:%M:%S'), i,latestRecords.loc[latestRecords['daysSinceLastUpdate']>=1]['symbol'].count(), row[1]['symbol'], row[1]['type/expiry'], row[1]['interval']) )
            _updateSingleRecord(ib_, row[1]['symbol'], row[1]['type/expiry'], row[1]['interval'], str(row[1]['daysSinceLastUpdate']+1)+' D')
            i+=1
    print('[green]----------------------------------------------[/green]')
    print('[green]---- Completed updating outdated records ----[/green]')
    print('[green]----------------------------------------------[/green]\n')
        
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

def _get_exchange_calendar(exchange):
    """
        Returns (calendar_code, exchange_calendars calendar) for an IBKR-style exchange code.
        Returns (None, None) and logs a warning when mapping is unavailable.
    """
    exchange = str(exchange or '').upper()
    calendar_code = config.exchange_calendar_mapping.get(exchange)
    if not calendar_code:
        print('%s: [red]No exchange_calendars mapping found for exchange %s[/red]' % (datetime.now().strftime('%H:%M:%S'), exchange))
        return None, None

    if calendar_code in _calendar_cache:
        return calendar_code, _calendar_cache[calendar_code]

    try:
        calendar = xcals.get_calendar(calendar_code)
    except Exception as ex:
        print('%s: [red]Unable to load exchange calendar %s for %s: %s[/red]' % (datetime.now().strftime('%H:%M:%S'), calendar_code, exchange, str(ex)))
        return calendar_code, None

    _calendar_cache[calendar_code] = calendar
    return calendar_code, calendar

def _get_calendar_schedule(exchange, start_date, end_date):
    """
        Returns schedule dataframe for [start_date, end_date] and calendar code.
    """
    calendar_code, calendar = _get_exchange_calendar(exchange)
    
    if calendar is None:
        return pd.DataFrame(), calendar_code

    try:
        # schedule = calendar.schedule.loc[pd.Timestamp(start_date):pd.Timestamp(end_date)].copy()
        schedule = calendar.schedule(pd.Timestamp(start_date), pd.Timestamp(end_date))
    except Exception as ex:
        print('%s: [red]Unable to fetch schedule for %s (%s): %s[/red]' % (datetime.now().strftime('%H:%M:%S'), exchange, calendar_code, str(ex)))
        return pd.DataFrame(), calendar_code

    return schedule, calendar_code

def _get_schedule_open_close_columns(schedule):
    """
        Resolves open/close column names across exchange_calendars versions.
    """
    open_candidates = ['open', 'market_open']
    close_candidates = ['close', 'market_close']

    open_col = next((col for col in open_candidates if col in schedule.columns), None)
    close_col = next((col for col in close_candidates if col in schedule.columns), None)
    return open_col, close_col

def _build_schedule_day_type_map(schedule):
    """
        Classifies schedule sessions with shortened regular trading hours.
        Note: exchange_calendars captures regular trading sessions, so this only
        identifies shortened RTH days. Evening-only behavior is inferred from
        observed bars in _build_intraday_day_type_counts.
    """
    if schedule.empty:
        return {}

    open_col, close_col = _get_schedule_open_close_columns(schedule)
    if (open_col is None) or (close_col is None):
        return {}

    session_df = schedule[[open_col, close_col]].copy()

    # convert open and close columns to est 
    session_df[open_col] = pd.to_datetime(session_df[open_col], utc=True).dt.tz_convert('US/Eastern')
    session_df[close_col] = pd.to_datetime(session_df[close_col], utc=True).dt.tz_convert('US/Eastern')

    # adjust close to be 1h before current close 
    # session_df[close_col] = session_df[close_col] - pd.Timedelta(minutes=60)

    session_df['trade_date'] = pd.to_datetime(session_df.index).date
    session_df['session_minutes'] = (session_df[close_col] - session_df[open_col]).dt.total_seconds() / 60.0

    session_df['weekday'] = pd.to_datetime(session_df['trade_date']).dt.weekday


    weekday_minutes = session_df.loc[session_df['weekday'] < 5, 'session_minutes']
    if weekday_minutes.empty:
        return {}

    regular_baseline_minutes = float(np.nanmedian(weekday_minutes.to_numpy()))
    short_ratio = float(getattr(config, 'exchange_calendar_short_session_ratio', 0.85))
    day_type_map = {}

    for row in session_df.itertuples(index=False):
        if pd.isna(row.session_minutes):
            continue

        is_short = float(row.session_minutes) < (regular_baseline_minutes * short_ratio)
        if not is_short:
            continue

        day_type_map[row.trade_date] = 'holiday_reduced_hours'
    
    return day_type_map

def _time_to_minutes(value):
    """
        Converts HH:MM[:SS] or datetime-like value to minutes from midnight.
    """
    if pd.isna(value):
        return None
    value = str(value)
    if ' ' in value:
        value = value.split(' ')[-1]
    parts = value.split(':')
    if len(parts) < 2:
        return None
    try:
        return (int(parts[0]) * 60) + int(parts[1])
    except Exception:
        return None

def _build_expected_trading_days(start_date, end_date, last_trade_date, exchange, country='US'):
    """
        Builds expected trading sessions list between two dates using exchange calendar schedule.
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

    schedule, calendar_code = _get_calendar_schedule(exchange, start_date, end_date)
    if schedule.empty:
        print('%s: [red]No schedule rows returned for exchange %s (%s). Failing closed.[/red]' % (datetime.now().strftime('%H:%M:%S'), exchange, calendar_code))
        return []

    schedule_dates = pd.to_datetime(schedule.index).date
    return [
        pd.to_datetime(d).strftime('%Y-%m-%d') for d in schedule_dates
        if d < last_trade_date
    ]

def _is_intraday_interval(interval_value):
    """
        Returns True when interval is intraday (minute/hour granularity).
    """
    interval_str = str(interval_value).lower().replace(' ', '')
    return ('min' in interval_str) or ('hour' in interval_str)

def _extract_last_trade_date_from_tablename(tablename, fallback_date=None):
    """
        Extracts YYYYMMDD token from table name and returns date.
        Uses fallback_date when parsing fails.
    """
    tokens = str(tablename).split('_')
    for token in tokens:
        if re.fullmatch(r'\d{8}', token):
            try:
                return pd.to_datetime(token, format='%Y%m%d').date()
            except Exception:
                continue

    if fallback_date is None:
        return None
    return pd.to_datetime(fallback_date).date()

def _get_intraday_daily_session_stats(conn, quoted_tablename):
    """
        Returns daily intraday session stats needed for day-type classification.
    """
    return pd.read_sql(
        'SELECT '
        'DATE(date) AS trade_date, '
        'COUNT(*) AS daily_count, '
        'MIN(TIME(date)) AS first_bar_time, '
        'MAX(TIME(date)) AS last_bar_time, '
        'CAST((JULIANDAY(MAX(date)) - JULIANDAY(MIN(date))) * 24 * 60 AS INTEGER) AS session_span_minutes '
        'FROM %s '
        'GROUP BY DATE(date)' % quoted_tablename,
        conn
    )

def _build_intraday_day_type_counts(daily_counts: pd.DataFrame, schedule_day_type_map, last_trade_date):
    """
        Classifies weekday daily bar counts into day types, combining:
        1) schedule-derived shortened RTH days, and
        2) observed all-hours bar shape for evening-dominant sessions.
    """
    if daily_counts.empty:
        return pd.DataFrame(columns=['trade_date', 'daily_count', 'first_bar_time', 'last_bar_time', 'session_span_minutes', 'day_type'])

    typed = daily_counts.copy()
    typed['trade_date'] = pd.to_datetime(typed['trade_date']).dt.date
    typed = typed.loc[pd.to_datetime(typed['trade_date']).dt.weekday < 5].copy()
    if typed.empty:
        return typed.assign(day_type=pd.Series(dtype='object'))

    if 'first_bar_time' not in typed.columns:
        typed['first_bar_time'] = None
    if 'last_bar_time' not in typed.columns:
        typed['last_bar_time'] = None
    if 'session_span_minutes' not in typed.columns:
        typed['session_span_minutes'] = np.nan

    typed['day_type'] = 'regular_weekday'
    typed.loc[pd.to_datetime(typed['trade_date']).dt.weekday == 4, 'day_type'] = 'friday'

    typed['day_type'] = typed.apply(
        lambda row: schedule_day_type_map.get(row['trade_date'], row['day_type']),
        axis=1
    )

    # exchange_calendars defines regular-hours sessions only; infer evening-only
    # from observed all-hours bars when a shortened RTH day starts in evening.
    evening_open_minute = int(getattr(config, 'exchange_calendar_evening_open_minute_utc', 15 * 60))
    first_minutes = typed['first_bar_time'].apply(_time_to_minutes)
    evening_only_mask = (
        (typed['day_type'] == 'holiday_reduced_hours')
        & first_minutes.notnull()
        & (first_minutes >= evening_open_minute)
    )
    typed.loc[evening_only_mask, 'day_type'] = 'holiday_evening_only'

    if last_trade_date is not None:
        typed.loc[typed['trade_date'] == last_trade_date, 'day_type'] = 'last_trade_date'
    return typed

def _compute_intraday_expected_counts_by_day_type(typed_counts: pd.DataFrame, min_samples=3):
    """
        Builds robust expected bar-count baselines per day type.
    """
    if typed_counts.empty:
        return {}, 0

    def _p80(series):
        if series.empty:
            return 0
        return int(round(float(np.percentile(series.to_numpy(), 80))))

    weekday_counts = typed_counts['daily_count']
    weekday_baseline = _p80(weekday_counts)

    holiday_counts_reduced = typed_counts.loc[
        typed_counts['day_type'].isin(['holiday_reduced_hours']),
        'daily_count'
    ]

    holiday_counts_evenings = typed_counts.loc[
        typed_counts['day_type'].isin(['holiday_evening_only']),
        'daily_count'
    ]

    holiday_baseline_reduced = _p80(holiday_counts_reduced) if not holiday_counts_reduced.empty else 0
    holiday_baseline_evening = _p80(holiday_counts_evenings) if not holiday_counts_evenings.empty else 0

    expected_by_type = {}
    for day_type in ['regular_weekday', 'friday', 'holiday_reduced_hours', 'holiday_evening_only', 'last_trade_date']:
        series = typed_counts.loc[typed_counts['day_type'] == day_type, 'daily_count']
        if day_type == 'holiday_evening_only':
            sparse_fallback = holiday_baseline_evening if holiday_baseline_evening > 0 else int(round(weekday_baseline * 0.25))
        elif day_type == 'holiday_reduced_hours':
            sparse_fallback = holiday_baseline_reduced if holiday_baseline_reduced > 0 else int(round(weekday_baseline * 0.75))
        else:
            sparse_fallback = weekday_baseline
        
        if len(series) >= min_samples:
            baseline = _p80(series)
        elif len(series) > 0:
            median_count = int(round(float(series.median())))
            if sparse_fallback > 0:
                baseline = max(median_count, int(round(sparse_fallback * 0.9)))
            else:
                baseline = median_count
        else:
            baseline = sparse_fallback

        if baseline < 0:
            baseline = 0
        expected_by_type[day_type] = baseline

    return expected_by_type, weekday_baseline

def _intraday_min_acceptable_count(expected_count, day_type):
    """
        Returns minimum acceptable bar count before a day is treated as incomplete.
    """
    tolerance_by_type = {
        'regular_weekday': (0.10, 2),
        'friday': (0.15, 2),
        'holiday_reduced_hours': (0.20, 2),
        'holiday_evening_only': (0.25, 2),
        'last_trade_date': (0.25, 2),
    }

    expected_count = int(expected_count)
    if expected_count <= 0:
        return 0

    tolerance_ratio, min_abs_tolerance = tolerance_by_type.get(day_type, (0.10, 2))
    allowed_shortfall = max(int(np.ceil(expected_count * tolerance_ratio)), min_abs_tolerance)
    return max(0, expected_count - allowed_shortfall)

def _get_incomplete_intraday_dates(daily_counts: pd.DataFrame, schedule_day_type_map, last_trade_date):
    """
        Returns dates with materially low intraday bar counts by day type.
    """
    typed_counts = _build_intraday_day_type_counts(daily_counts, schedule_day_type_map, last_trade_date)
    if typed_counts.empty:
        return []
    

    expected_by_type, weekday_baseline = _compute_intraday_expected_counts_by_day_type(typed_counts)

    if weekday_baseline <= 0:
        return []

    incomplete_dates = []
    for row in typed_counts.itertuples(index=False):
        expected = int(expected_by_type.get(row.day_type, weekday_baseline) or weekday_baseline)
        if expected <= 0:
            continue
        min_acceptable = _intraday_min_acceptable_count(expected, row.day_type)
        observed_count = pd.to_numeric(row.daily_count, errors='coerce')
        if pd.isna(observed_count):
            continue
        if int(observed_count) < min_acceptable:
            incomplete_dates.append(row.trade_date)

    return sorted(set(incomplete_dates))

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

        # if lastTradeDate is empty, infer it from table name by scanning for YYYYMMDD token
        inferred_last_trade_date = row.lastTradeDate
        if pd.isna(inferred_last_trade_date):
            inferred_date = _extract_last_trade_date_from_tablename(row.name)
            if inferred_date is None:
                print('%s: [red]Error inferring lastTradeDate for %s, skipping...[/red]'%(datetime.now().strftime('%H:%M:%S'), tablename))
                continue
            inferred_last_trade_date = inferred_date.strftime('%Y-%m-%d')

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

        schedule, calendar_code = _get_calendar_schedule(exchange, min_date, end_date)
        if schedule.empty:
            print('%s: [red]No calendar schedule for %s (%s), skipping table %s[/red]' % (datetime.now().strftime('%H:%M:%S'), exchange, calendar_code, tablename))
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

        # For intraday tables, treat materially undersized sessions as gaps using day-type-aware thresholds.
        last_incomplete_intraday_date = pd.NaT
        if _is_intraday_interval(row.interval):
            daily_counts = _get_intraday_daily_session_stats(conn, quoted_tablename)
            schedule_day_type_map = _build_schedule_day_type_map(schedule)
            incomplete_intraday_dates = _get_incomplete_intraday_dates(
                daily_counts=daily_counts,
                schedule_day_type_map=schedule_day_type_map,
                last_trade_date=last_trade_date
            )
            if incomplete_intraday_dates:
                last_incomplete_intraday_date = pd.to_datetime(max(incomplete_intraday_dates))

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
        Returns the newest candidate gap date (pd.Timestamp) in a table that is
        strictly earlier than start_after_date (resume cursor).

        If start_after_date is None, search starts from the newest candidate.
        Gap definitions:
        1) Missing expected trading session from exchange_calendars schedule
        2) Incomplete intraday session based on schedule-aware day-type baseline and tolerance
           - day types include regular weekdays, Fridays, holiday_reduced_hours,
             holiday_evening_only, and last_trade_date

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

    if (start_after_date is None) or pd.isna(start_after_date):
        cursor_date = max_date + pd.to_timedelta(1, unit='D')
    else:
        cursor_date = pd.to_datetime(start_after_date).date()

    last_trade_date = _extract_last_trade_date_from_tablename(tablename, fallback_date=max_date)
    if last_trade_date is None:
        return pd.NaT

    # get exchange calendar schedule for the date range in the table
    schedule, calendar_code = _get_calendar_schedule(exchange=exchange, start_date=min_date, end_date=max_date)
    if schedule.empty:
        print('%s: [red]No calendar schedule for %s (%s), failing closed for %s[/red]' % (datetime.now().strftime('%H:%M:%S'), exchange, calendar_code, tablename))
        return pd.NaT

    # build schedule day type map for classifying intraday sessions later when checking for incomplete sessions
    schedule_day_type_map = _build_schedule_day_type_map(schedule)
    
    expected_days = _build_expected_trading_days(min_date, max_date, last_trade_date, exchange=exchange, country=country)
    if not expected_days:
        return pd.NaT

    daily_counts = _get_intraday_daily_session_stats(conn, quoted_tablename)
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

    # Incomplete intraday weekdays by day type with tolerance.
    incomplete_intraday_dates = []
    if _is_intraday_interval(interval):
        incomplete_intraday_dates = _get_incomplete_intraday_dates(
            daily_counts=daily_counts,
            schedule_day_type_map=schedule_day_type_map,
            last_trade_date=last_trade_date
        )
    # print(incomplete_intraday_dates)
    # exit() 

    # print(missing_dates)
    # print('\n')
    gap_dates = sorted(set(missing_dates + incomplete_intraday_dates))
    next_gaps = [d for d in gap_dates if d < cursor_date]
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
    pxhistory_metadata = pxhistory_metadata.loc[pxhistory_metadata['date_of_last_gap_date_polled'] != "1989-12-30 00:00:00"].reset_index(drop=True)

    # select where tablename contains VIX
    # pxhistory_metadata = pxhistory_metadata.loc[pxhistory_metadata['tablename'].str.contains('VIX')].reset_index(drop=True)

    # print(pxhistory_metadata)
    # exit() 
    # update gaps for each table in the db 
    for idx, row in pxhistory_metadata.iterrows():
        tablename = row['tablename']
        exchange = _get_exchange_for_symbol(tablename.split('_')[0])
        print('%s: [yellow]Checking gaps for %s:%s...[/yellow]'%(datetime.now().strftime('%H:%M:%S'), exchange, tablename))
        # pxHistory = db.getTable(conn, tablename)
        symbol, expiry, interval = tablename.split('_')

        if symbol == 'SI':
            contract = Future(symbol=symbol, lastTradeDateOrContractMonth=expiry, exchange=exchange, currency='USD', includeExpired=True, multiplier="5000")
        else:
            contract = Future(symbol=symbol, lastTradeDateOrContractMonth=expiry, exchange=exchange, currency='USD', includeExpired=True)
        
        # if expiry is more than 2 years ago, update metadata to reflect no gaps and continue
        if pd.to_datetime(expiry) < datetime.today() - relativedelta(years=2):
            print('%s: [yellow]Expiry for %s is more than 2 years ago, marking as no gaps and skipping...[/yellow]'%(datetime.now().strftime('%H:%M:%S'), tablename))
            update_metadata(pxhistory_metadata, tablename, DEFAULT_DATE_IF_NO_GAPS, num_unique_gaps=0)
            continue

        # Determine next candidate gap date strictly before the saved cursor.
        date_with_missing_data = find_next_gap_date_in_table(
            conn,
            tablename,
            interval,
            exchange,
            start_after_date=row['date_of_last_gap_date_polled']
        )
        
        # if return is pd.NatT, that means there are no more gaps to update. Update metadata to reflect this and continue
        if pd.isna(date_with_missing_data):
            update_metadata(pxhistory_metadata, tablename, DEFAULT_DATE_IF_NO_GAPS, num_unique_gaps=0)
            continue

        # get gap data from ibkr 
        print('%s: [yellow]Updating gap history for %s, gap date: %s...[/yellow]'%(datetime.now().strftime('%H:%M:%S'), tablename, date_with_missing_data.strftime('%Y-%m-%d')))     
        

        # ibkr_pxhistory = ibkr.getBars_futures(ib, symbol=symbol, lastTradeDate=expiry, interval=_addspace(interval), endDate=date_with_missing_data + pd.to_timedelta(1, unit='D'), lookback=ibkr_lookback_period, exchange=exchange)
        ibkr_pxhistory = ibkr.getBars_futures(ib, contract, interval=_addspace(interval), endDate=date_with_missing_data + pd.to_timedelta(1, unit='D'), lookback=ibkr_lookback_period)
        # print(ibkr_pxhistory)
        # exit() 
        if ibkr_pxhistory is None:
            print('%s: [green]No records found for %s, with endDate: %s [/green]'%(datetime.now().strftime('%H:%M:%S'), tablename, date_with_missing_data.strftime('%Y-%m-%d')))
            # advance cursor even when no records are returned so resume continues backwards
            update_metadata(pxhistory_metadata, tablename, date_with_missing_data)
            continue        
        ibkr_pxhistory['symbol'] = symbol
        ibkr_pxhistory['interval'] = interval
        ibkr_pxhistory['lastTradeDate'] = expiry                 

        # save history to db 
        if expiry > datetime.today().strftime('%Y%m%d'):
            earliestTimestamp = ibkr.getEarliestTimeStamp_m(ib, symbol=symbol, lastTradeDate=expiry, exchange=exchange)
        else:
            earliestTimestamp = pd.to_datetime((datetime.today() - relativedelta(years=2)).strftime('%Y-%m-%d'))
        db.saveHistoryToDB(ibkr_pxhistory, conn, earliestTimestamp=earliestTimestamp, type='future')
        print('%s: [green]Record %s of %s updated for %s, sleeping for %ss...[/green]'%(datetime.now().strftime('%H:%M:%S'),idx+1,len(pxhistory_metadata), tablename, _defaultSleepTime/30))

        # Update metadata cursor to the candidate date that was just scanned.
        update_metadata(pxhistory_metadata, tablename, date_with_missing_data)

        # sleep before next record 
        time.sleep(_defaultSleepTime/30)
        print('%s: [yellow]Sleeping for %ss[/yellow]\n'%(datetime.now().strftime('%H:%M:%S'), _defaultSleepTime/30))
        

    print('%s: [green]DONE! Completed updating gaps in pxhistory_metadata, cleaning up metadata[/green]\n'%(datetime.now().strftime('%H:%M:%S')))
    # make sure updatde_date and date_og_last_gap_date_polled are datetime 
    pxhistory_metadata['update_date'] = pd.to_datetime(pxhistory_metadata['update_date'])
    pxhistory_metadata['date_of_last_gap_date_polled'] = pd.to_datetime(pxhistory_metadata['date_of_last_gap_date_polled'])

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
    
    # filter out records that dont need updating 
    lookupTable = lookupTable.loc[lookupTable['numMissingBusinessDays'] > 1].reset_index(drop=True)
    lookupTable = lookupTable.loc[lookupTable['lastTradeDate'] > (datetime.today() - relativedelta(years=2)).strftime('%Y%m')].reset_index(drop=True)
    lookupTable.sort_values(by=['interval'], inplace=True)
    lookupTable.reset_index(drop=True, inplace=True)
    print(lookupTable)
    
    
    # set Exchange lookup 
    uniqueSymbol = lookupTable.drop_duplicates(subset=['symbol'])
    # uniqueSymbol = uniqueSymbol.assign(exchange=uniqueSymbol.apply(lambda row: _get_exchange_for_symbol(row['symbol']), axis=1))   
    # uniqueSymbol['earliestTimeStamp'] = (datetime.today() - relativedelta(years=2)).strftime('%Y%m%d %H:%M:%S')

    i=0
    for index, record in lookupTable.iterrows():  
        lookback = 100
        i+=1

        # set exchange 
        exchange = _get_exchange_for_symbol(record['symbol'])#uniqueSymbol.loc[uniqueSymbol['symbol'] == record['symbol']]['exchange'].iloc[0]

        # define contract 
        if record['symbol'] == 'SI':
            contract = Future(symbol=record['symbol'], lastTradeDateOrContractMonth=record['lastTradeDate'], exchange=exchange, currency='USD', includeExpired=True, multiplier="5000")
        else:
            contract = Future(symbol=record['symbol'], lastTradeDateOrContractMonth=record['lastTradeDate'], exchange=exchange, currency='USD', includeExpired=True)

        print('%s: [yellow]Record [/yellow]%s of %s: %s-%s-%s'%(datetime.now().strftime("%H:%M:%S"),index, len(lookupTable), record.symbol, record['lastTradeDate'], record['interval']))
        
        # set end date 
        endDate = (record['firstRecordDate'] + relativedelta(days=1)).strftime('%Y%m%d %H:%M:%S')
        
        # earleist possible timestamp is limited by api at 2 years 
        earliestAvailableTimestamp = (datetime.today() - relativedelta(years=2))#.strftime('%Y%m%d %H:%M:%S')
    
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
                for i in range(5): 
                    currentIterationBars = ibkr.getBars_futures(ib, contract, interval=record['interval'], endDate=endDate, lookback=(str(lookback) + ' D'))
                    if (currentIterationBars is None): 
                        break
                    else: 
                        history = pd.concat([history, currentIterationBars], ignore_index=True)
                        endDate = history['date'].min()
                        
            else:
                history = ibkr.getBars_futures(ib, contract, interval=record['interval'], endDate=endDate, lookback=(str(lookback) + ' D'))
        
        # skip to next if no data is returned
        if history is None or history.empty:
            print(' [green]No data left [/green]for %s %s %s!'%(record['symbol'], record['lastTradeDate'], record['interval']))
            with db.sqlite_connection(dbName_futures) as conn:
                earliestAvailableTimestamp = ibkr.getEarliestTimeStamp(ib, contract) #db._getFirstRecordDate(record, conn)
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
    