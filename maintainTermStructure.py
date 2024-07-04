"""
    This module maintains term structure data for various futures contracts.
    Data is sourced from the local database with no connections required to the broker api.  
"""
import config 
import calendar
import traceback 

import datetime as dt
import holidays as hols 
import pandas as pd
import interface_localDB as db 
import checkDataIntegrity as cdi
from rich import print
from sys import argv
from dateutil.relativedelta import relativedelta

dbpath_termstructure = config.dbname_termstructure
dbpath_futures = config.dbname_futures
trackedIntervals = config.intervals

def _check_if_date_is_holiday(date):
    """
        Check if a given date is a holiday for a given country and exchange
    """
    holidays = hols.NYSE(years=date.year).keys()
    return date in holidays


def _get_expiry_date_for_month(symbol, date): 
    date = date.date()
    if symbol.upper() == 'VIX':
        ### https://www.cboe.com/tradable_products/vix/vix_futures/specifications/
        
        if date.month == 12: 
            third_friday_next_month = dt.date(date.year + 1, 1, 15)
        else:
            third_friday_next_month = dt.date(date.year, date.month + 1, 15)
        
        one_day = dt.timedelta(days=1)
        thirty_days = dt.timedelta(days=30)
        while third_friday_next_month.weekday() != 4:
            third_friday_next_month = third_friday_next_month + one_day 
        
        if _check_if_date_is_holiday(third_friday_next_month):
            third_friday_next_month = third_friday_next_month - one_day
        
        expiry = third_friday_next_month - thirty_days
        if _check_if_date_is_holiday(expiry):
            expiry = expiry - one_day

        return expiry

def _adjust_expiry_date_for_roll_days(expiry_table):
    """
        Adjusts the expiry date for each contract in the expiry_table for roll days. 
        - If the current date is greater than the expiry date, the contract is rolled to the next month
        - If the current date is equal to the expiry date, the contract is rolled to the next month if the expiry date is the same as the current date
    """
    # make sure all columns are a date object 
    for col in expiry_table.columns:
        if col != 'date':
            expiry_table[col] = pd.to_datetime(expiry_table[col])
    month_columns = [col for col in expiry_table.columns if col.startswith('month')]
    # handle missing month columns
    if len(month_columns) == 0:
        print('ERROR: No month columns found in expiry table.', traceback.format_exc())
        exit() 
    
    for i in range(len(month_columns)-1):
        # first check where contract is expired vs. current date 
        expiry_table[month_columns[i]] = expiry_table.apply(lambda x: x[month_columns[i+1]] if x['date'] > x[month_columns[i]] else x[month_columns[i]], axis=1)
        # then roll the following months on the current date 
        # for i in range(2, len(month_columns)-1):
        expiry_table[month_columns[i]] = expiry_table.apply(lambda x: x[month_columns[i+1]] if x[month_columns[i]] == x[month_columns[i-1]] else x[month_columns[i]], axis=1)
    
    return expiry_table

def _generate_futures_contract_expiry_table(symbol, start_date, end_date, num_months_ahead):
    start_date = start_date.date()
    end_date = end_date.date()    
    date_range = pd.date_range(start=start_date, end=end_date, freq='B')
    
    expiry_table = pd.DataFrame({'date': date_range})
    # create expiry table for each month ahead 
    for i in range(0, num_months_ahead):
        expiry_table['month%s'%(i+1)] = expiry_table['date'].apply(lambda x: x + relativedelta(months=i))
        expiry_table['month%s'%(i+1)] = expiry_table['month%s'%(i+1)].apply(lambda x: _get_expiry_date_for_month(symbol, x))
    
    # adjust expiry calendar for roll days (i.e. where date > expiry date) 
    return _adjust_expiry_date_for_roll_days(expiry_table)

def _get_available_term_structure_data(symbol, interval, expiry_table):
    """
        Filters the expiry table for dates where TS can be calculated across the entire curve 
    """
    with db.sqlite_connection(dbpath_futures) as conn_futures:
        records = db.getLookup_symbolRecords(conn_futures)
    records = records.loc[records['interval'] == interval]
    records = records.loc[records['symbol'] == symbol]
    records['lastTradeDate'] = pd.to_datetime(records['lastTradeDate'])
    
    # find what data we have available locally for the given expiry table 
    available_data = expiry_table.copy()
    for col in available_data.columns:
        if col != 'date':
            available_data[col] = available_data[col].apply(lambda x: x in records['lastTradeDate'].values)

    # remove any dates where TS cannot be determined across the entire curve 
    available_data = available_data.loc[available_data['month1'] == True]
    first_index = available_data.index[0]
    expiry_table = expiry_table.iloc[first_index:]

    return expiry_table

def _averageContractVolume(conn, tablename):
    """
        Lambda function - Returns average volume for given tablename 
            - Used to determine which contract is most liquid for a given month, 
            and therefore worth tracking data for 
    """
    # get data from tablename 
    sql = "SELECT * FROM %s"%(tablename)
    df = pd.read_sql(sql, conn)
    
    # calculate average volume 
    averageVolume = df['volume'].mean()
    
    return averageVolume

def _getNextContracts(conn, symbol, numContracts, interval='1day'):
    """
        Gets next n contracts with the highest liquidity in the db 
    """
    # get lookup table that is an index of all the tables in our db
    lookupTable = db.getLookup_symbolRecords(conn)

    # explicitly convert lastTradeDate to datetime
    lookupTable['lastTradeDate'] = pd.to_datetime(lookupTable['lastTradeDate'])

    # Slect contracts in our db expiring after today  
    lookupTable = lookupTable.loc[(
        lookupTable['symbol'] == symbol) & 
        (lookupTable['interval'] == interval) & 
        (lookupTable['lastTradeDate'] > pd.Timestamp.today())].sort_values(by='lastTradeDate').reset_index(drop=True)
    
    # select highest volume contracts for a given month since we care about the most 
    # liquid contracts which are generally the standard expiries 
    lookupTable['averageVolume'] = lookupTable.apply(lambda x: _averageContractVolume(conn, x['name']), axis=1)
    lookupTable['lastTradeDate_month'] = lookupTable['lastTradeDate'].dt.month
    lookupTable = lookupTable.groupby(['lastTradeDate_month']).apply(lambda x: x.sort_values(by='averageVolume', ascending=False).head(1)).reset_index(drop=True)

    # sort by lastTradeDate and select just the next n contracts
    lookupTable = lookupTable.sort_values(by='lastTradeDate').reset_index(drop=True)
    lookupTable = lookupTable.head(numContracts)

    return lookupTable

def getTermStructure(symbol:str, interval='1day', lookahead_months=8): 
    """
        Gets term structure data for a given symbol
        @param symbol: symbol to get term structure for 
        @param interval: interval to get data for
        @param lookahead_months: number of months to look ahead
        @return: dataframe of term structure data: [date, close1, close2, ...]
    """
    
    symbol = symbol.upper() # read in pxhistory for next n contracts 
    with db.sqlite_connection(dbpath_futures) as conn_futures:
        lookupTable = _getNextContracts(conn_futures, symbol, lookahead_months, interval)
        # create list of pxHistory dataframes
        termStructure_raw = []
        # get price history for each relevant contract 
        for index, row in lookupTable.iterrows():
            pxHistory = db.getTable(conn_futures, row['name'])
            # set index to date column
            pxHistory.set_index('date', inplace=True)
            # rename close column
            pxHistory.rename(columns={'close': 'month%s'%(index+1)}, inplace=True)
            termStructure_raw.append(pxHistory['month%s'%(index+1)])
    termStructure = pd.concat(termStructure_raw, axis=1).sort_values(by='date').reset_index()
    # print(termStructure)    
    # drop records with NaN values
    termStructure.dropna(inplace=True)

    # sort termstructure by date 
    termStructure.sort_values(by='date', inplace=True)

    # add descriptive columns for later reference 
    termStructure['symbol'] = symbol
    termStructure['interval'] = interval
    return termStructure.reset_index(drop=True)

def get_term_structure_v2(symbol, interval, expiry_table):
    available_data = _get_available_term_structure_data(symbol, interval, expiry_table)

    month_columns = [col for col in available_data.columns if col.startswith('month')]
    unique_expiries = available_data[month_columns].stack().unique().strftime('%Y%m%d')

    # query pxhistory for each unique expiry date, saving to a list with key as expiry date
    print(unique_expiries)
    
    with db.sqlite_connection(dbpath_futures) as conn_futures:
        pxHistory_dict = {expiry: db.getTable(conn_futures, '%s_%s_%s'%(symbol, expiry, interval)) for expiry in unique_expiries}
            
    print(available_data.head(30))    
    exit()

def saveTermStructure(termStructure):
    """
        Save term structure data to local db
        - Handles duplicate records in input df <termStructure>
    """
    # set the tablename for insertion 
    dbpath_termstructure = config.dbname_termstructure
    tablename = '%s_%s'%(termStructure['symbol'][0], termStructure['interval'][0])

    # filter out existing records if termstructure data already exists 
    with db.sqlite_connection(dbpath_termstructure) as conn:
        # get tablenames in db 
        sql_tableNames = "SELECT name FROM sqlite_master WHERE type='table'"
        tableNames = pd.read_sql(sql_tableNames, conn)['name'].tolist()
        # filter out duplicates  
        if tablename in tableNames:
            ts_db = db.getTable(conn, tablename)
            termStructure = termStructure.loc[~termStructure['date'].isin(ts_db['date'])].copy()
    
    # handle case where we don't have any new term structure data to update
    if termStructure.empty:
        print('%s: [green]Termstructure data up to date for %s [/green]'%(pd.Timestamp.today(), tablename))
        return
    
    # format termstructure dataframe for insertion 
    termStructure.drop(columns=['symbol', 'interval'], inplace=True)

    # save termstructure to db 
    with db.sqlite_connection(dbpath_termstructure) as conn:
         termStructure.to_sql(tablename, conn, if_exists='append', index=False)
         db._removeDuplicates(conn, tablename)
    print('%s: [green]Updated term structure data for %s[/green]'%(pd.Timestamp.today(), tablename))

def update_term_structure_data(symbol):
    """ 
        Updates term structure data for all symbols being tracked in the db 
    """
    # get list of all symbols being tracked 
    # with db.sqlite_connection(dbpath_futures) as conn_futures:
    #     lookupTable = db.getLookup_symbolRecords(conn_futures)
    #     symbols = lookupTable['symbol'].unique()
    symbols = ['VIX']

    expiry_table = _generate_futures_contract_expiry_table(symbol, pd.Timestamp.today() - pd.DateOffset(months=24), pd.Timestamp.today(), 9)

    # Update term structure data for each symbol and tracked interval
    for symbol in symbols:
        for interval in trackedIntervals:
            x = get_term_structure_v2(symbol, interval=interval.replace(' ', ''), expiry_table=expiry_table)
            exit()
            missing = cdi._check_for_missing_dates_in_timeseries(x)
            saveTermStructure(x)

def getVixTermstructureFromCSV(path='vix.csv'): 
    """ 
        Returns nicely formatted vix termstrucuture data from csv 
        - assumes interval=1day 
    """
    # read in vix.csv termstructure data 
    vix_ts_raw = pd.read_csv(path)
    vix_ts_raw['date'] = pd.to_datetime(vix_ts_raw['date'], format='mixed', dayfirst=True)
    
    # drop the last column month8
    vix_ts_raw.drop(columns=['8'], inplace=True)
    # rename columns to month1, month2, etc.
    vix_ts_raw.rename(columns={'0': 'month1', '1': 'month2', '2': 'month3', '3': 'month4', '4': 'month5', '5': 'month6', '6': 'month7', '7':'month8'}, inplace=True)
    vix_ts_raw['symbol'] = 'VIX'
    vix_ts_raw['interval'] = '1day'
    
    return vix_ts_raw

if __name__ == '__main__':
    # if console arg = csvupdate then update vix termstructure data from csv
    if len(argv) > 1:
        if argv[1] == 'csvupdate':
            vix_ts_raw = getVixTermstructureFromCSV()
            saveTermStructure(vix_ts_raw)
        elif argv[1] == 'test':
            _generate_futures_contract_expiry_table('VIX', pd.Timestamp.today() - pd.DateOffset(months=24), pd.Timestamp.today(), 9)
    
    else:
        update_term_structure_data('VIX')

