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

import pandas as pd
import interface_localDb as db
import interface_ibkr as ibkr

from datetime import datetime
from dateutil.relativedelta import relativedelta
from rich import print


"""
Config vars 
"""
_defaultSleepTime = 40 #seconds, wait time between ibkr api calls 

"""
    global variables
"""
filename_futuresWatchlist = 'futuresWatchlist.csv'
dbName_futures = 'historicalData_futures.db'
trackedIntervals = ['1 day']#, '1 hour', '30 mins', '5 mins', '1 min']
numExpiryMonths = 48 # number of future expiries we want to track at any given time 

## add a space between num and alphabet
def _addspace(myStr): 
    return re.sub("[A-Za-z]+", lambda elm: " "+elm[0],myStr )

"""
    returns [DataFrame] of watchlist (csv of futures)
"""
def _getWatchlist(filename):
    # read watchlist csv file
    watchlist = pd.read_csv(filename)

    #cleanup watchlist
    watchlist = pd.DataFrame(watchlist.columns).reset_index(drop=True)
    watchlist.rename(columns={0:'symbol'}, inplace=True)
    watchlist['symbol'] = watchlist['symbol'].str.strip(' ').str.upper()
    watchlist.drop_duplicates(inplace=True)
    watchlist.sort_values(by=['symbol'], inplace=True)

    return watchlist

"""
    Returns dataframe of records in the db  
"""
def _getLatestRecords():
    # get latest data from db
    with db.sqlite_connection(dbName_futures) as conn:
        latestData = db.getRecords(conn)
    return latestData

"""
    returns the minimum record date from target table
"""
def _getMinRecordDate(conn, tablename):
    sqlStatement = 'SELECT MIN(date) FROM \'%s\''%(tablename)
    minDate = pd.read_sql(sqlStatement, conn)

    return minDate.iloc[0]['MIN(date)']

""" returns number of business days 
    between two provided datetimes 
""" 
def _countWorkdays(startDate, endDate, excluded=(6,7)):
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

"""
    updates a single outdated table in the db
    input: 
    row
        lookupTable data with columns: 
        [ symbol, trade/expiry, interval, name(i.e. tablename), lastUpdateDate, daysSinceLastUpdate, firstRecordDate ]
    endDate
        datetime object of the date to look back from 
    algo: 
        1. calculate endDate as today or lastBusinessDay
        2. calculate lookback number of business days since last available record 
        3. get new data from ibkr
        4. update db
"""
def _updateSingleRecord(row, endDate):
    # query ibkr for futures history 
    record = ibkr.getBars_futures(ib, symbol=row['symbol'], lastTradeMonth=row['type/expiry'], interval=row['interval'], endDate=endDate, lookback=str(row['daysSinceLastUpdate'])+' D')
    record['symbol'] = row['symbol']
    record['interval'] = row['interval']
    record['lastTradeMonth'] = row['type/expiry']
    
    earlistTimestamp = ibkr.getEarliestTimeStamp(ib, symbol=row['symbol'], lastTradeMonth=row['type/expiry'])

    # save the data to db 
    with db.sqlite_connection(dbName_futures) as conn:
        # get earliest timestamp from ibkr
        db.saveHistoryToDB(record, conn, earlistTimestamp)


"""
    updates outdated records in the db 
"""
def updateRecords(updateThresholdDays = 1):
    ## get watchlist
    watchlist = _getWatchlist(filename_futuresWatchlist)

    ## get db records
    currentDbRecords = _getLatestRecords()
    
    # drop records where type/expiry column is the same as current date-month (formant YYYYMM)
    currentDbRecords = currentDbRecords.loc[currentDbRecords['type/expiry'] != datetime.today().strftime('%Y%m')].reset_index(drop=True)
    
    # calculate difference between numExpoiryMonths and max(index) of currentDbRecords
    deltaExpiryMonths = numExpiryMonths - currentDbRecords.index.max() - 1

    for i in range(deltaExpiryMonths):
        # get the next expiry month 
        print(i)
        expiryStr = datetime.strptime(currentDbRecords['type/expiry'].iloc[-1], '%Y%m')
        expiryStr += relativedelta(months=1)
        # add row to currentDbRecords
        newRow = pd.DataFrame({
                'symbol':[currentDbRecords['symbol'].iloc[-1]], 
                'type/expiry':[expiryStr.strftime('%Y%m')], 
                'interval':[currentDbRecords['interval'].iloc[-1]], 
                'name':[db._constructTableName(interval = currentDbRecords['interval'].iloc[-1], lastTradeMonth=expiryStr.strftime('%Y%m'), symbol=currentDbRecords['symbol'].iloc[-1])], 
                'lastUpdateDate':[''], 
                'daysSinceLastUpdate':[100], 
                'firstRecordDate':['']
            })
        #currentDbRecords = currentDbRecords.append(newRow, ignore_index=True)
        currentDbRecords = pd.concat([currentDbRecords, newRow], ignore_index=True)

    ## if there are records in the db, see what needs updating
    if not currentDbRecords.empty:
        records_with_old_data = currentDbRecords.loc[currentDbRecords['daysSinceLastUpdate'] > updateThresholdDays]
        newlyAddedSymbols = watchlist.loc[~watchlist['symbol'].isin(currentDbRecords['symbol'])]
    
    # update old records
    if not records_with_old_data.empty:
        print(' [yellow]Updating records with old data...[/yellow]')
        # for each row, index in records_with_old_data, call _updateSingleRecord(row)
        for index, row in records_with_old_data.iterrows():
            _updateSingleRecord(row, datetime.today())
    else:
        print('[green]  All FUTURES records are up to date![/green]\n')



def _dirtyRefreshLookupTable(ib): 
    with db.sqlite_connection(dbName_futures) as conn:
        # get lookup table 
        lookupTable = db.getLookup_symbolRecords(conn)
    # new df without duplicates in the symbol column
    trackedSymbols = lookupTable.drop_duplicates(subset=['symbol'])
    
    # add a new column with lambda function to get earliestimestamp from ibkr
    trackedSymbols['earliestTimestamp'] = trackedSymbols.apply(lambda row: ibkr.getEarliestTimeStamp(ib, symbol=row['symbol'], lastTradeMonth=row['lastTradingDate']), axis=1)

    # get list of all tablenames in the db
    with db.sqlite_connection(dbName_futures) as conn:
        # construct sql statement, excluding table names like '00-%'
        sqlStatement = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '00-%'"
        # get tablenames
        tablenames = pd.read_sql(sqlStatement, conn)
    
    # select only tablesnames that are not in the lookup table
    tablenames = tablenames.loc[~tablenames['name'].isin(lookupTable['name'])]

    # addd symbol, interval, and lastTradingDate columns to tablenames by splitting the name column by _
    tablenames[['symbol', 'lastTradingDate', 'interval']] = tablenames['name'].str.split('_', expand=True)

    # add new column firstRecordDate by applying lambda function _getMinRecordDate(tableName)
    with db.sqlite_connection(dbName_futures) as conn:
        tablenames['firstRecordDate'] = tablenames.apply(lambda row: _getMinRecordDate(conn, row['name']), axis=1)

    tablenames = tablenames.assign(numMissingBusinessDays=tablenames.apply(lambda row: _countWorkdays(row['firstRecordDate'], trackedSymbols.loc[trackedSymbols['symbol'] == row['symbol']]['earliestTimestamp'].iloc[0]), axis=1))

    # reorder columns as: 0, 1, 3, 4, 5, 2
    tablenames = tablenames.iloc[:,[0,1,3,4,5,2]]
    # connect to the futures db and append tablenames to the lookup table
    with db.sqlite_connection(dbName_futures) as conn:
        tablenames.to_sql('00-lookup_symbolRecords', conn, index=False, if_exists='append')

    
    # get the number of business days between calcdate and the firstRecordDate
    return

"""
    update pre-history for records in the db 
        inputs: 
            pd.lookuptable of records that need to be updated
            ibkr object 
        algo:
            1. add a space between digit and alphabet in the interval column 
            2. set lookback to 60
            3. iterate through each records and:
                a. set the endDate to the firstRecordDate
                b. query ibkr for history 
                c. skip to next if no data is returned
                d. append history to the db
"""
def _updatePreHistory(lookupTable, ib):
    
    # add a space between digit and alphabet in the interval column 
    lookupTable['interval'] = lookupTable.apply(lambda row: _addspace(row['interval']), axis=1)
    lookback = '100 D'
    # drop records where lastTradeMonth <= todays date in format YYYYMM
    lookupTable = lookupTable.loc[lookupTable['lastTradeMonth'] > datetime.today().strftime('%Y%m')].reset_index(drop=True)
    
    # select just the unique symbols from the lookup table
    uniqueSymbol = lookupTable.drop_duplicates(subset=['symbol'])

    # add the earliest available record date as a new column 
    uniqueSymbol = uniqueSymbol.assign(earliestTimeStamp = lambda row: ibkr.getEarliestTimeStamp(ib, symbol=row['symbol'][0], lastTradeMonth=row['lastTradeMonth'][0]), axis=1)

    # iterate through each record in the lookup table
    for index, record in lookupTable.iterrows():
        # set the endDate to the firstRecordDate
        endDate = record['firstRecordDate']
        
        # print loginfo
        print('looking up data for %s-%s, interval: %s'%(record.symbol, record['lastTradeMonth'], record['interval']))
        
        # query ibkr for history 
        history = ibkr.getBars_futures(ib, symbol=record['symbol'], lastTradeMonth=record['lastTradeMonth'], interval=record['interval'], endDate=endDate, lookback=lookback)
        
        # set earliestTimeStamp from the uniqueSymbol df
        earlistTimestamp = uniqueSymbol.loc[uniqueSymbol['symbol'] == record['symbol']]['earliestTimeStamp'].iloc[0]

        # skip to next if no data is returned
        if history.empty:
            print(' [yellow]No data found [/yellow]for %s %s %s. Skipping to next...'%(record['symbol'], record['lastTradeMonth'], record['interval']))
            continue
        
        # append symbol, interval, and lastTradeMonth columns to align with db schema
        history['symbol'] = record['symbol']
        history['interval'] = record['interval']
        history['lastTradeMonth'] = record['lastTradeMonth']
        
        # update history to the db
        with db.sqlite_connection(dbName_futures) as conn:
            print(' saving to db...')
            db.saveHistoryToDB(history, conn, earlistTimestamp)
        
        # sleep for 40s every 2 iterations
        if index != len(lookupTable)-1: # dont sleep if we're on the last record
            if index % 2 == 0:
                print('%s: [yellow]sleeping for %ss...[/yellow]\n'%(datetime.now().strftime('%H:%M:%S'), str(_defaultSleepTime)))
                time.sleep(_defaultSleepTime)
        
    return

"""
    Run this to initialize records in the db based on the watchlist 
"""
def initializeRecords(ib, watchlist,  updateThresholdDays=1):
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
            earlistTimestamp = ibkr.getEarliestTimeStamp(ib, symbol=watchlist['symbol'][numWatchlist], lastTradeMonth=expiryStr.strftime('%Y%m'))
            
            # get contracts for the next 45 months 
            for i in range(1, numExpiryMonths):
                for interval in trackedIntervals: # iterate through each interval
                    # get data for the contract, and interval 
                    print('looking up %s %s'%(watchlist['symbol'][numWatchlist], expiryStr.strftime('%Y%m')))
                    data = ibkr.getBars_futures(ib, symbol=watchlist['symbol'][numWatchlist], lastTradeMonth=expiryStr.strftime('%Y%m'), interval=interval)
                    
                    # skip if no data is returned
                    if data.empty:
                        print(' No data found, skipping to next contract...\n')
                        continue

                    # add columns to simplify life 
                    data['symbol'] = watchlist['symbol'][numWatchlist]
                    data['interval'] = interval.replace(' ', '')
                    data['lastTradeMonth'] = expiryStr.strftime('%Y%m')

                    # update local records
                    with db.sqlite_connection(dbName_futures) as conn:
                        print(' saving to db...\n')
                        db.saveHistoryToDB( data, conn, earlistTimestamp)
                    
                    # sleep for 40s
                    print('sleeping for %ss...'%(str(_defaultSleepTime)))
                    time.sleep(_defaultSleepTime)
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

#print(_getLatestData())
#watchlist = _getWatchlist(filename_futuresWatchlist)


#updateRecords(ib, watchlist)

#ng = ibkr.getBars_futures(ib, symbol='NG', lastTradeMonth='202310', interval='1 day')
#earlistTimestamp = ibkr.getEarliestTimeStamp(ib, symbol='NG', lastTradeMonth='202310')
#print(earlistTimestamp)

#with db.sqlite_connection(dbName_futures) as conn:
#    records = db.getRecords(conn)
#print(records)

#updateRecords()
#_dirtyRefreshLookupTable(ib)

print('\n[yellow] Checking FUTURES records... [/yellow]')
ib = ibkr.setupConnection()
updateRecords()

with db.sqlite_connection(dbName_futures) as conn:
    for i in (1,20):
        lookupTable = db.getLookup_symbolRecords(conn)
        _updatePreHistory(lookupTable, ib)
        time.sleep(300)


