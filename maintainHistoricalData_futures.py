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

import pandas as pd
import interface_localDb as db
import interface_ibkr as ibkr

from datetime import datetime
from dateutil.relativedelta import relativedelta
from rich import print

# set pands to print all rows in df 
pd.set_option('display.max_rows', None)
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
dbName_futures = 'historicalData_futures.db'
trackedIntervals = config.intervals
numExpiryMonths = 14 # number of future expiries we want to track at any given time 

"""
    adds a space between num and alphabet
"""
def _addspace(myStr): 
    return re.sub("[A-Za-z]+", lambda elm: " "+elm[0],myStr )

"""
    lambda function to set lookback based on interval
"""
def _setLookback(interval):
    lookback = '10 D'
    if interval in ['1 day']:
        lookback = '300 D'
    elif interval in ['30 mins', '5 mins']:
        lookback = '20 D'
    elif interval in ['1 min']:
        lookback = '5 D'
    return lookback

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

def _updateSingleRecord(ib, symbol, expiry, interval, lookback, endDate=''):
    # get exchange from lookup table 
    with db.sqlite_connection(dbName_futures) as conn:
        exchange = db.getLookup_exchange(conn, symbol)
    
    # Get futures history from ibkr
    # Split into multiple calls if interval is 1 min and lookback > 10  
    if (interval == '1 min') and int(lookback.strip(' D')) > 12:
        # calculate number of calls needed
        numCalls = int(int(lookback.strip(' D'))/12)
        record=pd.DataFrame()
        # loop for numCalls appending records and reducing endDate by lookback each time
        for i in range(1, numCalls):
            bars = ibkr.getBars_futures(ib, symbol=symbol, lastTradeDate=expiry, interval=interval, endDate=endDate, lookback='10 D', exchange=exchange)
            if not record.empty:
                record = record._append(bars)   
                endDate = record['date'].min() # update endDate for next loop 
                print('%s: [orange]sleeping for %ss...[/orange]'%(datetime.now().strftime('%H:%M:%S'), _defaultSleepTime/6))
                time.sleep(_defaultSleepTime/6)
            elif record.empty:
                i=numCalls # break loop when record is empty
        
        record.reset_index(drop=True, inplace=True)
    
    # otherwise made a single call to ibkr
    else:
        # query ibkr for futures history 
        record = ibkr.getBars_futures(ib, symbol=symbol, lastTradeDate=expiry, interval=interval, endDate=endDate, lookback=lookback, exchange=exchange)

    print(record)
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
                if interval in ['1day', '1month']:
                    _earliestTimeStamp = datetime.today().strftime('%Y%m%d')
                else:
                    _earliestTimeStamp = datetime.today().strftime('%Y%m%d %H:%M:%S')
                db._updateLookup_symbolRecords(conn, tablename, type = 'future', earliestTimestamp = _earliestTimeStamp, numMissingDays = 0)
    else:
        # format records before saving to db 
        record['symbol'] = symbol
        record['interval'] = interval
        record['lastTradeDate'] = expiry
        earlistTimestamp = ibkr.getEarliestTimeStamp_m(ib, symbol=symbol, lastTradeDate=expiry, exchange=exchange)

        # save the data to db 
        with db.sqlite_connection(dbName_futures) as conn:
            db.saveHistoryToDB(record, conn, earlistTimestamp, type='future')
    
    # sleep
    print('%s: [yellow]sleeping for %ss...[/yellow]\n'%(datetime.now().strftime('%H:%M:%S'), _defaultSleepTime/6))
    time.sleep(_defaultSleepTime/6)
        
"""
    returns contracts missing from the db for given specified 
    inputs: 
        latestRectords: [DataFrame] of latest records from the db (limited to 1 symbol)
        numMonths: [int] number of months into the future we want to track
    returns:
        [DataFrame] of missing contracts informat symbol_expiry_interval 
"""
def _getMissingContracts(ib, symbol, numMonths = numExpiryMonths):
    print(' %s:[yellow] Checking missing contracts for %s...[/yellow]'%(datetime.now().strftime('%H:%M:%S'), symbol))
    # get latest records from db 
    with db.sqlite_connection(dbName_futures) as conn:
        latestRecords = db.getRecords(conn)
        # select only records with symbol = symbol
        latestRecords = latestRecords.loc[latestRecords['symbol'] == symbol].reset_index(drop=True)

    # get contracts from ibkr 
    contracts = ibkr.getContractDetails(ib, symbol, 'future')
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
        print('  [green]No missing contracts found![/green]')
    else:
        print('[yellow] Found %s missing contracts[/yellow]\n'%(str(len(missingContracts))))
    return missingContracts.reset_index(drop=True)

""" 
    maps future expiry, symbol combo to ibkr unique id 
    TODO not implemented fully
"""
def uniqueIDMapper(ib, symbol, expiry): 
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

"""
    Updates existing, and adds missing records to the db
    logic: 
    - update existing records
    - add new contracts, if needed, to maintain numContract number of forward contracts being tracked 
    - finally add new contracts from the watchlist with numConctract number of forward contract 
"""
def updateRecords(ib_):     
    
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
    latestRecords = latestRecords.loc[latestRecords['type/expiry'] > datetime.today().strftime('%Y%m%d')].reset_index(drop=True)

    # open ibkr connection
    ib = ib_

    # find contracts missing from db 
    missingContracts = pd.DataFrame()
    for symbol in watchlist['symbol']:
        missingContracts = missingContracts._append(_getMissingContracts(ib, symbol))
    
    # print debug info to console 
    print('Total missing contracts: %s'%(str(len(missingContracts))))
    missingContracts['symbol'] = missingContracts['contract'].apply(lambda x: x.symbol)
    missingContracts['realExpirationDate'] = missingContracts['contract'].apply(lambda x: x.lastTradeDateOrContractMonth)
    print(missingContracts[['symbol', 'realExpirationDate', 'interval']])
    print('\n')
    
    # add lookback columnbased on interval
    missingContracts['lookback'] = missingContracts.apply(
        lambda row: _setLookback(row['interval']), axis=1)
    
    # add missing contracts to our db
    if not missingContracts.empty:
        print('[green]----------------------------------------------[/green]')
        print('[yellow]---------- Adding missing contracts ---------[/yellow]')
        print('[green]----------------------------------------------[/green]\n')
    for missingContract in missingContracts.iterrows():
        print('Adding contract %s %s %s'%(missingContract[1]['contract'].symbol, missingContract[1]['realExpirationDate'], missingContract[1]['interval']))
        _updateSingleRecord(ib, missingContract[1]['contract'].symbol, missingContract[1]['realExpirationDate'], missingContract[1]['interval'], missingContract[1]['lookback'])
        # sleep for defaulttime
        print('%s: sleeping for %ss...\n'%(datetime.now().strftime('%H:%M:%S'), _defaultSleepTime))
        time.sleep(_defaultSleepTime)

    print('[green]----------------------------------------------[/green]')
    print('[green]----- Completed adding missing contracts -----[/green]')
    print('[green]----------------------------------------------[/green]\n')

    # update records in our db that have not been updated in the last 24 hours 
    if not latestRecords.loc[latestRecords['daysSinceLastUpdate'] > 1].empty:
        print('[green]----------------------------------------------[/green]')
        print('[yellow]--------- Updating outdated records ----------[/yellow]')
        print('[green]----------------------------------------------[/green]\n')

        # print recodrs where type/expiry is before current date
        for row in (latestRecords.loc[latestRecords['daysSinceLastUpdate'] > 1]).iterrows():
            print('%s: Updating contract %s %s %s'%(datetime.now().strftime('%H:%M:%S'), row[1]['symbol'], row[1]['type/expiry'], row[1]['interval']))
            _updateSingleRecord(ib, row[1]['symbol'], row[1]['type/expiry'], row[1]['interval'], str(row[1]['daysSinceLastUpdate']+1)+' D')
    
    print('[green]----------------------------------------------[/green]')
    print('[green]---- Completed updating outdated records ----[/green]')
    print('[green]----------------------------------------------[/green]\n')
        

"""
    This function updates the past two years of futures data.
    use this when a symbol is first added from the watchlist  
"""
def loadExpiredContracts(ib, symbol, lastTradeDate, interval):
    ###############
    ## placeholder!!! needs to be implemented
    ###############
    
    ## manually setting contract expiry example
    conDetails = ibkr.getContractDetails(ib, symbol=symbol, type='future')
    
    conDetails[2].contract.lastTradeDateOrContractMonth = '20230820'
    record2 = ibkr._getHistoricalBars_futures(ib, conDetails[2].contract, interval=interval, endDate=datetime.today(), lookback='300 D', whatToShow='BID')

"""
    Gross. but use if necessary. 
    Use when lookup table hasnt been updated after a new contract is added 
"""
def _dirtyRefreshLookupTable(ib): 
    with db.sqlite_connection(dbName_futures) as conn:
        # get lookup table 
        lookupTable = db.getLookup_symbolRecords(conn)
    # new df without duplicates in the symbol column
    trackedSymbols = lookupTable.drop_duplicates(subset=['symbol'])
    
    # add a new column with lambda function to get earliestimestamp from ibkr
    trackedSymbols['earliestTimestamp'] = trackedSymbols.apply(lambda row: ibkr.getEarliestTimeStamp(ib, symbol=row['symbol'], lastTradeDate=row['lastTradingDate']), axis=1)

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
    print('[green]----------------------------------------------[/green]')
    print('[yellow]--------- Updating prehistorica data ---------[/yellow]')
    print('[green]----------------------------------------------[/green]\n')    
    # add a space between digit and alphabet in the interval column 
    lookupTable['interval'] = lookupTable.apply(lambda row: _addspace(row['interval']), axis=1)
    
    # drop records where lastTradeDate <= todays date in format YYYYMM
    lookupTable = lookupTable.loc[lookupTable['lastTradeDate'] > datetime.today().strftime('%Y%m')].reset_index(drop=True)

    # select records that are still missing dates 
    lookupTable = lookupTable.loc[lookupTable['numMissingBusinessDays'] > 0].reset_index(drop=True)

    # select just the unique symbols from the lookup table
    uniqueSymbol = lookupTable.drop_duplicates(subset=['symbol'])
    with db.sqlite_connection(dbName_futures) as conn:
        uniqueSymbol = uniqueSymbol.assign(exchange=uniqueSymbol.apply(lambda row: db.getLookup_exchange(conn, row['symbol']), axis=1))
    
    # set earliestTimestamp to 2 years ago in format YYYYMMDD HH:MM:SS
    uniqueSymbol['earliestTimeStamp'] = (datetime.today() - relativedelta(years=2)).strftime('%Y%m%d %H:%M:%S')
    
    # sort lookuptable by name
    lookupTable.sort_values(by=['name'], inplace=True)

    # iterate through each record in the lookup table
    for index, record in lookupTable.iterrows():  
        lookback = 100
        # print loginfo
        print('%s: [yellow]looking up data for [/yellow]%s-%s, interval: %s'%(datetime.now().strftime("%H:%M:%S"), record.symbol, record['lastTradeDate'], record['interval']))
        
        # set the endDate to the firstRecordDate minus 1 time-period
        if record['interval'] == '1 day':
            endDate = (record['firstRecordDate'] - relativedelta(days=1)).strftime('%Y%m%d %H:%M:%S')
        else:
            endDate = record['firstRecordDate'] - relativedelta(minutes=1)

        # set earliestTimeStamp from the constructed lookup table
        earliestAvailableTimestamp = pd.to_datetime(uniqueSymbol.loc[uniqueSymbol['symbol'] == record['symbol']]['earliestTimeStamp'].iloc[0])
        exchange = uniqueSymbol.loc[uniqueSymbol['symbol'] == record['symbol']]['exchange'].iloc[0]
    
        # set lookback based on history left and interval 
        if lookback >= (record['firstRecordDate'] - earliestAvailableTimestamp).days:
            lookback = (record['firstRecordDate'] - earliestAvailableTimestamp).days
        elif record['interval'] in ['1 day', '1 month']:
            lookback = 100
        elif record['interval'] in ['1 min']:
            lookback = 3
        else:
            lookback = 30

        # if the lookback is negative, this means we have more data than there is available
        if lookback < 0:
            history = None
        else: # otherwise, get history 
            # append ' D' to lookback
            lookback = str(lookback) + ' D'
            # query for history 
            history = ibkr.getBars_futures(ib, symbol=record['symbol'], lastTradeDate=record['lastTradeDate'], interval=record['interval'], endDate=endDate, lookback=lookback, exchange=exchange)
        
        # skip to next if no data is returned
        if history is None:
            print(' [green]No data left [/green]for %s %s %s!'%(record['symbol'], record['lastTradeDate'], record['interval']))
            # set earliestAvailableTimestamp  to min(date) record in the db 
            with db.sqlite_connection(dbName_futures) as conn:
                earliestAvailableTimestamp = db._getFirstRecordDate(record, conn)
                db._updateLookup_symbolRecords(conn, record['name'], earliestTimestamp=earliestAvailableTimestamp, numMissingDays=0, type='future')
            print('\n')
            continue
        
        # append symbol, interval, and lastTradeDate columns to align with db schema
        history['symbol'] = record['symbol']
        history['interval'] = record['interval'].replace(' ', '')
        history['lastTradeDate'] = record['lastTradeDate']
        
        # update history to the db
        with db.sqlite_connection(dbName_futures) as conn:
            print(' Saving to db...')
            db.saveHistoryToDB(history, conn, earliestAvailableTimestamp)
        print('\n')
        
        # sleep for 40s every 2 iterations
        if index != len(lookupTable)-1: # dont sleep if we're on the last record
            print('%s: [yellow]Sleeping for %ss...[/yellow]\n'%(datetime.now().strftime('%H:%M:%S'), str(_defaultSleepTime)))
            time.sleep(_defaultSleepTime)
    print('[green]----------------------------------------------[/green]')
    print('[green]---- Completed updating prehistoric data -----[/green]')
    print('[green]----------------------------------------------[/green]\n')
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

#print('\n[yellow] Checking FUTURES records... [/yellow]')
#updateRecords()
ib = ibkr.setupConnection()
updateRecords(ib)       

with db.sqlite_connection(dbName_futures) as conn:
    for i in range(3):
        lookupTable = db.getLookup_symbolRecords(conn)
        _updatePreHistory(lookupTable, ib)



