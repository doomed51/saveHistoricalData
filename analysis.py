"""
/* A playground for analyzing historical data 

"""
import datetime
from itertools import count
from turtle import color
from pytz import utc
from rich import print

import ffn #needed for to_returns() calc
import sqlite3
import tkinter
import matplotlib
from numpy import interp

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns

matplotlib.use('TkAgg')

"""
global vars
"""
# default list of symbols and timeframes to analyze
symbols_ = ['SSO', 'SPY']
intervals_ = ['dayByFive', 'dayByFifteen']#'yearByMonth', 'monthByDay', 'dayByHour', 'dayByFive']

# global reference list of index symbols 
index_ = ['VIX']

"""
Returns a list of returns for a specific symbol, aggregated over  intervals
----------
1. connect to DB 
2. iterate over each symbol, interval combo 
    2.a. construct table name 
    2.b. read OHLC data into DF 
    2.c. compute returns for OHLC data
    2.d. compute seasonal returns for the interval 
3. append the aggregated seasonal returns
4. return aggregated seasonal returns 

"""
def getSeasonalReturns(intervals = intervals_, symbols=symbols_, lookbackPeriod = 0):
    seasonalReturns = []
    dbName = 'historicalData_stock.db'

    for sym in symbols:
        if sym in index_:
            symbolType = 'index'
            dbName = 'historicalData_index.db'
        else: 
            symbolType = 'stock'
        
        for int in intervals:
            ## Tablename convention: <symbol>_<stock/opt>_<interval>
            if int == 'yearByMonth':
                tableName = sym+'_'+symbolType+'_'+'OneMonth'
            
            elif int == 'weekByDay':
                tableName = sym+'_'+symbolType+'_'+'OneDay'
            
            elif int == 'dayByHour':
                tableName = sym+'_'+symbolType+'_'+'OneHour'

            elif int == 'dayByThirty':
                tableName = sym+'_'+symbolType+'_'+'ThirtyMinutes'

            elif int == 'dayByFifteen':
                tableName = sym+'_'+symbolType+'_'+'FifteenMinutes'
            
            elif int == 'monthByDay':
                tableName = sym+'_'+symbolType+'_'+'OneDay'

            elif int == 'dayByFive':
                tableName = sym+'_'+symbolType+'_'+'FiveMinutes'

            else:
                tableName = sym+'_'+symbolType+'_'+int

            if lookbackPeriod == 0:
                sqlStatement = 'SELECT * FROM ' + tableName
            else:
                sqlStatement = 'SELECT * FROM ' + tableName
            conn = sqlite3.connect(dbName)
            symbolHistory = pd.read_sql(sqlStatement, conn)
            conn.close()
            myReturns = computeReturns(symbolHistory)
            seasonalReturns.append(aggregateSeasonalReturns(myReturns, sym, int))
    print(seasonalReturns)
    return seasonalReturns

"""
> Computes % return with OHLC data 

Params
---------
symbolHistory - [dataframe] raw timeseries OHLC data for a symbol
"""
def computeReturns(symbolHist):
    symbol = symbolHist['symbol'][1]
    symbolHist = symbolHist[['start', 'close']]
    symbolHist = symbolHist.set_index('start')
    symbolHist.rename(columns={'close':symbol}, inplace=True)

    ## compute returns data
    returns = symbolHist.to_returns()

    return returns


"""
> Aggregates 'returns' over specified time periods

Params
----------
returns - [dataframe] timeseries with returns column 
"""
def aggregateSeasonalReturns(returns, symbol, interval):
    symbol = returns.columns[0]
    
    returns = returns.reset_index()
    returns[['startDate', 'startTime']] = returns['start'].str.split("T", expand=True)
    returns['startDate'] = pd.to_datetime(returns['startDate'])
    returns.drop(['start'], axis=1, inplace=True)
    
    if interval in ['FiveMinutes', 'dayByHour', 'dayByFifteen', 'dayByFive']:

        ## drop after and before hours data 
        #returns = returns.loc[(returns['startTime'] >= "09:30:00") & (returns['startTime'] < "16:00:00")]

        # trim excess time info
        returns['startTime'] = returns['startTime'].astype(str).str[:-7]

        ## Aggregate by start time, computing mean and SD 
        agg_myReturns = returns.groupby('startTime').agg(
            { symbol : ['mean', 'std']}
        )

    elif interval in ['yearByMonth']:
        # OneMonth - > month 
        agg_myReturns = returns.groupby(returns['startDate'].dt.strftime('%m')).agg(
            { symbol : ['mean', 'std']}
        )
    
    elif interval in ['weekByDay']:
        # OneDay -> day of the week
        agg_myReturns = returns.groupby(returns['startDate'].dt.day_name()).agg(
            { symbol : ['mean', 'std']}
        )
    
    elif interval == 'monthByDay':
        # aggregate by startDate - > day value 
        agg_myReturns = returns.groupby(returns['startDate'].dt.strftime('%d')).agg(
            { symbol : ['mean', 'std']}
        )

    
    # cleanup agg object 
    agg_myReturns.columns = agg_myReturns.columns.get_level_values(1)
    agg_myReturns.reset_index(inplace=True)
    agg_myReturns['symbol'] = symbol
    agg_myReturns['interval'] = interval
    
    return agg_myReturns


"""
> plot seasonal returns 

Params
---------
seasonalReturns - [list] of [DataFrame] of seasonal returns 
intervals - [array] of intervals being plotted
symbols - [array] of symbols being plotted
"""
def plotSeasonalReturns(seasonalReturns, intervals=intervals_, symbols=symbols_):
    # create a grid of symbols & intervals to draw the plots into 
    # col x rows : interval x symbol 
    numCols = len(intervals)
    if symbols_:
        numRows = len(symbols_)
    else:
        numRows = 1
    
    ## set as static y-axis max such that symbols can be compared
    ymin = -0.005
    ymax = 0.0075


    count = 0
    with plt.style.context(("seaborn","ggplot")):
        
        fig = plt.figure(constrained_layout=True, figsize=(numCols*5, numRows*3))
        specs = gridspec.GridSpec(ncols=numCols, nrows=numRows, figure=fig)

        for rtr in seasonalReturns:
            count += 1
            intervalLabel = rtr['interval'][0]
            
            ## clean up the time column for prettier plots 
            if 'day' in intervalLabel:
                rtr['startTime'] = rtr['startTime'].str[:5]
            
            ## add two plots for mean and std dev. 
            x1 = fig.add_subplot(numRows, numCols, count)
            x2 = fig.add_subplot(numRows, numCols, count)
            
            x2.bar(
                rtr['startTime'],
                rtr['std'],
                color='blue'
            )
            
            x1.bar(
                rtr['startTime'],
                rtr['mean'],
                color='red'
            )

            x2.set(title=rtr['symbol'][0]+' - '+intervalLabel)
            x2.set_ylim([ymin, ymax])
            ## space out x-axis if plotting a particularly granular TimeFrame(TF)
            if intervalLabel in ['dayByFive']:
                loc = matplotlib.ticker.MultipleLocator(base=6.0)
                x1.xaxis.set_major_locator(loc)

            ## rotate x axis for prettier plots 
            plt.xticks(rotation=45)
            
            ## date formatting 
            #splt.gcf().autofmt_xdate()
            #timeFormat = matplotlib.dates.DateFormatter('%H:%M')
            #1.xaxis.set_major_formatter(timeFormat)

            #rtr['mean'].plot(color='r', kind='bar', title=rtr['symbol'][0]+' - '+intervalLabel, zorder=2)
            #rtr['std'].plot(color='b', kind='bar')

        ## format figure and plot 
        fig.tight_layout()
        plt.show()
        plt.close(fig)

"""
plot seasonal returns across various timeperiods
"""
def plotSeasonalReturns_timeperiodAnalysis(interval = ['FifteenMinutes'], symbol=['AAPL'], dbName = 'historicalData.db'):
    # timperiods to analyse in # days 
    timeperiods = [20, 60, 120]

    # grab OHLC data from the database 
    tableName = symbol[0]+'_'+'stock'+'_'+interval[0]
    conn = sqlite3.connect(dbName)
    sqlStatement = 'SELECT * FROM ' + tableName
    symbolHistory = pd.read_sql(sqlStatement, conn)
    conn.close()
    symbolHistory['end'] = pd.to_datetime(symbolHistory['end'])
    
    # settings for figure
    fig_numCols = 2
    fig_numRows = 2

    with plt.style.context(("seaborn", "ggplot")):
        fig = plt.figure(constrained_layout=True, figsize=(15,9))
        specs = gridspec.GridSpec(ncols=fig_numCols, nrows = fig_numRows, figure=fig)
        
        ## plot returns over the entire ohlc dataset (baseline)
        myReturns = aggregateSeasonalReturns(computeReturns(symbolHistory), symbol[0], interval[0])
        x1 = fig.add_subplot(2,2,1)
        myReturns['mean'].plot(color='r', kind='bar', title=symbol[0]+' - '+interval[0]+' - Baseline', zorder=2)
        myReturns['std'].plot(color='b', kind='bar')
        
        # loop through list of timeperiods, calculating & plotting returns 
        count = 2
        for tp in timeperiods:
            cutoffDate = datetime.datetime.now(tz=utc) - datetime.timedelta(days=tp)

            myReturns_30 = aggregateSeasonalReturns(computeReturns(symbolHistory.loc[symbolHistory['end'] >= cutoffDate].reset_index()), symbol[0], interval[0])

            x1 = fig.add_subplot(2,2,count)
            count += 1 
            myReturns_30['mean'].plot(color='r', kind='bar', title=symbol[0]+' - '+interval[0]+' - '+str(tp)+'d', zorder=2)
            myReturns_30['std'].plot(color='b', kind='bar')

"""
plots the distribution of returns for a given interval, and symbol
"""
def plotReturnsDist(interval = ['FifteenMinutes'], symbol=['AAPL'], dbName = 'historicalData.db', type='stock'):

    # grab OHLC data from the database 
    tableName = symbol[0]+'_'+type+'_'+interval[0]
    conn = sqlite3.connect(dbName)
    sqlStatement = 'SELECT * FROM ' + tableName
    symbolHistory = pd.read_sql(sqlStatement, conn)

    #myReturns = aggregateSeasonalReturns(computeReturns(symbolHistory), symbol[0], interval[0])
    myReturns = computeReturns(symbolHistory)

    fig = plt.figure(constrained_layout=True, figsize=(15,9))
    specs = gridspec.GridSpec(ncols=1, nrows = 1, figure=fig)
    x1 = fig.add_subplot(1,1,1)
    print(myReturns)
    myReturns[symbol].plot(color='r', kind='hist', title=symbol[0]+' - '+interval[0]+' - Returns', zorder=2)    

    plt.show()
    plt.close(fig)

"""
plot symbol, interval close price over last x days
"""
def plotPrice(interval = ['FifteenMinutes'], symbol=['AAPL'], numDays = 10,  dbName = 'historicalData.db', type='stock'): 
    # grab OHLC data from the database 
    tableName = symbol[0]+'_'+type+'_'+interval[0]
    conn = sqlite3.connect(dbName)
    sqlStatement = 'SELECT * FROM ' + tableName
    symbolHistory = pd.read_sql(sqlStatement, conn)
    symbolHistory['start'] = pd.to_datetime(symbolHistory['start']).dt.date

    fig = plt.figure(constrained_layout=True, figsize=(15,9))
    specs = gridspec.GridSpec(ncols=1, nrows = 1, figure=fig)
    x1 = fig.add_subplot(1,1,1)

    cutoffDate = datetime.datetime.now(tz=utc) - datetime.timedelta(days=numDays+1)
    symbolHistory = symbolHistory.loc[symbolHistory['start'] >= cutoffDate.date()].reset_index()

    symbolHistory['close'].plot(color='r', kind='line', title=symbol[0]+' - '+interval[0]+' - Close', zorder=2)
    symbolHistory['VWAP'].plot(color='g', kind='line', title=symbol[0]+' - '+interval[0]+' - vwap', zorder=2)

    print(symbolHistory)
    plt.show()
    plt.close(fig)

def updateSingleTF(intvlToUpdate='FiveMinutes', symbolToUpdate='AAPL'):
    print('%s interval for %s'%(intvlToUpdate, symbolToUpdate))

    # immediately update if table doesn't exist 
    # escape if table is already up to date


plotSeasonalReturns(getSeasonalReturns())
#plotSeasonalReturns_timeperiodAnalysis(symbol=['ASAN'])
#plotReturnsDist(symbol=['ASAN'])
#plotPrice(interval=['OneHour'])
