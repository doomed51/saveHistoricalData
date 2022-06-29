"""
/* A playground for analyzing historical data 

"""

from cgi import test
import datetime
from itertools import count
from pytz import utc
from rich import print

import ffn
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
# symbols and timeframes to analyze
symbols_ = ['USO', 'UNG', 'XLE']
intervals_ = ['yearByMonth', 'monthByDay', 'weekByDay']#, 'dayByHour']

#intervals_ = ['monthByDay', 'FifteenMinutes']

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
def getSeasonalReturns(dbName = 'historicalData.db', intervals = intervals_, symbols=symbols_, lookbackPeriod = 0):
    conn = sqlite3.connect('historicalData.db')
    seasonalReturns = []
    for sym in symbols:
        for int in intervals:
            ## Tablename convention: <symbol>_<stock/opt>_<interval>
            if int == 'yearByMonth':
                tableName = sym+'_'+'stock'+'_'+'OneMonth'
            
            elif int == 'weekByDay':
                tableName = sym+'_'+'stock'+'_'+'OneDay'
            
            elif int == 'dayByHour':
                tableName = sym+'_'+'stock'+'_'+'OneHour'
            
            elif int == 'monthByDay':
                tableName = sym+'_'+'stock'+'_'+'OneDay'

            else:
                tableName = sym+'_'+'stock'+'_'+int

            if lookbackPeriod == 0:
                sqlStatement = 'SELECT * FROM ' + tableName
            else:
                sqlStatement = 'SELECT * FROM ' + tableName
            
            symbolHistory = pd.read_sql(sqlStatement, conn)
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
    
    if interval in ['FiveMinutes', 'dayByHour', 'FifteenMinutes']:

        ## drop after and before hours data 
        returns = returns.loc[(returns['startTime'] >= "09:30:00") & (returns['startTime'] < "16:00:00")]

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
    numCols = len(intervals)
    if symbols_:
        numRows = len(symbols_)
    else:
        numRows = 1
    count = 0

    with plt.style.context(("seaborn","ggplot")):
        
        fig = plt.figure(constrained_layout=True, figsize=(numCols*5, numRows*3))
        specs = gridspec.GridSpec(ncols=numCols, nrows=numRows, figure=fig)

        for rtr in seasonalReturns:
            count += 1
            intervalLabel = rtr['interval'][0]

            x1 = fig.add_subplot(numRows, numCols, count)
            rtr['mean'].plot(color='r', kind='bar', title=rtr['symbol'][0]+' - '+intervalLabel, zorder=2)

            rtr['std'].plot(color='b', kind='bar')


        plt.show()
        plt.close(fig)

"""
plot seasonal returns across various timeperiods
"""
def plotSeasonalReturns_timeperiodAnalysis(interval = ['FifteenMinutes'], symbol=['AAPL']):
    # timperiods to analyse in # days 
    timeperiods = [20, 60, 120]

    # grab OHLC data from the database 
    tableName = symbol[0]+'_'+'stock'+'_'+interval[0]
    conn = sqlite3.connect('historicalData.db')
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
def plotReturnsDist(interval = ['FifteenMinutes'], symbol=['AAPL']):

    # grab OHLC data from the database 
    tableName = symbol[0]+'_'+'stock'+'_'+interval[0]
    conn = sqlite3.connect('historicalData.db')
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
def plotPrice(interval = ['FifteenMinutes'], symbol=['AAPL'], numDays = 10): 
    # grab OHLC data from the database 
    tableName = symbol[0]+'_'+'stock'+'_'+interval[0]
    conn = sqlite3.connect('historicalData.db')
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

plotSeasonalReturns(getSeasonalReturns())
#plotSeasonalReturns_timeperiodAnalysis(symbol=['ASAN'])
#plotReturnsDist(symbol=['ASAN'])
#plotPrice(interval=['OneHour'])
