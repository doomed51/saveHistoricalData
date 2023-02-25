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
import localDbInterface as dbInt 

matplotlib.use('TkAgg')

"""
global vars
"""
## default set of intervals 
## these are different as different APIs are being used for stock (qtrade) and index (ibkr) data
intervals_stock = ['FiveMinutes', 'FifteenMinutes', 'HalfHour', 'OneHour', 'OneDay', 'OneMonth']
intervals_index = ['5 mins', '15 mins', '30 mins', '1 day', '1 month']

## lookup table mapping plots to interval labels for questrade and ibkr respectively 

"""intervalMappings = pd.DataFrame(
    {
        "label":['yearByMonth', 'monthByDay', 'weekByDay', 'dayByHour', 'dayByThirty', 
        'dayByFifteen', 'dayByFive'], 
        "stock":['OneMonth', 'OneDay', 'OneDay', 'OneHour', 'HalfHour', 
        'FifteenMinutes','FiveMinutes'],
        'index':['1month', '1day', '1day', '1hour', '30mins', 
        '15mins', '5mins']
    }
)"""

intervalMappings = pd.DataFrame(
    {
        "analysisTimeframe":['yearByMonth', 'monthByDay', 'weekByDay', 'dayByHour', 'dayByThirty', 
        'dayByFifteen', 'dayByFive'], 
        "lookup":['1m', '1d', '1d', '1h', '30m', 
        '15m','5m']
    }
)

# global reference list of index symbols 
index_ = ['VIX', 'VIX3M', 'VVIX']

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
def getSeasonalReturns(intervals, symbols, lookbackPeriod = 0):
    seasonalReturns = []
    for sym in symbols:
        for int in intervals:   
            ## get raw px history
            symbolHistory = dbInt.getPriceHistory(sym, intervalMappings[intervalMappings['analysisTimeframe'] == int ]['lookup'].values[0])
            
            ## compute returns on the timeseries
            myReturns = computeReturns(symbolHistory)
        
            ## aggregate returns across the timeseries
            seasonalReturns.append(aggregateSeasonalReturns(myReturns, sym, int))
    
    return seasonalReturns

"""
> Computes % return with OHLC data 

Params
---------
symbolHistory - [dataframe] raw timeseries OHLC data for a symbol
"""
def computeReturns(symbolHist):
    symbol = symbolHist['symbol'][1]
    symbolHist = symbolHist[['start', 'close']] ## start date of interval, and close px 
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
    if symbol in index_:
        returns[['startDate', 'startTime']] = returns['start'].str.split(" ", expand=True)
    else:
        returns[['startDate', 'startTime']] = returns['start'].str.split("T", expand=True)
    
    returns['startDate'] = pd.to_datetime(returns['startDate'])
    returns.drop(['start'], axis=1, inplace=True)
    
    print(returns)
    if interval in ['FiveMinutes', 'dayByHour', 'dayByFifteen', 'dayByFive', 'dayByThirty']:

        # trim excess time info
        if not symbol in index_:
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
def plotSeasonalReturns(seasonalReturns, intervals, symbols):
    # create a grid of symbols & intervals to draw the plots into 
    # col x rows : interval x symbol 
    numCols = len(intervals)
    if symbols_:
        numRows = len(symbols)
    else:
        numRows = 1
    
    ## set as static y-axis max such that symbols can be compared
    ymin = -0.005
    ymax = 0.02


    count = 0
    with plt.style.context(("seaborn","ggplot")):
        
        fig = plt.figure(constrained_layout=False, figsize=(numCols*5, numRows*3))
        specs = gridspec.GridSpec(ncols=numCols, nrows=numRows, figure=fig)

        for rtr in seasonalReturns:
            count += 1
            intervalLabel = rtr['interval'][0]
            
            ## clean up the time column for prettier plots 
            if 'day' in intervalLabel:
                rtr['startTime'] = rtr['startTime'].str[:5]
            
            ## add two plots for mean and std dev. 
            x1 = fig.add_subplot(numRows, numCols, count)
            x2 = x1
            
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

    plt.show()
    plt.close(fig)


def shit():
    intervalLookup = pd.DataFrame(
    {
        "timeframe":['yearByMonth', 'monthByDay', 'weekByDay', 'dayByHour', 'dayByThirty', 
        'dayByFifteen', 'dayByFive'], 
        "stock":['OneMonth', 'OneDay', 'OneDay', 'OneHour', 'HalfHour', 
        'FifteenMinutes','FiveMinutes'],
        'index':['1 month', '1 day', '1 day', '1 hour', '30 mins', 
        '15 mins', '5 mins']
    }
    )
    chk = intervalLookup.loc[intervalLookup['timeframe'] == 'yearByMonth', ['index']].iat[0,0]

    print(chk)

#shit()
# symbols and intervals to analyze 
symbols_ = ['VIX', 'VIX3M', 'AAPL']
intervals_ = ['dayByFive', 'dayByFifteen', 'dayByThirty']#'yearByMonth', 'monthByDay', 'dayByHour', 'dayByFive']

plotSeasonalReturns(getSeasonalReturns(intervals_, symbols_), intervals_, symbols_)



#plotSeasonalReturns_timeperiodAnalysis(symbol=['ASAN'])
#plotReturnsDist(symbol=['ASAN'])
#plotPrice(interval=['OneHour'])
