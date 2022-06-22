"""
/* A playground for analyzing historical data 

"""

from datetime import datetime
from itertools import count
from pyexpat import features
from socket import gethostbyaddr
from symtable import SymbolTableFactory
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
symbols = ['SPY', 'AAPL', 'XLE']
#intervals = ['yearByMonth', 'monthByDay', 'weekByDay', 'dayByHour']
intervals = ['monthByDay']

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
def getSeasonalReturns(dbName = 'historicalData.db'):
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

            sqlStatement = 'SELECT * FROM ' + tableName
            symbolHistory = pd.read_sql(sqlStatement, conn)
            myReturns = computeReturns(symbolHistory)
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
def plotSeasonalReturns(seasonalReturns):
    numCols = len(intervals)
    numRows = len(symbols)
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
plot seasonal returns over most recent time periods
"""
def plotSeasonalReturns_recent(seasonalReturns):
    print('not complete')
    

plotSeasonalReturns(getSeasonalReturns())
