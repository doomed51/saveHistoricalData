"""
/* A playground for analyzing historical data 

"""

from datetime import datetime
from itertools import count
from symtable import SymbolTableFactory

import ffn
import sqlite3
import tkinter
import matplotlib
from numpy import interp

import pandas as pd
import matplotlib.pyplot as plt
matplotlib.use('TkAgg')
import matplotlib.gridspec as gridspec

"""
> Returns sesonal returns for the passed in OHLC timeseries

Params
----------
returns - [dataframe] timeseries with returns column 
"""
def getSeasonalReturns(returns, symbol, interval):
    symbol = returns.columns[0]
    
    returns = returns.reset_index()
    returns[['startDate', 'startTime']] = returns['start'].str.split("T", expand=True)
    returns.drop(['start'], axis=1, inplace=True)
    
    if interval in ['FiveMinutes', 'OneHour', 'FifteenMinutes']:

        ## drop after and before hours data 
        returns = returns.loc[(returns['startTime'] >= "09:30:00") & (returns['startTime'] < "16:00:00")]

        # trim excess time info
        returns['startTime'] = returns['startTime'].astype(str).str[:-7]

        ## Aggregate by start time
        agg_hourlyReturns = returns.groupby('startTime').agg(
            { symbol : ['mean', 'std']}
        )

    elif interval in ['OneMonth']:
        # OneMonth - > month 
        
        returns['startDate'] = pd.to_datetime(returns['startDate'])
        
        agg_hourlyReturns = returns.groupby(returns['startDate'].dt.strftime('%B')).agg(
            { symbol : ['mean', 'std']}
        )
    
    elif interval in ['OneDay']:
        # OneDay -> day of the week
        
        returns['startDate'] = pd.to_datetime(returns['startDate'])
        
        agg_hourlyReturns = returns.groupby(returns['startDate'].dt.day_name()).agg(
            { symbol : ['mean', 'std']}
        )
    
    # cleanup agg object 
    agg_hourlyReturns.columns = agg_hourlyReturns.columns.get_level_values(1)
    agg_hourlyReturns.reset_index(inplace=True)

    agg_hourlyReturns['symbol'] = symbol
    agg_hourlyReturns['interval'] = interval
    
    return agg_hourlyReturns

"""
> Returns dataframe of % return from timeseries data 

Params
---------
symbolHistory - [dataframe] raw timeseries OHLC data for a symbol
"""
def computeReturns(symbolHist):
    
    ## parse history data into format that ffn expects 
    ## i.e., [Date], [Price]
    symbol = symbolHist['symbol'][1]

    symbolHist = symbolHist[['start', 'close']]
    symbolHist = symbolHist.set_index('start')

    symbolHist.rename(columns={'close':symbol}, inplace=True)

    ## compute returns data
    returns = symbolHist.to_returns()

    return returns

"""
> plot seasonal returns 

Params
---------
seasonalReturns - [list] of [DataFrame] of seasonal returns 
intervals - [array] of intervals being plotted
symbols - [array] of symbols being plotted
"""
def plotSeasonalReturns(seasonalReturns, intervals, symbols):
    numCols = len(intervals)
    numRows = len(symbols)
    count = 0

    with plt.style.context(("seaborn","ggplot")):
        fig = plt.figure(constrained_layout=True, figsize=(numCols*5, numRows*2))
        specs = gridspec.GridSpec(ncols=numCols, nrows=numRows, figure=fig)

        for rtr in seasonalReturns:
            count += 1
            intervalLabel = rtr['interval'][0]
            # labeling charts to be more intuitive 
            if intervalLabel == 'OneMonth':
                intervalLabel = 'Year by Month'
            
            elif intervalLabel == 'OneDay':
                intervalLabel = 'Week by Day'
            
            elif intervalLabel == 'OneHour':
                intervalLabel = 'Day by Hour'

            x1 = fig.add_subplot(numRows, numCols, count)
            #x1.plot(rtr['startTime'], rtr['mean'], color='r', kind='bar')
            rtr['mean'].plot(color='r', kind='bar', title=rtr['symbol'][0]+' - '+intervalLabel, zorder=2)

            rtr['std'].plot(color='b', kind='bar')
            #x1.plot(rtr['startTime'], rtr['std'], color='b', kind='bar')

        plt.show()
        plt.close(fig)

"""
---------------------------------------------------------
"""
## declare vars
#symbols = ['SLX', 'TIP', 'ERX', 'MOO', 'XLE', 'XME']
symbols = ['SLX', 'FXY', 'TIP']
intervals = ['OneMonth', 'OneDay', 'OneHour']
seasonalReturns = list() #empty list to append history 

## load data into a pandas dataframe 
conn = sqlite3.connect('historicalData.db')
for sym in symbols:
    for int in intervals:
        ## Tablename convention: <symbol>_<stock/opt>_<interval>
        tableName = sym+'_'+'stock'+'_'+int
        sqlStatement = 'SELECT * FROM ' + tableName
        symbolHistory = pd.read_sql(sqlStatement, conn)
        myReturns = computeReturns(symbolHistory)
        seasonalReturns.append(getSeasonalReturns(myReturns, sym, int))

## get returns data for the symbol 

print(seasonalReturns[0:3])

plotSeasonalReturns(seasonalReturns, intervals, symbols)

## compute seasonal returns
#agg_hourlyReturns = getSeasonalReturns(myReturns)

""" print out resulting returns 
 1 day = 78 lines of FiveMinutes data """

"""

print(agg_hourlyReturns[63:68])
## plot the mean and std
with plt.style.context(("seaborn","ggplot")):
    plt.figure(figsize=(21,8))
    agg_hourlyReturns['mean'].plot(kind='bar', color='r')
    agg_hourlyReturns['std'].plot(kind='bar', color='b', zorder=-1)

    plt.show()

"""
#-----------------------------------------------
## ----------------graveyard--------------------
#-----------------------------------------------
#fig = plt.figure()
#ax = returns.hist(figsize=(12, 5))
#tb = returns.corr().as_format('.2f')

#print(tb)
#returns.plot_histogram()
#plt.plot(ax)
#plt.show()