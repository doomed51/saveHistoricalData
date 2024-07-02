"""
Author: Rachit Shankar 
Date: February, 2022

PURPOSE
----------
Maintains a local database of historical ohlc data for a list of specified symbols

VARIABLES
----------
# The LIST of symbols to be tracked 
tickerFilepath = 'tickerList.csv'

# The database where pricing data is stored
'historicalData_index.db'

# The data sources for historcal data 
IBKR

"""
from ib_insync import *
from datetime import datetime, timedelta
from rich import print 
from matplotlib.animation import FuncAnimation
import numpy as np


import config 
import interface_ibkr as ib
import interface_localDB as db
import matplotlib.pyplot as plt
import seaborn as sns

######### SET GLOBAL VARS #########

_tickerFilepath = config.watchlist_main ## List of symbols to keep track of
_dbName_index = config.dbname_stock ## Default DB names 

intervals_index = config.intervals
_indexList = config._index

ibkrThrottleTime = 10 # minimum seconds to wait between api requests to ibkr

def getRealtime(): 
    """
        Get realtime data for VIX3M and VIX and write to console 
    """
    ibkr = ib.setupConnection() 
    symbol = 'VIX3M'
    symbol2 = 'VIX'
    con = Index(symbol)
    con2 = Index(symbol2)
    ibkr.qualifyContracts(con)
    ibkr.qualifyContracts(con2)
    while(True): 
        p = ibkr.reqMktData(con, '', False, False) # returns ibinsync.Ticker object
        p2 = ibkr.reqMktData(con2, '', False, False) # returns ibinsync.Ticker object

        ibkr.sleep(10)
        print('%s: VIX3M: %.4f'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), p.last))
        print('%s: VIX:   %.4f'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), p2.last))

def getRealtime_v2():
# print ratio and average
    ibkr = ib.setupConnection() 
    
    movingaverage_ratio = 60
    symbol = 'VIX3M'
    symbol2 = 'VIX'
    con = Index(symbol)
    con2 = Index(symbol2)
    ibkr.qualifyContracts(con)
    ibkr.qualifyContracts(con2)
    ratio = []

    while(True): 
        p = ibkr.reqMktData(con, '', False, False) # returns ibinsync.Ticker object
        p2 = ibkr.reqMktData(con2, '', False, False) # returns ibinsync.Ticker object

        ibkr.sleep(10)
        print('%s: VIX3M: %.4f'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), p.last))
        print('%s: VIX:   %.4f'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), p2.last))
        ratio.append(round(p.last/p2.last, 5))
        ## calculate rolling 10 period avg of ratio
        avg = -1
        if len(ratio) > movingaverage_ratio: 
            avg = sum(ratio[-movingaverage_ratio:])/movingaverage_ratio
            
        print('%s: Ratio: %.5f, %speriod avg: %.5f, Delta: %.5f \n'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), p.last/p2.last, movingaverage_ratio, avg, round(p.last/p2.last - avg, 5)))

def getRealtime_v3(): 
# realtime plotting implementation   
    ibkr = ib.setupConnection() 
    
    ratio_movingaverage_lookback_period = 60
    symbol = 'VIX3M'
    symbol2 = 'VIX'
    con = Index(symbol)
    con2 = Index(symbol2)
    ibkr.qualifyContracts(con)
    ibkr.qualifyContracts(con2)
    ratio = []
    ratio_avg = []
    
    fig, ax = plt.subplots()
    
    # plot data from api  
    def animate(i):
         
        p = ibkr.reqMktData(con, '', False, False) # returns ibinsync.Ticker object
        p2 = ibkr.reqMktData(con2, '', False, False) # returns ibinsync.Ticker object
        ibkr.sleep(3)

        # calculate ratio between the two ticker prices
        ratio.append(round(p.last/p2.last, 5))
        
        ## calculate rolling 10 period avg of ratio
        avg = -1
        if len(ratio) > ratio_movingaverage_lookback_period: 
            avg = sum(ratio[-ratio_movingaverage_lookback_period:])/ratio_movingaverage_lookback_period
            ratio_avg.append(avg)
        else:
            ratio_avg.append(p.last/p2.last)

        # print the last price, and ratio 
        print('%s: VIX3M: %.4f'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), p.last))
        print('%s: VIX:   %.4f'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), p2.last))
        print('%s: Ratio: %.5f, %speriod avg: %.5f, Delta: %.5f \n'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), p.last/p2.last, ratio_movingaverage_lookback_period, avg, round(p.last/p2.last - avg, 5)))
        
        # clear axis so new plots are not just added on top of the old one 
        plt.cla() 

        # plot the ratio
        sns.lineplot(ratio, ax=ax, color='blue')
        # plot the moving average, make sure the line begins after the lookback period
        #if len(ratio_avg) > 0: 
        sns.lineplot(ratio_avg, ax=ax, color='red')

    # animate the plot 
    ani = FuncAnimation(fig, animate, interval=60000)

    # show the plot
    plt.tight_layout()
    plt.show()

def getRealtime_v4(): 
# realtime plotting implementation   
    ibkr = ib.setupConnection() 
    
    symbol = 'VIX3M'
    symbol2 = 'VIX'
    con = Index(symbol)
    con2 = Index(symbol2)
    ibkr.qualifyContracts(con)
    ibkr.qualifyContracts(con2)
    
    ratio_movingaverage_lookback_period = 60
    ratio_movingaverage_lookback_period_short = 30
    percentile_lookback = 390
    hlines_ratio_diff = [0.00599724, -0.00593816]  # historical 90th and 10th percentiles
    
    timestamp = []
    ratio = []
    ratio_avg = []
    ratio_avg_short = []
    ratio_avg_long_diff = []

    # get previous day's data from db
    with db.sqlite_connection(config.dbname_stock) as conn:
        vix3m =  db.getPriceHistory(conn, symbol, '1min')
        vix = db.getPriceHistory(conn, symbol2, '1min')
    
    # make sure sorted by date 
    vix3m = vix3m.sort_values(by='date')
    vix = vix.sort_values(by='date')


    # merge, joining on date, and keeping just the close from vix 
    vix3m = vix3m.merge(vix[['date', 'close']], on='date', suffixes=('_vix3m', '_vix'))

    # calculate ratio, ratios short ma, and ratios long ma 
    vix3m['ratio'] = (round(vix3m['close_vix3m']/vix3m['close_vix'], 5))
    vix3m['ratio_90p'] = vix3m['ratio'].rolling(window=percentile_lookback).quantile(0.9)
    vix3m['ratio_10p'] = vix3m['ratio'].rolling(window=percentile_lookback).quantile(0.1)
    vix3m['ratio_avg'] = vix3m['ratio'].rolling(window=ratio_movingaverage_lookback_period).mean()
    vix3m['ratio_avg_short'] = vix3m['ratio'].rolling(window=ratio_movingaverage_lookback_period_short).mean()
    vix3m['ratio_avg_long_diff'] = vix3m['ratio'] - vix3m['ratio_avg']
    vix3m['ratio_avg_long_diff_90p'] = vix3m['ratio_avg_long_diff'].rolling(window=percentile_lookback).quantile(0.9)
    vix3m['ratio_avg_long_diff_10p'] = vix3m['ratio_avg_long_diff'].rolling(window=percentile_lookback).quantile(0.1)
    
    # reduce to last n periods for plotting
    vix3m = vix3m.tail(percentile_lookback+1)

    # convert to list for plotting
    ratio = vix3m['ratio'].tolist()
    ratio_90p = vix3m['ratio_90p'].tolist()
    ratio_10p = vix3m['ratio_10p'].tolist()
    ratio_avg = vix3m['ratio_avg'].tolist()
    ratio_avg_short = vix3m['ratio_avg_short'].tolist()
    ratio_avg_long_diff = vix3m['ratio_avg_long_diff'].tolist()
    ratio_avg_long_diff_90p = vix3m['ratio_avg_long_diff_90p'].tolist()
    ratio_avg_long_diff_10p = vix3m['ratio_avg_long_diff_10p'].tolist()

    fig, ax = plt.subplots(1, 2, figsize=(15, 5))
    # plot the ratio with MA's
    sns.lineplot(ratio, ax=ax[0], color='blue')
    sns.lineplot(ratio_avg, ax=ax[0], color='red', alpha=0.7)
    sns.lineplot(ratio_avg_short, ax=ax[0], color='red', alpha=0.2)
    sns.lineplot(ratio_90p, ax=ax[0], color='green', linestyle='--', alpha=0.1)
    sns.lineplot(ratio_10p, ax=ax[0], color='green', linestyle='--', alpha=0.1)
    ax[0].axhline(y = 1.15903, color='grey', linestyle='--', alpha=0.6)
    
    # plot the ratio-MA diff 
    sns.lineplot(ratio_avg_long_diff, ax=ax[1], color='green')
    sns.lineplot(ratio_avg_long_diff_90p, ax=ax[1], color='red', linestyle='-', alpha=0.3)
    sns.lineplot(ratio_avg_long_diff_10p, ax=ax[1], color='red', linestyle='-', alpha=0.3)
    # plot formatting 
    ax[1].axhline(y=0, color='grey', linestyle='-', alpha=0.3) #0 line for diff plot
    for h in hlines_ratio_diff:
        ax[1].axhline(y=h, color='grey', linestyle='--', alpha=0.3)

    def animate(i):

        p = ibkr.reqMktData(con, '', False, False) # returns ibinsync.Ticker object
        p2 = ibkr.reqMktData(con2, '', False, False) # returns ibinsync.Ticker object
        timestamp.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        ## *** TESTING CODE **** 
        ## manually set p and p2 for testing 
        # import numpy as np
        # p = Ticker(con)
        # p.last = np.random.randint(10, 20)
        # p2 = Ticker(con2)
        # p2.last = np.random.randint(10, 20)

        ibkr.sleep(2)

        # calculate ratio between the two ticker prices
        ratio.append(round(p.last/p2.last, 5))
        avg = -1
        avg = sum(ratio[-ratio_movingaverage_lookback_period:])/ratio_movingaverage_lookback_period
        avg_short = sum(ratio[-ratio_movingaverage_lookback_period_short:])/ratio_movingaverage_lookback_period_short
        ratio_avg.append(avg)
        ratio_avg_short.append(avg_short)
        ratio_avg_long_diff.append(round(p.last/p2.last, 5) - avg)
        ratio_avg_long_diff_90p.append(np.percentile(ratio_avg_long_diff[-percentile_lookback:], 90))
        ratio_avg_long_diff_10p.append(np.percentile(ratio_avg_long_diff[-percentile_lookback:], 10))
        ratio_90p.append(np.percentile(ratio[-percentile_lookback:], 90))
        ratio_10p.append(np.percentile(ratio[-percentile_lookback:], 10))

        print('%s: VIX3M: %.4f'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), p.last))
        print('%s: VIX:   %.4f'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), p2.last))
        print('%s: Ratio: %.5f, %speriod avg: %.5f, Delta: %.5f \n'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), p.last/p2.last, ratio_movingaverage_lookback_period, ratio_avg[-1], ratio_avg_long_diff[-1] ))
        
        # clear axis so new plots are not just added on top of the old one 
        ax[0].cla() 
        ax[1].cla() 

        # plot the ratio with MA's
        sns.lineplot(ratio, ax=ax[0], color='blue')
        sns.lineplot(ratio_avg, ax=ax[0], color='red', alpha=0.7)
        sns.lineplot(ratio_avg_short, ax=ax[0], color='red', alpha=0.2)
        sns.lineplot(ratio_90p, ax=ax[0], color='green', linestyle='--', alpha=0.1)
        sns.lineplot(ratio_10p, ax=ax[0], color='green', linestyle='--', alpha=0.1)
        ax[0].axhline(y = 1.15903, color='grey', linestyle='--', alpha=0.6)
        
        # plot the ratio-MA diff 
        sns.lineplot(ratio_avg_long_diff, ax=ax[1], color='green')
        sns.lineplot(ratio_avg_long_diff_90p, ax=ax[1], color='red', linestyle='-', alpha=0.3)
        sns.lineplot(ratio_avg_long_diff_10p, ax=ax[1], color='red', linestyle='-', alpha=0.3)
        
        # plot formatting 
        ax[1].axhline(y=0, color='grey', linestyle='-', alpha=0.3) #0 line for diff plot
        for h in hlines_ratio_diff:
            ax[1].axhline(y=h, color='grey', linestyle='--', alpha=0.3)

    # animate the plot 
    ani = FuncAnimation(fig, animate, interval=60000)

    # show the plot
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    getRealtime_v4() 