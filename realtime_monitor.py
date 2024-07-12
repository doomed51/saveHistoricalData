"""
    plots vix / vix3m ratio w/ stats 

"""
from ib_insync import *
from datetime import datetime, timedelta
from rich import print 
from matplotlib.animation import FuncAnimation
import numpy as np
import pandas as pd
from utils import indicators


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
    # vix3m['ratio_avg'] = vix3m['ratio'].rolling(window=ratio_movingaverage_lookback_period).mean()
    vix3m = indicators.moving_average_weighted(vix3m, 'ratio', ratio_movingaverage_lookback_period)
    vix3m = vix3m.rename(columns={'ratio_wma': 'ratio_avg'})
    vix3m = indicators.moving_average_weighted(vix3m, 'ratio', ratio_movingaverage_lookback_period_short)
    vix3m = vix3m.rename(columns={'ratio_wma': 'ratio_avg_short'})
    vix3m = indicators.moving_average_crossover(vix3m, colname_long = 'ratio_avg', colname_short = 'ratio_avg_short')
    # vix3m['ratio_avg_short'] = vix3m['ratio'].rolling(window=ratio_movingaverage_lookback_period_short).mean()
    vix3m['ratio_avg_long_diff'] = vix3m['ratio'] - vix3m['ratio_avg']
    vix3m['ratio_avg_long_diff_90p'] = vix3m['ratio_avg_long_diff'].rolling(window=percentile_lookback).quantile(0.9)
    vix3m['ratio_avg_long_diff_10p'] = vix3m['ratio_avg_long_diff'].rolling(window=percentile_lookback).quantile(0.1)
    
    # reduce to last n periods for plotting
    vix3m = vix3m.tail(percentile_lookback+1)
    # set date as index 
    vix3m.set_index('date', inplace=True)
    
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
    sns.lineplot(vix3m['ratio'], ax=ax[0], color='blue')
    sns.lineplot(vix3m['ratio_avg'], ax=ax[0], color='red', alpha=0.7)
    sns.lineplot(vix3m['ratio_avg_short'], ax=ax[0], color='red', alpha=0.2)
    sns.lineplot(vix3m['ratio_90p'], ax=ax[0], color='green', linestyle='--', alpha=0.1)
    sns.lineplot(vix3m['ratio_10p'], ax=ax[0], color='green', linestyle='--', alpha=0.1)
    ax[0].axhline(y = 1.15903, color='grey', linestyle='--', alpha=0.6)
    
    # plot the ratio-MA diff 
    sns.lineplot(vix3m['ratio_avg_ratio_avg_short_crossover'], ax=ax[1], color='green')
    # sns.lineplot(ratio_avg_long_diff_90p, ax=ax[1], color='red', linestyle='-', alpha=0.3)
    # sns.lineplot(ratio_avg_long_diff_10p, ax=ax[1], color='red', linestyle='-', alpha=0.3)
    # plot formatting 
    ax[1].axhline(y=0, color='grey', linestyle='-', alpha=0.3) #0 line for diff plot
    for h in hlines_ratio_diff:
        ax[1].axhline(y=h, color='grey', linestyle='--', alpha=0.3)

    def animate(i):
        mode = 'GETBARS'

        # timestamp.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        ## *** TESTING CODE **** 
        ## manually set p and p2 for testing 
        # import numpy as np
        # p = Ticker(con)
        # p.last = np.random.randint(12, 15)
        # p2 = Ticker(con2)
        # p2.last = np.random.randint(12, 15)

        ibkr.sleep(2)
        if mode == 'MANUAL':
            p = ibkr.reqMktData(con, '', False, False) # returns ibinsync.Ticker object
            p2 = ibkr.reqMktData(con2, '', False, False) # returns ibinsync.Ticker object
            # calculate ratio between the two ticker prices
            ratio.append(round(p.last/p2.last, 5))

            avg = -1
            # avg = sum(ratio[-ratio_movingaverage_lookback_period:])/ratio_movingaverage_lookback_period
            avg = indicators.weighted_moving_average_returnsSeries(ratio, ratio_movingaverage_lookback_period)
            avg_short = indicators.weighted_moving_average_returnsSeries(ratio, ratio_movingaverage_lookback_period_short)
            # avg_short = sum(ratio[-ratio_movingaverage_lookback_period_short:])/ratio_movingaverage_lookback_period_short
            # ratio_avg.append(avg)
            ratio_avg = avg.tolist()
            # ratio_avg_short.append(avg_short)
            ratio_avg_short = avg_short.tolist()
            # ratio_avg_long_diff.append(round(p.last/p2.last, 5) - ratio_avg[-1])
            # append the difference between ratio_avg and ratio_avg_short of the last period to ratio_avg_long_diff
            ratio_avg_long_diff.append(ratio_avg_short[-1] - ratio_avg[-1])
            
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
            ax[1].axhline(y=0, color='grey', linestyle='-', alpha=0.3)
            # for h in hlines_ratio_diff:
            #     ax[1].axhline(y=h, color='grey', linestyle='--', alpha=0.3)
        elif mode == 'GETBARS': 
            symbol_bars = ib.getBars(ibkr, symbol = symbol, interval = '1 min', lookback = '1 D')
            symbol2_bars = ib.getBars(ibkr, symbol = symbol2, interval = '1 min', lookback = '1 D')
            print(symbol_bars)
            print(symbol2_bars)
    # animate the plot 
    ani = FuncAnimation(fig, animate, interval=60000)

    # show the plot
    plt.tight_layout()
    plt.show()

def getRealtime_v5(): 
    symbol = 'VIX3M'
    symbol2 = 'VIX'
    
    fig, ax = plt.subplots(1, 2, figsize=(15, 5))

    def animate(i):
        plt.cla()
        wma_period_long = 60
        wma_period_short = 15

        # timestamp.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        ## *** TESTING CODE **** 
        ## manually set p and p2 for testing 
        # import numpy as np
        # p = Ticker(con)
        # p.last = np.random.randint(12, 15)
        # p2 = Ticker(con2)
        # p2.last = np.random.randint(12, 15)

        ibkr = ib.setupConnection() 
        symbol_bars = ib.getBars(ibkr, symbol = symbol, interval = '1 min', lookback = '45000 S')
        symbol2_bars = ib.getBars(ibkr, symbol = symbol2, interval = '1 min', lookback = '45000 S')
        ibkr.sleep(1)
        ibkr.disconnect()
        symbol_bars.set_index('date', inplace=True)
        symbol2_bars.set_index('date', inplace=True)
        merged = symbol_bars.merge(symbol2_bars, on='date', suffixes=('_vix3m', '_vix'))
        merged.reset_index(inplace=True)
        merged['date'] = pd.to_datetime(merged['date'])
        
        merged['ratio'] = merged['close_vix3m']/merged['close_vix']
        merged = indicators.moving_average_weighted(merged, 'ratio', wma_period_long)
        merged.rename(columns={'ratio_wma': 'ratio_wma_long'}, inplace=True)
        merged = indicators.moving_average_weighted(merged, 'ratio', wma_period_short)
        merged.rename(columns={'ratio_wma': 'ratio_wma_short'}, inplace=True)
        merged = indicators.moving_average_crossover(merged, colname_long = 'ratio_wma_long', colname_short = 'ratio_wma_short')
        merged = indicators.intra_day_cumulative_signal(merged, 'ratio_wma_long_ratio_wma_short_crossover', intraday_reset=True)
        # convert date to datetime 
        sns.lineplot(y=merged['ratio'], x=merged['date'], ax=ax[0], color='blue')
        sns.lineplot(y=merged['ratio_wma_long'], x=merged['date'], ax=ax[0], color='red', alpha=0.7)
        sns.lineplot(y=merged['ratio_wma_short'], x=merged['date'], ax=ax[0], color='red', alpha=0.2)

        sns.lineplot(y=merged['ratio_wma_long_ratio_wma_short_crossover'], x=merged['date'], ax=ax[1], color='green')
        sns.lineplot(y=merged['ratio_wma_long_ratio_wma_short_crossover_cumsum'], x=merged['date'], ax=ax[1].twinx(), color='grey')
        ax[1].axhline(y=0, color='grey', linestyle='-', alpha=0.3)

        print('%s: VIX3M: %.4f'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), merged['close_vix3m'].iloc[-1]))
        print('%s: VIX:   %.4f'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), merged['close_vix'].iloc[-1]))
        print('%s: Ratio: %.5f, %speriod avg: %.5f, Crossover: %.5f'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), merged['close_vix3m'].iloc[-1]/merged['close_vix'].iloc[-1], wma_period_long, merged['ratio_wma_long'].iloc[-1], merged['ratio_wma_long_ratio_wma_short_crossover'].iloc[-1] ))
        print('%s: Cumulative Crossover: %.5f \n'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), merged['ratio_wma_long_ratio_wma_short_crossover_cumsum'].iloc[-1] ))
        # print(symbol_bars)
        # print(symbol2_bars)
    # animate the plot 
    ani = FuncAnimation(fig, animate, interval=60000)

    # show the plot
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    getRealtime_v5() 