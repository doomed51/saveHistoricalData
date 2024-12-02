"""
This module consists of two functions that return VIX term structure, and contango values between contracts.
    The current implementation uses vix contract data from vixcentral.com 
"""
import pandas as pd
import seaborn as sns

""" 
Returns raw term structure for the supplied symbol and interval
"""
def getRawTermStructure(termstructure_db_conn, symbol='VIX', interval='1day'):
    symbol = symbol.upper()
    tablename = f'{symbol}_{interval}'
    # read in vix term structure data
    vix_ts_raw = pd.read_sql(f'SELECT * FROM {tablename}', termstructure_db_conn)

    # convert date column to datetime
    vix_ts_raw['date'] = pd.to_datetime(vix_ts_raw['date'])
    vix_ts_raw['symbol'] = symbol

    # set date as index
    vix_ts_raw.set_index('date', inplace=True)
    return vix_ts_raw

"""
Returns vix term structure data with percent change between n and n+1 month futures
----------------
Params: 
    fourToSeven: bool, default False
        if True, adds a column to the dataframe that is the percent difference between the 4th and 7th month futures
    currentToLast: bool, default False
        if True, adds a column to the dataframe that is the percent difference between the current and longest term future

"""
def getTermStructurePctContango(ts_raw, oneToTwo=False, oneToThree=False, twoToThree = False, threeToFour= False, fourToSeven = False, currentToLast = False, averageContango = False):
    symbol = ts_raw['symbol'][0]

    ts_raw.drop(columns='symbol', inplace=True)
    # create a new df with the percent change between n and n+1 month futures
    ts_pctContango = (ts_raw.pct_change(axis='columns', periods=-1).drop(columns='month8')*-1).copy()
    
    if fourToSeven:
        # add contango from the 4th to 7th month
        ts_raw['fourToSevenMoContango'] = ((ts_raw['month7'] - ts_raw['month4'])/ts_raw['month4'])*100
        ts_pctContango = ts_pctContango.join(ts_raw['fourToSevenMoContango'], on='date')

    if currentToLast:
        # add contango between current and longest term future
        ts_raw['currentToLastContango'] = ((ts_raw['month8'] - ts_raw['month1'])/ts_raw['month1'])*100
        ts_pctContango = ts_pctContango.join(ts_raw['currentToLastContango'], on='date')
    
    if oneToTwo:
        # add contango between m1 and m2
        ts_raw['oneToTwoMoContango'] = ((ts_raw['month2'] - ts_raw['month1'])/ts_raw['month1'])*100
        ts_pctContango = ts_pctContango.join(ts_raw['oneToTwoMoContango'], on='date')
    
    if oneToThree:
        # add contango between m1 and m3
        ts_raw['oneToThreeMoContango'] = ((ts_raw['month3'] - ts_raw['month1'])/ts_raw['month1'])*100
        ts_pctContango = ts_pctContango.join(ts_raw['oneToThreeMoContango'], on='date')
    
    if twoToThree: 
        # add contango between m2 and m3
        ts_raw['twoToThreeMoContango'] = ((ts_raw['month3'] - ts_raw['month2'])/ts_raw['month2'])*100
        ts_pctContango = ts_pctContango.join(ts_raw['twoToThreeMoContango'], on='date')
    
    if threeToFour:
        # add contango between m3 and m4
        ts_raw['threeToFourMoContango'] = ((ts_raw['month4'] - ts_raw['month3'])/ts_raw['month3'])*100
        ts_pctContango = ts_pctContango.join(ts_raw['threeToFourMoContango'], on='date')

    if averageContango:
        # add averageContango column 
        ts_pctContango['averageContango'] = ts_pctContango.mean(axis=1)
    
    ts_pctContango['symbol'] = symbol
    ts_raw['symbol'] = symbol
    ## sort by Date column
    ts_pctContango.sort_values(by='date', inplace=True)
    return ts_pctContango

"""
    Returns a plot of term structure for the last n periods 
    ts: termstructure dataframe with columns: [date, month1, month2, ...]
"""
def plotTermStructure(ts, symbol_underlying, symbol_secondary, ax, numDays=5):
    ts.reset_index(inplace=True)
    # sort ts by date, and get the last 5 rows
    ts['date'] = pd.to_datetime(ts['date'])
    ts = ts.sort_values(by='date').tail(numDays)

    colors = sns.color_palette('YlGnBu', n_colors=numDays) # set color scheme of lineplots     
    for i, color in zip(range(len(ts.tail(numDays))), colors):
        sns.lineplot(x=ts.columns[1:], y=ts.iloc[i, 1:], ax=ax, label=ts['date'].iloc[i].strftime('%Y-%m-%d'), color=color)
    
    ax.set_title(f'{symbol_underlying} Term Structure for last {numDays} days')
    # set gridstyle
    ax.grid(True, which='both', axis='both', linestyle='--')
    sns.set_style('darkgrid')
    
"""
    Returns a plot of historical term structure contango for the last n periods
"""    
def plotHistoricalTermstructure(ts_data, pxHistory_underlying, ax, contangoColName='fourToSevenMoContango', **kwargs):
    smaPeriod_contango = kwargs.get('smaPeriod_contango', 20)
    ts_data.reset_index(inplace=True)
    pxHistory_underlying.reset_index(inplace=True)
    #sns.lineplot(x='date', y='oneToTwoMoContango', data=ts_data, ax=ax, label='oneToTwoMoContango', color='blue')
    #sns.lineplot(x='date', y='oneToThreeMoContango', data=ts_data, ax=ax, label='oneToThreeMoContango', color='green')
    #sns.lineplot(x='date', y='twoToThreeMoContango', data=ts_data, ax=ax, label='twoToThreeMoContango', color='red')
    #sns.lineplot(x='date', y='threeToFourMoContango', data=ts_data, ax=ax, label='threeToFourMoContango', color='pink')
    sns.lineplot(x='date', y=contangoColName, data=ts_data, ax=ax, label='fourToSevenMoContango', color='green')
    # plot 90th percentile rolling 252 period contango
    sns.lineplot(x='date', y=ts_data[contangoColName].rolling(252).quantile(0.9), data=ts_data, ax=ax, label='90th percentile', color='red', alpha=0.3)
    sns.lineplot(x='date', y=ts_data[contangoColName].rolling(252).quantile(0.5), data=ts_data, ax=ax, label='50th percentile', color='brown', alpha=0.4)
    sns.lineplot(x='date', y=ts_data[contangoColName].rolling(252).quantile(0.1), data=ts_data, ax=ax, label='10th percentile', color='red', alpha=0.3)

    # plot 5 period sma of contango
    sns.lineplot(x='date', y=ts_data[contangoColName].rolling(smaPeriod_contango).mean(), data=ts_data, ax=ax, label='%s period sma'%(smaPeriod_contango), color='blue', alpha=0.6)
    sns.lineplot(x='date', y=ts_data[contangoColName].rolling(int(smaPeriod_contango/2)).mean(), data=ts_data, ax=ax, label='%s period sma'%(int(smaPeriod_contango/2)), color='red', alpha=0.6)
    #sns.lineplot(x='date', y='currentToLastContango', data=ts_data, ax=ax, label='currentToLastContango', color='red')
    #sns.lineplot(x='date', y='averageContango', data=ts_data, ax=ax, label='averageContango', color='orange')

    # format plot 
    ax.set_title('Historical Contango')
    ax.grid(True, which='both', axis='both', linestyle='--')
    ax.axhline(0, color='black', linestyle='-', alpha=0.5)
    ax.legend(loc='upper left')

def plotTermstructureAutocorrelation(ts_data, ax, contangoColName='fourToSevenMoContango', max_lag=60):
    ts_data.reset_index(inplace=True)

    # Calculate autocorrelations for different lags
    autocorrelations = [ts_data[contangoColName].autocorr(lag=i) for i in range(max_lag)]

    # plot stem of autocorrelation
    ax.stem(range(max_lag), autocorrelations, use_line_collection=True, linefmt='--')
    ax.set_title(f'{contangoColName} Autocorrelation')

    # format plot
    ax.set_ylabel('Autocorrelation')
    ax.set_xlabel('Lag')

    ax.grid(True, which='both', axis='both', linestyle='--')

def plotTermstructureDistribution(ts_data, ax, contangoColName='fourToSevenMoContango'):
    ts_data.reset_index(inplace=True)
    sns.histplot(ts_data[contangoColName], ax=ax, bins=100, kde=True)

    # add vlines 
    ax.axvline(ts_data[contangoColName].mean(), color='black', linestyle='-', alpha=0.3)
    ax.axvline(ts_data[contangoColName].quantile(0.9), color='red', linestyle='--', alpha=0.3)
    ax.axvline(ts_data[contangoColName].quantile(0.1), color='red', linestyle='--', alpha=0.3)
    # vline at last close 
    ax.axvline(ts_data[contangoColName].iloc[-1], color='green', linestyle='-', alpha=0.6)

    # set vline labels 
    ax.text(ts_data[contangoColName].mean(), 0.5, 'mean: %0.2f'%(ts_data[contangoColName].mean()), color='black', fontsize=10, horizontalalignment='left')
    ax.text(ts_data[contangoColName].quantile(0.9) + 2, 10, '90th percentile: %0.2f'%(ts_data[contangoColName].quantile(0.9)), color='red', fontsize=10, horizontalalignment='right')
    ax.text(ts_data[contangoColName].quantile(0.1) - 3, 3, '10th percentile: %0.2f'%(ts_data[contangoColName].quantile(0.1)), color='red', fontsize=10)
    ax.text(ts_data[contangoColName].iloc[-1], 100, 'last: %0.2f'%(ts_data[contangoColName].iloc[-1]), color='green', fontsize=10, horizontalalignment='left')

    # format plot
    ax.set_title(f'{contangoColName} Distribution')
    ax.set_xlabel('Contango')
    ax.set_ylabel('Frequency')
    ax.grid(True, which='both', axis='both', linestyle='--')
