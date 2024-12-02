import math
import pandas as pd 
import numpy as np

""" 
    This function returns the last business day for the given year and month
    inputs:
        year: [int] year
        month: [int] month
"""
def getLastBusinessDay(year, month):
    # get last day of month
    lastDayOfMonth = pd.Timestamp(year, month, 1) + pd.offsets.MonthEnd(0)

    # if last day of month is a weekend, get the last business day
    if lastDayOfMonth.dayofweek == 5:
        lastBusinessDay = lastDayOfMonth - pd.offsets.Day(1)
    elif lastDayOfMonth.dayofweek == 6:
        lastBusinessDay = lastDayOfMonth - pd.offsets.Day(2)
    else:
        lastBusinessDay = lastDayOfMonth

    return lastBusinessDay.day

"""
    Returns mean and sd of target column grouped by month
    inputs:
        - history: dataframe of 1day ohlcv data
        - targetCol: str of column to aggregate
    outputs:
        - dataframe with columns: [month, volume_mean, volume_sd]
"""
def aggregate_by_month(history, targetCol):
    # throw error if interval is not 1day
    if history['interval'][0] != '1day':
        raise ValueError('interval must be 1day')
    
    # convert date column to datetime
    history['date'] = pd.to_datetime(history['date'])

    # sort by date
    history = history.sort_values(by='date')

    # add month column
    history['month'] = history['date'].dt.month

    # group by month and get mean and sd of volume
    aggregate_by_month = history.groupby('month')[targetCol].agg(['mean', 'std']).reset_index()

    return aggregate_by_month

"""
    Returns mean and sd of tagetcol grouped by day of month
    inputs:
        - history: dataframe of 1day ohlcv data
        - targetCol: str of column to aggregate
    outputs:
        - dataframe with columns: [month, volume_mean, volume_sd]
"""
def aggregate_by_dayOfMonth(history, targetCol):
    # throw error if interval is not 1day
    if history['interval'][0] != '1day':
        raise ValueError('interval must be 1day')
        
    # convert date column to datetime
    history['date'] = pd.to_datetime(history['date'])

    # sort by date
    history = history.sort_values(by='date')

    # add month column
    #history['dayOfMonth'] = history['date'].dt.day

    # add column business_day_of_month, which is the business day of the month represented by the date
    # Apply the function to each row
    history['dayOfMonth'] = history.apply(lambda row: calculate_business_day_of_month(row, holidays=[]), axis=1)

    # group by month and get mean and sd of volume
    aggregate_by_dayOfMonth = history.groupby('dayOfMonth')[targetCol].agg(['mean', 'std']).reset_index()

    return aggregate_by_dayOfMonth

"""
    Returns means and sd of tagetCol grouped by day of week
    inputs:
        - history: dataframe of 1day ohlcv data
        - targetCol: str of column to aggregate
    outputs:
        - dataframe with columns: [dayofweek, mean, sd]
"""
def aggregate_by_dayOfWeek(history, targetCol):
    # throw error if interval is not 1day
    if history['interval'][0] != '1day':
        raise ValueError('interval must be 1day')
        
    # convert date column to datetime
    history['date'] = pd.to_datetime(history['date'])

    # sort by date
    history = history.sort_values(by='date')

    # add month column
    history['dayOfWeek'] = history['date'].dt.dayofweek

    # group by month and get mean and sd of volume
    aggregate_by_dayOfWeek = history.groupby('dayOfWeek')[targetCol].agg(['mean', 'std']).reset_index()

    return aggregate_by_dayOfWeek

"""
    Returns mean and sd of tagetCol grouped by timestamp 
    inputs:
        - history: dataframe of <1day ohlcv data with date column in format 'YYYY-MM-DD HH:MM:SS'
        - targetCol: str of column to aggregate
    outputs:
        - dataframe with columns: [timestamp, mean, sd]
"""
def aggregate_by_timestamp(history, targetCol):
        
    # convert date column to datetime if it is not already
    if history['date'].dtype != 'datetime64[ns]':
        history['date'] = pd.to_datetime(history['date'])

    # sort by date
    history = history.sort_values(by='date')

    # add month column
    history['timestamp'] = history['date'].dt.strftime('%H:%M:%S')

    # group by timestamp and get mean and sd of volume
    aggregate_by_timestamp = history.groupby('timestamp')[targetCol].agg(['mean', 'std']).reset_index()

    return aggregate_by_timestamp

"""
    This function calculates log returns on the passed in values 
    inputs:
        - history - dataframe of timeseries data 
        - colName - name of the column to calculate log returns on
        - lag - number of days to lag the log return calculation
        - direction - 1 = open long, -1 = open short
    output:
        - history with a log return column added
"""
def calcLogReturns(history, colName, lag=1, direction=1):
    # calculate log returns 
    if direction == 1:
        history['logReturn'] = (history[colName].apply(lambda x: math.log(x)) - history[colName].shift(lag).apply(lambda x: math.log(x))).round(5)
    elif direction == -1:
        history['logReturn'] = (history[colName].apply(lambda x: math.log(x)) - history[colName].shift(lag).apply(lambda x: math.log(x))).round(5)
        history['logReturn'] = history['logReturn'] * -1  
    return history.reset_index(drop=True)

# Function to calculate business days of the month
def calculate_business_day_of_month(row, holidays=[]):
    # Start and end of month
    start_of_month = row['date'].replace(day=1)
    end_of_month = start_of_month + pd.offsets.MonthEnd(1)
    
    # Generate business days for the month, excluding weekends and optionally holidays
    business_days = pd.bdate_range(start=start_of_month, end=end_of_month, freq='B', holidays=holidays)
    
    # Calculate business day of the month
    business_day_of_month = np.where(business_days == row['date'])[0] + 1  # +1 because we want the count to start from 1
    return business_day_of_month[0] if business_day_of_month.size > 0 else np.nan


"""
    For timeseries data with gaps (e.g., only has business days), this function returns the closest date in pxHistory to the targetDay 
"""
def closest_day(pxHistory, targetDay):
   pxHistory['daydiff'] = abs(targetDay - pxHistory['day'])
   pxHistory = pxHistory.sort_values(by='daydiff', ascending=True)
   closest_date = pxHistory.iloc[0]['date']
   return closest_date

"""
    returns a string of the current date in YYYYMM format
"""
def futures_getExpiryDateString():
    return pd.Timestamp.today().strftime('%Y%m')

""" 
    returns dataframe of merged strategy returns
    intputs: 
        - returns: [array] array of dataframes of returns
"""
def mergeStrategyReturns(returns, strategyName='merged'):

    mergedReturns = pd.DataFrame()
    for ret in returns:
        mergedReturns = pd.concat([mergedReturns, ret])
    mergedReturns.sort_values(by='date', inplace=True)
    mergedReturns.reset_index(drop=True, inplace=True)
    mergedReturns['cumsum'] = mergedReturns['logReturn'].cumsum()

    return mergedReturns

"""
    returns correlation between two columns in pxHistory
"""
def calcCorrelation(pxHistory_, column1, column2, type='pearson'):
    if type == 'pearson':
        return pxHistory_[column1].corr(pxHistory_[column2], method='pearson')

"""
    Adds z-score and z-score decile of the target column
"""
def calcZScore(pxHistory, targetCol, numBuckets=10):
    pxHistory['zscore_%s'%(targetCol)] = (pxHistory[targetCol] - pxHistory[targetCol].mean())/pxHistory[targetCol].std(ddof=0)
    # add z-score decile 
    if numBuckets == 10:
        pxHistory['zscore_%s_decile'%(targetCol)] = pd.qcut(pxHistory['zscore_%s'%(targetCol)], numBuckets, labels=False)
    elif numBuckets == 5:
        pxHistory['zscore_%s_quintile'%(targetCol)] = pd.qcut(pxHistory['zscore_%s'%(targetCol)], numBuckets, labels=False)
    else:
        pxHistory['zscore_%s_%s-ile'%(targetCol, numBuckets)] = pd.qcut(pxHistory['zscore_%s'%(targetCol)], numBuckets, labels=False)
   #return pxHistory

"""
====================== /////  TESTS ///// ======================
"""

def test_closest_day():
    pxHistory = pd.DataFrame({'day': [1, 2, 3, 4, 5],
                              'date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04', '2022-01-05']})
    targetDay = 3
    closest_date = closest_day(pxHistory, targetDay)
    assert closest_date == '2022-01-03'
    

test_closest_day()