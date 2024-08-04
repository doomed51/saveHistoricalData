"""
This script is used to check the integrity of the data saved in the database.
    Check are housed by data types: {OHLC, and Termstructure}
"""
from datetime import datetime
import pandas as pd 
import holidays as hols

def _check_for_missing_dates_in_timeseries(timeseries, date_col_name = 'date', dates = None, **kwargs):
    """
        Check for missing dates in a timeseries. Ignores weekends and holidays. use <dates> if provided to check for specific dates otherwise do a generic test. 
        
        Returns a list of missing dates.
        Returns None if no missing dates are found.
    """
    # if date_col_name is index reset index 
    if date_col_name == 'index':
        timeseries.reset_index(inplace=True)
        date_col_name = 'date'
    ## set args for holiday lookup
    country = kwargs.get('country', 'US')
    exchange = kwargs.get('exchange', 'NYSE')
    timeseries[date_col_name] = pd.to_datetime(timeseries[date_col_name])
    
    # If dates are provided, use them; otherwise, use the date range from the timeseries
    if dates is None:
        start_date = min(timeseries[date_col_name].dt.date)
        end_date = max(timeseries[date_col_name].dt.date)
        all_dates = pd.date_range(start=start_date, end=end_date)#.to_pydatetime()
        all_dates = pd.to_datetime(all_dates)
    else:
        all_dates = pd.to_datetime(dates)#.to_pydatetime()
    
    years = timeseries[date_col_name].dt.year.unique()

    # handle holidays 
    holidays = hols.NYSE( years=years).keys()
    expected_dates = [
        date for date in all_dates
        if date.weekday() < 5
    ]
    # remove any dates in holidays that are in expected_dates
    expected_dates = [date for date in expected_dates if date.date() not in holidays]
    expected_dates = pd.to_datetime(expected_dates)

    # Identify missing dates
    missing_dates = [date for date in expected_dates if date.date() not in timeseries[date_col_name].dt.date.to_list()] 

    return pd.to_datetime(missing_dates)