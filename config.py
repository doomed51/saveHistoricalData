import pandas as pd 


###### DATABASE locations
dbname_stock = '/workbench/historicalData/venv/saveHistoricalData/historicalData_index.db'
dbname_future = '/workbench/historicalData/venv/saveHistoricalData/historicalData_futures.db'

###### Watchlist locations 
watchlist_main = 'tickerList.csv'
watchlist_futures = 'futuresWatchlist.csv'

lookupTableName = '00-lookup_symbolRecords'

# default list of tracked intervals
intervals = ['1 min', '5 mins', '30 mins', '1 day']

# reference list of indices
_index = ['VIX', 'VIX3M', 'VVIX', 'SPX', 'VIX1D']

# reference list of symbols with erroneous last historical date in ibkr 
# create dataframe with columns index, name, earliestTimestamp
#earliestTimestamp_lookup = pd.DataFrame(columns=['name', 'earliestTimestamp'])
#earliestTimestamp_lookup.loc[0] = ['VIX_index_1min', '2018-01-03 09:31:00']
#earliestTimestamp_lookup.loc[1] = ['VIX3M_index_1min', '2012-10-31 09:31:00']
#earliestTimestamp_lookup.loc[2] = ['VVIX_index_1min', '2014-05-14 09:31:00']