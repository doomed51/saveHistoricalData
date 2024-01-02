import pandas as pd 


###### DATABASE locations
dbname_stock = '/workbench/historicalData/venv/saveHistoricalData/data/historicalData_index.db'
dbname_futures = '/workbench/historicalData/venv/saveHistoricalData/data/historicalData_futures.db'
dbname_termstructure = '/workbench/historicalData/venv/saveHistoricalData/data/termstructure.db'

###### Watchlist locations 
watchlist_main = 'tickerList.csv'
watchlist_futures = 'futuresWatchlist.csv'

lookupTableName = '00-lookup_symbolRecords'

# default list of tracked intervals
intervals = ['1 min', '5 mins', '30 mins', '1 day']

# reference list of indices
_index = ['VIX', 'VIX3M', 'VVIX', 'SPX', 'VIX1D', 'TSX']

## dictionary of symbol, currency pairs
currency_mapping = {
    'CL': 'USD',
    'DXJ': 'USD',
    'ED': 'USD',
    'ES': 'USD',
    'ENB': 'CAD',
    'FV': 'USD',
    'GC': 'USD',
    'GE': 'USD',
    'HG': 'USD',
    'NQ': 'USD',
    'SPX': 'USD',
    'SI': 'USD',
    'TN': 'USD',
    'TSX': 'CAD',
    'TY': 'USD',
    'UB': 'USD',
    'US': 'USD',
    'VIX': 'USD',
    'VIX1D': 'USD',
    'VIX3M': 'USD',
    'VVIX': 'USD',
    'XIU': 'CAD',
    'Z': 'USD',
    'ZB': 'USD',
    'ZF': 'USD',
    'ZN': 'USD',
    'ZT': 'USD'
}


exchange_mapping = {
    'CL': 'NYMEX',
    'DXJ': 'ARCA',
    'ED': 'GLOBEX',
    'ES': 'GLOBEX',
    'ENB': 'SMART',
    'FV': 'ECBOT',
    'GC': 'NYMEX',
    'GE': 'ECBOT',
    'HG': 'COMEX',
    'NQ': 'GLOBEX',
    'SPX': 'CBOE',
    'SI': 'NYMEX',
    'TN': 'ECBOT',
    'TSX': 'TSE',
    'TY': 'ECBOT',
    'UB': 'ECBOT',
    'US': 'ECBOT',
    'VIX': 'CBOE',
    'VIX1D': 'CBOE',
    'VIX3M': 'CBOE',
    'VVIX': 'CBOE',
    'XIU': 'SMART',
    'Z': 'ECBOT',
    'ZB': 'ECBOT',
    'ZF': 'ECBOT',
    'ZN': 'ECBOT',
    'ZT': 'ECBOT'
}
# reference list of symbols with erroneous last historical date in ibkr 
# create dataframe with columns index, name, earliestTimestamp
#earliestTimestamp_lookup = pd.DataFrame(columns=['name', 'earliestTimestamp'])
#earliestTimestamp_lookup.loc[0] = ['VIX_index_1min', '2018-01-03 09:31:00']
#earliestTimestamp_lookup.loc[1] = ['VIX3M_index_1min', '2012-10-31 09:31:00']
#earliestTimestamp_lookup.loc[2] = ['VVIX_index_1min', '2014-05-14 09:31:00']