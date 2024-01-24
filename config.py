import pandas as pd 

###### DATABASE locations
dbname_stock = '/workbench/historicalData/venv/saveHistoricalData/data/historicalData_index.db'
dbname_futures = '/workbench/historicalData/venv/saveHistoricalData/data/historicalData_futures.db'
dbname_termstructure = '/workbench/historicalData/venv/saveHistoricalData/data/termstructure.db'

###### Watchlist locations 
watchlist_main = 'tickerList.csv'
watchlist_futures = 'futuresWatchlist.csv'

lookupTableName = '00-lookup_symbolRecords'

# Reference Lists
intervals = ['1 min', '5 mins', '30 mins', '1 day']
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
    'IBM': 'NYSE',
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

###############
############### API thresholds and timeouts 
ibkr_max_consecutive_calls = 50