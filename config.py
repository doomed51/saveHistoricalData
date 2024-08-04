
###### Local library paths 
path_lib_analysis = '/workbench/analysis/venv'
path_lib_analysis_backtests = '/workbench/analysis/venv/backtests'
path_lib_analysis_strategies = '../workbench/analysis/venv/strategy_implementation'

###### DATABASE locations
dbname_stock = '/workbench/historicalData/venv/saveHistoricalData/data/historicalData_index.db'
dbname_futures = '/workbench/historicalData/venv/saveHistoricalData/data/historicalData_futures.db'
dbname_termstructure = '/workbench/historicalData/venv/saveHistoricalData/data/termstructure.db'
dbname_rwtools_futures_vix_csv = '/workbench/historicalData/venv/saveHistoricalData/data/vix_chunks01.csv'

###### Watchlist locations 
watchlist_main = 'tickerList.csv'
watchlist_futures = 'futuresWatchlist.csv'
lookupTableName = '00-lookup_symbolRecords'
table_name_futures_pxhistory_metadata = '00-lookup_pxhistory_metadata'

# Reference Lists
intervals = ['1 min', '5 mins', '30 mins', '1 day']
_indexList = ['VIX', 'VIX3M', 'VVIX', 'SPX', 'VIX1D', 'TSX']
_index = ['VIX', 'VIX3M', 'VVIX', 'SPX', 'VIX1D', 'TSX']
delisted_symbols = ['BURU']

## Lookup dicts 
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
    'NG': 'NYMEX',
    'SPX': 'CBOE',
    'SI': 'NYMEX',
    'TN': 'ECBOT',
    'TSX': 'TSE',
    'TY': 'ECBOT',
    'UB': 'ECBOT',
    'US': 'ECBOT',
    'VIX': 'CFE',
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

############### API thresholds and timeouts 
ibkr_max_consecutive_calls = 50