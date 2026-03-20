
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
_indexList = ['GVZ', 'INDU', 'NDX', 'VIX', 'VIX3M', 'VVIX', 'SPX', 'VIX1D', 'TSX']
_index = ['GVZ', 'INDU', 'NDX', 'VIX', 'VIX3M', 'VVIX', 'SPX', 'VIX1D', 'TSX']
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
    'INDU': 'USD',
    'NQ': 'USD',
    'NDX': 'USD',
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
    'CLOI': 'SMART',
    'DXJ': 'ARCA',
    'ED': 'GLOBEX',
    'ES': 'CME',
    'ENB': 'SMART',
    'FV': 'ECBOT',
    'GC': 'COMEX',
    'GE': 'ECBOT',
    'GVZ': 'CBOE',
    'HG': 'COMEX',
    'IBM': 'NYSE',
    'INDU': 'CME',
    'NDX': 'NASDAQ',
    'NG': 'NYMEX',
    'SPX': 'CBOE',
    'SI': 'COMEX',
    'TN': 'ECBOT',
    'TSX': 'TSE',
    'TY': 'CBOT',
    'UB': 'CBOT',
    'US': 'CBOT',
    'VIX': 'CFE',
    'VIX1D': 'CFE',
    'VIX3M': 'CFE',
    'VVIX': 'CFE',
    'XIU': 'SMART',
    'Z': 'CBOT',
    'ZB': 'CBOT',
    'ZF': 'CBOT',
    'ZN': 'CBOT',
    'ZT': 'CBOT'
}

futures_symbol_metadata = {
    'CL': {
        'exchange': 'NYMEX',
        'listing_cycle': 'monthly',
        'roll_bdays': 3,
        'lookahead_months': 8,
    },
    'ES': {
        'exchange': 'CME',
        'listing_cycle': 'quarterly',
        'roll_bdays': 5,
        'lookahead_months': 8,
    },
    'GC': {
        'exchange': 'COMEX',
        'listing_cycle': 'monthly',
        'roll_bdays': 3,
        'lookahead_months': 8,
    },
    'NG': {
        'exchange': 'NYMEX',
        'listing_cycle': 'monthly',
        'roll_bdays': 3,
        'lookahead_months': 8,
    },
    'SI': {
        'exchange': 'COMEX',
        'listing_cycle': 'monthly',
        'roll_bdays': 3,
        'lookahead_months': 8,
    },
    'VIX': {
        'exchange': 'CFE',
        'listing_cycle': 'monthly',
        'roll_bdays': 3,
        'lookahead_months': 8,
    },
    'ZB': {
        'exchange': 'CBOT',
        'listing_cycle': 'quarterly',
        'roll_bdays': 5,
        'lookahead_months': 8,
    },
    'ZN': {
        'exchange': 'CBOT',
        'listing_cycle': 'quarterly',
        'roll_bdays': 5,
        'lookahead_months': 8,
    },
}

############### API thresholds and timeouts 
ibkr_max_consecutive_calls = 50