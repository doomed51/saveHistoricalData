
from urllib.error import HTTPError, URLError
from qtrade import Questrade 
from pathlib import Path
from requests.exceptions import HTTPError

import pandas as pd  #ease of printing to csv 

## define lookup parameters
ticker = 'AAPL'
startDate = '2020-11-01'
endDate = '2022-01-04'
interval = 'OneHour' #OneDay, ...

#####
# Setup connection to Questrade API
###
# first try the yaml file
# if yaml fails, try a refresh
# if that fails try the token (i.e. a new token will 
#   need to be manually updated from the qt API )
#
#####
def setupConnection():
    try:
        print("\n trying token yaml \n")
        qtrade = Questrade(token_yaml = "access_token.yml")
        w = qtrade.get_quote(['SPY'])
        print ('%s latest Price: %.2f'%(w['symbol'], w['lastTradePrice'])) 
        
    except(HTTPError, FileNotFoundError, URLError) as err:
        try: 
            print("\n Trying Refresh \n")
            qtrade = Questrade(token_yaml = "access_token.yml")
            qtrade.refresh_access_token(from_yaml = True, yaml_path="access_token.yml")
            print ('%s latest Price: %.2f'%(w['symbol'], w['lastTradePrice'])) 

        except(HTTPError, FileNotFoundError, URLError):
            print("\n Trying Access Code \n")
            
            try:
                with open("token.txt") as f:
                    ac = f.read().strip()
                    qtrade = Questrade(access_code = ac)
                    print ('%s latest Price: %.2f'%(w['symbol'], w['lastTradePrice'])) 
            
            except(HTTPError, FileNotFoundError) as err:
                print("\n Might neeed new access code from Questrade \n")
                print(err)
    return qtrade

## Initiate connection
qtrade = setupConnection()

## Retrieve historical data  
history = pd.DataFrame()
try:
    ## Retrieve data from questrade
    history = pd.DataFrame(qtrade.get_historical_data(ticker, startDate, endDate, interval))
    print('\n history retrieved! \n')

    res = history[ ::len(history)-1 ]
    ## cleanup timestamp formatting
    history['start'] = history['start'].astype(str).str[:-6]

except(HTTPError, FileNotFoundError):
    print ('ERROR History not retrieved')

# save the historical data in a csv file
if not history.empty:
    filepath = Path('output/'+ticker+'_'+interval+'_'+endDate+'.csv')
    print('Saving %s interval data for %s'%(interval, ticker))
    filepath.parent.mkdir(parents=True, exist_ok=True)
    history.to_csv(filepath, index=False)