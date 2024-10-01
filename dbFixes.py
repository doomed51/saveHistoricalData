import pandas as pd 
import interface_localDb as db 

import config
import sqlite3

## restores firstRecordDate column in lookoup table 
# get list of tables in db 
# for each table get the min(date) and update lookup table
def restoreFirstRecordDate():
    with sqlite3.connect(config.dbname_stock) as conn:
        # read in all tables 
        tables = db.getRecords(conn)
        # red in lookup table 
        sqlStatement_selectRecordsTable = 'SELECT * FROM \'00-lookup_symbolRecords\''
        lookup = pd.read_sql(sqlStatement_selectRecordsTable, conn)
    
    # merge tables['firstRecordDate'] with lookup on name column
    tables = pd.DataFrame(tables, columns=['name', 'firstRecordDate'])
    lookup = pd.merge(lookup, tables, on='name', how='left')
    lookup = lookup[['name', 'symbol', 'type', 'interval', 'numMissingBusinessDays', 'firstRecordDate']]

    # update lookup table
    with sqlite3.connect(config.dbname_stock) as conn:
        lookup.to_sql('00-lookup_symbolRecords', conn, if_exists='replace', index=False)

# restore name column in lookup table
def restoreName():
    with sqlite3.connect(config.dbname_stock) as conn:
        # read in the lookup table 
        sqlStatement_selectRecordsTable = 'SELECT * FROM \'00-lookup_symbolRecords\''
        lookup = pd.read_sql(sqlStatement_selectRecordsTable, conn)

        # add name column by appending column symbol_type_interval
        lookup['name'] = lookup['symbol'] + '_' + lookup['type'] + '_' + lookup['interval']
        # make name the first column 
        lookup = lookup[['name', 'symbol', 'type', 'interval', 'numMissingBusinessDays']]
        
        # update lookup table
        lookup.to_sql('00-lookup_symbolRecords', conn, if_exists='replace', index=False)

"""
    This function changes changes a column name for all tables in the db other than the lookup tables 
    inputs: 
        fromName - the name of the column to change
        toName - the new name of the column
        conn - the connection object to the db
"""
def changeColumnName(conn, fromName, toName): 
    print('RENAMING COLUMN %s TO %s'%(fromName, toName))
    # read in all tables in db 
    sql_getTables = 'SELECT name FROM sqlite_master WHERE type=\'table\' AND NOT name LIKE \'00-lookup%\''
    tables = pd.read_sql(sql_getTables, conn)
    
    print(tables)
    # for each table, rename the column
    for table in tables['name']:
        sqlStatement = 'ALTER TABLE \'%s\' RENAME COLUMN %s TO %s'%(table, fromName, toName)
        conn.execute(sqlStatement)
    
    print('DONE!')

def tablenames(conn):
    
    # read in all tables in db 
    sql_getTables = 'SELECT name FROM sqlite_master WHERE type=\'table\' AND NOT name LIKE \'00-lookup%\''
    tables = pd.read_sql(sql_getTables, conn)
    
    # split name into symbol, expiry, interval columns
    tables['symbol'] = tables['name'].str.split('_', expand=True)[0]
    tables['expiry'] = tables['name'].str.split('_', expand=True)[1]
    tables['interval'] = tables['name'].str.split('_', expand=True)[2]

    # select rows where expiry has only 6 chars
    tables = tables[tables['expiry'].str.len() == 6]

    # drop tables from db that are in tables 
    for table in tables['name']:
        sql_dropTable = 'DROP TABLE \'%s\''%(table)
        conn.execute(sql_dropTable)


with sqlite3.connect(config.dbname_future) as conn:
    #changeColumnName(conn, 'lastTradeMonth', 'lastTradeDate')
    tablenames(conn)
