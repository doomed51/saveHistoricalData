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

def drop_tables_with_malformed_expiry(conn):
    """
        This function drops all tables in the db that have an expiry column with 6 characters (malformed)
    """
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

def generate_subset_of_db(conn, subset_db_name:str, table_name_fragment:str):
    subset_conn = sqlite3.connect(subset_db_name)

    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?", (f'%{table_name_fragment.upper()}%',))
    tables_to_copy = cursor.fetchall()

    if not tables_to_copy:
        print(f"No tables found containing '{table_name_fragment}'.")
        return
    
    # Step 3: For each table, copy schema and data
    for table_name_tuple in tables_to_copy:
        table_name = table_name_tuple[0]
        print(f"Copying table: {table_name}")

        # Step 3a: Get the schema of the table
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        create_table_sql = cursor.fetchone()[0]

        # Step 3b: Execute the create table statement in the subset database
        subset_cursor = subset_conn.cursor()
        subset_cursor.execute(create_table_sql)
        print(f"Created table schema for: {table_name}")

        # Step 3c: Copy all data from the original table to the new table
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()

        if rows:
            # Get the number of columns in the table to prepare an insert statement
            num_columns = len(rows[0])
            placeholders = ','.join(['?' for _ in range(num_columns)])
            insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

            # Insert data into the new table
            subset_cursor.executemany(insert_sql, rows)
            print(f"Copied {len(rows)} rows into {table_name}")

    # Step 4: Commit and close the connection to the subset database
    subset_conn.commit()
    subset_conn.close()


with sqlite3.connect(config.dbname_future) as conn:
    #changeColumnName(conn, 'lastTradeMonth', 'lastTradeDate')
    drop_tables_with_malformed_expiry(conn)

