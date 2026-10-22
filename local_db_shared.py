"""Helpers shared by the local SQLite interface modules."""

import datetime
import re
import sqlite3

import pandas as pd


class sqlite_connection(object):
    """Context manager that commits and closes a SQLite connection."""

    def __init__(self, db_name):
        self.db_name = db_name

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.commit()
        self.conn.close()


def _addspace(myStr):
    return re.sub("[A-Za-z]+", lambda elm: " " + elm[0], myStr)


def _removeSpaces(myStr):
    return myStr.replace(" ", "")


def _getFirstRecordDate(row, conn):
    mintime = pd.read_sql('SELECT MIN(date) FROM ' + row['name'], conn)
    mintime = mintime['MIN(date)'][0]

    # Support dates, timestamps, and timestamps with timezone suffixes.
    if 19 >= len(mintime) > 10:
        mintime = datetime.datetime.strptime(mintime, '%Y-%m-%d %H:%M:%S')
    elif len(mintime) > 19:
        mintime = mintime[:19]
        mintime = datetime.datetime.strptime(mintime, '%Y-%m-%d %H:%M:%S')
    else:
        mintime = datetime.datetime.strptime(mintime, '%Y-%m-%d')

    return mintime


def getLookup_exchange(conn, symbol):
    exchange_lookup_table = '00-lookup_exchangeMapping'
    sql = "SELECT exchange FROM '%s' WHERE symbol='%s'" % (exchange_lookup_table, symbol)
    exchange = pd.read_sql(sql, conn).values[0][0]
    return exchange
