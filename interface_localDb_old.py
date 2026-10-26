"""Compatibility entry points for the original SQLite interface.

Implementations live in interface_localDB; this module preserves older signatures
and DataFrame formats for callers that still import it.
"""

import config
import interface_localDB as _current


dbname_stocks = config.dbname_stock
index_list = config._index
sqlite_connection = _current.sqlite_connection
_addspace = _current._addspace
_removeSpaces = _current._removeSpaces
_getDaysSinceLastUpdated = _current._getDaysSinceLastUpdated
_getLastUpdateDate = _current._getLastUpdateDate
_getFirstRecordDate = _current._getFirstRecordDate
getLookup_exchange = _current.getLookup_exchange
_PREHISTORY_STATUS_TABLE = _current._PREHISTORY_STATUS_TABLE
_ensure_prehistory_status_table = _current._ensure_prehistory_status_table


def construct_record_metadata(conn, table_names):
    return _current.construct_record_metadata(conn, table_names)


def _constructTableName(symbol, interval, lastTradeDate=''):
    return _current.construct_legacy_table_name(symbol, interval, lastTradeDate)


def _removeDuplicates(conn, tablename):
    return _current._removeDuplicates(tablename, conn)


def _updateLookup_symbolRecords(conn, tablename, type, earliestTimestamp, numMissingDays=5):
    return _current.refresh_lookup_from_history(
        conn, tablename, type, earliestTimestamp, numMissingDays
    )


def _formatpxHistory(pxHistory):
    return _current.format_price_history_legacy(pxHistory)


def saveHistoryToDB(history, conn, earliestTimestamp='', type=''):
    return _current.saveHistoryToDB_legacy(history, conn, earliestTimestamp, type)


def getRecords(conn):
    return _current.getRecords_legacy(conn)


def getPriceHistory(conn, symbol, interval, withpctchange=False, lastTradeDate=''):
    return _current.getPriceHistory_legacy(
        conn, symbol, interval, withpctchange, lastTradeDate
    )


def getHistoryFromCSV(filepath, symbol, interval):
    return _current.getHistoryFromCSV(filepath, symbol, interval)


def getTable(conn, tablename):
    return _current.getTable(conn, tablename)


def getLookup_symbolRecords(conn):
    return _current.getLookup_symbolRecords(conn, include_type=True)


def getExhaustedPrehistoryRows(conn):
    return _current.getExhaustedPrehistoryRows(conn)


def recordPrehistoryAttempt(conn, symbol, interval, had_backward_progress, max_failures=3):
    return _current.recordPrehistoryAttempt(
        conn, symbol, interval, had_backward_progress, max_failures
    )
