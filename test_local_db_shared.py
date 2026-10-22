import datetime
import os
import sqlite3
import tempfile
import unittest

import interface_localDB as current_db
import interface_localDb_old as legacy_db
from local_db_shared import (
    _addspace,
    _getFirstRecordDate,
    _removeSpaces,
    getLookup_exchange,
    sqlite_connection,
)


class TestLocalDbSharedHelpers(unittest.TestCase):
    def test_interfaces_reexport_the_shared_helpers(self):
        for module in (current_db, legacy_db):
            self.assertIs(module.sqlite_connection, sqlite_connection)
            self.assertIs(module._addspace, _addspace)
            self.assertIs(module._removeSpaces, _removeSpaces)
            self.assertIs(module._getFirstRecordDate, _getFirstRecordDate)
            self.assertIs(module.getLookup_exchange, getLookup_exchange)

    def test_string_helpers_preserve_existing_formatting(self):
        self.assertEqual(_addspace('1day'), '1 day')
        self.assertEqual(_removeSpaces('15 mins'), '15mins')

    def test_first_record_date_handles_date_and_timestamp(self):
        for date_value, expected in (
            ('2024-01-02', datetime.datetime(2024, 1, 2)),
            ('2024-01-02 03:04:05', datetime.datetime(2024, 1, 2, 3, 4, 5)),
            ('2024-01-02 03:04:05-05:00', datetime.datetime(2024, 1, 2, 3, 4, 5)),
        ):
            with self.subTest(date_value=date_value):
                conn = sqlite3.connect(':memory:')
                conn.execute('CREATE TABLE sample (date TEXT)')
                conn.execute('INSERT INTO sample(date) VALUES (?)', (date_value,))
                actual = _getFirstRecordDate({'name': 'sample'}, conn)
                conn.close()
                self.assertEqual(actual, expected)

    def test_exchange_lookup_reads_sqlite_value(self):
        conn = sqlite3.connect(':memory:')
        conn.execute('CREATE TABLE "00-lookup_exchangeMapping" (symbol TEXT, exchange TEXT)')
        conn.execute('INSERT INTO "00-lookup_exchangeMapping" VALUES (?, ?)', ('ES', 'CME'))
        self.assertEqual(getLookup_exchange(conn, 'ES'), 'CME')
        conn.close()

    def test_connection_context_manager_commits_and_closes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'shared.sqlite')
            with sqlite_connection(path) as conn:
                conn.execute('CREATE TABLE sample (value INTEGER)')
                conn.execute('INSERT INTO sample VALUES (7)')
                managed_connection = conn
            with self.assertRaises(sqlite3.ProgrammingError):
                managed_connection.execute('SELECT 1')
            check = sqlite3.connect(path)
            try:
                self.assertEqual(check.execute('SELECT value FROM sample').fetchone(), (7,))
            finally:
                check.close()


if __name__ == '__main__':
    unittest.main()
