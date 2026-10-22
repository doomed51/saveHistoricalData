"""Contracts for migrating equities storage to the canonical SQLite interface."""

from io import StringIO
import sqlite3
import unittest

import pandas as pd

import interface_localDB as current_db
import interface_localDb_old as legacy_db


def make_connection():
    conn = sqlite3.connect(':memory:')
    conn.execute(
        'CREATE TABLE "00-lookup_symbolRecords" ('
        'name TEXT, symbol TEXT, type TEXT, interval TEXT, '
        'firstRecordDate TEXT, numMissingBusinessDays INTEGER, lastTradeDate TEXT)'
    )
    return conn


def make_history(symbol, interval='1day', expiry=None):
    rows = pd.DataFrame({
        'date': ['2024-01-03', '2024-01-03'],
        'open': [10.0, 11.0],
        'high': [12.0, 13.0],
        'low': [9.0, 10.0],
        'close': [11.0, 12.0],
        'volume': [100, 200],
        'symbol': [symbol, symbol],
        'interval': [interval, interval],
    })
    if expiry is not None:
        rows['lastTradeDate'] = expiry
    return rows


def write_snapshot(module, history, earliest_timestamp='2024-01-01'):
    conn = make_connection()
    try:
        writer = (
            module.saveHistoryToDB_legacy
            if module is current_db else module.saveHistoryToDB
        )
        writer(history, conn, earliestTimestamp=earliest_timestamp)
        names = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '00_%'"
        )]
        assert len(names) == 1
        name = names[0]
        rows = conn.execute('SELECT date, close, interval FROM "%s"' % name).fetchall()
        lookup = conn.execute(
            'SELECT name, symbol, interval, firstRecordDate, numMissingBusinessDays, lastTradeDate '
            'FROM "00-lookup_symbolRecords"'
        ).fetchall()
        return name, rows, lookup
    finally:
        conn.close()


class TestLocalDbMigration(unittest.TestCase):
    def test_canonical_writer_matches_legacy_rows_and_lookup(self):
        for symbol, expiry, table_name in (
            ('SPY', None, 'SPY_stock_1day'),
            ('SPX', None, 'SPX_index_1day'),
            ('VIX', '202401', 'VIX_202401_1day'),
        ):
            with self.subTest(symbol=symbol, expiry=expiry):
                history = make_history(symbol, expiry=expiry)
                expected = (
                    table_name,
                    [('2024-01-03', 11.0, '1day')],
                    [(table_name, symbol, '1day', '2024-01-03 00:00:00', 5, expiry)],
                )
                self.assertEqual(write_snapshot(legacy_db, history.copy()), expected)
                self.assertEqual(write_snapshot(current_db, history.copy()), expected)

    def test_current_writer_uses_actual_instrument_type(self):
        for symbol, expiry in (('SPY', None), ('SPX', None), ('VIX', '202401')):
            with self.subTest(symbol=symbol):
                conn = make_connection()
                try:
                    current_db.saveHistoryToDB(
                        make_history(symbol, expiry=expiry), conn,
                        earliestTimestamp='2024-01-01',
                    )
                    first_date = conn.execute(
                        'SELECT firstRecordDate FROM "00-lookup_symbolRecords"'
                    ).fetchone()[0]
                    self.assertTrue(first_date.startswith('2024-01-03'))
                    table_name = (
                        '%s_%s_1day' %
                        (symbol, expiry or ('index' if symbol == 'SPX' else 'stock'))
                    )
                    self.assertEqual(
                        conn.execute('SELECT COUNT(*) FROM "%s"' % table_name).fetchone()[0],
                        1,
                    )
                finally:
                    conn.close()

    def test_current_records_support_vix_refresh_fields(self):
        conn = make_connection()
        try:
            make_history('VIX', expiry='202401').iloc[:1].to_sql(
                'VIX_202401_1day', conn, index=False
            )
            records = current_db.getRecords_legacy(conn)
            self.assertEqual(records.loc[0, 'name'], 'VIX_202401_1day')
            self.assertEqual(records.loc[0, 'interval'], '1 day')
            self.assertEqual(str(records.loc[0, 'lastUpdateDate'].date()), '2024-01-03')
        finally:
            conn.close()

    def test_canonical_lookup_can_include_legacy_type_projection(self):
        conn = make_connection()
        try:
            conn.execute(
                'INSERT INTO "00-lookup_symbolRecords" '
                '(name, symbol, interval, firstRecordDate) VALUES (?, ?, ?, ?)',
                ('SPY_stock_1day', 'SPY', '1day', '2024-01-03')
            )
            conn.execute(
                'INSERT INTO "00-lookup_symbolRecords" '
                '(name, symbol, interval, firstRecordDate) VALUES (?, ?, ?, ?)',
                ('VIX_202401_1day', 'VIX', '1day', '2024-01-03')
            )
            expected = legacy_db.getLookup_symbolRecords(conn)
            actual = current_db.getLookup_symbolRecords(conn, include_type=True)
            pd.testing.assert_frame_equal(actual, expected)
            self.assertEqual(actual.loc[1, 'type'], 'future')
            self.assertTrue(pd.isna(current_db.getLookup_symbolRecords(conn).loc[0, 'type']))
        finally:
            conn.close()

    def test_canonical_csv_reader_preserves_adjusted_close_and_dates(self):
        csv_text = 'Date,Close,Adj Close,Volume\n2024-01-03,10,11,100\n'
        expected = legacy_db.getHistoryFromCSV(StringIO(csv_text), 'SPY', '1day')
        actual = current_db.getHistoryFromCSV(StringIO(csv_text), 'SPY', '1day')
        pd.testing.assert_frame_equal(actual, expected)

    def test_canonical_prehistory_status_matches_legacy_contract(self):
        conn = sqlite3.connect(':memory:')
        try:
            self.assertEqual(
                current_db.recordPrehistoryAttempt(conn, 'SPY', '1day', False),
                (1, False),
            )
            self.assertEqual(current_db.getExhaustedPrehistoryRows(conn), set())
        finally:
            conn.close()

    def test_canonical_lookup_refresh_updates_first_date_and_missing_days(self):
        conn = make_connection()
        try:
            make_history('SPY').iloc[:1].to_sql('SPY_stock_1day', conn, index=False)
            conn.execute(
                'INSERT INTO "00-lookup_symbolRecords" '
                '(name, symbol, interval, firstRecordDate, numMissingBusinessDays) '
                'VALUES (?, ?, ?, ?, ?)',
                ('SPY_stock_1day', 'SPY', '1day', '2024-01-04', 5),
            )
            current_db.refresh_lookup_from_history(
                conn, 'SPY_stock_1day', 'stock', '2024-01-01'
            )
            row = conn.execute(
                'SELECT firstRecordDate, numMissingBusinessDays '
                'FROM "00-lookup_symbolRecords"'
            ).fetchone()
            self.assertTrue(row[0].startswith('2024-01-03'))
            self.assertEqual(row[1], len(pd.bdate_range('2024-01-01', '2024-01-03')))
        finally:
            conn.close()

    def test_legacy_records_and_price_history_shapes(self):
        conn = make_connection()
        try:
            make_history('SPY').iloc[:1].to_sql('SPY_stock_1day', conn, index=False)
            records = legacy_db.getRecords(conn)
            self.assertEqual(records.columns.tolist(), [
                'symbol', 'type/expiry', 'interval', 'name',
                'firstRecordDate', 'lastUpdateDate', 'daysSinceLastUpdate',
            ])
            self.assertEqual(records.loc[0, 'interval'], '1 day')
            self.assertEqual(records.loc[0, 'name'], 'SPY_stock_1day')

            intraday = make_history('SPY', interval='1min')
            intraday['date'] = ['2024-01-03 09:30:00', '2024-01-03 09:31:00']
            intraday.to_sql('SPY_stock_1min', conn, index=False)
            history = legacy_db.getPriceHistory(conn, 'SPY', '1min', withpctchange=True)
            self.assertEqual(history.columns.tolist(), [
                'Date', 'open', 'high', 'low', 'close', 'volume',
                'symbol', 'interval', 'Time', 'close_pctChange',
            ])
            self.assertEqual(history.loc[1, 'close_pctChange'], 12.0 / 11.0 - 1)
        finally:
            conn.close()


if __name__ == '__main__':
    unittest.main()
