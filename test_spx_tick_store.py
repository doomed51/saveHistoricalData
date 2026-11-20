import os
import sqlite3
import tempfile
import unittest

from interface_ticks import (
    connect_ticks,
    ensure_schema,
    ensure_sessions,
    get_session,
    get_ticks,
    list_sessions,
    save_session,
    session_metrics,
    tick_table,
    upsert_ticks,
)

SESSIONS = [
    {
        'session_date': '2026-11-16',
        'session_open': '2026-11-16 09:30:00',
        'session_close': '2026-11-16 16:00:00',
    },
    {
        'session_date': '2026-11-17',
        'session_open': '2026-11-17 09:30:00',
        'session_close': '2026-11-17 16:00:00',
    },
    {
        'session_date': '2026-11-18',
        'session_open': '2026-11-18 09:30:00',
        'session_close': '2026-11-18 16:00:00',
    },
    {
        'session_date': '2026-11-19',
        'session_open': '2026-11-19 09:30:00',
        'session_close': '2026-11-19 16:00:00',
    },
]

TICK_SESSION = {
    'symbol': 'TEST',
    'session_date': '2026-11-18',
    'session_open': '2026-11-18 09:30:00',
    'session_close': '2026-11-18 16:00:00',
    'status': 'pending',
}


def _set_status(conn, session, status):
    state = dict(session)
    state['symbol'] = 'TEST'
    state['status'] = status
    save_session(conn, state)


class TestTickStoreSchema(unittest.TestCase):
    def setUp(self):
        self.conn = connect_ticks(':memory:')

    def tearDown(self):
        self.conn.close()

    def test_tick_table_uses_symbol(self):
        self.assertEqual(tick_table('SPX'), 'SPX_index_ticks')
        self.assertEqual(tick_table('TEST'), 'TEST_index_ticks')

    def test_memory_connection_is_autocommit(self):
        self.assertIsNone(self.conn.isolation_level)
        ensure_schema(self.conn, symbol='TEST')
        self.assertFalse(self.conn.in_transaction)

    def test_schema_can_be_created_twice(self):
        ensure_schema(self.conn, symbol='TEST')
        ensure_schema(self.conn, symbol='TEST')

    def test_schema_matches_spec(self):
        ensure_schema(self.conn, symbol='TEST')

        tick_sql = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='TEST_index_ticks'"
        ).fetchone()[0]
        self.assertIn('WITHOUT ROWID', tick_sql.upper())

        tick_columns = [
            (row[1], row[2], row[3], row[5])
            for row in self.conn.execute('PRAGMA table_info("TEST_index_ticks")')
        ]
        self.assertEqual(
            tick_columns, [('date', 'TEXT', 1, 1), ('price', 'REAL', 1, 0)]
        )

        columns = [
            row[1]
            for row in self.conn.execute('PRAGMA table_info("00-tick_sessions")')
        ]
        self.assertEqual(
            columns,
            [
                'symbol',
                'session_date',
                'session_open',
                'session_close',
                'status',
                'tick_count',
                'first_tick',
                'last_tick',
                'requests',
                'attempts',
                'last_error',
                'fetched_at',
            ],
        )


class TestTickUpsert(unittest.TestCase):
    def setUp(self):
        self.conn = connect_ticks(':memory:')
        ensure_schema(self.conn, symbol='TEST')

    def tearDown(self):
        self.conn.close()

    def test_upsert_is_idempotent_and_revises_price(self):
        first = upsert_ticks(
            self.conn, [('2026-11-18 09:30:01', 100.0)], symbol='TEST'
        )
        revised = upsert_ticks(
            self.conn, [('2026-11-18 09:30:01', 101.0)], symbol='TEST'
        )

        self.assertEqual(first, 1)
        self.assertEqual(revised, 1)
        rows = self.conn.execute(
            'SELECT date, price FROM "TEST_index_ticks"'
        ).fetchall()
        self.assertEqual(rows, [('2026-11-18 09:30:01', 101.0)])

    def test_upsert_returns_affected_row_count_for_a_page(self):
        rows = [
            ('2026-11-18 09:30:01', 100.0),
            ('2026-11-18 09:30:02', 100.5),
            ('2026-11-18 09:30:03', 101.0),
        ]

        self.assertEqual(upsert_ticks(self.conn, rows, symbol='TEST'), 3)
        self.assertEqual(upsert_ticks(self.conn, rows, symbol='TEST'), 3)
        count = self.conn.execute(
            'SELECT COUNT(*) FROM "TEST_index_ticks"'
        ).fetchone()[0]
        self.assertEqual(count, 3)

    def test_upsert_keeps_symbol_tables_separate(self):
        upsert_ticks(self.conn, [('2026-11-18 09:30:01', 100.0)], symbol='TEST')
        ensure_schema(self.conn, symbol='OTHER')
        upsert_ticks(self.conn, [('2026-11-18 09:30:01', 200.0)], symbol='OTHER')

        self.assertEqual(
            self.conn.execute('SELECT price FROM "TEST_index_ticks"').fetchone()[0],
            100.0,
        )
        self.assertEqual(
            self.conn.execute('SELECT price FROM "OTHER_index_ticks"').fetchone()[0],
            200.0,
        )


class TestSessionState(unittest.TestCase):
    def setUp(self):
        self.conn = connect_ticks(':memory:')
        ensure_schema(self.conn, symbol='TEST')

    def tearDown(self):
        self.conn.close()

    def test_ensure_sessions_inserts_pending_rows(self):
        ensure_sessions(self.conn, SESSIONS, symbol='TEST')

        rows = list_sessions(self.conn, symbol='TEST')
        self.assertEqual(
            [row['session_date'] for row in rows],
            ['2026-11-16', '2026-11-17', '2026-11-18', '2026-11-19'],
        )
        for row in rows:
            self.assertEqual(row['status'], 'pending')
            self.assertEqual(row['tick_count'], 0)
            self.assertEqual(row['requests'], 0)
            self.assertEqual(row['attempts'], 0)
            self.assertIsNone(row['first_tick'])
            self.assertIsNone(row['last_tick'])
            self.assertIsNone(row['last_error'])
            self.assertIsNone(row['fetched_at'])

    def test_ensure_sessions_does_not_reset_complete(self):
        ensure_sessions(self.conn, SESSIONS, symbol='TEST')
        save_session(
            self.conn,
            {
                **TICK_SESSION,
                'status': 'complete',
                'tick_count': 137,
                'first_tick': '2026-11-18 09:30:00',
                'last_tick': '2026-11-18 15:59:59',
                'requests': 4,
                'attempts': 1,
                'fetched_at': '2026-11-18 17:00:00',
            },
        )

        ensure_sessions(self.conn, SESSIONS, symbol='TEST')

        state = get_session(self.conn, 'TEST', '2026-11-18')
        self.assertEqual(state['status'], 'complete')
        self.assertEqual(state['tick_count'], 137)
        self.assertEqual(state['attempts'], 1)
        self.assertEqual(state['requests'], 4)
        self.assertEqual(state['first_tick'], '2026-11-18 09:30:00')
        self.assertEqual(state['last_tick'], '2026-11-18 15:59:59')
        self.assertEqual(state['fetched_at'], '2026-11-18 17:00:00')

    def test_get_session_returns_none_when_missing(self):
        self.assertIsNone(get_session(self.conn, 'TEST', '2026-11-18'))
        self.assertIsNone(get_session(self.conn, 'OTHER', '2026-11-18'))

    def test_save_session_applies_defaults(self):
        save_session(self.conn, dict(TICK_SESSION))

        state = get_session(self.conn, 'TEST', '2026-11-18')
        self.assertEqual(state['status'], 'pending')
        self.assertEqual(state['session_open'], '2026-11-18 09:30:00')
        self.assertEqual(state['session_close'], '2026-11-18 16:00:00')
        self.assertEqual(state['tick_count'], 0)
        self.assertEqual(state['requests'], 0)
        self.assertEqual(state['attempts'], 0)
        self.assertIsNone(state['first_tick'])
        self.assertIsNone(state['last_tick'])
        self.assertIsNone(state['last_error'])
        self.assertIsNone(state['fetched_at'])

    def test_save_session_updates_on_conflict(self):
        save_session(self.conn, dict(TICK_SESSION))
        save_session(
            self.conn,
            {
                **TICK_SESSION,
                'status': 'in_progress',
                'first_tick': '2026-11-18 09:30:00',
                'last_tick': '2026-11-18 10:00:00',
                'requests': 2,
                'last_error': 'connection lost',
            },
        )

        state = get_session(self.conn, 'TEST', '2026-11-18')
        self.assertEqual(state['status'], 'in_progress')
        self.assertEqual(state['first_tick'], '2026-11-18 09:30:00')
        self.assertEqual(state['last_tick'], '2026-11-18 10:00:00')
        self.assertEqual(state['requests'], 2)
        self.assertEqual(state['last_error'], 'connection lost')
        self.assertEqual(state['tick_count'], 0)

    def test_list_sessions_filters_status_dates_and_orders(self):
        ensure_sessions(self.conn, SESSIONS, symbol='TEST')
        statuses = {
            '2026-11-16': 'pending',
            '2026-11-17': 'complete',
            '2026-11-18': 'failed',
            '2026-11-19': 'complete',
        }
        for session in SESSIONS:
            _set_status(self.conn, session, statuses[session['session_date']])

        def dates(**filters):
            return [
                row['session_date']
                for row in list_sessions(self.conn, symbol='TEST', **filters)
            ]

        self.assertEqual(
            dates(), ['2026-11-16', '2026-11-17', '2026-11-18', '2026-11-19']
        )
        self.assertEqual(
            dates(statuses=['complete', 'failed']),
            ['2026-11-17', '2026-11-18', '2026-11-19'],
        )
        self.assertEqual(
            dates(start_date='2026-11-17'),
            ['2026-11-17', '2026-11-18', '2026-11-19'],
        )
        self.assertEqual(dates(end_date='2026-11-17'), ['2026-11-16', '2026-11-17'])
        self.assertEqual(
            dates(start_date='2026-11-17', end_date='2026-11-18'),
            ['2026-11-17', '2026-11-18'],
        )
        self.assertEqual(dates(statuses=[]), [])

    def test_list_sessions_defaults_to_config_symbol(self):
        ensure_schema(self.conn)
        ensure_sessions(self.conn, SESSIONS)

        self.assertEqual(len(list_sessions(self.conn)), 4)
        self.assertEqual(list_sessions(self.conn, symbol='TEST'), [])


class TestTickQueries(unittest.TestCase):
    def setUp(self):
        self.conn = connect_ticks(':memory:')
        ensure_schema(self.conn, symbol='TEST')
        self.rows = [
            ('2026-11-18 16:00:01', 103.0),
            ('2026-11-18 16:00:00', 102.0),
            ('2026-11-18 12:00:00', 101.0),
            ('2026-11-18 09:29:59', 99.0),
            ('2026-11-18 09:30:00', 100.0),
        ]
        upsert_ticks(self.conn, self.rows, symbol='TEST')

    def tearDown(self):
        self.conn.close()

    def test_session_metrics_use_inclusive_bounds(self):
        metrics = session_metrics(
            self.conn,
            'TEST',
            '2026-11-18 09:30:00',
            '2026-11-18 16:00:00',
        )

        self.assertEqual(
            metrics,
            {
                'tick_count': 3,
                'first_tick': '2026-11-18 09:30:00',
                'last_tick': '2026-11-18 16:00:00',
            },
        )

    def test_session_metrics_without_rows_return_zero_and_none(self):
        metrics = session_metrics(
            self.conn,
            'TEST',
            '2026-11-19 09:30:00',
            '2026-11-19 16:00:00',
        )

        self.assertEqual(
            metrics, {'tick_count': 0, 'first_tick': None, 'last_tick': None}
        )

    def test_get_ticks_uses_half_open_bounds_and_sorts(self):
        ticks = get_ticks(
            self.conn,
            '2026-11-18 09:30:00',
            '2026-11-18 16:00:00',
            symbol='TEST',
        )

        self.assertEqual(list(ticks.columns), ['date', 'price'])
        self.assertEqual(
            ticks['date'].tolist(),
            ['2026-11-18 09:30:00', '2026-11-18 12:00:00'],
        )
        self.assertEqual(ticks['price'].tolist(), [100.0, 101.0])

    def test_get_ticks_empty_window_keeps_columns(self):
        ticks = get_ticks(
            self.conn,
            '2026-11-19 09:30:00',
            '2026-11-19 16:00:00',
            symbol='TEST',
        )

        self.assertTrue(ticks.empty)
        self.assertEqual(list(ticks.columns), ['date', 'price'])


class TestConnectionModes(unittest.TestCase):
    def test_writer_uses_wal_and_busy_timeout(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = connect_ticks(os.path.join(tmp, 'ticks.db'))
            try:
                self.assertIsNone(conn.isolation_level)
                self.assertEqual(
                    conn.execute('PRAGMA journal_mode').fetchone()[0], 'wal'
                )
                self.assertEqual(
                    conn.execute('PRAGMA busy_timeout').fetchone()[0], 5000
                )
                self.assertEqual(conn.execute('PRAGMA synchronous').fetchone()[0], 1)
            finally:
                conn.close()

    def test_readonly_connection_rejects_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'ticks.db')
            writer = connect_ticks(path)
            try:
                ensure_schema(writer, symbol='TEST')
                upsert_ticks(
                    writer, [('2026-11-18 09:30:01', 100.0)], symbol='TEST'
                )
            finally:
                writer.close()

            reader = connect_ticks(path, readonly=True)
            try:
                self.assertEqual(
                    reader.execute('PRAGMA query_only').fetchone()[0], 1
                )
                rows = reader.execute(
                    'SELECT date, price FROM "TEST_index_ticks"'
                ).fetchall()
                self.assertEqual(rows, [('2026-11-18 09:30:01', 100.0)])

                with self.assertRaises(sqlite3.OperationalError):
                    reader.execute(
                        'INSERT INTO "TEST_index_ticks" (date, price)'
                        ' VALUES (?, ?)',
                        ('2026-11-18 09:30:02', 101.0),
                    )
            finally:
                reader.close()


if __name__ == '__main__':
    unittest.main()
