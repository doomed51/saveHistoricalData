import sqlite3
import unittest

import interface_localDB as db


class TestPrehistoryTracking(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')

    def tearDown(self):
        self.conn.close()

    def test_three_zero_progress_runs_mark_only_that_interval_exhausted(self):
        for expected_failures in (1, 2):
            failures, exhausted = db.recordPrehistoryAttempt(
                self.conn, 'ABC', '1day', had_backward_progress=False
            )
            self.assertEqual(failures, expected_failures)
            self.assertFalse(exhausted)

        failures, exhausted = db.recordPrehistoryAttempt(
            self.conn, 'ABC', '1day', had_backward_progress=False
        )
        self.assertEqual(failures, 3)
        self.assertTrue(exhausted)
        self.assertEqual(db.getExhaustedPrehistoryRows(self.conn), {('ABC', '1day')})

        db.recordPrehistoryAttempt(self.conn, 'ABC', '1min', had_backward_progress=False)
        self.assertEqual(db.getExhaustedPrehistoryRows(self.conn), {('ABC', '1day')})

    def test_backward_progress_resets_failure_count_and_exhaustion(self):
        for _ in range(3):
            db.recordPrehistoryAttempt(self.conn, 'ABC', '1day', had_backward_progress=False)

        failures, exhausted = db.recordPrehistoryAttempt(
            self.conn, 'ABC', '1day', had_backward_progress=True
        )

        self.assertEqual(failures, 0)
        self.assertFalse(exhausted)
        self.assertEqual(db.getExhaustedPrehistoryRows(self.conn), set())

    def test_failure_count_persists_across_database_connections(self):
        self.conn.close()
        keeper = sqlite3.connect('file:prehistory-test?mode=memory&cache=shared', uri=True)
        first_conn = sqlite3.connect('file:prehistory-test?mode=memory&cache=shared', uri=True)
        db.recordPrehistoryAttempt(first_conn, 'ABC', '1day', had_backward_progress=False)
        first_conn.commit()
        first_conn.close()

        second_conn = sqlite3.connect('file:prehistory-test?mode=memory&cache=shared', uri=True)
        failures, exhausted = db.recordPrehistoryAttempt(
            second_conn, 'ABC', '1day', had_backward_progress=False
        )
        second_conn.close()
        keeper.close()

        self.assertEqual(failures, 2)
        self.assertFalse(exhausted)


if __name__ == '__main__':
    unittest.main()
