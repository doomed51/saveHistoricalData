import unittest

import pandas as pd

from interface_ticks import (
    audit_exit_code,
    audit_sessions,
    connect_ticks,
    ensure_schema,
    save_session,
    session_max_gap_seconds,
    upsert_ticks,
)

SYMBOL = 'TEST'


def _save_session(conn, session_date, status='complete', open_time='09:30:00',
                  close_time='16:00:00'):
    save_session(
        conn,
        {
            'symbol': SYMBOL,
            'session_date': session_date,
            'session_open': '%s %s' % (session_date, open_time),
            'session_close': '%s %s' % (session_date, close_time),
            'status': status,
        },
    )


def _grid(session_date, step_seconds=300, start='09:30:00', end='16:00:00',
          price=100.0):
    """Return ``(date, price)`` rows every ``step_seconds`` in the window."""
    moment = pd.Timestamp('%s %s' % (session_date, start))
    last = pd.Timestamp('%s %s' % (session_date, end))
    rows = []
    while moment <= last:
        rows.append((moment.strftime('%Y-%m-%d %H:%M:%S'), price))
        moment += pd.Timedelta(seconds=step_seconds)
    return rows


def _drop(rows, *timestamps):
    dropped = set(timestamps)
    return [row for row in rows if row[0] not in dropped]


def _findings_for(findings, session_date, finding=None):
    subset = findings[findings['session_date'] == session_date]
    if finding is not None:
        subset = subset[subset['finding'] == finding]
    return subset


def _finding_names(findings, session_date):
    return set(_findings_for(findings, session_date)['finding'])


class AuditTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = connect_ticks(':memory:')
        ensure_schema(self.conn, symbol=SYMBOL)

    def tearDown(self):
        self.conn.close()

    def audit(self, start_date, end_date):
        return audit_sessions(self.conn, start_date, end_date, symbol=SYMBOL)

    def seed_complete(self, session_date, rows):
        _save_session(self.conn, session_date)
        upsert_ticks(self.conn, rows, symbol=SYMBOL)


class TestSessionMaxGap(AuditTestCase):
    def test_returns_zero_without_rows(self):
        self.assertEqual(
            session_max_gap_seconds(
                self.conn, SYMBOL, '2026-11-16 09:30:00', '2026-11-16 16:00:00'
            ),
            0.0,
        )

    def test_returns_zero_with_one_row(self):
        upsert_ticks(
            self.conn, [('2026-11-16 12:00:00', 100.0)], symbol=SYMBOL
        )
        self.assertEqual(
            session_max_gap_seconds(
                self.conn, SYMBOL, '2026-11-16 09:30:00', '2026-11-16 16:00:00'
            ),
            0.0,
        )

    def test_returns_largest_internal_interval(self):
        upsert_ticks(
            self.conn,
            [
                ('2026-11-16 09:30:00', 100.0),
                ('2026-11-16 09:35:00', 100.0),
                ('2026-11-16 09:45:00', 100.0),
                ('2026-11-16 09:46:00', 100.0),
            ],
            symbol=SYMBOL,
        )
        self.assertEqual(
            session_max_gap_seconds(
                self.conn, SYMBOL, '2026-11-16 09:30:00', '2026-11-16 16:00:00'
            ),
            600.0,
        )

    def test_ignores_rows_outside_the_window(self):
        upsert_ticks(
            self.conn,
            [
                ('2026-11-16 08:00:00', 100.0),
                ('2026-11-16 09:30:00', 100.0),
                ('2026-11-16 09:40:00', 100.0),
                ('2026-11-16 17:00:00', 100.0),
            ],
            symbol=SYMBOL,
        )
        self.assertEqual(
            session_max_gap_seconds(
                self.conn, SYMBOL, '2026-11-16 09:30:00', '2026-11-16 16:00:00'
            ),
            600.0,
        )


class TestAuditShape(AuditTestCase):
    def test_findings_have_the_documented_columns(self):
        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertEqual(
            list(findings.columns), ['session_date', 'severity', 'finding', 'detail']
        )

    def test_clean_complete_session_has_no_findings(self):
        self.seed_complete('2026-11-16', _grid('2026-11-16'))

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertTrue(findings.empty)
        self.assertEqual(
            list(findings.columns), ['session_date', 'severity', 'finding', 'detail']
        )


class TestStatusFindings(AuditTestCase):
    def test_missing_status_is_an_error(self):
        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertIn('missing_status', _finding_names(findings, '2026-11-16'))
        missing = _findings_for(findings, '2026-11-16', 'missing_status')
        self.assertEqual(missing['severity'].tolist(), ['error'])

    def test_not_started_is_an_error(self):
        _save_session(self.conn, '2026-11-16', status='pending')

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertEqual(
            _findings_for(findings, '2026-11-16', 'not_started')['severity'].tolist(),
            ['error'],
        )

    def test_in_progress_is_an_error(self):
        _save_session(self.conn, '2026-11-16', status='in_progress')

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertEqual(
            _findings_for(findings, '2026-11-16', 'in_progress')['severity'].tolist(),
            ['error'],
        )

    def test_partial_is_an_error(self):
        _save_session(self.conn, '2026-11-16', status='partial')

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertEqual(
            _findings_for(findings, '2026-11-16', 'partial')['severity'].tolist(),
            ['error'],
        )

    def test_failed_is_an_error(self):
        _save_session(self.conn, '2026-11-16', status='failed')

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertEqual(
            _findings_for(findings, '2026-11-16', 'failed')['severity'].tolist(),
            ['error'],
        )

    def test_retry_is_a_warning(self):
        _save_session(self.conn, '2026-11-16', status='retry')

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertEqual(
            _findings_for(findings, '2026-11-16', 'retry')['severity'].tolist(),
            ['warning'],
        )

    def test_unavailable_is_informational_with_expected_no_rows_info(self):
        _save_session(self.conn, '2026-11-16', status='unavailable')

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertEqual(
            _findings_for(findings, '2026-11-16', 'unavailable')['severity'].tolist(),
            ['info'],
        )
        self.assertEqual(
            _findings_for(
                findings, '2026-11-16', 'no_rows_unavailable'
            )['severity'].tolist(),
            ['info'],
        )
        self.assertEqual(findings['severity'].tolist(), ['info', 'info'])

    def test_complete_with_zero_rows_is_an_error(self):
        _save_session(self.conn, '2026-11-16', status='complete')

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertEqual(
            _findings_for(
                findings, '2026-11-16', 'no_rows_complete'
            )['severity'].tolist(),
            ['error'],
        )


class TestWindowFindings(AuditTestCase):
    def test_early_end_is_an_error(self):
        self.seed_complete('2026-11-16', _grid('2026-11-16', end='15:00:00'))

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertEqual(
            _findings_for(findings, '2026-11-16', 'early_end')['severity'].tolist(),
            ['error'],
        )

    def test_late_start_is_a_warning(self):
        self.seed_complete(
            '2026-11-16', _grid('2026-11-16', step_seconds=240, start='09:36:00')
        )

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertEqual(
            _findings_for(findings, '2026-11-16', 'late_start')['severity'].tolist(),
            ['warning'],
        )

    def test_ten_minute_gap_is_a_warning(self):
        rows = _drop(_grid('2026-11-16'), '2026-11-16 10:05:00')
        self.seed_complete('2026-11-16', rows)

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertEqual(
            _findings_for(findings, '2026-11-16', 'gap')['severity'].tolist(),
            ['warning'],
        )
        self.assertNotIn('large_gap', _finding_names(findings, '2026-11-16'))

    def test_thirty_minute_gap_is_only_a_large_gap_error(self):
        rows = _drop(
            _grid('2026-11-16'),
            '2026-11-16 11:05:00',
            '2026-11-16 11:10:00',
            '2026-11-16 11:15:00',
            '2026-11-16 11:20:00',
            '2026-11-16 11:25:00',
        )
        self.seed_complete('2026-11-16', rows)

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertEqual(
            _findings_for(findings, '2026-11-16', 'large_gap')['severity'].tolist(),
            ['error'],
        )
        self.assertNotIn('gap', _finding_names(findings, '2026-11-16'))

    def test_one_and_two_second_gaps_are_not_reported(self):
        rows = _grid('2026-11-16')
        rows.extend(
            [
                ('2026-11-16 10:00:01', 100.0),
                ('2026-11-16 10:00:02', 100.0),
            ]
        )
        self.seed_complete('2026-11-16', rows)

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertNotIn('gap', _finding_names(findings, '2026-11-16'))
        self.assertNotIn('large_gap', _finding_names(findings, '2026-11-16'))

    def test_invalid_prices_are_errors(self):
        self.seed_complete('2026-11-16', _grid('2026-11-16'))
        self.conn.execute(
            'INSERT INTO "TEST_index_ticks" (date, price) VALUES (?, ?)',
            ('2026-11-16 10:00:01', -1.0),
        )
        self.conn.execute(
            'INSERT INTO "TEST_index_ticks" (date, price) VALUES (?, ?)',
            ('2026-11-16 10:00:02', float('inf')),
        )

        findings = self.audit('2026-11-16', '2026-11-16')

        self.assertEqual(
            _findings_for(findings, '2026-11-16', 'invalid_price')['severity'].tolist(),
            ['error'],
        )

    def test_rows_outside_windows_and_on_a_weekend_are_unexpected(self):
        for session_date in (
            '2026-11-16',
            '2026-11-17',
            '2026-11-18',
            '2026-11-19',
            '2026-11-20',
        ):
            self.seed_complete(session_date, _grid(session_date))
        upsert_ticks(
            self.conn,
            [
                ('2026-11-18 08:00:00', 100.0),
                ('2026-11-21 12:00:00', 100.0),
            ],
            symbol=SYMBOL,
        )

        findings = self.audit('2026-11-16', '2026-11-21')

        unexpected = findings[findings['finding'] == 'unexpected_rows']
        self.assertEqual(set(unexpected['severity']), {'error'})
        self.assertEqual(
            set(unexpected['session_date']), {'2026-11-18', '2026-11-21'}
        )


class TestDensityFindings(AuditTestCase):
    def test_early_close_with_equal_density_is_not_flagged(self):
        for session_date in (
            '2026-11-16',
            '2026-11-17',
            '2026-11-18',
            '2026-11-19',
            '2026-11-20',
            '2026-11-23',
            '2026-11-24',
            '2026-11-25',
        ):
            self.seed_complete(session_date, _grid(session_date, step_seconds=600))
        self.seed_complete(
            '2026-11-27', _grid('2026-11-27', step_seconds=600, end='13:00:00')
        )

        findings = self.audit('2026-11-16', '2026-11-27')

        self.assertNotIn('low_density', _finding_names(findings, '2026-11-27'))

    def test_density_below_half_the_median_is_a_warning(self):
        for session_date in (
            '2027-02-01',
            '2027-02-02',
            '2027-02-03',
            '2027-02-04',
            '2027-02-05',
        ):
            self.seed_complete(session_date, _grid(session_date, step_seconds=600))
        self.seed_complete('2027-02-08', _grid('2027-02-08', step_seconds=1560))

        findings = self.audit('2027-02-01', '2027-02-08')

        self.assertEqual(
            _findings_for(
                findings, '2027-02-08', 'low_density'
            )['severity'].tolist(),
            ['warning'],
        )

    def test_density_is_skipped_with_fewer_than_five_baselines(self):
        for session_date in (
            '2027-03-01',
            '2027-03-02',
            '2027-03-03',
            '2027-03-04',
        ):
            self.seed_complete(session_date, _grid(session_date, step_seconds=600))
        self.seed_complete('2027-03-05', _grid('2027-03-05', step_seconds=1560))

        findings = self.audit('2027-03-01', '2027-03-05')

        self.assertNotIn('low_density', _finding_names(findings, '2027-03-05'))

    def test_partial_and_unavailable_sessions_do_not_form_baselines(self):
        for session_date in (
            '2027-04-05',
            '2027-04-06',
            '2027-04-07',
            '2027-04-08',
        ):
            self.seed_complete(session_date, _grid(session_date, step_seconds=600))
        _save_session(self.conn, '2027-04-09', status='partial')
        upsert_ticks(
            self.conn, _grid('2027-04-09', step_seconds=600), symbol=SYMBOL
        )
        _save_session(self.conn, '2027-04-12', status='unavailable')
        upsert_ticks(
            self.conn, _grid('2027-04-12', step_seconds=600), symbol=SYMBOL
        )
        self.seed_complete('2027-04-13', _grid('2027-04-13', step_seconds=1560))

        findings = self.audit('2027-04-05', '2027-04-13')

        self.assertNotIn('low_density', _finding_names(findings, '2027-04-13'))


class TestAuditExitCode(unittest.TestCase):
    def findings(self, *pairs):
        return pd.DataFrame(
            [
                {
                    'session_date': '2026-11-16',
                    'severity': severity,
                    'finding': finding,
                    'detail': '',
                }
                for severity, finding in pairs
            ],
            columns=['session_date', 'severity', 'finding', 'detail'],
        )

    def test_error_returns_one(self):
        self.assertEqual(audit_exit_code(self.findings(('error', 'failed'))), 1)

    def test_warning_alone_returns_zero(self):
        self.assertEqual(
            audit_exit_code(self.findings(('warning', 'retry'))), 0
        )

    def test_warning_alone_returns_one_when_strict(self):
        self.assertEqual(
            audit_exit_code(self.findings(('warning', 'retry')), strict=True), 1
        )

    def test_info_alone_returns_zero(self):
        self.assertEqual(
            audit_exit_code(
                self.findings(
                    ('info', 'unavailable'), ('info', 'no_rows_unavailable')
                )
            ),
            0,
        )

    def test_empty_findings_return_zero(self):
        self.assertEqual(audit_exit_code(self.findings()), 0)
