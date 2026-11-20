import unittest
from unittest.mock import Mock, patch

import config
import interface_ibkr


class TestSpxTickConfig(unittest.TestCase):
    def test_tick_settings_match_spec(self):
        self.assertEqual(config.dbname_ticks, 'X:/data/ibkr/historicalData_ticks.db')
        self.assertEqual(config.tick_symbol, 'SPX')
        self.assertEqual(config.tick_exchange, 'CBOE')
        self.assertEqual(config.tick_currency, 'USD')
        self.assertEqual(config.tick_client_id, 11)
        self.assertEqual(config.tick_backfill_years, 3)
        self.assertEqual(config.tick_request_size, 1000)
        self.assertEqual(config.tick_max_requests_per_session, 40)
        self.assertEqual(config.tick_complete_tolerance_seconds, 60)
        self.assertEqual(config.tick_start_tolerance_seconds, 300)
        self.assertEqual(config.tick_max_attempts, 3)
        self.assertEqual(config.tick_connection_retries, 3)
        self.assertEqual(config.tick_gap_warning_seconds, 600)
        self.assertEqual(config.tick_gap_error_seconds, 1800)
        self.assertEqual(config.tick_density_baseline_sessions, 20)
        self.assertEqual(config.tick_density_min_baseline, 5)
        self.assertEqual(config.tick_density_warning_ratio, 0.5)

    def test_tick_request_is_paced(self):
        self.assertEqual(interface_ibkr._IBKR_MIN_INTERVAL_BY_REQUEST['reqHistoricalTicks'], 3)


class TestSpxTickIbkrPlumbing(unittest.TestCase):
    @patch('interface_ibkr.IB')
    def test_setup_connection_uses_client_id(self, IB):
        interface_ibkr.setupConnection(clientId=11)

        IB.return_value.connect.assert_called_once_with('127.0.0.1', 7496, clientId=11)

    @patch('interface_ibkr.IB')
    def test_setup_connection_defaults_to_client_id_10(self, IB):
        interface_ibkr.setupConnection()

        IB.return_value.connect.assert_called_once_with('127.0.0.1', 7496, clientId=10)

    @patch('interface_ibkr._paceIbkrRequest')
    def test_get_historical_ticks_request(self, paceIbkrRequest):
        contract = Mock(name='SPX')
        response = [object()]
        fake_ibkr = Mock()
        fake_ibkr.isConnected.return_value = True
        fake_ibkr.reqHistoricalTicks.return_value = response

        result = interface_ibkr.getHistoricalTicks(
            fake_ibkr, contract, '20261118-14:30:00'
        )

        paceIbkrRequest.assert_called_once_with(fake_ibkr, 'reqHistoricalTicks')
        fake_ibkr.reqHistoricalTicks.assert_called_once_with(
            contract=contract,
            startDateTime='20261118-14:30:00',
            endDateTime='',
            numberOfTicks=1000,
            whatToShow='TRADES',
            useRth=True,
            ignoreSize=False,
        )
        self.assertIs(result, response)

    @patch('interface_ibkr._paceIbkrRequest')
    def test_get_historical_ticks_propagates_request_errors(self, paceIbkrRequest):
        fake_ibkr = Mock()
        fake_ibkr.isConnected.return_value = True
        fake_ibkr.reqHistoricalTicks.side_effect = RuntimeError('request failed')

        with self.assertRaises(RuntimeError):
            interface_ibkr.getHistoricalTicks(
                fake_ibkr, Mock(name='SPX'), '20261118-14:30:00'
            )


if __name__ == '__main__':
    unittest.main()
