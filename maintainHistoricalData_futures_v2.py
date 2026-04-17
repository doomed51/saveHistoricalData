"""
Replacement futures historical-data maintainer.

Key behavior:
1) Uses the same local futures DB and IBKR interfaces.
2) Fetches history forward in time (oldest to newest).
3) Processes one contract at a time within each interval pass.
4) Persists progress in 00-progress_tracker so reruns resume without repeating API calls.
5) Verifies/fills contract gaps before moving to the next contract.
"""

import time
from datetime import datetime

import pandas as pd
from ib_insync import Future
from dateutil.relativedelta import relativedelta
from rich import print

import config
import interface_ibkr as ibkr
import interface_localDB as db

from maintainHistoricalData_futures import (
    _getWatchlist,
    _get_exchange_for_symbol,
    _setLookback,
    _addspace,
    find_next_gap_date_in_table,
)

DB_NAME_FUTURES = config.dbname_futures
TRACKED_INTERVALS = config.intervals
PROGRESS_TABLE = '00-progress_tracker'
GAP_DATES_TABLE = config.table_name_futures_pxhistory_metadata
LOOKAHEAD_MONTHS = 14
DEFAULT_SLEEP_SECONDS = 1
DEFAULT_DATE_IF_NO_GAPS = pd.to_datetime('1989-12-30')


def _parse_expiry_to_datetime(expiry):
    expiry_str = str(expiry)
    for fmt in ['%Y%m%d', '%Y%m']:
        try:
            return pd.to_datetime(expiry_str, format=fmt)
        except Exception:
            continue
    return pd.to_datetime(expiry_str, errors='coerce')


def _build_contract(symbol, expiry, currency='USD'):
    exchange = _get_exchange_for_symbol(symbol)
    if symbol == 'SI':
        return Future(
            symbol=symbol,
            lastTradeDateOrContractMonth=str(expiry),
            exchange=exchange,
            currency=currency,
            includeExpired=True,
            multiplier='5000',
        )
    return Future(
        symbol=symbol,
        lastTradeDateOrContractMonth=str(expiry),
        exchange=exchange,
        currency=currency,
        includeExpired=True,
    )


def _get_target_end_date(expiry):
    now = pd.Timestamp.now().floor('s')
    expiry_dt = _parse_expiry_to_datetime(expiry)
    if pd.isna(expiry_dt):
        return now
    if expiry_dt <= now:
        return expiry_dt
    return now


def _lookback_days(interval):
    lookback = _setLookback(interval)
    return int(str(lookback).split()[0])


def _list_contracts_for_symbol(ib, symbol, lookahead_months=LOOKAHEAD_MONTHS):
    exchange = _get_exchange_for_symbol(symbol)
    details = ibkr.getContractDetails(ib, symbol, type='future', exchange=exchange)
    details_df = ibkr.util.df(details)

    if details_df.empty:
        return pd.DataFrame(columns=['symbol', 'expiry', 'contract'])

    if symbol == 'VIX' and 'marketName' in details_df.columns:
        details_df = details_df.loc[details_df['marketName'] == 'VX'].reset_index(drop=True)

    details_df['exchange'] = details_df['contract'].apply(lambda c: c.exchange)
    details_df = details_df.loc[details_df['exchange'] == exchange].reset_index(drop=True)

    details_df['realExpirationDate'] = details_df['realExpirationDate'].astype(str)
    max_date = datetime.today() + relativedelta(months=lookahead_months)
    details_df = details_df.loc[
        pd.to_datetime(details_df['realExpirationDate'], format='%Y%m%d', errors='coerce')
        <= pd.to_datetime(max_date)
    ].reset_index(drop=True)

    if details_df.empty:
        return pd.DataFrame(columns=['symbol', 'expiry', 'contract'])

    details_df = details_df.sort_values('realExpirationDate').reset_index(drop=True)
    details_df['expiry'] = details_df['contract'].apply(lambda c: str(c.lastTradeDateOrContractMonth))
    details_df['symbol'] = symbol
    return details_df[['symbol', 'expiry', 'contract']]


def _list_all_target_contracts(ib):
    watchlist = _getWatchlist(config.watchlist_futures)
    all_contracts = []

    for symbol in watchlist['symbol'].tolist():
        one_symbol_contracts = _list_contracts_for_symbol(ib, symbol)
        if not one_symbol_contracts.empty:
            all_contracts.append(one_symbol_contracts)

    if not all_contracts:
        return pd.DataFrame(columns=['symbol', 'expiry', 'contract'])

    contracts = pd.concat(all_contracts, ignore_index=True)
    contracts['expiry_dt'] = contracts['expiry'].apply(_parse_expiry_to_datetime)
    contracts = contracts.sort_values(['expiry_dt', 'symbol']).reset_index(drop=True)
    return contracts[['symbol', 'expiry', 'contract']]


def _safe_get_table_bounds(conn, tablename):
    quoted = tablename.replace('"', '""')
    sql = 'SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM "%s"' % quoted
    try:
        row = pd.read_sql(sql, conn).iloc[0]
    except Exception:
        return None, None

    if pd.isna(row['max_date']):
        return None, None
    return pd.to_datetime(row['min_date'], errors='coerce'), pd.to_datetime(row['max_date'], errors='coerce')


def _upsert_progress_row(conn, symbol, expiry, interval, status, earliest_timestamp=None, last_fetched_end_date=None):
    tablename = '%s_%s_%s' % (symbol, expiry, interval.replace(' ', ''))
    now_ts = pd.Timestamp.now().floor('s')
    target_end_date = _get_target_end_date(expiry)

    existing = db.get_progress_tracker(conn, PROGRESS_TABLE)
    created_at = now_ts
    if not existing.empty and tablename in existing['tablename'].values:
        created_at = existing.loc[existing['tablename'] == tablename, 'created_at'].iloc[0]
        if pd.isna(created_at):
            created_at = now_ts

    payload = {
        'tablename': tablename,
        'symbol': symbol,
        'interval': interval,
        'expiry': str(expiry),
        'status': status,
        'earliest_timestamp': earliest_timestamp,
        'last_fetched_end_date': last_fetched_end_date,
        'target_end_date': target_end_date,
        'created_at': created_at,
        'updated_at': now_ts,
    }

    db.upsert_progress_tracker_row(conn, payload, PROGRESS_TABLE)


def _initialize_progress_tracker(conn, ib):
    db.create_progress_tracker_table(conn, PROGRESS_TABLE)

    contracts = _list_all_target_contracts(ib)
    if contracts.empty:
        return
    
    existing_tracker = db.get_progress_tracker(conn, PROGRESS_TABLE)

    for interval in TRACKED_INTERVALS:
        for row in contracts.itertuples(index=False):
            symbol = row.symbol
            expiry = str(row.expiry)
            tablename = '%s_%s_%s' % (symbol, expiry, interval.replace(' ', ''))



            if not existing_tracker.empty and tablename in existing_tracker['tablename'].values:
                continue

            _, max_date = _safe_get_table_bounds(conn, tablename)
            target_end_date = _get_target_end_date(expiry)

            if max_date is None or pd.isna(max_date):
                _upsert_progress_row(
                    conn,
                    symbol=symbol,
                    expiry=expiry,
                    interval=interval,
                    status='not_started',
                    earliest_timestamp=None,
                    last_fetched_end_date=None,
                )
            elif max_date >= target_end_date:
                _upsert_progress_row(
                    conn,
                    symbol=symbol,
                    expiry=expiry,
                    interval=interval,
                    status='gap_fill_in_progress',
                    earliest_timestamp=None,
                    last_fetched_end_date=max_date,
                )
            else:
                _upsert_progress_row(
                    conn,
                    symbol=symbol,
                    expiry=expiry,
                    interval=interval,
                    status='forward_fetch_in_progress',
                    earliest_timestamp=None,
                    last_fetched_end_date=max_date,
                )
            


def _get_work_queue(conn, interval):
    tracker = db.get_progress_tracker(conn, PROGRESS_TABLE)
    if tracker.empty:
        return tracker

    tracker = tracker.loc[tracker['interval'] == interval].copy()
    tracker = tracker.loc[tracker['status'] != 'complete'].copy()

    
    tracker['expiry_dt'] = tracker['expiry'].apply(_parse_expiry_to_datetime)
    tracker = tracker.sort_values(['expiry_dt', 'symbol']).reset_index(drop=True)

    return tracker


def _forward_fetch_contract(conn, ib, row):
    symbol = row['symbol']
    expiry = str(row['expiry'])
    interval = row['interval']
    tablename = row['tablename']

    contract = _build_contract(symbol, expiry)
    target_end_date = pd.to_datetime(row['target_end_date'], errors='coerce')
    if pd.isna(target_end_date):
        target_end_date = _get_target_end_date(expiry)

    earliest_ts = pd.to_datetime(row['earliest_timestamp'], errors='coerce')
    if pd.isna(earliest_ts):
        earliest_ts = pd.to_datetime(ibkr.getEarliestTimeStamp(ib, contract), errors='coerce')
        db.update_progress_tracker_fields(
            conn,
            tablename,
            {
                'status': 'forward_fetch_in_progress',
                'earliest_timestamp': earliest_ts,
                'updated_at': pd.Timestamp.now().floor('s'),
            },
            PROGRESS_TABLE,
        )

    if pd.isna(earliest_ts):
        return 0

    cursor = pd.to_datetime(row['last_fetched_end_date'], errors='coerce')
    if pd.isna(cursor):
        cursor = earliest_ts

    chunk_days = _lookback_days(interval)
    call_count = 0

    while cursor < target_end_date:
        next_end = min(cursor + pd.Timedelta(days=chunk_days), target_end_date)
        if next_end <= cursor:
            break

        bars = ibkr.getBars_futures(
            ib,
            contract,
            interval=interval,
            endDate=next_end,
            lookback='%s D' % chunk_days,
        )
        call_count += 1

        if bars is not None and not bars.empty:
            # Keep rows chronologically ordered before persisting.
            bars = bars.sort_values('date').reset_index(drop=True)
            if len(bars) > 1:
                bars = bars.iloc[:-1, :]

            if not bars.empty:
                bars['symbol'] = symbol
                bars['interval'] = interval.replace(' ', '')
                bars['lastTradeDate'] = expiry
                db.saveHistoryToDB(bars, conn, earliestTimestamp=earliest_ts, type='future')

        db.update_progress_tracker_fields(
            conn,
            tablename,
            {
                'status': 'forward_fetch_in_progress',
                'last_fetched_end_date': next_end,
                'updated_at': pd.Timestamp.now().floor('s'),
            },
            PROGRESS_TABLE,
        )

        cursor = next_end
        time.sleep(DEFAULT_SLEEP_SECONDS)

    db.update_progress_tracker_fields(
        conn,
        tablename,
        {
            'status': 'gap_fill_in_progress',
            'updated_at': pd.Timestamp.now().floor('s'),
        },
        PROGRESS_TABLE,
    )
    return call_count


def _verify_and_fill_gaps(conn, ib, row, date_of_last_gap_date_polled):
    symbol = row['symbol']
    expiry = str(row['expiry'])
    interval = row['interval']
    tablename = row['tablename']

    exchange = _get_exchange_for_symbol(symbol)
    contract = _build_contract(symbol, expiry)

    call_count = 0
    while True:
        gap_date = find_next_gap_date_in_table(
            conn,
            tablename,
            interval.replace(' ', ''),
            exchange,
            start_after_date=date_of_last_gap_date_polled,
        )

        if (pd.isna(gap_date) or 
            date_of_last_gap_date_polled == DEFAULT_DATE_IF_NO_GAPS or 
            (date_of_last_gap_date_polled is not None and date_of_last_gap_date_polled <= gap_date)
        ):
            db.update_progress_tracker_fields(
                conn,
                tablename,
                {
                    'status': 'complete',
                    'updated_at': pd.Timestamp.now().floor('s'),
                },
                PROGRESS_TABLE,
            )
            return call_count

        bars = ibkr.getBars_futures(
            ib,
            contract,
            interval=_addspace(interval.replace(' ', '')),
            endDate=pd.to_datetime(gap_date) + pd.to_timedelta(1, unit='D'),
            lookback='5 D',
        )
        call_count += 1

        if bars is not None and not bars.empty:
            bars = bars.sort_values('date').reset_index(drop=True)
            if len(bars) > 1:
                bars = bars.iloc[:-1, :]
            if not bars.empty:
                earliest_ts = pd.to_datetime(row['earliest_timestamp'], errors='coerce')
                if pd.isna(earliest_ts):
                    earliest_ts = pd.to_datetime(ibkr.getEarliestTimeStamp(ib, contract), errors='coerce')

                bars['symbol'] = symbol
                bars['interval'] = interval.replace(' ', '')
                bars['lastTradeDate'] = expiry
                db.saveHistoryToDB(bars, conn, earliestTimestamp=earliest_ts, type='future')

                ### update last gap date polled 
                date_of_last_gap_date_polled = bars['date'].min()
                db.update_gap_metadata(conn, tablename, date_of_last_gap_date_polled)
        else:
            # If we fail to retrieve bars, we should still update the last polled gap date to avoid infinite loops on unresolvable gaps.
            date_of_last_gap_date_polled = gap_date - pd.to_timedelta(4, unit='D')
            db.update_gap_metadata(conn, tablename, date_of_last_gap_date_polled)
                

        # time.sleep(DEFAULT_SLEEP_SECONDS)


def main():
    ib = ibkr.setupConnection()
    # ib.disconnect() 

    # if (hasattr(ib, "isConnected") and not ib.isConnected()):
    #     print("disconnected!")
    #     ibkr._exit_if_disconnected(ib, "initial connection")
    #     # ib.disconnect() 
    #     # SystemExit(1)
    # else:
    #     print("connected!")
    #     exit() 

    if ib is None:
        raise RuntimeError('Unable to connect to IBKR.')

    api_calls_since_refresh = 0

    with db.sqlite_connection(DB_NAME_FUTURES) as conn:
        _initialize_progress_tracker(conn, ib)
        
        # get gaps metadata 
        gap_metadata = db.getTable(conn, GAP_DATES_TABLE)
        gap_metadata['date_of_last_gap_date_polled'] = pd.to_datetime(gap_metadata['date_of_last_gap_date_polled'], errors='coerce')

        for interval in TRACKED_INTERVALS:
        # for interval in ['1 min']:
            print('%s: [yellow]Starting interval pass: %s[/yellow]' % (datetime.now().strftime('%H:%M:%S'), interval))
            while True:
                queue = _get_work_queue(conn, interval)
                if queue.empty:
                    break

                row = queue.iloc[0]
                print(
                    '%s: [yellow]Processing %s %s %s[/yellow]' % (
                        datetime.now().strftime('%H:%M:%S'),
                        row['symbol'],
                        row['expiry'],
                        row['interval'],
                    )
                )


                ################## FORWARD FETCH
                if row['status'] in ['not_started', 'forward_fetch_in_progress']:
                    print('%s: [yellow]============================== Starting forward fetch for %s %s %s ==============================[/yellow]' % (datetime.now().strftime('%H:%M:%S'), row['symbol'], row['expiry'], row['interval']))
                    api_calls_since_refresh += _forward_fetch_contract(conn, ib, row)

                refreshed_row = _get_work_queue(conn, interval)
                if not refreshed_row.empty:
                    refreshed_row = refreshed_row.loc[
                        refreshed_row['tablename'] == row['tablename']
                    ]
                

                ################## GAP FILL 
                if not refreshed_row.empty and refreshed_row.iloc[0]['status'] == 'gap_fill_in_progress':
                    print('%s: [yellow]============================== Starting gap fill for %s %s %s ==============================[/yellow]' % (datetime.now().strftime('%H:%M:%S'), refreshed_row.iloc[0]['symbol'], refreshed_row.iloc[0]['expiry'], refreshed_row.iloc[0]['interval']))

                    # print(gap_metadata)
                    # print(gap_metadata.loc[
                    #     (gap_metadata['tablename'] == refreshed_row.iloc[0]['tablename'])].iloc[0])
                    # exit() 

                    date_of_last_gap_date_polled = gap_metadata.loc[
                        (gap_metadata['tablename'] == refreshed_row.iloc[0]['tablename'])]['date_of_last_gap_date_polled'].iloc[0] if not gap_metadata.loc[
                        (gap_metadata['tablename'] == refreshed_row.iloc[0]['tablename'])]['date_of_last_gap_date_polled'].empty else None
                    # print(refreshed_row)
                    # print(date_of_last_gap_date_polled)
                    # exit() 

                    api_calls_since_refresh += _verify_and_fill_gaps(conn, ib, refreshed_row.iloc[0], date_of_last_gap_date_polled)

                max_calls = int(getattr(config, 'ibkr_max_consecutive_calls', 50))
                if api_calls_since_refresh >= max_calls:
                    print('%s: [yellow]Refreshing IBKR connection[/yellow]' % datetime.now().strftime('%H:%M:%S'))
                    ib = ibkr.refreshConnection(ib)
                    api_calls_since_refresh = 0

    print('%s: [green]Completed futures v2 maintenance run.[/green]' % datetime.now().strftime('%H:%M:%S'))


if __name__ == '__main__':
    main()
