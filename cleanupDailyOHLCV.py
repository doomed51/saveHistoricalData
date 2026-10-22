"""
Data cleanup module for 1-day OHLCV history.

PURPOSE
----------
Hourly-interval updates may have corrupted 1day OHLCV records in the local
stock/index DB. This module fully rebuilds the '1day' table for every tracked
symbol straight from IBKR, and provides a notebook-callable spot-check to
diff a single symbol's local data against a fresh IBKR pull without writing
anything to the DB.

USAGE
----------
Main update cycle (rebuilds every symbol's 1day history):
    python cleanupDailyOHLCV.py
    # or, from a script/notebook:
    from cleanupDailyOHLCV import run_daily_ohlcv_cleanup
    run_daily_ohlcv_cleanup()

Notebook spot-check (read-only, manages its own IBKR connection):
    from cleanupDailyOHLCV import spot_check_symbol
    diff = spot_check_symbol('SPY')
"""

import datetime

import pandas as pd
from ib_insync import Stock, Index, util as ib_util
from ib_insync import IB, Stock, Index, util
from rich import print

import config
import interface_ibkr as ib
import interface_localDB as db

_CHUNK_DAYS = 90  # backward lookback window per IBKR request while rebuilding history


def _build_contract(symbol):
    currency = config.currency_mapping.get(symbol, 'USD')
    exchange = config.exchange_mapping_stocks.get(symbol, 'SMART')
    if symbol in config._index:
        return Index(symbol, currency=currency, exchange=exchange)
    return Stock(symbol, currency=currency, exchange=exchange)


def _get_daily_symbols(conn):
    """
    Returns list of symbols with an existing '1day' table, high-priority symbols first.
    """
    records = db.getRecords(conn)
    if records.empty:
        return []

    daily = records.loc[records['interval'].str.replace(' ', '') == '1day']
    symbols = daily.loc[~daily['symbol'].isin(config.delisted_symbols), 'symbol'].unique().tolist()

    high_priority = [s for s in symbols if s in config.HIGH_PRIORITY_SYMBOLS]
    others = sorted(s for s in symbols if s not in config.HIGH_PRIORITY_SYMBOLS)
    return high_priority + others


def _rebuild_symbol_daily_history(ibkr, symbol, call_count, chunk_days=_CHUNK_DAYS):
    """
    Walks backward from now to the earliest available IBKR timestamp in
    chunk_days windows, returning the full concatenated, deduped history.

    Returns (history, earliestTimestamp, ibkr, call_count).
    """
    contract = _build_contract(symbol)
    earliestTimestamp = ib.getEarliestTimeStamp(ibkr, contract)
    if not earliestTimestamp:
        return pd.DataFrame(), None, ibkr, call_count

    chunks = []
    cursor = ''  # '' -> IBKR interprets end date as "now"
    empty_retries = 0
    max_empty_retries = 3

    while True:
        if call_count and call_count % config.ibkr_max_consecutive_calls == 0:
            ibkr = ib.refreshConnection(ibkr)

        bars = ib.getBars(ibkr, symbol=symbol, lookback='%s D' % chunk_days, interval='1 day', endDate=cursor)
        call_count += 1

        if bars.empty:
            empty_retries += 1
            if empty_retries > max_empty_retries:
                break
            continue

        empty_retries = 0
        chunks.append(bars)

        chunk_min_date = pd.to_datetime(bars['date'].min())
        # guard against non-progressing windows that would otherwise loop forever
        if cursor and (chunk_min_date >= cursor):
            break
        if chunk_min_date <= earliestTimestamp:
            break
        cursor = chunk_min_date

    if not chunks:
        return pd.DataFrame(), earliestTimestamp, ibkr, call_count

    history = pd.concat(chunks, ignore_index=True)
    history.drop_duplicates(subset='date', keep='first', inplace=True)
    history.sort_values(by='date', inplace=True)
    history.reset_index(drop=True, inplace=True)
    history['interval'] = '1day'
    history['symbol'] = symbol

    return history, earliestTimestamp, ibkr, call_count


def _replace_symbol_table(conn, symbol, history, earliestTimestamp):
    """
    Renames any existing '<symbol>_<type>_1day' table to a '_corrupt_<timestamp>'
    backup (instead of dropping it), then writes the fresh history in its place.
    """
    type_ = 'index' if symbol in config._index else 'stock'
    tableName = '%s_%s_1day' % (symbol, type_)

    existing = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", conn, params=(tableName,)
    )
    if not existing.empty:
        backupName = '%s_corrupt_%s' % (tableName, datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
        conn.execute('ALTER TABLE "%s" RENAME TO "%s"' % (tableName, backupName))
        print('%s: [yellow]Backed up existing table to %s[/yellow]' % (
            datetime.datetime.now().strftime('%H:%M:%S'), backupName))

    db.saveHistoryToDB(history, conn, earliestTimestamp=earliestTimestamp)


def run_daily_ohlcv_cleanup(symbols=None):
    """
    Main update cycle: fully rebuilds the '1day' OHLCV table for every tracked
    symbol (or the given <symbols> list) from IBKR.

    Returns dict of {'rebuilt': [...], 'skipped': [...], 'failed': [...]}
    """
    print('%s: [green]Starting daily OHLCV cleanup...[/green]' % datetime.datetime.now().strftime('%H:%M:%S'))
    ib_util.startLoop()  # patches asyncio loop so ib_insync also works from Jupyter
    ibkr = ib.setupConnection()
    call_count = 0
    rebuilt, skipped, failed = [], [], []

    try:
        with db.sqlite_connection(config.dbname_stock) as conn:
            target_symbols = symbols if symbols else _get_daily_symbols(conn)

            for i, symbol in enumerate(target_symbols, start=1):
                print('%s: (%s/%s) Rebuilding 1day history for %s' % (
                    datetime.datetime.now().strftime('%H:%M:%S'), i, len(target_symbols), symbol))
                try:
                    history, earliestTimestamp, ibkr, call_count = _rebuild_symbol_daily_history(
                        ibkr, symbol, call_count)

                    if history.empty:
                        print('%s: [yellow]No history returned for %s, skipping.[/yellow]' % (
                            datetime.datetime.now().strftime('%H:%M:%S'), symbol))
                        skipped.append(symbol)
                        continue

                    _replace_symbol_table(conn, symbol, history, earliestTimestamp)
                    print('%s: [green]%s rebuilt! %s - %s (%s rows)[/green]\n' % (
                        datetime.datetime.now().strftime('%H:%M:%S'), symbol,
                        history['date'].min(), history['date'].max(), len(history)))
                    rebuilt.append(symbol)
                except Exception as e:
                    print('%s: [red]Failed to rebuild %s: %s[/red]\n' % (
                        datetime.datetime.now().strftime('%H:%M:%S'), symbol, e))
                    failed.append(symbol)
    finally:
        if ibkr:
            ibkr.disconnect()

    print('%s: [green]Cleanup complete. Rebuilt: %s | Skipped: %s | Failed: %s[/green]' % (
        datetime.datetime.now().strftime('%H:%M:%S'), len(rebuilt), len(skipped), len(failed)))
    if failed:
        print('[red]Failed symbols: %s[/red]' % failed)

    return {'rebuilt': rebuilt, 'skipped': skipped, 'failed': failed}


def spot_check_symbol(symbol, lookback_days=90, ibkr=None, tolerance=0.005):
    """
    Notebook-callable, read-only check: pulls fresh '1day' bars from IBKR for
    <symbol> and diffs them against the local DB over the same date range.

    If <ibkr> is not provided, a connection is created and disconnected
    automatically for this single call; pass an existing connection to reuse it.

    Returns a DataFrame with one row per date and a 'status' column:
    'match' | 'mismatch' | 'missing_locally' | 'missing_in_ib'.
    """
    owns_connection = ibkr is None
    if owns_connection:
        ib_util.startLoop()  # patches asyncio loop so ib_insync also works from Jupyter
        ibkr = ib.setupConnection()

    try:
        freshBars = ib.getBars(ibkr, symbol=symbol, lookback='%s D' % lookback_days, interval='1 day')
    finally:
        if owns_connection:
            ibkr.disconnect()

    if freshBars.empty:
        print('%s: [yellow]No data returned from IBKR for %s[/yellow]' % (
            datetime.datetime.now().strftime('%H:%M:%S'), symbol))
        return pd.DataFrame()

    freshBars = freshBars[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
    freshBars['date'] = pd.to_datetime(freshBars['date']).dt.normalize()

    type_ = 'index' if symbol in config._index else 'stock'
    tableName = '%s_%s_1day' % (symbol, type_)

    with db.sqlite_connection(config.dbname_stock) as conn:
        try:
            localBars = pd.read_sql(
                'SELECT date, open, high, low, close, volume FROM "%s" WHERE date >= ?' % tableName,
                conn, params=(freshBars['date'].min().strftime('%Y-%m-%d'),)
            )
        except Exception:
            print('%s: [red]No local table found: %s[/red]' % (
                datetime.datetime.now().strftime('%H:%M:%S'), tableName))
            return pd.DataFrame()

    localBars['date'] = pd.to_datetime(localBars['date']).dt.normalize()

    comparison = pd.merge(
        freshBars, localBars, on='date', how='outer', suffixes=('_ib', '_local'), indicator=True
    )

    def _classify(row):
        if row['_merge'] == 'left_only':
            return 'missing_locally'
        if row['_merge'] == 'right_only':
            return 'missing_in_ib'
        for col in ['open', 'high', 'low', 'close', 'volume']:
            ib_val, local_val = row['%s_ib' % col], row['%s_local' % col]
            if pd.isna(ib_val) or pd.isna(local_val):
                return 'mismatch'
            if abs(ib_val - local_val) > tolerance * max(abs(ib_val), 1):
                return 'mismatch'
        return 'match'

    comparison['status'] = comparison.apply(_classify, axis=1)
    comparison.drop(columns=['_merge'], inplace=True)
    comparison.sort_values(by='date', inplace=True)
    comparison.reset_index(drop=True, inplace=True)

    summary = comparison['status'].value_counts().to_dict()
    print('%s: [cyan]Spot check for %s (%s day lookback)[/cyan]' % (
        datetime.datetime.now().strftime('%H:%M:%S'), symbol, lookback_days))
    print(' %s' % summary)

    if (comparison['status'] == 'match').all():
        print('[green] No discrepancies found![/green]')
    else:
        print('[yellow] %s discrepant date(s) found[/yellow]' % int((comparison['status'] != 'match').sum()))

    return comparison

def spot_check_symbol_directib(
    symbol,
    lookback_days=90,
    ibkr=None,
    tolerance=0.005,
    host='127.0.0.1',
    port=7496,
    client_id=99,
    stock_exchange='SMART',
    index_exchange='CBOE',
    currency='USD',
):
    """
    Notebook-callable, read-only check.

    Pulls fresh daily bars directly from IBKR using ib_insync and compares
    them against the local SQLite database over the same date range.

    If `ibkr` is not provided, a temporary IB connection is created and
    disconnected automatically.

    An existing connected ib_insync.IB instance can be passed via `ibkr`
    to reuse a connection.

    Returns
    -------
    pd.DataFrame
        One row per date with status:

        'match'
        'mismatch'
        'missing_locally'
        'missing_in_ib'
    """

    owns_connection = ibkr is None

    if owns_connection:
        # Required for ib_insync inside Jupyter / RStudio notebooks
        util.startLoop()

        ibkr = IB()
        ibkr.connect(
            host=host,
            port=port,
            clientId=client_id,
            readonly=True,
        )

    try:
        # -------------------------------------------------------------
        # Build IB contract
        # -------------------------------------------------------------
        if symbol in config._index:
            contract = Index(
                symbol=symbol,
                exchange=index_exchange,
                currency=currency,
            )
        else:
            contract = Stock(
                symbol=symbol,
                exchange=stock_exchange,
                currency=currency,
            )

        # Resolve the contract before requesting historical data
        qualified = ibkr.qualifyContracts(contract)

        if not qualified:
            print(
                '%s: Could not qualify IBKR contract for %s'
                % (
                    datetime.datetime.now().strftime('%H:%M:%S'),
                    symbol,
                )
            )
            return pd.DataFrame()

        contract = qualified[0]

        # -------------------------------------------------------------
        # Retrieve historical daily bars directly from IBKR
        # -------------------------------------------------------------
        bars = ibkr.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr='%s D' % lookback_days if lookback_days <= 365 else '%s Y' % (lookback_days // 365 + 1),
            barSizeSetting='1 day',
            whatToShow='TRADES',
            useRTH=False,
            formatDate=1,
            keepUpToDate=False,
        )

        if not bars:
            print(
                '%s: No data returned from IBKR for %s'
                % (
                    datetime.datetime.now().strftime('%H:%M:%S'),
                    symbol,
                )
            )
            return pd.DataFrame()

        freshBars = util.df(bars)

    finally:
        if owns_connection and ibkr.isConnected():
            ibkr.disconnect()

    # -----------------------------------------------------------------
    # Clean IBKR data
    # -----------------------------------------------------------------
    freshBars = freshBars[
        ['date', 'open', 'high', 'low', 'close', 'volume']
    ]

    freshBars['date'] = (
        pd.to_datetime(freshBars['date'])
        .dt.normalize()
    )

    # -----------------------------------------------------------------
    # Load local database data
    # -----------------------------------------------------------------
    type_ = 'index' if symbol in config._index else 'stock'
    tableName = '%s_%s_1day' % (symbol, type_)

    with db.sqlite_connection(config.dbname_stock) as conn:
        try:
            localBars = pd.read_sql(
                '''
                SELECT
                    date,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM "%s"
                WHERE date >= ?
                ''' % tableName,
                conn,
                params=(
                    freshBars['date'].min().strftime('%Y-%m-%d'),
                ),
            )

        except Exception:
            print(
                '%s: No local table found: %s'
                % (
                    datetime.datetime.now().strftime('%H:%M:%S'),
                    tableName,
                )
            )
            return pd.DataFrame()

    localBars['date'] = (
        pd.to_datetime(localBars['date'])
        .dt.normalize()
    )

    # -----------------------------------------------------------------
    # Compare IBKR vs local data
    # -----------------------------------------------------------------
    comparison = pd.merge(
        freshBars,
        localBars,
        on='date',
        how='outer',
        suffixes=('_ib', '_local'),
        indicator=True,
    )

    def _classify(row):

        if row['_merge'] == 'left_only':
            return 'missing_locally'

        if row['_merge'] == 'right_only':
            return 'missing_in_ib'

        for col in ['open', 'high', 'low', 'close', 'volume']:

            ib_val = row['%s_ib' % col]
            local_val = row['%s_local' % col]

            if pd.isna(ib_val) or pd.isna(local_val):
                return 'mismatch'

            if (
                abs(ib_val - local_val)
                > tolerance * max(abs(ib_val), 1)
            ):
                return 'mismatch'

        return 'match'

    comparison['status'] = comparison.apply(
        _classify,
        axis=1,
    )

    comparison.drop(
        columns=['_merge'],
        inplace=True,
    )

    comparison.sort_values(
        by='date',
        inplace=True,
    )

    comparison.reset_index(
        drop=True,
        inplace=True,
    )

    # -----------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------
    summary = (
        comparison['status']
        .value_counts()
        .to_dict()
    )

    print(
        '%s: Spot check for %s (%s day lookback)'
        % (
            datetime.datetime.now().strftime('%H:%M:%S'),
            symbol,
            lookback_days,
        )
    )

    print(' %s' % summary)

    if (comparison['status'] == 'match').all():
        print('No discrepancies found!')
    else:
        n_discrepant = int(
            (comparison['status'] != 'match').sum()
        )

        print(
            '%s discrepant date(s) found'
            % n_discrepant
        )

    return comparison


if __name__ == '__main__':
    run_daily_ohlcv_cleanup()
