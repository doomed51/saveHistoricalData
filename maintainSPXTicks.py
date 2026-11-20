"""SPX historical tick maintenance.

Provides the pure session and timestamp conversions shared by tick ingestion.
Collection and maintenance jobs are added alongside these helpers.
"""

import datetime
import math
from zoneinfo import ZoneInfo

_EASTERN = ZoneInfo('America/New_York')
_UTC = datetime.timezone.utc
_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
_REQUEST_FORMAT = '%Y%m%d-%H:%M:%S'


def _as_datetime(value):
    if isinstance(value, datetime.datetime):
        return value
    return datetime.datetime.fromisoformat(str(value))


def _et_naive(value):
    """Return ``value`` as a naive datetime in Eastern time.

    Naive input is interpreted as Eastern time; aware input is converted.
    """
    moment = _as_datetime(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_EASTERN)
    return moment.astimezone(_EASTERN).replace(tzinfo=None)


def _valid_price(value):
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(price) or price <= 0:
        return None
    return price


def et_to_utc_start(value):
    """Return an ET-naive datetime as a ``YYYYMMDD-HH:MM:SS`` UTC string."""
    moment = _as_datetime(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_EASTERN)
    return moment.astimezone(_UTC).strftime(_REQUEST_FORMAT)


def page_to_rows(ticks, session_open, session_close):
    """Convert one tick page to ascending ``(date_text, price)`` rows.

    Tick times are UTC-aware and are stored as naive Eastern text. Non-finite
    or non-positive prices and timestamps outside the inclusive session window
    are dropped. Duplicate timestamps keep the last value in page order.
    """
    window_open = _et_naive(session_open)
    window_close = _et_naive(session_close)

    rows = {}
    for tick in ticks:
        price = _valid_price(tick.price)
        if price is None:
            continue
        moment = _et_naive(tick.time)
        if moment < window_open or moment > window_close:
            continue
        rows[moment] = price

    return [
        (moment.strftime(_TIMESTAMP_FORMAT), rows[moment])
        for moment in sorted(rows)
    ]
