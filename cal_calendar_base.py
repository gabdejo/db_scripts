# src/calendars/calendar_base.py
# ---------------------------------------------------------------
# Base calendar utilities backed by exchange_calendars.
# Each exchange-specific module wraps these with its own calendar
# instance. Do not import this module directly in pipelines -
# use calendar_nyse, calendar_xlim, or calendar_sbs instead.
# ---------------------------------------------------------------

from datetime import date
from functools import lru_cache

import pandas as pd
import exchange_calendars as xcals


@lru_cache(maxsize=8)
def _get_calendar(exchange_code: str):
    """
    Loads and caches an exchange_calendars calendar instance.
    Cached to avoid re-loading on every call.
    exchange_code: e.g. "XNYS", "XLIM"
    """
    return xcals.get_calendar(exchange_code)


def is_business_day(d: date, exchange_code: str) -> bool:
    """Returns True if d is a trading session for the given exchange."""
    cal = _get_calendar(exchange_code)
    return cal.is_session(pd.Timestamp(d))


def prev_business_day(d: date, exchange_code: str) -> date:
    """
    Returns the most recent trading session strictly before d.
    Monday -> Friday (or last trading day accounting for holidays).
    """
    cal = _get_calendar(exchange_code)
    return cal.previous_session(pd.Timestamp(d)).date()


def next_business_day(d: date, exchange_code: str) -> date:
    """Returns the next trading session strictly after d."""
    cal = _get_calendar(exchange_code)
    return cal.next_session(pd.Timestamp(d)).date()


def business_days_in_range(
    start: date,
    end: date,
    exchange_code: str,
) -> list[date]:
    """Returns all trading sessions in [start, end] inclusive."""
    cal = _get_calendar(exchange_code)
    sessions = cal.sessions_in_range(
        pd.Timestamp(start),
        pd.Timestamp(end),
    )
    return [s.date() for s in sessions]


def first_business_day_of_month(
    year: int,
    month: int,
    exchange_code: str,
) -> date:
    """Returns the first trading session of the given month."""
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    cal = _get_calendar(exchange_code)
    sessions = cal.sessions_in_range(
        pd.Timestamp(date(year, month, 1)),
        pd.Timestamp(date(year, month, last_day)),
    )
    if len(sessions) == 0:
        raise ValueError(
            f"No trading sessions found in {year}-{month:02d} "
            f"for exchange {exchange_code}."
        )
    return sessions[0].date()


def first_business_day_after_quarter_close(
    year: int,
    quarter: int,
    exchange_code: str,
    lag_days: int = 0,
) -> date:
    """
    Returns the first trading session after quarter close + lag_days.
    Quarter close months: Q1=Mar, Q2=Jun, Q3=Sep, Q4=Dec.
    """
    import calendar
    quarter_end_month = quarter * 3
    last_day = calendar.monthrange(year, quarter_end_month)[1]
    quarter_close = date(year, quarter_end_month, last_day)

    from datetime import timedelta
    candidate_start = quarter_close + timedelta(days=1 + lag_days)

    cal = _get_calendar(exchange_code)
    # Find next session from candidate_start
    return cal.next_session(pd.Timestamp(candidate_start)).date()
