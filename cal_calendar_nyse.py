# src/calendars/calendar_nyse.py
# ---------------------------------------------------------------
# NYSE calendar (XNYS) backed by exchange_calendars.
# Used by Bloomberg prices and fundamentals pipelines to skip
# non-trading days.
#
# exchange_calendars maintains the holiday schedule - no manual
# holiday list needed.
# ---------------------------------------------------------------

from datetime import date
from src.calendars.calendar_base import (
    is_business_day as _is_business_day,
    prev_business_day as _prev_business_day,
    next_business_day as _next_business_day,
    business_days_in_range as _business_days_in_range,
    first_business_day_of_month as _first_business_day_of_month,
    first_business_day_after_quarter_close as _first_bday_after_quarter_close,
)

_EXCHANGE = "XNYS"


def is_business_day(d: date) -> bool:
    """Returns True if d is a NYSE trading session."""
    return _is_business_day(d, _EXCHANGE)


def prev_business_day(d: date) -> date:
    """Returns the previous NYSE trading session before d."""
    return _prev_business_day(d, _EXCHANGE)


def next_business_day(d: date) -> date:
    """Returns the next NYSE trading session after d."""
    return _next_business_day(d, _EXCHANGE)


def business_days_in_range(start: date, end: date) -> list[date]:
    """Returns all NYSE trading sessions in [start, end] inclusive."""
    return _business_days_in_range(start, end, _EXCHANGE)


def first_business_day_of_month(year: int, month: int) -> date:
    """Returns the first NYSE trading session of the given month."""
    return _first_business_day_of_month(year, month, _EXCHANGE)


def first_business_day_after_quarter_close(
    year: int,
    quarter: int,
    lag_days: int = 0,
) -> date:
    """
    Returns the first NYSE trading session after quarter close.
    lag_days: additional calendar days to wait after quarter close
              before looking for a session (e.g. 5 to give Bloomberg
              time to publish).
    """
    return _first_bday_after_quarter_close(year, quarter, _EXCHANGE, lag_days)
