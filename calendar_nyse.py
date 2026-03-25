# src/calendars/calendar_nyse.py
# ---------------------------------------------------------------
# NYSE market calendar.
# Used by Bloomberg prices pipeline to skip non-trading days.
#
# Holidays sourced from NYSE official calendar.
# Update _NYSE_HOLIDAYS set annually or replace with
# pandas_market_calendars if available:
#   pip install pandas-market-calendars
#   import pandas_market_calendars as mcal
#   nyse = mcal.get_calendar("NYSE")
# ---------------------------------------------------------------

from datetime import date
from src.calendars.calendar_base import (
    is_business_day as _is_business_day,
    prev_business_day as _prev_business_day,
    next_business_day as _next_business_day,
    business_days_in_range as _business_days_in_range,
    first_business_day_of_month as _first_business_day_of_month,
)

# NYSE holidays - extend annually
# Format: date(YYYY, MM, DD)
_NYSE_HOLIDAYS: set[date] = {
    # 2024
    date(2024, 1, 1),   # New Year's Day
    date(2024, 1, 15),  # MLK Day
    date(2024, 2, 19),  # Presidents Day
    date(2024, 3, 29),  # Good Friday
    date(2024, 5, 27),  # Memorial Day
    date(2024, 6, 19),  # Juneteenth
    date(2024, 7, 4),   # Independence Day
    date(2024, 9, 2),   # Labor Day
    date(2024, 11, 28), # Thanksgiving
    date(2024, 12, 25), # Christmas
    # 2025
    date(2025, 1, 1),
    date(2025, 1, 20),
    date(2025, 2, 17),
    date(2025, 4, 18),
    date(2025, 5, 26),
    date(2025, 6, 19),
    date(2025, 7, 4),
    date(2025, 9, 1),
    date(2025, 11, 27),
    date(2025, 12, 25),
    # 2026
    date(2026, 1, 1),
    date(2026, 1, 19),
    date(2026, 2, 16),
    date(2026, 4, 3),
    date(2026, 5, 25),
    date(2026, 6, 19),
    date(2026, 7, 3),   # observed
    date(2026, 9, 7),
    date(2026, 11, 26),
    date(2026, 12, 25),
}


def _is_nyse_holiday(d: date) -> bool:
    return d in _NYSE_HOLIDAYS


def is_business_day(d: date) -> bool:
    """Returns True if d is a NYSE trading day."""
    return _is_business_day(d, calendar=_is_nyse_holiday)


def prev_business_day(d: date) -> date:
    """Returns the previous NYSE trading day before d."""
    return _prev_business_day(d, calendar=_is_nyse_holiday)


def next_business_day(d: date) -> date:
    """Returns the next NYSE trading day after d."""
    return _next_business_day(d, calendar=_is_nyse_holiday)


def business_days_in_range(start: date, end: date) -> list[date]:
    """Returns all NYSE trading days in [start, end] inclusive."""
    return _business_days_in_range(start, end, calendar=_is_nyse_holiday)


def first_business_day_of_month(year: int, month: int) -> date:
    """Returns the first NYSE trading day of the given month."""
    return _first_business_day_of_month(year, month, calendar=_is_nyse_holiday)
