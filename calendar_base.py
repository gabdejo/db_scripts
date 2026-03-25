# src/calendars/calendar_base.py
# ---------------------------------------------------------------
# Base calendar utilities shared across all calendar modules.
# Pure weekend-only logic with no holiday awareness.
# Use calendar-specific modules (calendar_nyse, calendar_local)
# for holiday-aware checks.
# ---------------------------------------------------------------

from datetime import date, timedelta


def is_weekend(d: date) -> bool:
    """Returns True if d is Saturday or Sunday."""
    return d.weekday() >= 5


def prev_business_day(d: date, calendar=None) -> date:
    """
    Returns the most recent business day strictly before d.
    Monday -> Friday (skipping weekend).
    If calendar is provided, also skips holidays in that calendar.

    calendar: optional callable is_holiday(date) -> bool
    """
    candidate = d - timedelta(days=1)
    while candidate.weekday() >= 5 or (calendar and calendar(candidate)):
        candidate -= timedelta(days=1)
    return candidate


def next_business_day(d: date, calendar=None) -> date:
    """
    Returns the next business day strictly after d.
    Friday -> Monday (skipping weekend).
    If calendar is provided, also skips holidays in that calendar.
    """
    candidate = d + timedelta(days=1)
    while candidate.weekday() >= 5 or (calendar and calendar(candidate)):
        candidate += timedelta(days=1)
    return candidate


def is_business_day(d: date, calendar=None) -> bool:
    """
    Returns True if d is not a weekend and not a holiday.
    If calendar is provided, also checks holidays.
    """
    if d.weekday() >= 5:
        return False
    if calendar and calendar(d):
        return False
    return True


def business_days_in_range(
    start: date,
    end: date,
    calendar=None,
) -> list[date]:
    """
    Returns all business days in [start, end] inclusive.
    If calendar is provided, excludes holidays.
    """
    days = []
    current = start
    while current <= end:
        if is_business_day(current, calendar=calendar):
            days.append(current)
        current += timedelta(days=1)
    return days


def first_business_day_of_month(year: int, month: int, calendar=None) -> date:
    """Returns the first business day of the given month."""
    d = date(year, month, 1)
    while not is_business_day(d, calendar=calendar):
        d += timedelta(days=1)
    return d


def first_business_day_after_quarter_close(
    year: int,
    quarter: int,
    lag_days: int = 0,
    calendar=None,
) -> date:
    """
    Returns the first business day after quarter close + lag_days.
    Quarter close months: Q1=Mar, Q2=Jun, Q3=Sep, Q4=Dec.
    """
    import calendar as cal
    quarter_end_month = quarter * 3
    last_day = cal.monthrange(year, quarter_end_month)[1]
    quarter_close = date(year, quarter_end_month, last_day)
    candidate = quarter_close + timedelta(days=1 + lag_days)
    while not is_business_day(candidate, calendar=calendar):
        candidate += timedelta(days=1)
    return candidate
