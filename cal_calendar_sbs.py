# src/calendars/calendar_sbs.py
# ---------------------------------------------------------------
# SBS reporting day calendar.
# The SBS web portal publishes data for a given day if and only
# if NYSE OR Lima Stock Exchange (XLIM) was open that day.
# A day is a reporting day when at least one exchange is open.
#
# Used by:
#   - scripts/scheduler_scraper.py  to determine prev_reporting_day
#   - src/scrapers/sbs.py acquire_bulk to filter eligible dates
#   - src/pipeline/macro/sbs/run.py to skip non-reporting days
# ---------------------------------------------------------------

from datetime import date

from src.calendars.calendar_nyse import is_business_day as nyse_open
from src.calendars.calendar_xlim import is_business_day as xlim_open
from src.calendars.calendar_base import _get_calendar

import pandas as pd


def is_reporting_day(d: date) -> bool:
    """
    Returns True if the SBS portal reports data for d.
    True when NYSE or BVL (or both) were open.
    False only when both exchanges were closed.
    """
    return nyse_open(d) or xlim_open(d)


def prev_reporting_day(d: date) -> date:
    """
    Returns the most recent SBS reporting day strictly before d.
    Used by scheduler_scraper.py to determine which date to
    acquire on next-morning runs (Monday -> Friday, etc.).
    """
    from datetime import timedelta
    candidate = d - timedelta(days=1)
    while not is_reporting_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def next_reporting_day(d: date) -> date:
    """Returns the next SBS reporting day strictly after d."""
    from datetime import timedelta
    candidate = d + timedelta(days=1)
    while not is_reporting_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def reporting_days_in_range(start: date, end: date) -> list[date]:
    """
    Returns all SBS reporting days in [start, end] inclusive.
    Used by acquire_bulk to build the list of eligible dates
    to diff against data/raw/.
    """
    # Union of NYSE and XLIM sessions is more efficient than
    # iterating day by day for large ranges
    nyse_cal = _get_calendar("XNYS")
    xlim_cal = _get_calendar("XLIM")

    nyse_sessions = set(
        s.date()
        for s in nyse_cal.sessions_in_range(
            pd.Timestamp(start), pd.Timestamp(end)
        )
    )
    xlim_sessions = set(
        s.date()
        for s in xlim_cal.sessions_in_range(
            pd.Timestamp(start), pd.Timestamp(end)
        )
    )

    return sorted(nyse_sessions | xlim_sessions)
