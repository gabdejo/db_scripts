# src/calendars/calendar_local.py
# ---------------------------------------------------------------
# Local (Peru) calendar.
# Used by SBS acquisition scheduler to determine previous
# business day for next-morning acquisition runs.
# Used by local macro/regulatory pipelines.
#
# Holidays sourced from Peru official calendar (BCRP / MEF).
# Update _LOCAL_HOLIDAYS set annually.
# ---------------------------------------------------------------

from datetime import date
from src.calendars.calendar_base import (
    is_business_day as _is_business_day,
    prev_business_day as _prev_business_day,
    next_business_day as _next_business_day,
    business_days_in_range as _business_days_in_range,
    first_business_day_of_month as _first_business_day_of_month,
)

# Peru public holidays - extend annually
_LOCAL_HOLIDAYS: set[date] = {
    # 2024
    date(2024, 1, 1),   # Año Nuevo
    date(2024, 3, 28),  # Jueves Santo
    date(2024, 3, 29),  # Viernes Santo
    date(2024, 5, 1),   # Día del Trabajo
    date(2024, 6, 7),   # Batalla de Arica
    date(2024, 6, 29),  # San Pedro y San Pablo
    date(2024, 7, 28),  # Fiestas Patrias
    date(2024, 7, 29),  # Fiestas Patrias
    date(2024, 8, 30),  # Santa Rosa de Lima
    date(2024, 10, 8),  # Combate de Angamos
    date(2024, 11, 1),  # Todos los Santos
    date(2024, 12, 8),  # Inmaculada Concepción
    date(2024, 12, 9),  # Batalla de Ayacucho
    date(2024, 12, 25), # Navidad
    # 2025
    date(2025, 1, 1),
    date(2025, 4, 17),
    date(2025, 4, 18),
    date(2025, 5, 1),
    date(2025, 6, 7),
    date(2025, 6, 29),
    date(2025, 7, 28),
    date(2025, 7, 29),
    date(2025, 8, 30),
    date(2025, 10, 8),
    date(2025, 11, 1),
    date(2025, 12, 8),
    date(2025, 12, 9),
    date(2025, 12, 25),
    # 2026
    date(2026, 1, 1),
    date(2026, 4, 2),
    date(2026, 4, 3),
    date(2026, 5, 1),
    date(2026, 6, 7),
    date(2026, 6, 29),
    date(2026, 7, 28),
    date(2026, 7, 29),
    date(2026, 8, 30),
    date(2026, 10, 8),
    date(2026, 11, 1),
    date(2026, 12, 8),
    date(2026, 12, 9),
    date(2026, 12, 25),
}


def _is_local_holiday(d: date) -> bool:
    return d in _LOCAL_HOLIDAYS


def is_business_day(d: date) -> bool:
    """Returns True if d is a local (Peru) business day."""
    return _is_business_day(d, calendar=_is_local_holiday)


def prev_business_day(d: date) -> date:
    """
    Returns the previous local business day before d.
    Key use case: called on Monday morning to get Friday's date
    for next-morning SBS acquisition runs.
    """
    return _prev_business_day(d, calendar=_is_local_holiday)


def next_business_day(d: date) -> date:
    """Returns the next local business day after d."""
    return _next_business_day(d, calendar=_is_local_holiday)


def business_days_in_range(start: date, end: date) -> list[date]:
    """Returns all local business days in [start, end] inclusive."""
    return _business_days_in_range(start, end, calendar=_is_local_holiday)


def first_business_day_of_month(year: int, month: int) -> date:
    """Returns the first local business day of the given month."""
    return _first_business_day_of_month(year, month, calendar=_is_local_holiday)
