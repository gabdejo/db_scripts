# src/pipeline/positions/fms/forwards/extract.py
# ---------------------------------------------------------------
# Executes the FMS forwards query and returns a raw DataFrame with
# vendor-native (PascalCase) column names. All translation to
# staging shape happens in transform.py, not here.
#
# The query lives in ./queries/forwards.sql with two positional
# ? placeholders for start_date and end_date, both INT yyyymmdd.
# This module converts Python date objects to that format at the
# boundary.
#
# Range size validation lives here (not in the SQL): reject
# ranges wider than MAX_RANGE_DAYS unless force=True. Prevents
# accidentally pulling a year of forwards across all funds when
# someone means 'yesterday'.
# ---------------------------------------------------------------

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from src.vendors.fms import get_fms_connection

logger = logging.getLogger(__name__)

QUERY_PATH = Path(__file__).parent / "queries" / "forwards.sql"
MAX_RANGE_DAYS = 90


def extract(start_date: date, end_date: date, force: bool = False) -> pd.DataFrame:
    """
    Pull FMS forwards for the given inclusive date range.

    Returns a DataFrame with vendor-native (PascalCase) column
    names. Empty DataFrame if no rows in range.

    Raises ValueError if end_date < start_date, or if the range
    exceeds MAX_RANGE_DAYS and force=False.
    """
    if end_date < start_date:
        raise ValueError(f"end_date {end_date} < start_date {start_date}")

    span_days = (end_date - start_date).days
    if span_days > MAX_RANGE_DAYS and not force:
        raise ValueError(
            f"date range {span_days} days exceeds MAX_RANGE_DAYS={MAX_RANGE_DAYS}. "
            f"pass force=True to override."
        )

    sql = QUERY_PATH.read_text(encoding="utf-8")
    start_int = _date_to_yyyymmdd(start_date)
    end_int = _date_to_yyyymmdd(end_date)

    logger.info(
        f"executing FMS forwards query: {start_date} to {end_date} "
        f"({start_int} to {end_int})"
    )

    with get_fms_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (start_int, end_int))
        columns = [c[0] for c in cur.description] if cur.description else []
        rows = cur.fetchall()

    if not rows:
        logger.warning(f"FMS forwards query returned zero rows for {start_date}..{end_date}")
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame.from_records(rows, columns=columns)
    logger.info(f"FMS forwards extract: {len(df)} rows fetched")
    return df


def _date_to_yyyymmdd(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day
