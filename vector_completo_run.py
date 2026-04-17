# src/pipeline/prices/sbs/vector_completo/run.py
import logging
from datetime import date, timedelta
from typing import Optional
import pandas as pd

from src.db.session import get_connection
from src.db.queries import get_active_series, get_last_price_date, update_series_run_metadata, update_series_status
from src.calendars.calendar_sbs import is_reporting_day
from src.pipeline.prices.sbs.registry import discover_and_register
from src.pipeline.prices.sbs.vector_completo.extract import read_raw, load_stg
from src.pipeline.prices.sbs.vector_completo.transform import transform
from src.pipeline.prices.sbs.vector_completo.loader import load_facts, load_dims

logger = logging.getLogger(__name__)
FILE_TYPE = "vector_completo"


def run(run_date: Optional[date] = None, series_override: Optional[list[dict]] = None) -> None:
    run_date = run_date or date.today()
    logger.info(f"=== prices/sbs/vector_completo | run_date={run_date} | mode={'backfill' if series_override else 'incremental'} ===")

    if series_override is None and not is_reporting_day(run_date):
        logger.info(f"{run_date} is not an SBS reporting day. Skipping.")
        return

    # Step 1: read raw file
    raw_df = read_raw(run_date)

    # Step 2: discover and register new instruments
    # Runs before _classify so newly registered series are immediately
    # visible as backfill-pending in the same run
    if not raw_df.empty:
        discover_and_register(raw_df, FILE_TYPE, run_date)

    # Step 3: resolve series
    if series_override is not None:
        securities = series_override
    else:
        with get_connection() as conn:
            securities = [
                s for s in get_active_series(conn, domain="prices", source="sbs", frequency="daily")
                if s["field"] == "PX_LAST"
            ]

    if not securities:
        logger.warning("No active vector_completo series. Exiting.")
        return

    # Step 4: classify
    securities = _classify(securities, run_date)
    to_process = [s for s in securities if s.get("start_date")]
    if not to_process:
        logger.info("All series up to date. Exiting.")
        return

    # Step 5: stage (raw_df already loaded above)
    if raw_df.empty:
        logger.warning("Raw file empty. Nothing to stage.")
        _mark_all(to_process, "partial")
        return

    with get_connection() as conn:
        load_stg(conn, raw_df, run_date)

    # Step 6: read stg and transform
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM stg_prices_sbs_vector_completo WHERE reference_date = ? ORDER BY loaded_at DESC",
            (run_date.isoformat(),),
        ).fetchall()
        stg_df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()

    facts_df, dims_df = transform(stg_df, to_process)
    if facts_df.empty:
        logger.warning("Transform returned no fact rows.")
        _mark_all(to_process, "partial")
        return

    # Step 7: load
    with get_connection() as conn:
        loaded, skipped = load_facts(conn, facts_df)
        load_dims(conn, dims_df)

    logger.info(f"Loaded {loaded} rows, skipped {skipped}.")
    _update_metadata(to_process, facts_df, "success")

    if series_override is not None:
        with get_connection() as conn:
            for sec in to_process:
                update_series_status(conn, sec["series_id"], "active")

    logger.info("=== prices/sbs/vector_completo complete ===")


def _classify(securities, run_date):
    classified = []
    with get_connection() as conn:
        for sec in securities:
            sec = dict(sec)
            last = get_last_price_date(conn, sec["series_id"])
            if last is None:
                sec["start_date"] = sec["default_start_date"] if isinstance(sec["default_start_date"], date) else date.fromisoformat(sec["default_start_date"])
                sec["is_new"] = True
            elif last >= run_date:
                sec["start_date"] = None
                sec["is_new"] = False
            else:
                sec["start_date"] = last + timedelta(days=1)
                sec["is_new"] = False
            classified.append(sec)
    return classified


def _update_metadata(securities, facts_df, run_status):
    with get_connection() as conn:
        for sec in securities:
            rows = facts_df[facts_df["series_id"] == sec["series_id"]]
            last_loaded = date.fromisoformat(rows["reference_date"].max()) if not rows.empty else None
            update_series_run_metadata(conn, sec["series_id"], run_status, last_loaded)


def _mark_all(securities, run_status):
    with get_connection() as conn:
        for sec in securities:
            update_series_run_metadata(conn, sec["series_id"], run_status)
