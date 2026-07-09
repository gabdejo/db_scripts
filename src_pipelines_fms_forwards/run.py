# src/pipeline/positions/fms/forwards/run.py
# ---------------------------------------------------------------
# Callable run wrapper for the FMS forwards pipeline.
#
# Two modes, both exposed as callables. The entry-point script
# (scripts/run_fms_forwards.py) is the argparse shell that calls
# these; nothing here does argparse or __main__.
#
# run_full(start_date, end_date, force)
#   1. Assert FMS ingestion enabled on this machine
#   2. Build batch_id from timestamp
#   3. Extract from FMS for start_date..end_date
#   4. Transform to staging shape
#   5. Load staging (idempotent upsert)
#   6. Load dim_portfolio (fms slice) into memory
#   7. Transform to fact shape (portfolio resolution)
#   8. Load fact (idempotent upsert)
#   9. Flip any backfill-pending portfolios to active
#
# run_from_stg(batch_id)
#   Bypasses FMS entirely. Reads existing staging rows by
#   batch_id, transforms to fact shape, upserts fact. Used to
#   rebuild fact after fixing a portfolio registration or MTM
#   source without re-hitting FMS.
# ---------------------------------------------------------------

import logging
from datetime import date, datetime

import pandas as pd

from src.configs.machine_config import assert_fms_ingestion
from src.db.session import get_connection
from src.pipeline.positions.fms.forwards import extract, transform, loader

logger = logging.getLogger(__name__)


def run_full(start_date: date, end_date: date, force: bool = False) -> None:
    """
    Full pipeline: extract from FMS, load staging, load fact.
    Idempotent - safe to re-run for the same date range.
    """
    assert_fms_ingestion()

    batch_id = _new_batch_id()
    logger.info(f"batch_id={batch_id} start={start_date} end={end_date} force={force}")

    raw_df = extract.extract(start_date, end_date, force=force)
    if raw_df.empty:
        logger.info("no rows extracted, nothing to load")
        return

    stg_df = transform.transform_for_staging(raw_df, batch_id)

    with get_connection() as conn:
        loader.load_staging(conn, stg_df)

        portfolios = _load_portfolios(conn)
        fact_df = transform.transform_for_fact(stg_df, portfolios)
        loader.load_fact(conn, fact_df)

        _flip_backfill_pending_to_active(conn, fact_df)


def run_from_stg(batch_id: str) -> None:
    """
    Rebuild fact from an existing staging batch, without re-hitting FMS.
    """
    assert_fms_ingestion()
    logger.info(f"from-stg mode: batch_id={batch_id}")

    with get_connection() as conn:
        stg_df = _read_staging_by_batch(conn, batch_id)
        if stg_df.empty:
            logger.warning(f"no rows in stg_positions_fms_forwards for batch_id={batch_id}")
            return

        portfolios = _load_portfolios(conn)
        fact_df = transform.transform_for_fact(stg_df, portfolios)
        loader.load_fact(conn, fact_df)

        _flip_backfill_pending_to_active(conn, fact_df)


# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------

def _new_batch_id() -> str:
    return "fms_forwards_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _load_portfolios(conn) -> pd.DataFrame:
    """Load dim_portfolio (source='fms') into a DataFrame."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT procode, portfolio_id FROM dim_portfolio WHERE source = 'fms'"
        )
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["procode", "portfolio_id"])
    logger.info(f"loaded {len(df)} FMS portfolios from dim_portfolio")
    return df


def _read_staging_by_batch(conn, batch_id: str) -> pd.DataFrame:
    """Read stg_positions_fms_forwards rows for a given batch_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT batch_id, id_secuencial_fecha_proceso, date,
                   codigo_fondo, codigo_sbs,
                   codigo_iso_moneda_nocional, codigo_iso_moneda_contraparte,
                   valor_nocional, tipo_cambio_spot, nocional_soles,
                   moneda_compra, moneda_venta,
                   raw_payload
              FROM stg_positions_fms_forwards
             WHERE batch_id = %s
            """,
            (batch_id,),
        )
        cols = [c.name for c in cur.description]
        rows = cur.fetchall()
    df = pd.DataFrame.from_records(rows, columns=cols)
    logger.info(f"read {len(df)} rows from stg_positions_fms_forwards for batch_id={batch_id}")
    return df


def _flip_backfill_pending_to_active(conn, fact_df: pd.DataFrame) -> None:
    """
    Any FMS portfolio in status 'backfill-pending' that produced at
    least one fact row in this run gets promoted to 'active'.
    """
    if fact_df.empty:
        return
    portfolio_ids = fact_df["portfolio_id"].unique().tolist()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE dim_portfolio
               SET status = 'active', updated_at = CURRENT_TIMESTAMP
             WHERE portfolio_id = ANY(%s)
               AND source = 'fms'
               AND status = 'backfill-pending'
            """,
            (portfolio_ids,),
        )
        flipped = cur.rowcount
    if flipped:
        logger.info(f"flipped {flipped} FMS portfolios from backfill-pending to active")
