# src/pipeline/positions/fms/forwards/run.py
# ---------------------------------------------------------------
# Orchestrator for the FMS forwards pipeline.
#
# Default mode:
#   1. Assert FMS ingestion enabled on this machine
#   2. Build batch_id from timestamp
#   3. Extract from FMS for --start-date..--end-date
#   4. Transform to staging shape
#   5. Load staging (idempotent upsert)
#   6. Load dim_portfolio (fms slice) into memory
#   7. Transform to fact shape (portfolio resolution)
#   8. Load fact (idempotent upsert)
#
# Flags:
#   --start-date YYYY-MM-DD    required unless --from-stg
#   --end-date YYYY-MM-DD      required unless --from-stg
#   --from-stg                 skip 1-5, read staging by batch_id
#   --batch-id STR             required with --from-stg
#   --force                    override MAX_RANGE_DAYS guard
#
# from-stg mode: bypasses FMS entirely. Reads existing staging
# rows by batch_id, transforms to fact shape, upserts fact. Used
# to rebuild fact after fixing a portfolio registration or MTM
# source without re-hitting FMS.
# ---------------------------------------------------------------

import argparse
import logging
from datetime import date, datetime

import pandas as pd

from src.configs.machine_config import assert_fms_ingestion
from src.db.session import get_connection
from src.pipeline.positions.fms.forwards import extract, transform, loader

logger = logging.getLogger(__name__)


def main() -> None:
    args = _parse_args()
    assert_fms_ingestion()

    if args.from_stg:
        _run_from_staging(args.batch_id)
    else:
        _run_full(args.start_date, args.end_date, args.force)


def _run_full(start_date: date, end_date: date, force: bool) -> None:
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


def _run_from_staging(batch_id: str) -> None:
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


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", type=date.fromisoformat)
    p.add_argument("--end-date", type=date.fromisoformat)
    p.add_argument("--from-stg", action="store_true")
    p.add_argument("--batch-id", type=str)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    if args.from_stg:
        if not args.batch_id:
            p.error("--from-stg requires --batch-id")
    else:
        if not (args.start_date and args.end_date):
            p.error("--start-date and --end-date are required unless --from-stg")

    return args


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    main()
