# scripts/delete_series.py
# ---------------------------------------------------------------
# Maintenance script: completely removes one or more series and
# optionally their parent entity from the database.
#
# Deletes in FK dependency order:
#   1. fact_prices / fact_macro / fact_fundamentals rows
#   2. series_registry rows
#   3. dim_source_priority rows
#   4. dim_internal_attributes rows
#   5. stg_* staging rows
#   6. dim_entity_identifiers rows       (if --delete-entity)
#   7. dim_security / extension tables   (if --delete-entity)
#   8. dim_macro                         (if --delete-entity)
#   9. dim_entity row                    (if --delete-entity)
#
# USE WITH CARE - destructive and irreversible.
# Always run --dry-run first to verify scope.
#
# Usage:
#   # Dry run first - see what would be deleted
#   python scripts/delete_series.py --ticker AAPL --dry-run
#
#   # Delete all series for a ticker (keeps dim_entity)
#   python scripts/delete_series.py --ticker AAPL
#
#   # Delete a specific field/source combination only
#   python scripts/delete_series.py --ticker AAPL --field PX_LAST --source bloomberg
#
#   # Delete series AND the parent entity + all dim rows
#   python scripts/delete_series.py --ticker AAPL --delete-entity
#
#   # Delete multiple tickers at once
#   python scripts/delete_series.py --ticker AAPL MSFT GOOGL --dry-run
#   python scripts/delete_series.py --ticker AAPL MSFT GOOGL --delete-entity
# ---------------------------------------------------------------

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db.session import get_connection
from src.utils.logging import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete series and optionally entity from the database.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--ticker",
        nargs="+",
        required=True,
        help="One or more internal ticker codes to delete (e.g. AAPL MSFT).",
    )
    parser.add_argument(
        "--field",
        type=str,
        default=None,
        help=(
            "Filter to a specific field (e.g. PX_LAST). "
            "Deletes all fields if omitted."
        ),
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help=(
            "Filter to a specific source (e.g. bloomberg). "
            "Deletes all sources if omitted."
        ),
    )
    parser.add_argument(
        "--delete-entity",
        action="store_true",
        default=False,
        dest="delete_entity",
        help=(
            "Also delete the parent entity and all associated dim rows "
            "(dim_entity, dim_security, dim_macro, dim_entity_identifiers, "
            "dim_source_priority, extension tables). "
            "Only safe when ALL series for the entity are being deleted."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        dest="dry_run",
        help="Show what would be deleted without making any changes.",
    )

    args = parser.parse_args()
    setup_logging("delete_series")

    for ticker in args.ticker:
        _delete_ticker(
            ticker=ticker,
            field=args.field,
            source=args.source,
            delete_entity=args.delete_entity,
            dry_run=args.dry_run,
        )

    if args.dry_run:
        logger.info("Dry run complete. No changes made.")
    else:
        logger.info("Deletion complete.")


# ---- Per-ticker deletion ---------------------------------------

def _delete_ticker(
    ticker: str,
    field: str | None,
    source: str | None,
    delete_entity: bool,
    dry_run: bool,
) -> None:

    with get_connection() as conn:

        # Resolve entity
        entity = conn.execute(
            "SELECT entity_id, entity_type FROM dim_entity WHERE ticker = ?",
            (ticker,),
        ).fetchone()

        if not entity:
            logger.warning(f"Ticker '{ticker}' not found in dim_entity. Skipping.")
            return

        entity_id   = entity["entity_id"]
        entity_type = entity["entity_type"]

        # Resolve series to delete
        query  = "SELECT series_id, field, domain, source FROM series_registry WHERE entity_id = ?"
        params = [entity_id]

        if field:
            query  += " AND field = ?"
            params.append(field)
        if source:
            query  += " AND source = ?"
            params.append(source)

        series_rows = conn.execute(query, params).fetchall()

        if not series_rows:
            logger.warning(
                f"No series found for ticker='{ticker}' "
                f"field={field or 'any'} source={source or 'any'}. Skipping."
            )
            return

        series_ids = [r["series_id"] for r in series_rows]

        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}"
            f"ticker='{ticker}' entity_id={entity_id} "
            f"entity_type={entity_type} | "
            f"{len(series_ids)} series to delete"
        )
        for sr in series_rows:
            logger.info(
                f"  series_id={sr['series_id']} "
                f"field={sr['field']} "
                f"domain={sr['domain']} "
                f"source={sr['source']}"
            )

        # Count fact rows
        ph = ",".join("?" * len(series_ids))
        counts = {
            "fact_prices":        _count(conn, "fact_prices",        series_ids, ph),
            "fact_macro":         _count(conn, "fact_macro",         series_ids, ph),
            "fact_fundamentals":  _count(conn, "fact_fundamentals",  series_ids, ph),
            "stg_prices_bloomberg": _count_stg(conn, "stg_prices_bloomberg", entity_id),
            "stg_macro_bloomberg":  _count_stg(conn, "stg_macro_bloomberg",  entity_id),
        }
        for table, cnt in counts.items():
            if cnt:
                logger.info(f"  {table}: {cnt:,} rows will be deleted")

        if dry_run:
            return

        placeholders = ",".join("?" * len(series_ids))

        # 1. Fact tables
        for fact_table in ("fact_prices", "fact_macro", "fact_fundamentals"):
            deleted = conn.execute(
                f"DELETE FROM {fact_table} WHERE series_id IN ({placeholders})",
                series_ids,
            ).rowcount
            if deleted:
                logger.info(f"  Deleted {deleted:,} rows from {fact_table}.")

        # 2. series_registry
        deleted = conn.execute(
            f"DELETE FROM series_registry WHERE series_id IN ({placeholders})",
            series_ids,
        ).rowcount
        logger.info(f"  Deleted {deleted} rows from series_registry.")

        # 3. dim_source_priority
        conn.execute(
            "DELETE FROM dim_source_priority WHERE entity_id = ?",
            (entity_id,),
        )

        # 4. dim_internal_attributes
        conn.execute(
            "DELETE FROM dim_internal_attributes WHERE entity_id = ?",
            (entity_id,),
        )

        # 5. Staging tables (keyed by bloomberg_ticker, resolve first)
        bbg_ticker_row = conn.execute(
            """
            SELECT id_value FROM dim_entity_identifiers
            WHERE entity_id = ? AND id_type = 'bloomberg_ticker'
            """,
            (entity_id,),
        ).fetchone()

        if bbg_ticker_row:
            bbg_ticker = bbg_ticker_row["id_value"]
            for stg_table in (
                "stg_prices_bloomberg",
                "stg_macro_bloomberg",
                "stg_security_bloomberg",
            ):
                try:
                    deleted = conn.execute(
                        f"DELETE FROM {stg_table} WHERE bloomberg_ticker = ?",
                        (bbg_ticker,),
                    ).rowcount
                    if deleted:
                        logger.info(f"  Deleted {deleted:,} rows from {stg_table}.")
                except Exception:
                    pass  # staging table may not exist yet

        if not delete_entity:
            logger.info(
                f"Series deleted for '{ticker}'. "
                f"Entity and dim rows retained (use --delete-entity to remove)."
            )
            return

        # Check no remaining series exist for this entity before
        # deleting entity-level rows
        remaining = conn.execute(
            "SELECT COUNT(*) FROM series_registry WHERE entity_id = ?",
            (entity_id,),
        ).fetchone()[0]

        if remaining > 0:
            logger.warning(
                f"'{ticker}' still has {remaining} series in series_registry "
                f"after deletion (from other field/source combinations). "
                f"Skipping entity deletion to preserve referential integrity. "
                f"Delete all series first or add --field / --source filters."
            )
            return

        # 6. dim_entity_identifiers
        deleted = conn.execute(
            "DELETE FROM dim_entity_identifiers WHERE entity_id = ?",
            (entity_id,),
        ).rowcount
        logger.info(f"  Deleted {deleted} rows from dim_entity_identifiers.")

        # 7. Extension tables and dim_security / dim_macro
        if entity_type == "security":
            sec_row = conn.execute(
                "SELECT security_id FROM dim_security WHERE entity_id = ?",
                (entity_id,),
            ).fetchone()

            if sec_row:
                security_id = sec_row["security_id"]
                for ext_table in (
                    "dim_security_equity",
                    "dim_security_fund",
                    "dim_security_bond",
                    "dim_security_future",
                    "dim_security_fx",
                    "dim_security_rate_index",
                    "dim_security_index",
                ):
                    try:
                        conn.execute(
                            f"DELETE FROM {ext_table} WHERE security_id = ?",
                            (security_id,),
                        )
                    except Exception:
                        pass  # extension table may not exist

                conn.execute(
                    "DELETE FROM dim_security WHERE entity_id = ?",
                    (entity_id,),
                )
                logger.info(f"  Deleted dim_security and extension rows.")

        elif entity_type == "macro":
            conn.execute(
                "DELETE FROM dim_macro WHERE entity_id = ?",
                (entity_id,),
            )
            logger.info(f"  Deleted dim_macro row.")

        # 8. dim_entity (last - everything else references it)
        conn.execute(
            "DELETE FROM dim_entity WHERE entity_id = ?",
            (entity_id,),
        )
        logger.info(f"  Deleted dim_entity row for '{ticker}' (entity_id={entity_id}).")
        logger.info(f"Entity '{ticker}' fully removed from database.")


# ---- Helpers ---------------------------------------------------

def _count(conn, table: str, series_ids: list, placeholders: str) -> int:
    try:
        row = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE series_id IN ({placeholders})",
            series_ids,
        ).fetchone()
        return row[0] if row else 0
    except Exception:
        return 0


def _count_stg(conn, table: str, entity_id: int) -> int:
    try:
        bbg_row = conn.execute(
            """
            SELECT id_value FROM dim_entity_identifiers
            WHERE entity_id = ? AND id_type = 'bloomberg_ticker'
            """,
            (entity_id,),
        ).fetchone()
        if not bbg_row:
            return 0
        row = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE bloomberg_ticker = ?",
            (bbg_row["id_value"],),
        ).fetchone()
        return row[0] if row else 0
    except Exception:
        return 0


if __name__ == "__main__":
    main()
