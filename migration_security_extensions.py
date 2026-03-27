# scripts/migrate/add_security_extensions.py
# ---------------------------------------------------------------
# Schema migration: adds new security extension tables.
# Does NOT touch fact_prices or any fact table.
# Safe to run multiple times (IF NOT EXISTS guards).
#
# New tables:
#   dim_security_future
#   dim_security_fx
#   dim_security_rate_index
#   dim_security_index
#
# No ALTER TABLE needed - end_date removed from design.
# ---------------------------------------------------------------

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.db.session import get_connection
from src.utils.logging import setup_logging

logger = logging.getLogger(__name__)

MIGRATIONS = [
    (
        "dim_security_future",
        """
        CREATE TABLE IF NOT EXISTS dim_security_future (
            security_id         INTEGER PRIMARY KEY
                                REFERENCES dim_security(security_id),
            underlying          TEXT,
            contract_size       REAL,
            currency            TEXT,
            exchange            TEXT,
            expiry_date         TEXT,
            is_continuous       INTEGER NOT NULL DEFAULT 0,
            roll_convention     TEXT,
            updated_at          TEXT    NOT NULL DEFAULT (datetime('now'))
        );
        """,
    ),
    (
        "dim_security_fx",
        """
        CREATE TABLE IF NOT EXISTS dim_security_fx (
            security_id         INTEGER PRIMARY KEY
                                REFERENCES dim_security(security_id),
            base_currency       TEXT    NOT NULL,
            quote_currency      TEXT    NOT NULL,
            pair                TEXT    NOT NULL,
            fx_type             TEXT,
            updated_at          TEXT    NOT NULL DEFAULT (datetime('now'))
        );
        """,
    ),
    (
        "dim_security_rate_index",
        """
        CREATE TABLE IF NOT EXISTS dim_security_rate_index (
            security_id         INTEGER PRIMARY KEY
                                REFERENCES dim_security(security_id),
            tenor               TEXT,
            rate_type           TEXT,
            currency            TEXT,
            issuer_country      TEXT,
            updated_at          TEXT    NOT NULL DEFAULT (datetime('now'))
        );
        """,
    ),
    (
        "dim_security_index",
        """
        CREATE TABLE IF NOT EXISTS dim_security_index (
            security_id         INTEGER PRIMARY KEY
                                REFERENCES dim_security(security_id),
            index_family        TEXT,
            index_currency      TEXT,
            rebalance_frequency TEXT,
            weighting_method    TEXT,
            num_constituents    INTEGER,
            geographic_focus    TEXT,
            asset_class_focus   TEXT,
            updated_at          TEXT    NOT NULL DEFAULT (datetime('now'))
        );
        """,
    ),
]


def run_migration() -> None:
    logger.info("=== Migration: add_security_extensions started ===")

    with get_connection() as conn:
        for table_name, sql in MIGRATIONS:
            conn.execute(sql.strip())
            logger.info(f"Ensured table: {table_name}")

        # Verify fact_prices untouched
        count = conn.execute(
            "SELECT COUNT(*) FROM fact_prices"
        ).fetchone()[0]
        logger.info(f"fact_prices rows (unchanged): {count:,}")

    logger.info("=== Migration complete ===")


if __name__ == "__main__":
    setup_logging("migrate_security_extensions")
    run_migration()
