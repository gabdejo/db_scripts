# scripts/migrate/migrate_status_constraint.py
# ---------------------------------------------------------------
# Migrates series_registry.status CHECK constraint to reflect
# the updated status vocabulary:
#
#   Old values: backfill-pending, active, paused, deprecated, error-hold
#   New values: backfill-pending, active, suspended, inactive, error-hold
#
# SQLite does not support ALTER TABLE ... MODIFY COLUMN or
# ALTER TABLE ... ADD CONSTRAINT, so the only way to change a
# CHECK constraint is to rebuild the table:
#   1. Create new table with correct constraint
#   2. Copy all data, mapping old status values to new ones
#   3. Drop old table
#   4. Rename new table
#
# IMPORTANT: uses a raw sqlite3 connection (not get_connection())
# because PRAGMA foreign_keys = OFF must be set before any
# transaction begins. get_connection() starts a transaction on
# entry which causes the PRAGMA to be silently ignored, leaving
# FK checks active and causing DROP TABLE to fail with
# IntegrityError.
#
# Data mapping:
#   paused     -> suspended  (closest equivalent - temporary halt)
#   deprecated -> inactive   (permanent terminal state)
#   all others -> unchanged
#
# Run once:
#   python scripts/migrate/migrate_status_constraint.py
# ---------------------------------------------------------------

import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.utils.paths import DB_PATH
from src.utils.logging import setup_logging

logger = logging.getLogger(__name__)

# Maps old status values to new ones
STATUS_MIGRATION_MAP = {
    "paused":     "suspended",
    "deprecated": "inactive",
}

# New CHECK constraint values
NEW_STATUS_VALUES = (
    "backfill-pending",
    "active",
    "suspended",
    "inactive",
    "error-hold",
)


# ---- Helpers ---------------------------------------------------

def _get_current_constraint(conn: sqlite3.Connection) -> str:
    """Returns the CREATE TABLE SQL for series_registry from sqlite_master."""
    row = conn.execute(
        """
        SELECT sql FROM sqlite_master
        WHERE type = 'table' AND name = 'series_registry'
        """
    ).fetchone()
    return row[0] if row else ""


def _migration_already_applied(conn: sqlite3.Connection) -> bool:
    """
    Returns True if the new constraint is already in place.
    Checks sqlite_master for the new status values in the table definition.
    """
    sql = _get_current_constraint(conn)
    return "suspended" in sql and "inactive" in sql


def _get_status_counts(conn: sqlite3.Connection) -> dict:
    """Returns count per status value for pre/post verification."""
    rows = conn.execute(
        "SELECT status, COUNT(*) AS cnt FROM series_registry GROUP BY status"
    ).fetchall()
    return {r[0]: r[1] for r in rows}


# ---- Migration -------------------------------------------------

def run_migration() -> None:
    logger.info("=== Migration: migrate_status_constraint started ===")
    logger.info(f"Database: {DB_PATH}")

    # Raw connection - NOT using get_connection() context manager.
    # PRAGMA foreign_keys = OFF must be set before BEGIN to take
    # effect. get_connection() starts a transaction on entry which
    # causes the PRAGMA to be silently ignored.
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    try:
        # Set FK off BEFORE any transaction begins
        conn.execute("PRAGMA foreign_keys = OFF")
        logger.info("PRAGMA foreign_keys = OFF (before transaction).")

        # Check if already applied - read-only, no transaction needed
        if _migration_already_applied(conn):
            logger.info(
                "Migration already applied - series_registry already has "
                "the updated status constraint. Nothing to do."
            )
            return

        # Pre-migration state
        before = _get_status_counts(conn)
        total  = sum(before.values())
        logger.info(f"series_registry rows before migration: {total:,}")
        for status, cnt in sorted(before.items()):
            logger.info(f"  {status}: {cnt:,}")

        for old, new in STATUS_MIGRATION_MAP.items():
            if old in before:
                logger.info(
                    f"Will remap: '{old}' -> '{new}' "
                    f"({before[old]:,} rows)"
                )

        # Build CHECK constraint
        values_str    = ", ".join(f"'{v}'" for v in NEW_STATUS_VALUES)
        check_constraint = f"CHECK (status IN ({values_str}))"

        # All DDL/DML inside explicit transaction
        conn.execute("BEGIN")
        try:

            # Step 1: create new table with updated constraint
            logger.info("Step 1: Creating series_registry_new.")
            conn.execute(
                f"""
                CREATE TABLE series_registry_new (
                    series_id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id           INTEGER NOT NULL
                                        REFERENCES dim_entity(entity_id),
                    field               TEXT    NOT NULL,
                    domain              TEXT    NOT NULL,
                    source              TEXT    NOT NULL,
                    frequency           TEXT    NOT NULL,
                    default_start_date  TEXT    NOT NULL,
                    status              TEXT    NOT NULL
                                        DEFAULT 'backfill-pending'
                                        {check_constraint},
                    release_pattern     TEXT,
                    release_lag_days    INTEGER,
                    allow_revisions     INTEGER NOT NULL DEFAULT 0,
                    revision_lookback   TEXT,
                    last_run_at         TEXT,
                    last_run_status     TEXT,
                    last_loaded_date    TEXT,
                    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),
                    updated_at          TEXT    NOT NULL DEFAULT (datetime('now')),
                    UNIQUE (entity_id, field, source)
                )
                """
            )

            # Step 2: copy data with status remapping via CASE
            logger.info("Step 2: Copying data with status remapping.")
            case_parts = "\n".join(
                f"            WHEN status = '{old}' THEN '{new}'"
                for old, new in STATUS_MIGRATION_MAP.items()
            )
            case_expr = f"CASE\n{case_parts}\n            ELSE status\n        END"

            conn.execute(
                f"""
                INSERT INTO series_registry_new (
                    series_id, entity_id, field, domain, source, frequency,
                    default_start_date, status,
                    release_pattern, release_lag_days,
                    allow_revisions, revision_lookback,
                    last_run_at, last_run_status, last_loaded_date,
                    created_at, updated_at
                )
                SELECT
                    series_id, entity_id, field, domain, source, frequency,
                    default_start_date,
                    {case_expr},
                    release_pattern, release_lag_days,
                    allow_revisions, revision_lookback,
                    last_run_at, last_run_status, last_loaded_date,
                    created_at, updated_at
                FROM series_registry
                """
            )

            # Step 3: verify row counts before destroying old table
            logger.info("Step 3: Verifying row counts.")
            old_count = conn.execute(
                "SELECT COUNT(*) FROM series_registry"
            ).fetchone()[0]
            new_count = conn.execute(
                "SELECT COUNT(*) FROM series_registry_new"
            ).fetchone()[0]

            if old_count != new_count:
                raise RuntimeError(
                    f"Row count mismatch after copy: "
                    f"old={old_count:,} new={new_count:,}. "
                    f"Aborting - original table untouched."
                )
            logger.info(f"Row counts match: {new_count:,} rows.")

            # Step 4: drop old table and rename new table
            # Works because PRAGMA foreign_keys = OFF was set
            # before BEGIN above
            logger.info("Step 4: Dropping old table and renaming new table.")
            conn.execute("DROP TABLE series_registry")
            conn.execute(
                "ALTER TABLE series_registry_new RENAME TO series_registry"
            )

            # Step 5: recreate indexes
            logger.info("Step 5: Recreating indexes.")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_series_registry_status
                ON series_registry (status)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_series_registry_entity
                ON series_registry (entity_id)
                """
            )

            conn.execute("COMMIT")
            logger.info("Transaction committed.")

        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(
                f"Migration failed, rolled back: {e}",
                exc_info=True,
            )
            raise

        # Post-migration verification
        after = _get_status_counts(conn)
        logger.info(f"series_registry rows after migration: {sum(after.values()):,}")
        for status, cnt in sorted(after.items()):
            logger.info(f"  {status}: {cnt:,}")

        if not _migration_already_applied(conn):
            raise RuntimeError(
                "Constraint verification failed: new values not found "
                "in sqlite_master after migration."
            )

        logger.info("Constraint verified in sqlite_master.")

    finally:
        # Always re-enable FK checks and close cleanly
        conn.execute("PRAGMA foreign_keys = ON")
        logger.info("PRAGMA foreign_keys = ON restored.")
        conn.close()

    logger.info("=== Migration complete ===")


if __name__ == "__main__":
    setup_logging("migrate_status_constraint")
    run_migration()