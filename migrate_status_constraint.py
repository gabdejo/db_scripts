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
# All steps run inside a single transaction - atomic, all-or-nothing.
# Safe to re-run: checks if migration already applied before running.
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
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.db.session import get_connection
from src.utils.logging import setup_logging

logger = logging.getLogger(__name__)

# Maps old status values to new ones.
# Values not in this map are kept unchanged.
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


def _get_current_constraint(conn) -> str:
    """Returns the CREATE TABLE SQL for series_registry from sqlite_master."""
    row = conn.execute(
        """
        SELECT sql FROM sqlite_master
        WHERE type = 'table' AND name = 'series_registry'
        """
    ).fetchone()
    return row[0] if row else ""


def _migration_already_applied(conn) -> bool:
    """
    Checks if the new constraint is already in place by inspecting
    the current CHECK constraint in sqlite_master.
    Returns True if migration has already been applied.
    """
    sql = _get_current_constraint(conn)
    # If the new values are already present, migration is done
    return "suspended" in sql and "inactive" in sql


def _get_current_status_counts(conn) -> dict:
    """Returns count of each status value for pre/post verification."""
    rows = conn.execute(
        "SELECT status, COUNT(*) AS cnt FROM series_registry GROUP BY status"
    ).fetchall()
    return {r["status"]: r["cnt"] for r in rows}


def run_migration() -> None:
    logger.info("=== Migration: migrate_status_constraint started ===")

    with get_connection() as conn:

        # Check if already applied
        if _migration_already_applied(conn):
            logger.info(
                "Migration already applied - series_registry already has "
                "the updated status constraint. Nothing to do."
            )
            return

        # Pre-migration state
        before = _get_current_status_counts(conn)
        total  = sum(before.values())
        logger.info(f"series_registry rows before migration: {total:,}")
        for status, cnt in sorted(before.items()):
            logger.info(f"  {status}: {cnt:,}")

        # Log what will be remapped
        for old, new in STATUS_MIGRATION_MAP.items():
            if old in before:
                logger.info(
                    f"Will remap: '{old}' -> '{new}' "
                    f"({before[old]:,} rows affected)"
                )

        # Build CHECK constraint string
        values_str = ", ".join(f"'{v}'" for v in NEW_STATUS_VALUES)
        check_constraint = f"CHECK (status IN ({values_str}))"

        # Step 1: create new table with updated constraint
        # Copy the existing schema but replace the status CHECK
        logger.info("Step 1: Creating series_registry_new with updated constraint.")
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

        # Step 2: copy data with status remapping
        logger.info("Step 2: Copying data with status remapping.")

        # Build CASE expression for status remapping
        case_parts = "\n".join(
            f"        WHEN status = '{old}' THEN '{new}'"
            for old, new in STATUS_MIGRATION_MAP.items()
        )
        case_expr = f"CASE\n{case_parts}\n        ELSE status\n    END"

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

        # Step 3: verify row counts match before dropping old table
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
                f"Migration aborted - original table untouched."
            )
        logger.info(f"Row count verified: {new_count:,} rows in new table.")

        # Step 4: drop old table and rename new table
        logger.info("Step 3: Dropping old table and renaming new table.")
        conn.execute("DROP TABLE series_registry")
        conn.execute("ALTER TABLE series_registry_new RENAME TO series_registry")

        # Step 5: recreate any indexes that existed on the old table
        # Add indexes here if you had any beyond the PK and UNIQUE constraint
        logger.info("Step 4: Recreating indexes.")
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

        # Post-migration verification
        after = _get_current_status_counts(conn)
        logger.info(f"series_registry rows after migration: {sum(after.values()):,}")
        for status, cnt in sorted(after.items()):
            logger.info(f"  {status}: {cnt:,}")

        # Verify new constraint is in place
        if not _migration_already_applied(conn):
            raise RuntimeError(
                "Constraint verification failed after migration. "
                "New constraint not detected in sqlite_master."
            )

    logger.info("=== Migration complete ===")


if __name__ == "__main__":
    setup_logging("migrate_status_constraint")
    run_migration()
