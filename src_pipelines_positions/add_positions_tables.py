# scripts/migrate/add_positions_tables.py
# ---------------------------------------------------------------
# Migration: introduces the positions domain.
#
# Loads DDL from src/db/schema/ rather than inlining it here, so
# schema files stay the single source of truth for both bootstrap
# and migrations. Files are executed in order — dim before fact
# (FK dependency), staging tables last.
#
# Each .sql file is split on semicolons and executed statement
# by statement. All statements are idempotent (IF NOT EXISTS),
# so re-running this script is safe.
# ---------------------------------------------------------------

import logging
from pathlib import Path

from src.db.session import get_connection

logger = logging.getLogger(__name__)


# Resolved at import time. Walks up from this file to project root,
# then into src/db/schema/.
SCHEMA_DIR = (
    Path(__file__).resolve().parents[2] / "src" / "db" / "schema"
)

# Order matters: parents before children (FK references).
SCHEMA_FILES = [
    "dim_portfolio.sql",
    "fact_positions.sql",
    "stg_positions_fms.sql",
]


def run() -> None:
    conn = get_connection()
    cur = conn.cursor()
    try:
        for filename in SCHEMA_FILES:
            path = SCHEMA_DIR / filename
            if not path.exists():
                raise FileNotFoundError(f"schema file missing: {path}")
            logger.info(f"applying {filename}")
            _execute_sql_file(cur, path)

        conn.commit()
        logger.info("positions tables migration: done")
    except Exception:
        conn.rollback()
        logger.exception("positions tables migration failed; rolled back")
        raise
    finally:
        cur.close()
        conn.close()


def _execute_sql_file(cur, path: Path) -> None:
    """
    Reads a .sql file, splits on semicolons, executes each non-empty
    statement. Strips line comments (-- ...) so they don't confuse
    the split. Block comments aren't supported — keep DDL files simple.
    """
    raw = path.read_text(encoding="utf-8")
    cleaned = "\n".join(
        line for line in raw.splitlines()
        if not line.strip().startswith("--")
    )
    statements = [s.strip() for s in cleaned.split(";") if s.strip()]
    for stmt in statements:
        cur.execute(stmt)


if __name__ == "__main__":
    run()
