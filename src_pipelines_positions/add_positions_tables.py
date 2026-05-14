# scripts/migrate/add_positions_tables.py
# ---------------------------------------------------------------
# Migration: introduces the positions domain.
#
# Creates three tables:
#   - dim_portfolio          one row per (internal_code, source).
#                            Reuses the standard status lifecycle
#                            (backfill-pending | active | suspended
#                             | inactive | error-hold), same as
#                            series_registry.
#   - fact_positions         daily holdings, grain is
#                            (portfolio_id, security_entity_id,
#                             as_of_date, source). Multi-source
#                            ready from day one.
#   - stg_positions_fms      raw landing for FMS sproc output,
#                            permissive types, no FKs. Each run
#                            tags rows with a batch_id.
#
# Idempotent (IF NOT EXISTS everywhere). Safe to re-run.
# SQLite '?' placeholders for now — swap to '%s' as part of the
# PostgreSQL migration (see project pending tasks).
# ---------------------------------------------------------------

import logging

from src.db.session import get_connection

logger = logging.getLogger(__name__)


DDL_DIM_PORTFOLIO = """
CREATE TABLE IF NOT EXISTS dim_portfolio (
    portfolio_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    internal_code    TEXT    NOT NULL,
    source           TEXT    NOT NULL,            -- fms | sbs | bloomberg | scraper
    portfolio_type   TEXT    NOT NULL,            -- own_account | regulator_filing | etf
    display_name     TEXT,
    base_currency    TEXT,
    parent_entity_id INTEGER,                     -- links ETFs back to dim_entity
    status           TEXT    NOT NULL DEFAULT 'active',
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (internal_code, source),
    FOREIGN KEY (parent_entity_id) REFERENCES dim_entity(entity_id)
);
"""

DDL_DIM_PORTFOLIO_IX = [
    "CREATE INDEX IF NOT EXISTS ix_dim_portfolio_source ON dim_portfolio(source);",
    "CREATE INDEX IF NOT EXISTS ix_dim_portfolio_status ON dim_portfolio(status);",
    "CREATE INDEX IF NOT EXISTS ix_dim_portfolio_parent ON dim_portfolio(parent_entity_id);",
]


DDL_FACT_POSITIONS = """
CREATE TABLE IF NOT EXISTS fact_positions (
    portfolio_id       INTEGER NOT NULL,
    security_entity_id INTEGER NOT NULL,
    as_of_date         DATE    NOT NULL,
    source             TEXT    NOT NULL,          -- fms | sbs | bloomberg | scraper
    quantity           NUMERIC,
    market_value       NUMERIC,
    cost_basis         NUMERIC,
    accrued_interest   NUMERIC,
    weight             NUMERIC,                   -- 0..1, portfolio weight by MV
    price_used         NUMERIC,
    currency           TEXT,
    yield_to_maturity  NUMERIC,
    duration           NUMERIC,
    loaded_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (portfolio_id, security_entity_id, as_of_date, source),
    FOREIGN KEY (portfolio_id)       REFERENCES dim_portfolio(portfolio_id),
    FOREIGN KEY (security_entity_id) REFERENCES dim_entity(entity_id)
);
"""

DDL_FACT_POSITIONS_IX = [
    "CREATE INDEX IF NOT EXISTS ix_fact_positions_asof    ON fact_positions(as_of_date);",
    "CREATE INDEX IF NOT EXISTS ix_fact_positions_pf_date ON fact_positions(portfolio_id, as_of_date);",
    "CREATE INDEX IF NOT EXISTS ix_fact_positions_sec     ON fact_positions(security_entity_id);",
    "CREATE INDEX IF NOT EXISTS ix_fact_positions_source  ON fact_positions(source);",
]


DDL_STG_POSITIONS_FMS = """
CREATE TABLE IF NOT EXISTS stg_positions_fms (
    batch_id          TEXT      NOT NULL,         -- e.g. 'fms_20260514_081532'
    extracted_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    as_of_date        DATE,
    account_code      TEXT,                       -- FMS internal account id
    instrument_id     TEXT,                       -- FMS internal instrument id
    isin              TEXT,
    ticker            TEXT,
    description       TEXT,
    quantity          NUMERIC,
    market_value      NUMERIC,
    cost_basis        NUMERIC,
    accrued_interest  NUMERIC,
    currency          TEXT,
    price_used        NUMERIC,
    yield_to_maturity NUMERIC,
    duration          NUMERIC,
    raw_payload       TEXT                        -- JSON of the full sproc row, for audit
);
"""

DDL_STG_POSITIONS_FMS_IX = [
    "CREATE INDEX IF NOT EXISTS ix_stg_positions_fms_batch ON stg_positions_fms(batch_id);",
    "CREATE INDEX IF NOT EXISTS ix_stg_positions_fms_asof  ON stg_positions_fms(as_of_date);",
]


def run() -> None:
    conn = get_connection()
    cur = conn.cursor()
    try:
        logger.info("creating dim_portfolio")
        cur.execute(DDL_DIM_PORTFOLIO)
        for stmt in DDL_DIM_PORTFOLIO_IX:
            cur.execute(stmt)

        logger.info("creating fact_positions")
        cur.execute(DDL_FACT_POSITIONS)
        for stmt in DDL_FACT_POSITIONS_IX:
            cur.execute(stmt)

        logger.info("creating stg_positions_fms")
        cur.execute(DDL_STG_POSITIONS_FMS)
        for stmt in DDL_STG_POSITIONS_FMS_IX:
            cur.execute(stmt)

        conn.commit()
        logger.info("positions tables migration: done")
    except Exception:
        conn.rollback()
        logger.exception("positions tables migration failed; rolled back")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run()
