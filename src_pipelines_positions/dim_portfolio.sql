-- src/db/schema/dim_portfolio.sql
-- ---------------------------------------------------------------
-- dim_portfolio: one row per (internal_code, source).
--
-- A "portfolio" here is anything that holds positions:
--   - own_account       (FMS-sourced accounts we manage)
--   - regulator_filing  (SBS-sourced filings from peer institutions)
--   - etf               (Bloomberg or scraped ETF holdings)
--
-- The same logical portfolio coming from two sources is two rows
-- (different source values). parent_entity_id links ETF portfolios
-- back to their dim_entity row so price + holdings can be joined.
--
-- Status reuses the standard lifecycle:
--   backfill-pending | active | suspended | inactive | error-hold
-- ---------------------------------------------------------------

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

CREATE INDEX IF NOT EXISTS ix_dim_portfolio_source ON dim_portfolio(source);
CREATE INDEX IF NOT EXISTS ix_dim_portfolio_status ON dim_portfolio(status);
CREATE INDEX IF NOT EXISTS ix_dim_portfolio_parent ON dim_portfolio(parent_entity_id);
