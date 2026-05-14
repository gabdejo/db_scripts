-- src/db/schema/fact_positions.sql
-- ---------------------------------------------------------------
-- fact_positions: daily holdings.
--
-- Grain: (portfolio_id, security_entity_id, as_of_date, source)
--   - one row per holding per portfolio per day per source
--   - source is on the PK so the same logical holding from two
--     sources (e.g. an own account that also appears in an SBS
--     filing) doesn't collide
--
-- Cash positions point security_entity_id at synthetic per-currency
-- dim_entity rows (entity_type='cash'), so this table stays uniform
-- and doesn't need a separate fact_cash_positions.
--
-- weight is 0..1, MV-based, computed at transform time per
-- (portfolio_id, as_of_date).
-- ---------------------------------------------------------------

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

CREATE INDEX IF NOT EXISTS ix_fact_positions_asof    ON fact_positions(as_of_date);
CREATE INDEX IF NOT EXISTS ix_fact_positions_pf_date ON fact_positions(portfolio_id, as_of_date);
CREATE INDEX IF NOT EXISTS ix_fact_positions_sec     ON fact_positions(security_entity_id);
CREATE INDEX IF NOT EXISTS ix_fact_positions_source  ON fact_positions(source);
