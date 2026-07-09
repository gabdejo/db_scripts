-- 27_fact_positions_forwards.sql
-- ---------------------------------------------------------------
-- fact_positions_forwards: forward contract positions.
--
-- Grain: (portfolio_id, codigo_sbs, date, source) - one row per
-- forward contract per portfolio per date per source. Contract-
-- level detail preserved; leg-oriented queries synthesize legs
-- via CTE at query time (see analytics/fx_exposure/compute.sql).
--
-- Not entity-resolved: codigo_sbs is a raw vendor identifier,
-- not an FK to dim_entity. Forwards aren't registered as entities
-- (too ephemeral, too many, no analytical value per-contract).
-- Opportunistic linkage to dim_entity is a LEFT JOIN at query
-- time via dim_entity_identifiers where id_type='sbs'.
--
-- Two-leg encoding: one row per contract, both legs as columns
-- (moneda_compra, moneda_venta). Downstream code that needs the
-- leg-oriented view (positive buy leg, negative sell leg) does
-- the split via CTE - it's not materialized here.
--
-- Source-unified: same table receives forwards from FMS, from
-- SBS filings (if peer institutions report forwards), and from
-- any future source. source is on the PK so same-contract-
-- different-source rows coexist.
--
-- Upsert policy: ON CONFLICT DO UPDATE - forwards get restated
-- when FMS revises valuations. Matches fact_positions policy.
--
-- MTM: mtm_soles is nullable, populated from FMS's valuation
-- field (precio_vector or equivalent) when available. Downstream
-- analytics prefer mtm_soles over nocional_soles when non-null.
--
-- NUMERIC precision: monetary NUMERIC(18, 4), FX rates
-- NUMERIC(18, 8), matches staging convention. Departs from the
-- project-wide DOUBLE PRECISION default for cent-exact
-- reconciliation against custody/accounting.
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_positions_forwards (
    -- Grain
    portfolio_id                  INTEGER NOT NULL REFERENCES dim_portfolio(portfolio_id),
    codigo_sbs                    TEXT    NOT NULL,
    date                          DATE    NOT NULL,
    source                        TEXT    NOT NULL,             -- 'fms' | 'sbs' | ...

    -- Notional and FX
    codigo_iso_moneda_nocional    TEXT    NOT NULL,
    valor_nocional                NUMERIC(18, 4) NOT NULL,
    tipo_cambio_spot              NUMERIC(18, 8) NOT NULL,
    nocional_soles                NUMERIC(18, 4) NOT NULL,      -- valor_nocional * tipo_cambio_spot

    -- Legs
    moneda_compra                 TEXT    NOT NULL,
    moneda_venta                  TEXT    NOT NULL,

    -- Contract terms (nullable - not all sources report all fields)
    fecha_vencimiento             DATE,                          -- from id_secuencial_fecha_vencimiento
    precio_forward                NUMERIC(18, 8),                -- forward rate
    valor_strike                  NUMERIC(18, 8),                -- strike if different from precio_forward

    -- Valuation
    mtm_soles                     NUMERIC(18, 4),                -- mark-to-market in soles, from FMS

    -- Audit
    loaded_at                     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (portfolio_id, codigo_sbs, date, source),
    CHECK (moneda_compra <> moneda_venta)                        -- reject degenerate same-currency forwards
);

CREATE INDEX IF NOT EXISTS idx_fact_positions_forwards_date
    ON fact_positions_forwards (date);

CREATE INDEX IF NOT EXISTS idx_fact_positions_forwards_portfolio
    ON fact_positions_forwards (portfolio_id, date);

CREATE INDEX IF NOT EXISTS idx_fact_positions_forwards_codigo_sbs
    ON fact_positions_forwards (codigo_sbs);

CREATE INDEX IF NOT EXISTS idx_fact_positions_forwards_vencimiento
    ON fact_positions_forwards (fecha_vencimiento)
    WHERE fecha_vencimiento IS NOT NULL;

-- ---------------------------------------------------------------
-- Downstream consumers:
--   - analytics/fx_exposure/compute.sql: splits into two legs via
--     CTE (buy leg +nocional_soles, sell leg -nocional_soles)
--   - future portfolio NAV rollup: uses mtm_soles for valuation
--     when non-null, nocional_soles as fallback
--   - future maturity concentration analysis: uses fecha_vencimiento
--     to bucket by time-to-maturity
-- ---------------------------------------------------------------
