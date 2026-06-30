-- 26_stg_positions_fms_forwards.sql
-- ---------------------------------------------------------------
-- stg_positions_fms_forwards: raw landing for the FMS forwards
-- feed. One of four feeds that together compose the full FMS
-- positions snapshot (alongside investment_portfolio,
-- current_account, net_receivables).
--
-- Grain: (codigo_fondo, codigo_sbs, date) — one row per forward
-- contract per fund per business date. Each row carries both
-- legs of the contract via moneda_compra/moneda_venta and the
-- IdTipoOperacion (preserved in raw_payload) that drove the leg
-- assignment.
--
-- date is a proper DATE, NOT the int yyyymmdd that FMS uses.
-- Conversion happens in extract.py on the way in, so staging
-- aligns with project convention. id_secuencial_fecha_proceso
-- is kept as INTEGER for traceability back to the source row.
--
-- Tier 1 attributes are typed columns; Tier 2 attributes are
-- preserved in raw_payload JSONB for forensic backfill without
-- re-hitting FMS. codigo_sbs is the grain identifier; FMS
-- guarantees it non-null for forwards. codigo_referencia is
-- preserved in raw_payload (Tier 2) — promote if it becomes
-- needed for analysis.
--
-- NUMERIC (not DOUBLE PRECISION) is used here for monetary and
-- FX fields because forwards require cent-exact reconciliation
-- against custody/accounting. Departs from the project default
-- of DOUBLE PRECISION — flag for schema-policy discussion before
-- the other three FMS feeds land.
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stg_positions_fms_forwards (
    -- Audit / batch
    batch_id                      TEXT NOT NULL,                -- e.g. 'fms_forwards_20260622_081532'
    loaded_at                     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Tier 1: typed business attributes
    id_secuencial_fecha_proceso   INTEGER NOT NULL,             -- FMS yyyymmdd int (traceability)
    date                          DATE NOT NULL,                -- DATE-converted from above
    codigo_fondo                  TEXT NOT NULL,                -- fund code; -> dim_portfolio.procode
    codigo_sbs                    TEXT NOT NULL,                -- SBS instrument code (grain)
    codigo_iso_moneda_nocional    TEXT,
    codigo_iso_moneda_contraparte TEXT,
    valor_nocional                NUMERIC(18, 4),
    tipo_cambio_spot              NUMERIC(18, 8),
    nocional_soles                NUMERIC(18, 4),               -- valor_nocional * tipo_cambio_spot, materialized
    moneda_compra                 TEXT,                         -- buy-side ccy, derived from IdTipoOperacion
    moneda_venta                  TEXT,                         -- sell-side ccy, derived from IdTipoOperacion

    -- Tier 2: full vendor payload for forensic / future analysis
    raw_payload                   JSONB,

    PRIMARY KEY (codigo_fondo, codigo_sbs, date)
);

CREATE INDEX IF NOT EXISTS idx_stg_positions_fms_forwards_batch
    ON stg_positions_fms_forwards (batch_id);

CREATE INDEX IF NOT EXISTS idx_stg_positions_fms_forwards_date
    ON stg_positions_fms_forwards (date);

CREATE INDEX IF NOT EXISTS idx_stg_positions_fms_forwards_fondo
    ON stg_positions_fms_forwards (codigo_fondo);

-- ---------------------------------------------------------------
-- Identifier resolution expectations (used by transform.py):
--   codigo_fondo -> dim_portfolio.procode (source='fms')
--   codigo_sbs   -> dim_entity_identifiers.id_value (id_type='sbs')
-- ---------------------------------------------------------------
