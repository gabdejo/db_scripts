-- 20_stg_prices_sbs_tipo_cambio.sql
-- ---------------------------------------------------------------
-- Staging table for SBS FX rates file (tipo_cambio).
-- Reports bid/ask for currency pairs in both original currency
-- and PEN terms, from multiple sources (SBS, Bloomberg, etc.).
-- One row per currency pair per source per reference_date per load.
-- Loaded by pipeline/prices/sbs/tipo_cambio/extract.py.
--
-- Note: fecha column from the source file is used as reference_date.
-- The file reports the date internally so reference_date is derived
-- from fecha rather than from the acquisition date.
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stg_prices_sbs_tipo_cambio (
    -- Currency pair identifiers
    moneda_nocional             TEXT    NOT NULL,   -- base currency
    moneda_contraparte          TEXT    NOT NULL,   -- quote currency
    fuente                      TEXT    NOT NULL,   -- price source (SBS, Bloomberg, etc.)

    -- Daily FX facts
    bid_original                REAL,   -- bid in original currency terms
    ask_original                REAL,   -- ask in original currency terms
    pen_bid                     REAL,   -- bid expressed in PEN
    pen_ask                     REAL,   -- ask expressed in PEN
    var_bid                     REAL,   -- daily variation bid
    var_ask                     REAL,   -- daily variation ask

    -- Pipeline metadata
    reference_date              TEXT    NOT NULL,   -- derived from fecha column
    loaded_at                   TEXT    NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (moneda_nocional, moneda_contraparte, fuente, reference_date, loaded_at)
);

CREATE INDEX IF NOT EXISTS idx_stg_sbs_tipo_cambio_date
    ON stg_prices_sbs_tipo_cambio (reference_date);

CREATE INDEX IF NOT EXISTS idx_stg_sbs_tipo_cambio_pair
    ON stg_prices_sbs_tipo_cambio (moneda_nocional, moneda_contraparte);
