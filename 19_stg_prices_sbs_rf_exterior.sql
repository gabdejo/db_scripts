-- 19_stg_prices_sbs_rf_exterior.sql
-- ---------------------------------------------------------------
-- Staging table for SBS foreign fixed income file (rf_exterior).
-- Similar to rf_local but fewer analytics fields - no TIR, spread,
-- duration, or rating. Clean/dirty prices and coupon dates retained.
-- One row per instrument per reference_date per load.
-- Loaded by pipeline/prices/sbs/rf_exterior/extract.py.
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stg_prices_sbs_rf_exterior (
    -- SBS identifiers
    codigo_sbs                  TEXT    NOT NULL,
    isin                        TEXT,

    -- Instrument metadata (slowly changing - also fed to dim_security_bond)
    tipo_instrumento            TEXT,
    emisor                      TEXT,
    moneda                      TEXT,
    valor_facial                REAL,
    origen_precio               TEXT,
    fecha_emision               TEXT,   -- YYYY-MM-DD
    fecha_vencimiento           TEXT,   -- YYYY-MM-DD
    tasa_cupon                  REAL,
    ultimo_cupon                TEXT,   -- YYYY-MM-DD
    proximo_cupon               TEXT,   -- YYYY-MM-DD

    -- Daily price facts
    precio_limpio_monto         REAL,
    precio_limpio_pct           REAL,
    precio_sucio_monto          REAL,
    precio_sucio_pct            REAL,
    interes_corrido_monto       REAL,
    variacion_precio_sucio      REAL,

    -- Pipeline metadata
    reference_date              TEXT    NOT NULL,
    loaded_at                   TEXT    NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (codigo_sbs, reference_date, loaded_at)
);

CREATE INDEX IF NOT EXISTS idx_stg_sbs_rf_exterior_date
    ON stg_prices_sbs_rf_exterior (reference_date);

CREATE INDEX IF NOT EXISTS idx_stg_sbs_rf_exterior_isin
    ON stg_prices_sbs_rf_exterior (isin);
