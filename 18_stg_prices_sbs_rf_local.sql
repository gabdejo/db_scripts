-- 18_stg_prices_sbs_rf_local.sql
-- ---------------------------------------------------------------
-- Staging table for SBS local fixed income file (rf_local).
-- Contains full bond analytics: YTM, spreads, duration, ratings,
-- coupon dates, clean/dirty prices.
-- One row per instrument per reference_date per load.
-- Loaded by pipeline/prices/sbs/rf_local/extract.py.
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stg_prices_sbs_rf_local (
    -- SBS identifiers
    codigo_sbs                  TEXT    NOT NULL,
    isin                        TEXT,
    nemonico                    TEXT,

    -- Instrument metadata (slowly changing - also fed to dim_security_bond)
    tipo_instrumento            TEXT,
    emisor                      TEXT,
    moneda                      TEXT,
    valor_facial                REAL,
    origen_precio               TEXT,   -- source of price (e.g. BVL, Bloomberg, SBS)
    fecha_emision               TEXT,   -- YYYY-MM-DD
    fecha_vencimiento           TEXT,   -- YYYY-MM-DD
    tasa_cupon                  REAL,
    margen_libor                REAL,
    rating                      TEXT,
    ultimo_cupon                TEXT,   -- YYYY-MM-DD
    proximo_cupon               TEXT,   -- YYYY-MM-DD

    -- Daily price facts (change every day)
    precio_limpio_monto         REAL,
    precio_limpio_pct           REAL,
    precio_sucio_monto          REAL,
    precio_sucio_pct            REAL,
    interes_corrido_monto       REAL,
    tir                         REAL,
    spreads                     REAL,
    tir_sin_opciones            REAL,
    duracion                    REAL,
    variacion_precio_limpio     REAL,
    variacion_precio_sucio      REAL,
    variacion_tir               REAL,

    -- Pipeline metadata
    reference_date              TEXT    NOT NULL,
    loaded_at                   TEXT    NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (codigo_sbs, reference_date, loaded_at)
);

CREATE INDEX IF NOT EXISTS idx_stg_sbs_rf_local_date
    ON stg_prices_sbs_rf_local (reference_date);

CREATE INDEX IF NOT EXISTS idx_stg_sbs_rf_local_isin
    ON stg_prices_sbs_rf_local (isin);
