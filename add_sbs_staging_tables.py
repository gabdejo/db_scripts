# scripts/migrate/add_sbs_staging_tables.py
# ---------------------------------------------------------------
# Migration: creates SBS staging tables.
# Safe to run multiple times (IF NOT EXISTS guards).
# Does NOT touch any fact or dim tables.
#
# Tables created:
#   stg_prices_sbs_vector_completo
#   stg_prices_sbs_rf_local
#   stg_prices_sbs_rf_exterior
#   stg_prices_sbs_tipo_cambio
#
# Run once:
#   python scripts/migrate/add_sbs_staging_tables.py
# ---------------------------------------------------------------

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.db.session import get_connection
from src.utils.logging import setup_logging

logger = logging.getLogger(__name__)

MIGRATIONS = [
    (
        "stg_prices_sbs_vector_completo",
        """
        CREATE TABLE IF NOT EXISTS stg_prices_sbs_vector_completo (
            codigo_sbs          TEXT    NOT NULL,
            isin                TEXT,
            nemonico            TEXT,
            tipo_instrumento    TEXT,
            emisor              TEXT,
            moneda              TEXT,
            precio              REAL,
            variacion           REAL,
            reference_date      TEXT    NOT NULL,
            loaded_at           TEXT    NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (codigo_sbs, reference_date, loaded_at)
        );
        """,
        [
            "CREATE INDEX IF NOT EXISTS idx_stg_sbs_vc_date ON stg_prices_sbs_vector_completo (reference_date);",
            "CREATE INDEX IF NOT EXISTS idx_stg_sbs_vc_isin ON stg_prices_sbs_vector_completo (isin);",
        ],
    ),
    (
        "stg_prices_sbs_rf_local",
        """
        CREATE TABLE IF NOT EXISTS stg_prices_sbs_rf_local (
            codigo_sbs                  TEXT    NOT NULL,
            isin                        TEXT,
            nemonico                    TEXT,
            tipo_instrumento            TEXT,
            emisor                      TEXT,
            moneda                      TEXT,
            valor_facial                REAL,
            origen_precio               TEXT,
            fecha_emision               TEXT,
            fecha_vencimiento           TEXT,
            tasa_cupon                  REAL,
            margen_libor                REAL,
            rating                      TEXT,
            ultimo_cupon                TEXT,
            proximo_cupon               TEXT,
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
            reference_date              TEXT    NOT NULL,
            loaded_at                   TEXT    NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (codigo_sbs, reference_date, loaded_at)
        );
        """,
        [
            "CREATE INDEX IF NOT EXISTS idx_stg_sbs_rf_local_date ON stg_prices_sbs_rf_local (reference_date);",
            "CREATE INDEX IF NOT EXISTS idx_stg_sbs_rf_local_isin ON stg_prices_sbs_rf_local (isin);",
        ],
    ),
    (
        "stg_prices_sbs_rf_exterior",
        """
        CREATE TABLE IF NOT EXISTS stg_prices_sbs_rf_exterior (
            codigo_sbs                  TEXT    NOT NULL,
            isin                        TEXT,
            tipo_instrumento            TEXT,
            emisor                      TEXT,
            moneda                      TEXT,
            valor_facial                REAL,
            origen_precio               TEXT,
            fecha_emision               TEXT,
            fecha_vencimiento           TEXT,
            tasa_cupon                  REAL,
            ultimo_cupon                TEXT,
            proximo_cupon               TEXT,
            precio_limpio_monto         REAL,
            precio_limpio_pct           REAL,
            precio_sucio_monto          REAL,
            precio_sucio_pct            REAL,
            interes_corrido_monto       REAL,
            variacion_precio_sucio      REAL,
            reference_date              TEXT    NOT NULL,
            loaded_at                   TEXT    NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (codigo_sbs, reference_date, loaded_at)
        );
        """,
        [
            "CREATE INDEX IF NOT EXISTS idx_stg_sbs_rf_ext_date ON stg_prices_sbs_rf_exterior (reference_date);",
            "CREATE INDEX IF NOT EXISTS idx_stg_sbs_rf_ext_isin ON stg_prices_sbs_rf_exterior (isin);",
        ],
    ),
    (
        "stg_prices_sbs_tipo_cambio",
        """
        CREATE TABLE IF NOT EXISTS stg_prices_sbs_tipo_cambio (
            moneda_nocional             TEXT    NOT NULL,
            moneda_contraparte          TEXT    NOT NULL,
            fuente                      TEXT    NOT NULL,
            bid_original                REAL,
            ask_original                REAL,
            pen_bid                     REAL,
            pen_ask                     REAL,
            var_bid                     REAL,
            var_ask                     REAL,
            reference_date              TEXT    NOT NULL,
            loaded_at                   TEXT    NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (moneda_nocional, moneda_contraparte, fuente, reference_date, loaded_at)
        );
        """,
        [
            "CREATE INDEX IF NOT EXISTS idx_stg_sbs_fx_date ON stg_prices_sbs_tipo_cambio (reference_date);",
            "CREATE INDEX IF NOT EXISTS idx_stg_sbs_fx_pair ON stg_prices_sbs_tipo_cambio (moneda_nocional, moneda_contraparte);",
        ],
    ),
]


def run_migration() -> None:
    logger.info("=== Migration: add_sbs_staging_tables started ===")

    with get_connection() as conn:
        for table_name, create_sql, index_sqls in MIGRATIONS:
            conn.execute(create_sql.strip())
            for idx_sql in index_sqls:
                conn.execute(idx_sql.strip())
            logger.info(f"Ensured: {table_name}")

        for fact_table in ("fact_prices", "fact_macro", "fact_fundamentals"):
            try:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM {fact_table}"
                ).fetchone()[0]
                logger.info(f"{fact_table}: {count:,} rows (unchanged).")
            except Exception:
                pass

    logger.info("=== Migration complete ===")


if __name__ == "__main__":
    setup_logging("migrate_sbs_staging")
    run_migration()
