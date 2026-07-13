# src/pipeline/positions/fms/forwards/loader.py
# ---------------------------------------------------------------
# Two writers, each idempotent via ON CONFLICT DO UPDATE.
#
# load_staging(conn, stg_df)
#   Writes to stg_positions_fms_forwards. PK is
#   (codigo_fondo, codigo_sbs, date). Rerunning the same batch
#   replaces existing rows for that batch. raw_payload is
#   serialized to JSON via psycopg's Jsonb adapter.
#
# load_fact(conn, fact_df)
#   Writes to fact_positions_forwards. PK is
#   (portfolio_id, codigo_sbs, date, source). Restatements land
#   cleanly - matches the fact_positions upsert-DO-UPDATE policy.
#
# Both iterate the DataFrame row-by-row and call cur.execute per
# row. This matches the pattern used by the Bloomberg prices
# pipeline and keeps consistency across the ETL. Slower per-row
# than batched inserts, but bounded by columns-per-statement
# (never trips Postgres's 65535 parameter cap), simpler to
# debug, and per-row errors point at the specific offending row.
#
# Both accept an already-open connection (caller controls the
# transaction); empty DataFrames are no-ops.
# ---------------------------------------------------------------

import logging

import pandas as pd
from psycopg import Connection
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)


STG_COLUMNS = [
    "batch_id", "id_secuencial_fecha_proceso", "date",
    "codigo_fondo", "codigo_sbs",
    "codigo_iso_moneda_nocional", "codigo_iso_moneda_contraparte",
    "valor_nocional", "tipo_cambio_spot", "nocional_soles",
    "moneda_compra", "moneda_venta",
    "raw_payload",
]

STG_UPSERT = f"""
INSERT INTO stg_positions_fms_forwards ({", ".join(STG_COLUMNS)})
VALUES ({", ".join(["%s"] * len(STG_COLUMNS))})
ON CONFLICT (codigo_fondo, codigo_sbs, date) DO UPDATE SET
    batch_id                      = EXCLUDED.batch_id,
    loaded_at                     = CURRENT_TIMESTAMP,
    id_secuencial_fecha_proceso   = EXCLUDED.id_secuencial_fecha_proceso,
    codigo_iso_moneda_nocional    = EXCLUDED.codigo_iso_moneda_nocional,
    codigo_iso_moneda_contraparte = EXCLUDED.codigo_iso_moneda_contraparte,
    valor_nocional                = EXCLUDED.valor_nocional,
    tipo_cambio_spot              = EXCLUDED.tipo_cambio_spot,
    nocional_soles                = EXCLUDED.nocional_soles,
    moneda_compra                 = EXCLUDED.moneda_compra,
    moneda_venta                  = EXCLUDED.moneda_venta,
    raw_payload                   = EXCLUDED.raw_payload
"""


FACT_COLUMNS = [
    "portfolio_id", "codigo_sbs", "date", "source",
    "codigo_iso_moneda_nocional",
    "valor_nocional", "tipo_cambio_spot", "nocional_soles",
    "moneda_compra", "moneda_venta",
    "fecha_vencimiento", "precio_forward", "valor_strike", "mtm_soles",
]

FACT_UPSERT = f"""
INSERT INTO fact_positions_forwards ({", ".join(FACT_COLUMNS)})
VALUES ({", ".join(["%s"] * len(FACT_COLUMNS))})
ON CONFLICT (portfolio_id, codigo_sbs, date, source) DO UPDATE SET
    codigo_iso_moneda_nocional = EXCLUDED.codigo_iso_moneda_nocional,
    valor_nocional             = EXCLUDED.valor_nocional,
    tipo_cambio_spot           = EXCLUDED.tipo_cambio_spot,
    nocional_soles             = EXCLUDED.nocional_soles,
    moneda_compra              = EXCLUDED.moneda_compra,
    moneda_venta               = EXCLUDED.moneda_venta,
    fecha_vencimiento          = EXCLUDED.fecha_vencimiento,
    precio_forward             = EXCLUDED.precio_forward,
    valor_strike               = EXCLUDED.valor_strike,
    mtm_soles                  = EXCLUDED.mtm_soles,
    loaded_at                  = CURRENT_TIMESTAMP
"""


def load_staging(conn: Connection, stg_df: pd.DataFrame) -> int:
    """Upsert into stg_positions_fms_forwards. Returns row count."""
    if stg_df.empty:
        logger.info("load_staging: empty DataFrame, nothing to write")
        return 0

    _validate_columns(stg_df, STG_COLUMNS)
    n = _execute_row_by_row(conn, STG_UPSERT, stg_df, STG_COLUMNS, jsonb_column="raw_payload")
    logger.info(f"load_staging: upserted {n} rows into stg_positions_fms_forwards")
    return n


def load_fact(conn: Connection, fact_df: pd.DataFrame) -> int:
    """Upsert into fact_positions_forwards. Returns row count."""
    if fact_df.empty:
        logger.info("load_fact: empty DataFrame, nothing to write")
        return 0

    _validate_columns(fact_df, FACT_COLUMNS)
    n = _execute_row_by_row(conn, FACT_UPSERT, fact_df, FACT_COLUMNS)
    logger.info(f"load_fact: upserted {n} rows into fact_positions_forwards")
    return n


def _validate_columns(df: pd.DataFrame, expected: list[str]) -> None:
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing expected columns: {missing}")


def _execute_row_by_row(
    conn: Connection,
    sql: str,
    df: pd.DataFrame,
    columns: list[str],
    jsonb_column: str = None,
) -> int:
    """
    Iterate the DataFrame row-by-row and execute one INSERT per row.
    Matches the pattern used by the Bloomberg prices pipeline.
    NaN/NaT converted to None. jsonb_column values wrapped in Jsonb.
    """
    with conn.cursor() as cur:
        for i, row in df.iterrows():
            params = _row_to_params(row, columns, jsonb_column)
            try:
                cur.execute(sql, params)
            except Exception:
                logger.error(f"insert failed for DataFrame row {i}: {dict(row[columns])}")
                raise
    return len(df)


def _row_to_params(row: pd.Series, columns: list[str], jsonb_column: str = None) -> tuple:
    """Convert a DataFrame row to a psycopg-ready parameter tuple."""
    out = []
    for col in columns:
        v = row[col]
        if pd.isna(v):
            out.append(None)
        elif col == jsonb_column:
            out.append(Jsonb(v))
        else:
            out.append(v.item() if hasattr(v, "item") else v)
    return tuple(out)
