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
# Both use psycopg execute_values for batched inserts. Both accept
# an already-open connection (caller controls the transaction);
# empty DataFrames are no-ops.
# ---------------------------------------------------------------

import logging

import pandas as pd
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool  # type hint only; caller injects
from psycopg import Connection

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
VALUES %s
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
VALUES %s
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

    rows = _df_to_tuples(stg_df, STG_COLUMNS, jsonb_column="raw_payload")
    with conn.cursor() as cur:
        # psycopg3 does not ship execute_values - use executemany with a
        # rewritten multi-row INSERT for the batched path.
        _execute_batch_upsert(cur, STG_UPSERT, rows)
    logger.info(f"load_staging: upserted {len(rows)} rows into stg_positions_fms_forwards")
    return len(rows)


def load_fact(conn: Connection, fact_df: pd.DataFrame) -> int:
    """Upsert into fact_positions_forwards. Returns row count."""
    if fact_df.empty:
        logger.info("load_fact: empty DataFrame, nothing to write")
        return 0

    rows = _df_to_tuples(fact_df, FACT_COLUMNS)
    with conn.cursor() as cur:
        _execute_batch_upsert(cur, FACT_UPSERT, rows)
    logger.info(f"load_fact: upserted {len(rows)} rows into fact_positions_forwards")
    return len(rows)


def _df_to_tuples(df: pd.DataFrame, columns: list[str], jsonb_column: str = None) -> list[tuple]:
    """
    Convert a DataFrame to a list of tuples in the order of `columns`.
    NaN/NaT converted to None. If jsonb_column is specified, that
    column's values are wrapped in psycopg's Jsonb adapter.
    """
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing expected columns: {missing}")

    out = []
    for _, row in df[columns].iterrows():
        rec = []
        for col in columns:
            v = row[col]
            if pd.isna(v):
                rec.append(None)
            elif col == jsonb_column:
                rec.append(Jsonb(v))
            else:
                rec.append(v.item() if hasattr(v, "item") else v)
        out.append(tuple(rec))
    return out


def _execute_batch_upsert(cur, sql_template: str, rows: list[tuple]) -> None:
    """
    Execute a batched INSERT ... VALUES %s statement.

    psycopg3 doesn't have psycopg2's execute_values. We build a
    literal multi-row VALUES clause using cur.executemany's replacement
    by constructing the placeholder string ourselves.
    """
    if not rows:
        return
    n_cols = len(rows[0])
    placeholder = "(" + ", ".join(["%s"] * n_cols) + ")"
    values_clause = ", ".join([placeholder] * len(rows))
    flat = [v for row in rows for v in row]
    sql = sql_template.replace("VALUES %s", f"VALUES {values_clause}")
    cur.execute(sql, flat)
