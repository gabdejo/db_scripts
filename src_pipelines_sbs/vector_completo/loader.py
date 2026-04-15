# src/pipeline/prices/sbs/vector_completo/loader.py
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def load_facts(conn, df: pd.DataFrame) -> tuple[int, int]:
    if df.empty:
        return 0, 0
    loaded = skipped = 0
    for _, row in df.iterrows():
        conn.execute(
            """
            INSERT INTO fact_prices (series_id, reference_date, value, source)
            VALUES (?,?,?,?)
            ON CONFLICT (series_id, reference_date) DO NOTHING
            """,
            (int(row["series_id"]), row["reference_date"], float(row["value"]), row["source"]),
        )
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            loaded += 1
        else:
            skipped += 1
    logger.info(f"fact_prices (vector_completo): {loaded} loaded, {skipped} skipped.")
    return loaded, skipped


def load_dims(conn, df: pd.DataFrame) -> None:
    if df.empty:
        return
    for _, row in df.iterrows():
        conn.execute(
            """
            UPDATE dim_security
            SET security_type = COALESCE(security_type, ?),
                updated_at = CASE WHEN security_type IS NULL THEN datetime('now') ELSE updated_at END
            WHERE entity_id = ?
            """,
            (row.get("tipo_instrumento"), int(row["entity_id"])),
        )
    logger.info(f"dim_security partial update: {len(df)} rows processed.")
