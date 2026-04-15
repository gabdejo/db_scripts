# src/pipeline/prices/sbs/tipo_cambio/loader.py
# No dim updates - dim_security_fx populated by Bloomberg enrichment
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def load_facts(conn, df: pd.DataFrame) -> tuple[int, int]:
    if df.empty:
        return 0, 0
    loaded = skipped = 0
    for _, row in df.iterrows():
        conn.execute(
            "INSERT INTO fact_prices (series_id, reference_date, value, source) VALUES (?,?,?,?) ON CONFLICT (series_id, reference_date) DO NOTHING",
            (int(row["series_id"]), row["reference_date"], float(row["value"]), row["source"]),
        )
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            loaded += 1
        else:
            skipped += 1
    logger.info(f"fact_prices (tipo_cambio): {loaded} loaded, {skipped} skipped.")
    return loaded, skipped
