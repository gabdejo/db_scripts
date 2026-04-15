# src/pipeline/prices/sbs/vector_completo/extract.py
import logging
from datetime import date
import pandas as pd
from src.scrapers.sbs import find_latest_file

logger = logging.getLogger(__name__)
SBS_SUBDIR = "vector_completo"

RAW_COLUMNS = {
    "codigo_sbs": "codigo_sbs", "isin": "isin", "nemonico": "nemonico",
    "tipo_instrumento": "tipo_instrumento", "emisor": "emisor",
    "moneda": "moneda", "precio": "precio", "variacion": "variacion",
}


def extract(run_date: date) -> pd.DataFrame:
    path = find_latest_file(SBS_SUBDIR, run_date)
    if path is None:
        logger.warning(f"vector_completo: no file found for {run_date}.")
        return pd.DataFrame()
    logger.info(f"Reading {path.name}")
    try:
        raw = pd.read_excel(path, header=0)
    except Exception as e:
        logger.error(f"Failed to read {path.name}: {e}", exc_info=True)
        return pd.DataFrame()
    raw.columns = [str(c).strip().lower().replace(" ", "_") for c in raw.columns]
    raw = raw.rename(columns=RAW_COLUMNS)
    for col in RAW_COLUMNS.values():
        if col not in raw.columns:
            raw[col] = None
    raw = raw[list(RAW_COLUMNS.values())].copy().dropna(how="all")
    raw["reference_date"] = run_date.isoformat()
    raw["loaded_at"] = pd.Timestamp.now().isoformat(timespec="seconds")
    logger.info(f"vector_completo: {len(raw)} rows extracted.")
    return raw


def load_stg(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    inserted = 0
    for _, row in df.iterrows():
        conn.execute(
            """
            INSERT INTO stg_prices_sbs_vector_completo
                (codigo_sbs, isin, nemonico, tipo_instrumento, emisor,
                 moneda, precio, variacion, reference_date, loaded_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (codigo_sbs, reference_date, loaded_at) DO NOTHING
            """,
            (_s(row, "codigo_sbs"), _s(row, "isin"), _s(row, "nemonico"),
             _s(row, "tipo_instrumento"), _s(row, "emisor"), _s(row, "moneda"),
             _f(row.get("precio")), _f(row.get("variacion")),
             row["reference_date"], row["loaded_at"]),
        )
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            inserted += 1
    logger.info(f"stg_prices_sbs_vector_completo: {inserted} rows staged.")
    return inserted


def _f(val):
    try:
        return float(val) if val is not None and str(val).strip() not in ("", "nan") else None
    except (ValueError, TypeError):
        return None


def _s(row, col):
    v = row.get(col)
    return str(v).strip() if v is not None and str(v).strip() not in ("", "nan") else None
