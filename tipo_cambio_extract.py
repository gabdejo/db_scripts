# src/pipeline/prices/sbs/tipo_cambio/extract.py
# reference_date derived from fecha column in file, not from run_date
import logging
from datetime import date
import pandas as pd
from src.scrapers.sbs import find_latest_file

logger = logging.getLogger(__name__)
SBS_SUBDIR = "tipo_cambio"

RAW_COLUMNS = {
    "fecha": "fecha", "moneda_nocional": "moneda_nocional",
    "moneda_contraparte": "moneda_contraparte", "fuente": "fuente",
    "bid_original": "bid_original", "ask_original": "ask_original",
    "pen_bid": "pen_bid", "pen_ask": "pen_ask",
    "var_bid": "var_bid", "var_ask": "var_ask",
}


def read_raw(run_date: date) -> pd.DataFrame:
    """
    Reads and normalises the raw .xls file.
    Called by run.py (for registration) and load_stg (for staging).
    """
    path = find_latest_file(SBS_SUBDIR, run_date)
    if path is None:
        logger.warning(f"tipo_cambio: no file found for {run_date}.")
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
    raw = raw[list(set(RAW_COLUMNS.values()))].copy().dropna(how="all")
    # Derive reference_date from the fecha column inside the file
    raw["reference_date"] = pd.to_datetime(raw["fecha"], errors="coerce").dt.date.astype(str)
    raw = raw[raw["reference_date"] != "NaT"].copy()
    raw["loaded_at"] = pd.Timestamp.now().isoformat(timespec="seconds")
    raw = raw.drop(columns=["fecha"])
    logger.info(f"tipo_cambio: {len(raw)} rows extracted.")
    return raw


def load_stg(conn, df: pd.DataFrame, run_date: date) -> int:
    """Stages raw DataFrame. Adds reference_date and loaded_at at staging time."""
    if df.empty:
        return 0
    reference_date = run_date.isoformat()
    loaded_at      = pd.Timestamp.now().isoformat(timespec="seconds")
    inserted = 0
    for _, row in df.iterrows():
        conn.execute(
            """
            INSERT INTO stg_prices_sbs_tipo_cambio
                (moneda_nocional, moneda_contraparte, fuente,
                 bid_original, ask_original, pen_bid, pen_ask,
                 var_bid, var_ask, reference_date, loaded_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (moneda_nocional, moneda_contraparte, fuente, reference_date, loaded_at) DO NOTHING
            """,
            (
                _s(row,"moneda_nocional"), _s(row,"moneda_contraparte"), _s(row,"fuente"),
                _f(row.get("bid_original")), _f(row.get("ask_original")),
                _f(row.get("pen_bid")), _f(row.get("pen_ask")),
                _f(row.get("var_bid")), _f(row.get("var_ask")),
                row["reference_date"], row["loaded_at"],
            ),
        )
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            inserted += 1
    logger.info(f"stg_prices_sbs_tipo_cambio: {inserted} rows staged.")
    return inserted


def _f(val):
    try:
        return float(val) if val is not None and str(val).strip() not in ("","nan") else None
    except (ValueError, TypeError):
        return None

def _s(row, col):
    v = row.get(col)
    return str(v).strip() if v is not None and str(v).strip() not in ("","nan") else None
