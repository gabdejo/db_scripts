# src/pipeline/prices/sbs/rf_local/extract.py
import logging
from datetime import date
import pandas as pd
from src.scrapers.sbs import find_latest_file

logger = logging.getLogger(__name__)
SBS_SUBDIR = "rf_local"

RAW_COLUMNS = {
    "codigo_sbs": "codigo_sbs", "isin": "isin", "nemonico": "nemonico",
    "tipo_instrumento": "tipo_instrumento", "emisor": "emisor", "moneda": "moneda",
    "valor_facial": "valor_facial", "origen_precio": "origen_precio",
    "precio_limpio_monto": "precio_limpio_monto", "precio_limpio_%": "precio_limpio_pct",
    "precio_sucio_monto": "precio_sucio_monto", "precio_sucio_%": "precio_sucio_pct",
    "interes_corrido_monto": "interes_corrido_monto", "tir": "tir", "spreads": "spreads",
    "fecha_emision": "fecha_emision", "fecha_vencimiento": "fecha_vencimiento",
    "tasa_cupon": "tasa_cupon", "margen_libor": "margen_libor",
    "tir_sin_opciones": "tir_sin_opciones", "rating": "rating",
    "ultimo_cupon": "ultimo_cupon", "proximo_cupon": "proximo_cupon",
    "duracion": "duracion", "variacion_precio_limpio": "variacion_precio_limpio",
    "variacion_precio_sucio": "variacion_precio_sucio", "variacion_tir": "variacion_tir",
}
DATE_COLS = ["fecha_emision", "fecha_vencimiento", "ultimo_cupon", "proximo_cupon"]


def read_raw(run_date: date) -> pd.DataFrame:
    """
    Reads and normalises the raw .xls file.
    Called by run.py (for registration) and load_stg (for staging).
    """
    path = find_latest_file(SBS_SUBDIR, run_date)
    if path is None:
        logger.warning(f"rf_local: no file found for {run_date}.")
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
    for col in DATE_COLS:
        if col in raw.columns:
            raw[col] = pd.to_datetime(raw[col], errors="coerce").dt.date.astype(str)
            raw[col] = raw[col].where(raw[col] != "NaT", None)
    logger.info(f"rf_local: {len(raw)} rows extracted.")
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
            INSERT INTO stg_prices_sbs_rf_local (
                codigo_sbs, isin, nemonico, tipo_instrumento, emisor, moneda,
                valor_facial, origen_precio, fecha_emision, fecha_vencimiento,
                tasa_cupon, margen_libor, rating, ultimo_cupon, proximo_cupon,
                precio_limpio_monto, precio_limpio_pct, precio_sucio_monto,
                precio_sucio_pct, interes_corrido_monto, tir, spreads,
                tir_sin_opciones, duracion, variacion_precio_limpio,
                variacion_precio_sucio, variacion_tir, reference_date, loaded_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (codigo_sbs, reference_date, loaded_at) DO NOTHING
            """,
            (
                _s(row,"codigo_sbs"), _s(row,"isin"), _s(row,"nemonico"),
                _s(row,"tipo_instrumento"), _s(row,"emisor"), _s(row,"moneda"),
                _f(row.get("valor_facial")), _s(row,"origen_precio"),
                _s(row,"fecha_emision"), _s(row,"fecha_vencimiento"),
                _f(row.get("tasa_cupon")), _f(row.get("margen_libor")),
                _s(row,"rating"), _s(row,"ultimo_cupon"), _s(row,"proximo_cupon"),
                _f(row.get("precio_limpio_monto")), _f(row.get("precio_limpio_pct")),
                _f(row.get("precio_sucio_monto")), _f(row.get("precio_sucio_pct")),
                _f(row.get("interes_corrido_monto")), _f(row.get("tir")),
                _f(row.get("spreads")), _f(row.get("tir_sin_opciones")),
                _f(row.get("duracion")), _f(row.get("variacion_precio_limpio")),
                _f(row.get("variacion_precio_sucio")), _f(row.get("variacion_tir")),
                row["reference_date"], row["loaded_at"],
            ),
        )
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            inserted += 1
    logger.info(f"stg_prices_sbs_rf_local: {inserted} rows staged.")
    return inserted


def _f(val):
    try:
        return float(val) if val is not None and str(val).strip() not in ("","nan") else None
    except (ValueError, TypeError):
        return None

def _s(row, col):
    v = row.get(col)
    return str(v).strip() if v is not None and str(v).strip() not in ("","nan") else None
