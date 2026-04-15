# src/pipeline/prices/sbs/tipo_cambio/transform.py
import logging
import pandas as pd

logger = logging.getLogger(__name__)

FACT_FIELD_MAP = {
    "pen_bid": "PX_BID", "pen_ask": "PX_ASK",
    "var_bid": "CHG_BID", "var_ask": "CHG_ASK",
}


def transform(stg_df: pd.DataFrame, securities: list[dict]) -> pd.DataFrame:
    """
    series_registry must include moneda_nocional, moneda_contraparte, fuente
    as identifier attributes to build the series lookup.
    """
    if stg_df.empty:
        return pd.DataFrame()
    series_map = {
        (s["moneda_nocional"], s["moneda_contraparte"], s["fuente"], s["field"]): s["series_id"]
        for s in securities
        if s.get("moneda_nocional") and s.get("moneda_contraparte") and s.get("fuente")
    }
    fact_rows = []
    for _, row in stg_df.iterrows():
        mn = row.get("moneda_nocional")
        mc = row.get("moneda_contraparte")
        src = row.get("fuente")
        for stg_col, field_name in FACT_FIELD_MAP.items():
            sid = series_map.get((mn, mc, src, field_name))
            if sid is None:
                continue
            val = _f(row.get(stg_col))
            if val is None:
                continue
            fact_rows.append({"series_id": sid, "reference_date": row["reference_date"], "value": val, "source": "sbs"})
    facts_df = pd.DataFrame(fact_rows) if fact_rows else pd.DataFrame(columns=["series_id","reference_date","value","source"])
    logger.info(f"tipo_cambio transform: {len(facts_df)} fact rows.")
    return facts_df


def _f(val):
    try:
        return float(val) if val is not None and str(val).strip() not in ("","nan") else None
    except (ValueError, TypeError):
        return None
