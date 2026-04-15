# src/pipeline/prices/sbs/vector_completo/transform.py
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def transform(stg_df: pd.DataFrame, securities: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Maps stg rows to (facts_df, dims_df).
    facts_df: [series_id, reference_date, value, source]
    dims_df:  [entity_id, tipo_instrumento, emisor, moneda]
    """
    if stg_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    sbs_map = {s["codigo_sbs"]: s for s in securities if s.get("codigo_sbs")}
    fact_rows = []
    dim_rows  = []

    for _, row in stg_df.iterrows():
        sec = sbs_map.get(row.get("codigo_sbs"))
        if not sec:
            continue
        precio = _f(row.get("precio"))
        if precio is not None:
            fact_rows.append({
                "series_id":      sec["series_id"],
                "reference_date": row["reference_date"],
                "value":          precio,
                "source":         "sbs",
            })
        dim_rows.append({
            "entity_id":        sec["entity_id"],
            "tipo_instrumento": row.get("tipo_instrumento"),
            "emisor":           row.get("emisor"),
            "moneda":           row.get("moneda"),
        })

    facts_df = pd.DataFrame(fact_rows) if fact_rows else pd.DataFrame(columns=["series_id","reference_date","value","source"])
    dims_df  = pd.DataFrame(dim_rows).drop_duplicates(subset=["entity_id"]) if dim_rows else pd.DataFrame()
    logger.info(f"vector_completo transform: {len(facts_df)} fact rows, {len(dims_df)} dim rows.")
    return facts_df, dims_df


def _f(val):
    try:
        return float(val) if val is not None and str(val).strip() not in ("","nan") else None
    except (ValueError, TypeError):
        return None
