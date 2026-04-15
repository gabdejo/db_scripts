# src/pipeline/prices/sbs/rf_exterior/transform.py
import logging
import pandas as pd

logger = logging.getLogger(__name__)

FACT_FIELD_MAP = {
    "precio_limpio_monto": "PX_CLEAN_MNT", "precio_limpio_pct": "PX_CLEAN_PCT",
    "precio_sucio_monto": "PX_DIRTY_MNT", "precio_sucio_pct": "PX_DIRTY_PCT",
    "interes_corrido_monto": "ACCRUED_INT", "variacion_precio_sucio": "CHG_DIRTY",
}

DIM_BOND_MAP = {
    "tipo_instrumento": "bond_type", "emisor": "issuer", "moneda": "currency",
    "valor_facial": "face_value", "fecha_emision": "issue_date",
    "fecha_vencimiento": "maturity_date", "tasa_cupon": "coupon_rate",
    "ultimo_cupon": "last_coupon_date", "proximo_cupon": "next_coupon_date",
}


def transform(stg_df, securities):
    if stg_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    series_map = {(s["codigo_sbs"], s["field"]): s["series_id"] for s in securities if s.get("codigo_sbs")}
    entity_map = {s["codigo_sbs"]: s["entity_id"] for s in securities if s.get("codigo_sbs")}
    fact_rows = []
    dim_rows  = []
    for _, row in stg_df.iterrows():
        codigo = row.get("codigo_sbs")
        if not codigo:
            continue
        for stg_col, field_name in FACT_FIELD_MAP.items():
            sid = series_map.get((codigo, field_name))
            if sid is None:
                continue
            val = _f(row.get(stg_col))
            if val is None:
                continue
            fact_rows.append({"series_id": sid, "reference_date": row["reference_date"], "value": val, "source": "sbs"})
        eid = entity_map.get(codigo)
        if eid:
            dim_row = {"entity_id": eid}
            for stg_col, dim_col in DIM_BOND_MAP.items():
                dim_row[dim_col] = row.get(stg_col)
            dim_rows.append(dim_row)
    facts_df = pd.DataFrame(fact_rows) if fact_rows else pd.DataFrame(columns=["series_id","reference_date","value","source"])
    dims_df  = pd.DataFrame(dim_rows).drop_duplicates(subset=["entity_id"]) if dim_rows else pd.DataFrame()
    logger.info(f"rf_exterior transform: {len(facts_df)} fact rows, {len(dims_df)} dim rows.")
    return facts_df, dims_df


def _f(val):
    try:
        return float(val) if val is not None and str(val).strip() not in ("","nan") else None
    except (ValueError, TypeError):
        return None
