# src/pipeline/prices/sbs/vector_completo/transform.py
# ---------------------------------------------------------------
# Transforms stg_prices_sbs_vector_completo into:
#   - facts: daily price observations -> fact_prices
#   - dims:  instrument metadata -> dim_security (partial update)
#
# FACT_FIELD_MAP defines all fact columns in the vector_completo
# file and their internal field codes. Series lookup uses
# (codigo_sbs, field) as the key, consistent with all other
# SBS pipeline transforms.
# ---------------------------------------------------------------

import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Maps stg column name -> internal field code in series_registry
FACT_FIELD_MAP = {
    "precio":    "PX_LAST",
    "variacion": "CHG_PRICE",
}


def transform(
    stg_df: pd.DataFrame,
    securities: list[dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Maps stg rows to (facts_df, dims_df).

    facts_df: [series_id, reference_date, value, source]
              one row per security per field per date
    dims_df:  [entity_id, tipo_instrumento, emisor, moneda]
              one row per security for dim_security partial update
    """
    if stg_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Build lookup: (codigo_sbs, field) -> series_id
    series_map = {
        (s["codigo_sbs"], s["field"]): s["series_id"]
        for s in securities
        if s.get("codigo_sbs")
    }

    # Build lookup: codigo_sbs -> entity_id
    entity_map = {
        s["codigo_sbs"]: s["entity_id"]
        for s in securities
        if s.get("codigo_sbs")
    }

    fact_rows = []
    dim_rows  = []

    for _, row in stg_df.iterrows():
        codigo = row.get("codigo_sbs")
        if not codigo:
            continue

        # Fact rows - one per field in FACT_FIELD_MAP
        for stg_col, field_name in FACT_FIELD_MAP.items():
            sid = series_map.get((codigo, field_name))
            if sid is None:
                continue
            val = _f(row.get(stg_col))
            if val is None:
                continue
            fact_rows.append({
                "series_id":      sid,
                "reference_date": row["reference_date"],
                "value":          val,
                "source":         "sbs",
            })

        # Dim row - slowly changing instrument attributes
        eid = entity_map.get(codigo)
        if eid:
            dim_rows.append({
                "entity_id":        eid,
                "tipo_instrumento": row.get("tipo_instrumento"),
                "emisor":           row.get("emisor"),
                "moneda":           row.get("moneda"),
            })

    facts_df = pd.DataFrame(fact_rows) if fact_rows else pd.DataFrame(
        columns=["series_id", "reference_date", "value", "source"]
    )
    dims_df = (
        pd.DataFrame(dim_rows).drop_duplicates(subset=["entity_id"])
        if dim_rows else pd.DataFrame()
    )

    logger.info(
        f"vector_completo transform: {len(facts_df)} fact rows, "
        f"{len(dims_df)} dim rows."
    )
    return facts_df, dims_df


def _f(val) -> float | None:
    try:
        return float(val) if val is not None and str(val).strip() not in ("", "nan") else None
    except (ValueError, TypeError):
        return None
