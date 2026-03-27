# src/pipeline/dim/bloomberg/security/transform.py
# ---------------------------------------------------------------
# Additions to transform.py:
#
# 1. Add MARKET_STATUS and EXCH_MARKET_STATUS to SHARED_FIELD_MAP
#    (or leave them out of the map and handle in the extractor below)
#
# 2. Add _extract_series_status_updates function
#
# 3. Wire into transform_security_attributes return dict
#
# Also update pipelines.yaml shared_fields:
#   - MARKET_STATUS
#   - EXCH_MARKET_STATUS
# ---------------------------------------------------------------

from src.pipeline.dim.bloomberg.security.loaders.series_status import (
    MARKET_STATUS_MAP,
    EXCH_MARKET_STATUS_MAP,
    _FALLBACK_TRIGGER,
    _clean,
)
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def _extract_series_status_updates(wide: pd.DataFrame) -> pd.DataFrame:
    """
    Resolves series_registry status from MARKET_STATUS and
    EXCH_MARKET_STATUS columns in the pivoted BDP response.

    Resolution logic:
      1. MARKET_STATUS present and not in _FALLBACK_TRIGGER
         -> use MARKET_STATUS_MAP (authoritative)
      2. MARKET_STATUS missing or in _FALLBACK_TRIGGER (e.g. PRNA)
         -> fall back to EXCH_MARKET_STATUS_MAP
         -> covers futures, FX, and PRNA cases
      3. Both missing or unrecognised
         -> skip row, log warning, leave status unchanged

    Returns DataFrame:
        [entity_id, bloomberg_status, registry_status, resolved_from]
    """
    rows = []

    for _, row in wide.iterrows():
        mkt_status  = _clean(row.get("MARKET_STATUS"))
        exch_status = _clean(row.get("EXCH_MARKET_STATUS"))

        registry_status = None
        resolved_from   = None

        # Step 1: try MARKET_STATUS
        if mkt_status and mkt_status not in _FALLBACK_TRIGGER:
            registry_status = MARKET_STATUS_MAP.get(mkt_status)
            if registry_status:
                resolved_from = "MARKET_STATUS"

        # Step 2: fall back to EXCH_MARKET_STATUS
        if registry_status is None and exch_status:
            registry_status = EXCH_MARKET_STATUS_MAP.get(exch_status)
            if registry_status:
                resolved_from = "EXCH_MARKET_STATUS"

        # Step 3: both missing or unrecognised
        if registry_status is None:
            logger.warning(
                f"entity_id={row['entity_id']}: could not resolve status "
                f"from MARKET_STATUS={mkt_status!r} / "
                f"EXCH_MARKET_STATUS={exch_status!r}. Status unchanged."
            )
            continue

        rows.append({
            "entity_id":        row["entity_id"],
            "bloomberg_status": mkt_status or exch_status,
            "registry_status":  registry_status,
            "resolved_from":    resolved_from,
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["entity_id", "bloomberg_status",
                 "registry_status", "resolved_from"]
    )


# Wire into transform_security_attributes:
#
# def transform_security_attributes(stg_df, conn):
#     ...
#     return {
#         "dim_security":           _map_shared(wide),
#         "dim_security_equity":    _map_equity(wide),
#         "dim_security_fund":      _map_fund(wide),
#         "dim_security_bond":      _map_bond(wide),
#         "dim_entity_identifiers": _map_identifiers(wide),
#         "series_status_updates":  _extract_series_status_updates(wide),
#     }
