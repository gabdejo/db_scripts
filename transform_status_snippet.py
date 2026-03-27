# src/pipeline/dim/bloomberg/security/transform.py
# ---------------------------------------------------------------
# Addition to transform_security_attributes:
# _extract_series_status_updates reads MARKET_STATUS from the
# pivoted BDP response and maps to registry status values.
# ---------------------------------------------------------------

# Add to SHARED_FIELD_MAP - MARKET_STATUS is a shared field
# requested for all security types in the BDP call
#
# In pipelines.yaml shared_fields, add:
#   - MARKET_STATUS
#
# In transform.py, add this function and wire into
# transform_security_attributes return dict:

from src.pipeline.dim.bloomberg.security.loaders.series_status import MARKET_STATUS_MAP
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def _extract_series_status_updates(wide: pd.DataFrame) -> pd.DataFrame:
    """
    Reads MARKET_STATUS from the pivoted BDP response.
    Maps Bloomberg status values to series_registry status values.
    Returns long-format DataFrame:
        [entity_id, bloomberg_status, registry_status]

    Only includes rows where MARKET_STATUS is present and recognised.
    """
    rows = []
    for _, row in wide.iterrows():
        bbg_status = row.get("MARKET_STATUS")
        if bbg_status is None or (isinstance(bbg_status, float) and pd.isna(bbg_status)):
            continue

        bbg_status = str(bbg_status).strip().upper()
        registry_status = MARKET_STATUS_MAP.get(bbg_status)

        if registry_status is None:
            logger.warning(
                f"Unknown MARKET_STATUS '{bbg_status}' for "
                f"entity_id={row['entity_id']}. Skipping."
            )
            continue

        rows.append({
            "entity_id":        row["entity_id"],
            "bloomberg_status": bbg_status,
            "registry_status":  registry_status,
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["entity_id", "bloomberg_status", "registry_status"]
    )


# Wire into transform_security_attributes return dict:
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
