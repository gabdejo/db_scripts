# src/pipeline/dim/bloomberg/security/run.py
# ---------------------------------------------------------------
# Addition to run():
# Load series status updates after all dim loaders.
# ---------------------------------------------------------------

# Add import at top of run.py:
# from src.pipeline.dim.bloomberg.security.loaders.series_status import (
#     load_series_status_from_enrichment,
# )

# Add as final step in run() after dim loaders:
#
# with get_connection() as conn:
#     load_series_status_from_enrichment(
#         conn,
#         transformed.get("series_status_updates", pd.DataFrame())
#     )
#
# logger.info("=== Dim enrichment: bloomberg/security complete ===")

# Full updated run() sequence:
#
# Step 5a: load dim_security (parent table first)
# with get_connection() as conn:
#     load_dim_security(conn, transformed.get("dim_security"))
#
# Step 5b: load extension tables
# with get_connection() as conn:
#     load_dim_security_equity(conn, transformed.get("dim_security_equity"))
#     load_dim_security_fund(conn, transformed.get("dim_security_fund"))
#     load_dim_security_bond(conn, transformed.get("dim_security_bond"))
#
# Step 5c: load identifiers
# with get_connection() as conn:
#     load_identifiers_from_enrichment(conn, transformed.get("dim_entity_identifiers"))
#
# Step 5d: update series status   ← new
# with get_connection() as conn:
#     load_series_status_from_enrichment(
#         conn, transformed.get("series_status_updates", pd.DataFrame())
#     )
