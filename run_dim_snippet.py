# src/pipeline/dim/bloomberg/security/run.py
# ---------------------------------------------------------------
# Updated run() sequence showing where series status loader fits.
# Add this import at the top of run.py:
#
# from src.pipeline.dim.bloomberg.security.loaders.series_status import (
#     load_series_status_from_enrichment,
# )
#
# Full step sequence in run():
# ---------------------------------------------------------------

# Step 1: resolve securities
# with get_connection() as conn:
#     unique_securities = get_registered_securities(conn)

# Step 2: extract - one BDP call for all fields including
#         MARKET_STATUS and EXCH_MARKET_STATUS
# raw_df = extract_security_attributes(unique_securities, all_fields)

# Step 3: stage
# with get_connection() as conn:
#     load_stg_security_bloomberg(conn, raw_df)

# Step 4: transform - splits into all target DataFrames
# with get_connection() as conn:
#     stg_df = read_latest_stg_security_bloomberg(conn)
#     transformed = transform_security_attributes(stg_df, conn)

# Step 5a: dim_security (parent first - extension tables FK to this)
# with get_connection() as conn:
#     load_dim_security(conn, transformed.get("dim_security"))

# Step 5b: extension tables
# with get_connection() as conn:
#     load_dim_security_equity(conn, transformed.get("dim_security_equity"))
#     load_dim_security_fund(conn,   transformed.get("dim_security_fund"))
#     load_dim_security_bond(conn,   transformed.get("dim_security_bond"))

# Step 5c: identifiers
# with get_connection() as conn:
#     load_identifiers_from_enrichment(
#         conn, transformed.get("dim_entity_identifiers")
#     )

# Step 5d: series status  <- final step, depends on series_registry
#                            which already exists from bootstrap
# with get_connection() as conn:
#     load_series_status_from_enrichment(
#         conn,
#         transformed.get("series_status_updates", pd.DataFrame())
#     )

# logger.info("=== Dim enrichment: bloomberg/security complete ===")
