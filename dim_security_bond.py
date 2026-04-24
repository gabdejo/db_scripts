# src/pipeline/dim/bloomberg/security/loaders/dim_security_bond.py
# ---------------------------------------------------------------
# Upserts bond-specific attributes into dim_security_bond.
# Only called for rows where security_type = bond.
# ---------------------------------------------------------------

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def load_dim_security_bond(conn, df: pd.DataFrame) -> None:
    """
    Upserts bond extension attributes.
    Skips rows where security_id cannot be resolved from entity_id.
    """
    if df.empty:
        logger.info("dim_security_bond: nothing to load.")
        return

    updated = 0
    for _, row in df.iterrows():
        sec = conn.execute(
            "SELECT security_id FROM dim_security WHERE entity_id = ?",
            (row["entity_id"],),
        ).fetchone()
        if not sec:
            continue

        conn.execute(
            """
            INSERT INTO dim_security_bond (
                security_id, bond_type, issuer, maturity_date,
                coupon_rate, coupon_frequency, rating, seniority
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (security_id) DO UPDATE SET
                bond_type        = COALESCE(excluded.bond_type,        bond_type),
                issuer           = COALESCE(excluded.issuer,           issuer),
                maturity_date    = COALESCE(excluded.maturity_date,    maturity_date),
                coupon_rate      = COALESCE(excluded.coupon_rate,      coupon_rate),
                coupon_frequency = COALESCE(excluded.coupon_frequency, coupon_frequency),
                rating           = COALESCE(excluded.rating,           rating),
                seniority        = COALESCE(excluded.seniority,        seniority),
                updated_at       = datetime('now')
            """,
            (
                sec["security_id"],
                row.get("bond_type"),
                row.get("issuer"),
                row.get("maturity_date"),
                row.get("coupon_rate"),
                row.get("coupon_frequency"),
                row.get("rating"),
                row.get("seniority"),
            ),
        )
        updated += 1
    logger.info(f"dim_security_bond: {updated} rows upserted.")
