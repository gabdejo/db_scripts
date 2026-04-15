# src/pipeline/prices/sbs/rf_exterior/loader.py
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def load_facts(conn, df: pd.DataFrame) -> tuple[int, int]:
    if df.empty:
        return 0, 0
    loaded = skipped = 0
    for _, row in df.iterrows():
        conn.execute(
            "INSERT INTO fact_prices (series_id, reference_date, value, source) VALUES (?,?,?,?) ON CONFLICT (series_id, reference_date) DO NOTHING",
            (int(row["series_id"]), row["reference_date"], float(row["value"]), row["source"]),
        )
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            loaded += 1
        else:
            skipped += 1
    logger.info(f"fact_prices (rf_exterior): {loaded} loaded, {skipped} skipped.")
    return loaded, skipped


def load_bond_dims(conn, df: pd.DataFrame) -> None:
    """
    Upserts bond attributes into dim_security_bond.
    COALESCE ensures existing values are never overwritten by None.
    updated_at changes only when rating or maturity_date changes.
    """
    if df.empty:
        return
    updated = 0
    for _, row in df.iterrows():
        sec = conn.execute("SELECT security_id FROM dim_security WHERE entity_id = ?", (int(row["entity_id"]),)).fetchone()
        if not sec:
            continue
        conn.execute(
            """
            INSERT INTO dim_security_bond (security_id, bond_type, issuer, maturity_date, coupon_rate, rating, seniority)
            VALUES (?, ?, ?, ?, ?, ?, NULL)
            ON CONFLICT (security_id) DO UPDATE SET
                bond_type     = COALESCE(excluded.bond_type,     bond_type),
                issuer        = COALESCE(excluded.issuer,        issuer),
                maturity_date = COALESCE(excluded.maturity_date, maturity_date),
                coupon_rate   = COALESCE(excluded.coupon_rate,   coupon_rate),
                rating        = COALESCE(excluded.rating,        rating),
                updated_at    = CASE
                    WHEN excluded.rating != rating OR excluded.maturity_date != maturity_date
                    THEN datetime('now') ELSE updated_at END
            """,
            (sec["security_id"], row.get("bond_type"), row.get("issuer"),
             row.get("maturity_date"), row.get("coupon_rate"), row.get("credit_rating")),
        )
        updated += 1
    logger.info(f"dim_security_bond (rf_exterior): {updated} rows processed.")
