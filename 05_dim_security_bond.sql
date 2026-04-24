-- 05_dim_security_bond.sql
-- =============================================================
-- dim_security_bond
-- Bond-specific extension of dim_security.
-- Populated by:
--   - Bloomberg dim enrichment (MATURITY, CPN, RTG_SP, DEBT_SEN, etc.)
--   - SBS rf_local and rf_exterior pipelines (rating, maturity,
--     coupon dates, via daily file side effect)
--
-- SCD Type 1: attributes overwritten in place via COALESCE upserts.
-- updated_at changes only when rating or maturity_date changes.
-- =============================================================

CREATE TABLE IF NOT EXISTS dim_security_bond (
    security_id         INTEGER PRIMARY KEY
                        REFERENCES dim_security (security_id),

    -- Instrument classification
    bond_type           TEXT    CHECK (bond_type IN (
                            'sovereign', 'corporate', 'municipal',
                            'agency', 'supranational', NULL
                        )),
    issuer              TEXT,
    seniority           TEXT    CHECK (seniority IN (
                            'senior', 'subordinated', 'junior', NULL
                        )),

    -- Cash flow structure
    maturity_date       TEXT,                   -- YYYY-MM-DD
    coupon_rate         REAL,
    coupon_frequency    TEXT    CHECK (coupon_frequency IN (
                            'annual', 'semi-annual', 'quarterly', 'zero', NULL
                        )),
    libor_margin        REAL,                   -- for floating rate bonds
    last_coupon_date    TEXT,                   -- YYYY-MM-DD
    next_coupon_date    TEXT,                   -- YYYY-MM-DD

    -- Credit
    rating              TEXT,                   -- e.g. AAA, BBB+, NR

    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT    NOT NULL DEFAULT (datetime('now'))
);
