-- src/db/schema/stg_positions_fms.sql
-- ---------------------------------------------------------------
-- stg_positions_fms: raw landing for FMS sproc output.
--
-- Permissive types, no FKs. Each run tags its rows with batch_id
-- (e.g. 'fms_20260514_081532') so transform can scope to its own
-- batch and so we can replay or audit historical loads.
--
-- raw_payload holds the full sproc row as JSON. If the sproc adds
-- a new column we don't yet map in extract.py, it survives here
-- and we can backfill from staging without re-hitting FMS.
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stg_positions_fms (
    batch_id          TEXT      NOT NULL,         -- e.g. 'fms_20260514_081532'
    extracted_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    as_of_date        DATE,
    account_code      TEXT,                       -- FMS internal account id
    instrument_id     TEXT,                       -- FMS internal instrument id
    isin              TEXT,
    ticker            TEXT,
    description       TEXT,
    quantity          NUMERIC,
    market_value      NUMERIC,
    cost_basis        NUMERIC,
    accrued_interest  NUMERIC,
    currency          TEXT,
    price_used        NUMERIC,
    yield_to_maturity NUMERIC,
    duration          NUMERIC,
    raw_payload       TEXT                        -- JSON of the full sproc row, for audit
);

CREATE INDEX IF NOT EXISTS ix_stg_positions_fms_batch ON stg_positions_fms(batch_id);
CREATE INDEX IF NOT EXISTS ix_stg_positions_fms_asof  ON stg_positions_fms(as_of_date);
