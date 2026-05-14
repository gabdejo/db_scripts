# src/pipeline/positions/fms/extract.py
# ---------------------------------------------------------------
# Pulls holdings from FMS sprocs and returns a staging DataFrame.
#
# One sproc call per portfolio (account_code). Failures on a
# single portfolio are logged and skipped — they don't poison
# the rest of the run. The orchestrator (run.py) is responsible
# for writing the returned frame to stg_positions_fms.
#
# SPROC_NAME is the holdings sproc; KNOWN_COLUMNS lists the
# stg columns we explicitly map. Anything else the sproc returns
# is preserved in raw_payload as JSON for forensics.
#
# Sproc signature assumed:
#   EXEC dbo.usp_get_holdings @account_code = ?, @as_of_date = ?
# Adapt SPROC_NAME and the column rename map below to match.
# ---------------------------------------------------------------

import json
import logging
from datetime import date

import pandas as pd

from src.vendors.fms import call_sproc

logger = logging.getLogger(__name__)


SPROC_NAME = "dbo.usp_get_holdings"  # TODO: replace with real sproc name

# stg column -> tuple of acceptable sproc column names (first match wins)
COLUMN_ALIASES = {
    "as_of_date":        ("as_of_date", "fecha_corte", "reference_date"),
    "account_code":      ("account_code", "cuenta", "portfolio_code"),
    "instrument_id":     ("instrument_id", "inst_id", "codigo_instrumento"),
    "isin":              ("isin",),
    "ticker":            ("ticker", "bloomberg_ticker"),
    "description":       ("description", "instrument_name", "nombre"),
    "quantity":          ("quantity", "nominal", "cantidad"),
    "market_value":      ("market_value", "mv", "valor_mercado"),
    "cost_basis":        ("cost_basis", "costo"),
    "accrued_interest":  ("accrued_interest", "accrued", "interes_devengado"),
    "currency":          ("currency", "ccy", "moneda"),
    "price_used":        ("price_used", "price", "precio"),
    "yield_to_maturity": ("yield_to_maturity", "ytm", "rendimiento"),
    "duration":          ("duration", "duracion"),
}


def extract(
    portfolios: list[dict],
    as_of: date,
    batch_id: str,
) -> pd.DataFrame:
    """
    Calls the holdings sproc once per portfolio and returns a
    single staging DataFrame keyed by batch_id.

    portfolios: [{'portfolio_id': int, 'internal_code': str, 'status': str}, ...]
                Caller pre-filters to active + backfill-pending.
    as_of:      reference date for the holdings snapshot.
    batch_id:   shared across all rows of this run, e.g. 'fms_20260514_081532'.

    Returns DataFrame with columns matching stg_positions_fms.
    Empty DataFrame if no portfolios or all sprocs failed.
    """
    if not portfolios:
        logger.info("no FMS portfolios to extract")
        return pd.DataFrame()

    all_rows: list[dict] = []
    failed: list[str] = []

    for pf in portfolios:
        code = pf["internal_code"]
        try:
            raw_rows = call_sproc(SPROC_NAME, params=(code, as_of))
        except Exception:
            logger.exception(f"sproc failed for portfolio {code}; skipping")
            failed.append(code)
            continue

        for raw in raw_rows:
            all_rows.append(_normalize_row(raw, code, as_of, batch_id))

        logger.info(f"extracted {len(raw_rows)} rows for portfolio {code}")

    if failed:
        logger.warning(
            f"FMS extract: {len(failed)} portfolio(s) failed: {failed}"
        )

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    logger.info(
        f"FMS extract: {len(df)} total rows from {len(portfolios) - len(failed)} portfolios"
    )
    return df


def _normalize_row(
    raw: dict,
    account_code: str,
    as_of: date,
    batch_id: str,
) -> dict:
    """
    Maps a sproc row to a stg_positions_fms-shaped dict.
    Tries each alias in COLUMN_ALIASES; unmapped keys land in raw_payload.
    """
    out = {"batch_id": batch_id, "raw_payload": json.dumps(raw, default=str, ensure_ascii=False)}

    for stg_col, aliases in COLUMN_ALIASES.items():
        out[stg_col] = _first_present(raw, aliases)

    # Defensive defaults if the sproc omitted these
    if not out.get("as_of_date"):
        out["as_of_date"] = as_of
    if not out.get("account_code"):
        out["account_code"] = account_code

    # Strip strings so blank-padded fields don't break downstream lookups
    for col in ("isin", "ticker", "currency"):
        v = out.get(col)
        if isinstance(v, str):
            out[col] = v.strip() or None

    return out


def _first_present(row: dict, keys: tuple[str, ...]):
    """Returns row[k] for the first k in keys that's present and non-empty."""
    for k in keys:
        if k in row:
            v = row[k]
            if v is not None and (not isinstance(v, str) or v.strip() != ""):
                return v
    return None
