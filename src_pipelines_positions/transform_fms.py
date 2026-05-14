# src/pipeline/positions/fms/transform.py
# ---------------------------------------------------------------
# Transforms stg_positions_fms into:
#   - facts:       resolved holdings -> fact_positions
#   - unresolved:  rows whose security or portfolio couldn't be
#                  identified, for ops review
#
# Resolution strategy per row (security side):
#   1) ISIN match in dim_entity_identifiers
#   2) Bloomberg ticker match
#   3) FMS internal code match (id_type='codigo_fms')
#   4) Cashlike fallback: no security ids but has a currency ->
#      synthetic per-currency cash entity (id_type='currency_cash')
#
# Portfolio resolution is by (internal_code, source='fms') against
# dim_portfolio. Identifier lookups are loaded in bulk once per
# transform call to avoid per-row queries.
#
# Weights are computed post-resolution as market_value / sum(MV)
# per (portfolio_id, as_of_date).
# ---------------------------------------------------------------

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def transform(
    stg_df: pd.DataFrame,
    portfolios: list[dict],
    identifiers: dict[str, dict[str, int]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Maps stg rows to (facts_df, unresolved_df).

    facts_df:      [portfolio_id, security_entity_id, as_of_date, source,
                    quantity, market_value, cost_basis, accrued_interest,
                    weight, price_used, currency, yield_to_maturity, duration]
                   one row per holding per portfolio per date.

    unresolved_df: [account_code, instrument_id, isin, ticker, description, reason]
                   rows we couldn't place into the fact table.

    portfolios:    [{'internal_code': str, 'portfolio_id': int}, ...]
                   pre-filtered to source='fms' in the caller.

    identifiers:   {
                       'isin':            {value: entity_id, ...},
                       'bloomberg_ticker':{value: entity_id, ...},
                       'codigo_fms':      {value: entity_id, ...},
                       'currency_cash':   {ccy:   entity_id, ...},
                   }
                   loaded in bulk by the caller (run.py).
    """
    if stg_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Build lookup: account_code -> portfolio_id
    portfolio_map = {
        p["internal_code"]: p["portfolio_id"]
        for p in portfolios
        if p.get("internal_code")
    }

    isin_map     = identifiers.get("isin", {})
    ticker_map   = identifiers.get("bloomberg_ticker", {})
    fms_code_map = identifiers.get("codigo_fms", {})
    cash_map     = identifiers.get("currency_cash", {})

    fact_rows       = []
    unresolved_rows = []

    for _, row in stg_df.iterrows():
        account_code = row.get("account_code")
        pid = portfolio_map.get(account_code)
        if pid is None:
            unresolved_rows.append(_unresolved(row, f"portfolio not in dim_portfolio: {account_code!r}"))
            continue

        eid, reason = _resolve_security(row, isin_map, ticker_map, fms_code_map, cash_map)
        if eid is None:
            unresolved_rows.append(_unresolved(row, reason))
            continue

        fact_rows.append({
            "portfolio_id":       pid,
            "security_entity_id": eid,
            "as_of_date":         row["as_of_date"],
            "source":             "fms",
            "quantity":           _f(row.get("quantity")),
            "market_value":       _f(row.get("market_value")),
            "cost_basis":         _f(row.get("cost_basis")),
            "accrued_interest":   _f(row.get("accrued_interest")),
            "weight":             None,  # filled in below
            "price_used":         _f(row.get("price_used")),
            "currency":           row.get("currency"),
            "yield_to_maturity":  _f(row.get("yield_to_maturity")),
            "duration":           _f(row.get("duration")),
        })

    facts_df = pd.DataFrame(fact_rows) if fact_rows else pd.DataFrame(
        columns=[
            "portfolio_id", "security_entity_id", "as_of_date", "source",
            "quantity", "market_value", "cost_basis", "accrued_interest",
            "weight", "price_used", "currency", "yield_to_maturity", "duration",
        ]
    )

    if not facts_df.empty:
        _attach_weights(facts_df)

    unresolved_df = pd.DataFrame(unresolved_rows) if unresolved_rows else pd.DataFrame()

    logger.info(
        f"FMS transform: {len(facts_df)} fact rows, {len(unresolved_df)} unresolved."
    )
    if not unresolved_df.empty:
        logger.warning(
            f"unresolved sample (first 5):\n{unresolved_df.head(5).to_string(index=False)}"
        )

    return facts_df, unresolved_df


def _resolve_security(
    row: pd.Series,
    isin_map: dict[str, int],
    ticker_map: dict[str, int],
    fms_code_map: dict[str, int],
    cash_map: dict[str, int],
) -> tuple[int | None, str]:
    """Returns (entity_id, reason). reason is '' on success."""
    isin = (row.get("isin") or "").strip() if row.get("isin") else ""
    if isin:
        eid = isin_map.get(isin)
        if eid is not None:
            return eid, ""

    ticker = (row.get("ticker") or "").strip() if row.get("ticker") else ""
    if ticker:
        eid = ticker_map.get(ticker)
        if eid is not None:
            return eid, ""

    inst_id = row.get("instrument_id")
    if inst_id:
        eid = fms_code_map.get(str(inst_id).strip())
        if eid is not None:
            return eid, ""

    # Cashlike fallback
    if not isin and not ticker and row.get("currency"):
        ccy = str(row["currency"]).strip()
        eid = cash_map.get(ccy)
        if eid is not None:
            return eid, ""
        return None, f"cash entity missing for currency={ccy!r}"

    return None, "no identifier matched (isin/ticker/instrument_id)"


def _attach_weights(facts_df: pd.DataFrame) -> None:
    """In-place: weight = market_value / sum(MV) per (portfolio_id, as_of_date)."""
    totals = (
        facts_df.groupby(["portfolio_id", "as_of_date"])["market_value"]
        .transform("sum")
    )
    mask = (totals.notna()) & (totals != 0)
    facts_df.loc[mask, "weight"] = (
        facts_df.loc[mask, "market_value"] / totals[mask]
    )


def _unresolved(row: pd.Series, reason: str) -> dict:
    return {
        "account_code":  row.get("account_code"),
        "instrument_id": row.get("instrument_id"),
        "isin":          row.get("isin"),
        "ticker":        row.get("ticker"),
        "description":   row.get("description"),
        "reason":        reason,
    }


def _f(val) -> float | None:
    try:
        return float(val) if val is not None and str(val).strip() not in ("", "nan") else None
    except (ValueError, TypeError):
        return None
