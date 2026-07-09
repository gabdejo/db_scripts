# src/pipeline/positions/fms/forwards/transform.py
# ---------------------------------------------------------------
# Two pure DataFrame transforms:
#
# transform_for_staging(raw_df) -> stg-shaped DataFrame
#   - Renames PascalCase FMS columns to snake_case staging columns
#   - Converts id_secuencial_fecha_proceso (int yyyymmdd) to date DATE
#   - Splits Tier 1 (typed columns) from Tier 2 (raw_payload JSONB dict)
#
# transform_for_fact(stg_df, portfolios) -> fact-shaped DataFrame
#   - Resolves codigo_fondo -> portfolio_id via dim_portfolio lookup
#   - Extracts fact-relevant columns from staging + raw_payload
#   - Converts id_secuencial_fecha_vencimiento (in raw_payload) to
#     fecha_vencimiento DATE
#   - Materializes source='fms' constant
#
# Both are pure functions. No DB access. Called by run.py.
# ---------------------------------------------------------------

import json
import logging
from datetime import date
from decimal import Decimal

import pandas as pd

logger = logging.getLogger(__name__)


# Tier 1 columns: FMS PascalCase -> stg_positions_fms_forwards snake_case
STAGING_COLUMN_MAP = {
    "IdSecuencialFechaProceso":     "id_secuencial_fecha_proceso",
    "CodigoFondo":                  "codigo_fondo",
    "CodigoSbs":                    "codigo_sbs",
    "CodigoIsoMonedaNocional":      "codigo_iso_moneda_nocional",
    "CodigoIsoMonedaContraparte":   "codigo_iso_moneda_contraparte",
    "ValorNocional":                "valor_nocional",
    "TipoCambioSpot":               "tipo_cambio_spot",
    "NocionalSoles":                "nocional_soles",
    "MonedaCompra":                 "moneda_compra",
    "MonedaVenta":                  "moneda_venta",
}

# Everything not in STAGING_COLUMN_MAP goes into raw_payload JSONB.
# Enumerated explicitly to fail loudly if FMS adds columns we don't know about.
TIER_2_COLUMNS = [
    "CodigoReferencia",
    "IdTipoOperacion",
    "IdSecuencialFechaForwardPrecio",
    "IdSecuencialFechaOperacion",
    "IdSecuencialFechaVencimiento",
    "Remanente",
    "ValorStrike",
    "PrecioForward",
    "PrecioVector",
    "PrecioInversion",
    "PrecioDesinversion",
    "IndCxcCxp",
    "MonedaNocional",
    "MonedaContraparte",
    "Importe",
    "TipoMovimiento",
    "IdCuentaCobrarPagar",
    "ValorNocionalCarga",
    "PrecioInversionCarga",
    "PrecioDesinversionCarga",
]


def transform_for_staging(raw_df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """
    Normalize a raw FMS forwards DataFrame to staging shape.

    Returns a DataFrame matching stg_positions_fms_forwards columns:
    batch_id, id_secuencial_fecha_proceso, date, codigo_fondo,
    codigo_sbs, codigo_iso_moneda_nocional, codigo_iso_moneda_contraparte,
    valor_nocional, tipo_cambio_spot, nocional_soles, moneda_compra,
    moneda_venta, raw_payload (dict, JSON-serializable).

    Empty DataFrame in => empty DataFrame out.
    """
    if raw_df.empty:
        return pd.DataFrame()

    _validate_expected_columns(raw_df)

    stg = pd.DataFrame({
        stg_col: raw_df[fms_col]
        for fms_col, stg_col in STAGING_COLUMN_MAP.items()
    })

    stg["batch_id"] = batch_id
    stg["date"] = stg["id_secuencial_fecha_proceso"].map(_yyyymmdd_to_date)
    stg["raw_payload"] = raw_df.apply(_build_raw_payload, axis=1)

    logger.info(f"transform_for_staging: {len(stg)} rows shaped for stg_positions_fms_forwards")
    return stg


def transform_for_fact(stg_df: pd.DataFrame, portfolios: pd.DataFrame) -> pd.DataFrame:
    """
    Convert staging-shaped DataFrame to fact-shaped DataFrame.

    portfolios: DataFrame with (procode, portfolio_id) columns, filtered
    to dim_portfolio where source='fms'. Loaded once per run and passed in.

    Rows whose codigo_fondo can't be resolved to a portfolio_id are
    dropped with a WARNING - they end up as unresolved. Ops decides
    whether to register the missing portfolio and re-run --from-stg,
    or ignore.

    Returns a DataFrame matching fact_positions_forwards columns.
    """
    if stg_df.empty:
        return pd.DataFrame()

    # Portfolio resolution
    pmap = dict(zip(portfolios["procode"], portfolios["portfolio_id"]))
    resolved = stg_df["codigo_fondo"].map(pmap)
    unresolved_mask = resolved.isna()
    if unresolved_mask.any():
        missing = stg_df.loc[unresolved_mask, "codigo_fondo"].unique().tolist()
        logger.warning(
            f"transform_for_fact: dropping {int(unresolved_mask.sum())} rows with "
            f"unresolved codigo_fondo: {missing}"
        )
    stg_df = stg_df.loc[~unresolved_mask].copy()
    stg_df["portfolio_id"] = resolved.loc[~unresolved_mask].astype(int)

    # Pull fact-relevant Tier 2 fields out of raw_payload
    stg_df["fecha_vencimiento"] = stg_df["raw_payload"].map(
        lambda p: _yyyymmdd_to_date(p.get("IdSecuencialFechaVencimiento"))
        if p and p.get("IdSecuencialFechaVencimiento") else None
    )
    stg_df["precio_forward"] = stg_df["raw_payload"].map(lambda p: p.get("PrecioForward") if p else None)
    stg_df["valor_strike"]   = stg_df["raw_payload"].map(lambda p: p.get("ValorStrike")   if p else None)
    stg_df["mtm_soles"]      = stg_df["raw_payload"].map(lambda p: p.get("PrecioVector")  if p else None)

    fact = stg_df[[
        "portfolio_id",
        "codigo_sbs",
        "date",
        "codigo_iso_moneda_nocional",
        "valor_nocional",
        "tipo_cambio_spot",
        "nocional_soles",
        "moneda_compra",
        "moneda_venta",
        "fecha_vencimiento",
        "precio_forward",
        "valor_strike",
        "mtm_soles",
    ]].copy()
    fact["source"] = "fms"

    logger.info(f"transform_for_fact: {len(fact)} rows shaped for fact_positions_forwards")
    return fact


def _validate_expected_columns(raw_df: pd.DataFrame) -> None:
    expected = set(STAGING_COLUMN_MAP.keys()) | set(TIER_2_COLUMNS)
    actual = set(raw_df.columns)
    missing = expected - actual
    unexpected = actual - expected
    if missing:
        raise ValueError(f"FMS forwards query missing expected columns: {sorted(missing)}")
    if unexpected:
        logger.warning(
            f"FMS forwards query returned unexpected columns (will land in raw_payload): "
            f"{sorted(unexpected)}"
        )


def _build_raw_payload(row: pd.Series) -> dict:
    """
    Build the raw_payload JSONB dict from Tier 2 columns of one row.
    Values are coerced to JSON-serializable primitives.
    """
    payload = {}
    for col in TIER_2_COLUMNS:
        if col in row.index:
            payload[col] = _to_json_value(row[col])
    # Catch any unexpected columns too
    for col in row.index:
        if col not in STAGING_COLUMN_MAP and col not in TIER_2_COLUMNS:
            payload[col] = _to_json_value(row[col])
    return payload


def _to_json_value(v):
    """Coerce a pandas cell value to a JSON-serializable primitive."""
    if pd.isna(v):
        return None
    if isinstance(v, Decimal):
        return float(v)
    if hasattr(v, "isoformat"):  # date/datetime
        return v.isoformat()
    if hasattr(v, "item"):  # numpy scalar
        return v.item()
    return v


def _yyyymmdd_to_date(v) -> date | None:
    """Convert an int like 20260622 to date(2026, 6, 22)."""
    if v is None or pd.isna(v):
        return None
    n = int(v)
    return date(n // 10000, (n // 100) % 100, n % 100)
