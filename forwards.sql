-- src/pipeline/positions/fms/queries/forwards.sql
-- ---------------------------------------------------------------
-- FMS extraction query: forwards feed.
--
-- Returns: one row per forward contract per fund per business
-- date, with both legs encoded via MonedaCompra/MonedaVenta and
-- IdTipoOperacion driving the leg assignment.
--
-- Parameters (positional, pyodbc ? style):
--   1) start_date  INT yyyymmdd, inclusive lower bound
--   2) end_date    INT yyyymmdd, inclusive upper bound
--
-- For single-day runs, pass the same date twice.
-- Range size validation belongs in extract.py, not here.
--
-- Target staging:    stg_positions_fms_forwards
-- Grain:             (CodigoFondo, CodigoSbs,
--                     IdSecuencialFechaProceso)
--
-- Column naming: PascalCase here matches the FMS source schema.
-- Python-side renames them to snake_case before binding to the
-- staging insert (see FMS_FORWARDS_COLUMN_MAP in extract.py).
--
-- Tier 1 vs Tier 2 split:
--   - Tier 1 (top of SELECT): materialized as typed columns on
--     staging; includes the three derived fields (NocionalSoles,
--     MonedaCompra, MonedaVenta) computed inline here so the
--     business-meaning interpretation is encoded once, at extract.
--   - Tier 2 (lower in SELECT): preserved verbatim in
--     raw_payload JSONB. Promote to Tier 1 when filter/join usage
--     justifies it. CodigoReferencia lives here — it's a forward
--     contract id used internally by FMS but not the staging grain.
-- ---------------------------------------------------------------

SELECT
    -- ===================== Tier 1 =====================
    fp.IdSecuencialFechaProceso,
    fond.CodigoFondo,
    fp.CodigoSbs,
    fp.CodigoIsoMonedaNocional,
    fp.CodigoIsoMonedaContraparte,
    fp.ValorNocional,
    fp.TipoCambioSpot,
    fp.ValorNocional * fp.TipoCambioSpot                       AS NocionalSoles,
    CASE
        WHEN fp.IdTipoOperacion = 1 THEN fp.CodigoIsoMonedaNocional
        ELSE fp.CodigoIsoMonedaContraparte
    END                                                        AS MonedaCompra,
    CASE
        WHEN fp.IdTipoOperacion = 1 THEN fp.CodigoIsoMonedaContraparte
        ELSE fp.CodigoIsoMonedaNocional
    END                                                        AS MonedaVenta,

    -- ===================== Tier 2 =====================
    fp.CodigoReferencia,
    fp.IdTipoOperacion,
    fp.IdSecuencialFechaForwardPrecio,
    fp.IdSecuencialFechaOperacion,
    fp.IdSecuencialFechaVencimiento,
    fp.Remanente,
    fp.ValorStrike,
    fp.PrecioForward,
    fp.PrecioVector,
    fp.PrecioInversion,
    fp.PrecioDesinversion,
    fp.IndCxcCxp,
    fp.MonedaNocional,
    fp.MonedaContraparte,
    fp.Importe,
    fp.TipoMovimiento,
    fp.IdCuentaCobrarPagar,
    fp.ValorNocionalCarga,
    fp.PrecioInversionCarga,
    fp.PrecioDesinversionCarga
FROM FMS.ForwardPrecio fp
JOIN FMS.FondoPension fond
    ON fp.IdFondo = fond.IdFondo
WHERE fp.IdSecuencialFechaProceso BETWEEN ? AND ?;
