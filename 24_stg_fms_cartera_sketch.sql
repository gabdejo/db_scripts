SELECT
    -- Tier 1
    fp.IdSecuencialFechaProceso, -- int
    fond.CodigoFondo, --string
    fp.CodigoSbs, --string
    fp.CodigoIsoMonedaNocional, --string
    fp.CodigoIsoMonedaContraparte,  --string
    fp.ValorNocional, --float
    fp.TipoCambioSpot, --float
    fp.ValorNocional * fp.TipoCambioSpot AS NocionalSoles, --float
    CASE
        WHEN fp.IdTipoOperacion = 1 THEN fp.CodigoIsoMonedaNocional
        ELSE fp.CodigoIsoMonedaContraparte
    END AS MonedaCompra, --string
    CASE
        WHEN fp.IdTipoOperacion = 1 THEN fp.CodigoIsoMonedaContraparte
        ELSE fp.CodigoIsoMonedaNocional
    END AS MonedaVenta, --string

    -- Tier 2
    fp.IdTipoOperacion,
    fp.IdSecuencialFechaProceso,
    fp.IdSecuencialFEchaForwardPrecio,
    fp.CodigoReferencia,
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
JOIN FMS.FOndoPension fond
    ON fp.IdFondo = fond.IdFondo
WHERE fp.IdSecuencialFechaProceso > 20260528