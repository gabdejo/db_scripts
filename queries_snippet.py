# src/db/queries.py
# ---------------------------------------------------------------
# Addition: get_active_series needs to resolve codigo_sbs
# alongside bloomberg_ticker for SBS pipelines.
# Also adds get_registered_sbs_codes for registry.py diff.
# ---------------------------------------------------------------


def get_active_series(
    conn,
    domain: str,
    source: str,
    frequency: str,
) -> list[dict]:
    """
    Returns active series with identifiers resolved inline.
    Now also resolves codigo_sbs for SBS source pipelines.
    """
    rows = conn.execute(
        """
        SELECT
            sr.series_id,
            sr.entity_id,
            e.ticker            AS internal_code,
            e.name,
            sr.field,
            sr.source,
            sr.frequency,
            sr.default_start_date,
            sr.release_pattern,
            sr.allow_revisions,
            sr.revision_lookback,
            sr.last_loaded_date,
            bbg_ticker.id_value AS bbg_ticker,
            isin_t.id_value     AS isin,
            ric_t.id_value      AS ric,
            sbs_code.id_value   AS codigo_sbs,
            mn.id_value         AS moneda_nocional,
            mc.id_value         AS moneda_contraparte,
            fuente.id_value     AS fuente
        FROM series_registry sr
        JOIN dim_entity e ON sr.entity_id = e.entity_id
        LEFT JOIN dim_entity_identifiers bbg_ticker
               ON bbg_ticker.entity_id = sr.entity_id
              AND bbg_ticker.id_type    = 'bloomberg_ticker'
              AND bbg_ticker.source     = 'bloomberg'
        LEFT JOIN dim_entity_identifiers isin_t
               ON isin_t.entity_id = sr.entity_id
              AND isin_t.id_type   = 'isin'
              AND isin_t.source    = 'sbs'
        LEFT JOIN dim_entity_identifiers ric_t
               ON ric_t.entity_id = sr.entity_id
              AND ric_t.id_type   = 'ric'
              AND ric_t.source    = 'refinitiv'
        LEFT JOIN dim_entity_identifiers sbs_code
               ON sbs_code.entity_id = sr.entity_id
              AND sbs_code.id_type   = 'codigo_sbs'
              AND sbs_code.source    = 'sbs'
        LEFT JOIN dim_entity_identifiers mn
               ON mn.entity_id = sr.entity_id
              AND mn.id_type   = 'moneda_nocional'
              AND mn.source    = 'sbs'
        LEFT JOIN dim_entity_identifiers mc
               ON mc.entity_id = sr.entity_id
              AND mc.id_type   = 'moneda_contraparte'
              AND mc.source    = 'sbs'
        LEFT JOIN dim_entity_identifiers fuente
               ON fuente.entity_id = sr.entity_id
              AND fuente.id_type   = 'fuente'
              AND fuente.source    = 'sbs'
        WHERE sr.domain    = ?
          AND sr.source    = ?
          AND sr.frequency = ?
          AND sr.status    = 'active'
        ORDER BY sr.series_id
        """,
        (domain, source, frequency),
    ).fetchall()
    return [dict(r) for r in rows]


def get_registered_sbs_codes(conn) -> set[str]:
    """
    Returns all codigo_sbs values already registered in
    dim_entity_identifiers.
    Used by registry.py to diff file instruments against DB.
    """
    rows = conn.execute(
        """
        SELECT id_value FROM dim_entity_identifiers
        WHERE id_type = 'codigo_sbs' AND source = 'sbs'
        """
    ).fetchall()
    return {r["id_value"] for r in rows}
