# src/db/queries.py
# ---------------------------------------------------------------
# Updates to get_active_series and get_backfill_pending_series
# to reflect the new status values:
#   backfill-pending, active, suspended, inactive, error-hold
# ---------------------------------------------------------------

# get_active_series - no change needed
# Already filters: WHERE sr.status = 'active'
# suspended and inactive are naturally excluded

# get_backfill_pending_series - no change needed
# Already filters: WHERE sr.status = 'backfill-pending'

# New utility: get_suspended_series
# Useful for a periodic review job or monitoring dashboard

def get_suspended_series(conn, domain=None, source=None):
    """
    Returns all series with status = suspended.
    These are exchange-halted securities that may resume.
    Useful for monitoring and for deciding whether to
    manually flip to inactive or wait for resumption.
    """
    query = """
        SELECT
            sr.series_id,
            sr.entity_id,
            e.ticker        AS internal_code,
            e.name,
            sr.field,
            sr.source,
            sr.domain,
            sr.frequency,
            sr.updated_at   AS status_changed_at,
            bbg_ticker.id_value AS bbg_ticker
        FROM series_registry sr
        JOIN dim_entity e ON sr.entity_id = e.entity_id
        LEFT JOIN dim_entity_identifiers bbg_ticker
               ON bbg_ticker.entity_id = sr.entity_id
              AND bbg_ticker.id_type    = 'bloomberg_ticker'
              AND bbg_ticker.source     = 'bloomberg'
        WHERE sr.status = 'suspended'
    """
    params = []
    if domain:
        query += " AND sr.domain = ?"
        params.append(domain)
    if source:
        query += " AND sr.source = ?"
        params.append(source)
    query += " ORDER BY sr.updated_at DESC"

    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


# New utility: get_inactive_series
# Useful for audit - which securities have been marked inactive and when

def get_inactive_series(conn, domain=None, source=None):
    """
    Returns all series with status = inactive.
    These are permanently ended securities.
    History is retained in fact tables, no further updates.
    """
    query = """
        SELECT
            sr.series_id,
            sr.entity_id,
            e.ticker        AS internal_code,
            e.name,
            sr.field,
            sr.source,
            sr.domain,
            sr.updated_at   AS inactivated_at,
            bbg_ticker.id_value AS bbg_ticker
        FROM series_registry sr
        JOIN dim_entity e ON sr.entity_id = e.entity_id
        LEFT JOIN dim_entity_identifiers bbg_ticker
               ON bbg_ticker.entity_id = sr.entity_id
              AND bbg_ticker.id_type    = 'bloomberg_ticker'
              AND bbg_ticker.source     = 'bloomberg'
        WHERE sr.status = 'inactive'
    """
    params = []
    if domain:
        query += " AND sr.domain = ?"
        params.append(domain)
    if source:
        query += " AND sr.source = ?"
        params.append(source)
    query += " ORDER BY sr.updated_at DESC"

    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]
