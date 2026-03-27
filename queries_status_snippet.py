# src/db/queries.py
# ---------------------------------------------------------------
# Status-related query additions.
#
# Existing queries need no changes:
#   get_active_series         filters status = 'active'
#   get_backfill_pending_series filters status = 'backfill-pending'
#   suspended and inactive are naturally excluded from both
#
# New utility queries added below.
# ---------------------------------------------------------------

import logging

logger = logging.getLogger(__name__)


def get_suspended_series(conn, domain=None, source=None) -> list[dict]:
    """
    Returns all series with status = suspended.
    These are exchange-halted securities that may resume trading.
    Useful for monitoring and deciding whether to wait for
    resumption or manually flip to inactive.
    """
    query = """
        SELECT
            sr.series_id,
            sr.entity_id,
            e.ticker            AS internal_code,
            e.name,
            sr.field,
            sr.source,
            sr.domain,
            sr.frequency,
            sr.updated_at       AS status_changed_at,
            bbg_ticker.id_value AS bbg_ticker
        FROM series_registry sr
        JOIN dim_entity e
          ON sr.entity_id = e.entity_id
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


def get_inactive_series(conn, domain=None, source=None) -> list[dict]:
    """
    Returns all series with status = inactive.
    These are permanently ended securities.
    Historical data is retained in fact tables, no further updates.
    Useful for auditing which securities have been marked inactive
    and when the status change occurred.
    """
    query = """
        SELECT
            sr.series_id,
            sr.entity_id,
            e.ticker            AS internal_code,
            e.name,
            sr.field,
            sr.source,
            sr.domain,
            sr.updated_at       AS inactivated_at,
            bbg_ticker.id_value AS bbg_ticker
        FROM series_registry sr
        JOIN dim_entity e
          ON sr.entity_id = e.entity_id
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


def get_error_hold_series(conn, domain=None, source=None) -> list[dict]:
    """
    Returns all series with status = error-hold.
    These require manual review before pipeline resumes.
    """
    query = """
        SELECT
            sr.series_id,
            sr.entity_id,
            e.ticker            AS internal_code,
            e.name,
            sr.field,
            sr.source,
            sr.domain,
            sr.last_run_at,
            sr.updated_at       AS error_at,
            bbg_ticker.id_value AS bbg_ticker
        FROM series_registry sr
        JOIN dim_entity e
          ON sr.entity_id = e.entity_id
        LEFT JOIN dim_entity_identifiers bbg_ticker
               ON bbg_ticker.entity_id = sr.entity_id
              AND bbg_ticker.id_type    = 'bloomberg_ticker'
              AND bbg_ticker.source     = 'bloomberg'
        WHERE sr.status = 'error-hold'
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
