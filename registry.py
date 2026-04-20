# src/pipeline/prices/sbs/registry.py
# ---------------------------------------------------------------
# Shared SBS universe registration logic.
# Called as Step 1 in each SBS pipeline run() before _classify().
#
# Registration runs in two passes per file:
#   Pass 1: normal ISIN instruments - resolve against existing
#           dim_entity_identifiers or create new entities.
#   Pass 2: X-ISIN instruments - always create separate entities
#           (schema constraint: entity+field+source must be unique,
#           so two codigo_sbs variants of the same bond cannot
#           share an entity_id in series_registry).
#           Attempts parent ISIN prefix lookup for Bloomberg
#           enrichment linkage. Flags unresolved cases.
#
# X-ISIN definition:
#   SBS replaces the check digit (last char) of a real ISIN with 'X'
#   to denote a regulatory variant. e.g. CA4436281022 -> CA443628102X
#   The first 11 characters identify the parent bond.
#   X-ISINs are stored as id_type='isin_x'.
#   Parent prefix stored as id_type='isin_prefix' for Bloomberg linkage.
#
# Bypasses series.csv - SBS universe is discovered from files.
# created_at in series_registry provides the audit trail.
# ---------------------------------------------------------------

import logging
from datetime import date
from typing import Optional

import pandas as pd

from src.db.session import get_connection
from src.db.queries import (
    get_or_create_entity_id,
    upsert_entity_identifier,
    resolve_entity_id_from_identifier,
)

logger = logging.getLogger(__name__)


# ---- Field sets per file type ---------------------------------

FILE_TYPE_FIELDS = {
    "vector_completo": ["PX_LAST", "CHG_PRICE"],
    "rf_local": [
        "PX_CLEAN_MNT", "PX_CLEAN_PCT",
        "PX_DIRTY_MNT", "PX_DIRTY_PCT",
        "ACCRUED_INT", "YTM", "SPREAD",
        "YTW", "DURATION",
        "CHG_CLEAN", "CHG_DIRTY", "CHG_YTM",
    ],
    "rf_exterior": [
        "PX_CLEAN_MNT", "PX_CLEAN_PCT",
        "PX_DIRTY_MNT", "PX_DIRTY_PCT",
        "ACCRUED_INT", "CHG_DIRTY",
    ],
    "tipo_cambio": ["PX_BID", "PX_ASK", "CHG_BID", "CHG_ASK"],
}


# ---- Public entry point ----------------------------------------

def discover_and_register(
    raw_df: pd.DataFrame,
    file_type: str,
    run_date: date,
    domain: str = "prices",
    source: str = "sbs",
    frequency: str = "daily",
) -> int:
    """
    Discovers new instruments in raw_df and registers them in the DB.
    Called as Step 1 in each SBS pipeline run() before _classify().

    Two-pass registration:
      Pass 1: normal ISIN instruments
      Pass 2: X-ISIN instruments (separate entities, Bloomberg linkage via prefix)

    Returns total number of new series registered.
    """
    if raw_df.empty:
        logger.info(f"registry [{file_type}]: empty DataFrame, nothing to register.")
        return 0

    fields = FILE_TYPE_FIELDS.get(file_type)
    if not fields:
        logger.warning(f"registry: unknown file_type '{file_type}'. Skipping.")
        return 0

    if file_type == "tipo_cambio":
        return _register_fx(
            raw_df=raw_df,
            fields=fields,
            run_date=run_date,
            domain=domain,
            source=source,
            frequency=frequency,
        )
    else:
        return _register_securities(
            raw_df=raw_df,
            file_type=file_type,
            fields=fields,
            run_date=run_date,
            domain=domain,
            source=source,
            frequency=frequency,
        )


# ---- Securities registration -----------------------------------

def _register_securities(
    raw_df: pd.DataFrame,
    file_type: str,
    fields: list[str],
    run_date: date,
    domain: str,
    source: str,
    frequency: str,
) -> int:
    """
    Two-pass registration for bond/equity securities.

    Pass 1: instruments with normal ISINs
    Pass 2: instruments with X-ISINs (separate entities)
    """
    # Get already-registered codigo_sbs values to skip known instruments
    with get_connection() as conn:
        existing_codes = _get_existing_sbs_codes(conn)

    # Unique instruments in file
    instruments = (
        raw_df[["codigo_sbs", "isin", "tipo_instrumento", "emisor"]]
        .drop_duplicates(subset=["codigo_sbs"])
        .dropna(subset=["codigo_sbs"])
    )

    new_instruments = instruments[
        ~instruments["codigo_sbs"].astype(str).isin(existing_codes)
    ]

    if new_instruments.empty:
        logger.info(
            f"registry [{file_type}]: no new instruments "
            f"({len(instruments)} already registered)."
        )
        return 0

    logger.info(
        f"registry [{file_type}]: {len(new_instruments)} new instruments found."
    )

    # Split into normal and X-ISIN
    normal_rows = new_instruments[
        ~new_instruments["isin"].apply(_is_x_isin)
    ]
    x_isin_rows = new_instruments[
        new_instruments["isin"].apply(_is_x_isin)
    ]

    registered = 0

    # Pass 1: normal ISINs
    if not normal_rows.empty:
        logger.info(
            f"registry [{file_type}]: Pass 1 - "
            f"{len(normal_rows)} normal ISIN instruments."
        )
        with get_connection() as conn:
            for _, row in normal_rows.iterrows():
                registered += _register_normal_instrument(
                    conn, row, fields, run_date, domain, source, frequency
                )

    # Pass 2: X-ISINs (always separate entities, after Pass 1)
    if not x_isin_rows.empty:
        logger.info(
            f"registry [{file_type}]: Pass 2 - "
            f"{len(x_isin_rows)} X-ISIN instruments."
        )
        with get_connection() as conn:
            for _, row in x_isin_rows.iterrows():
                registered += _register_x_isin_instrument(
                    conn, row, fields, run_date, domain, source, frequency
                )

    logger.info(
        f"registry [{file_type}]: {registered} new series registered."
    )
    return registered


def _register_normal_instrument(
    conn,
    row,
    fields: list[str],
    run_date: date,
    domain: str,
    source: str,
    frequency: str,
) -> int:
    """
    Registers a normal ISIN instrument.
    Attempts to resolve against existing entity via ISIN.
    Creates new entity if not found.
    """
    codigo_sbs       = str(row["codigo_sbs"]).strip()
    isin             = _clean(row.get("isin"))
    tipo_instrumento = _clean(row.get("tipo_instrumento"))

    # Try to resolve existing entity via ISIN
    entity_id = None
    if isin:
        entity_id = resolve_entity_id_from_identifier(
            conn, id_type="isin", id_value=isin, source=None
        )

    if entity_id is None:
        # New entity
        internal_code = f"SBS_{codigo_sbs}"
        entity_id = get_or_create_entity_id(
            conn,
            ticker=internal_code,
            entity_type="security",
            name=_derive_name(row),
        )
        # dim_security skeleton
        conn.execute(
            """
            INSERT INTO dim_security (entity_id, security_type)
            VALUES (?, ?)
            ON CONFLICT (entity_id) DO NOTHING
            """,
            (entity_id, tipo_instrumento),
        )
    else:
        logger.debug(
            f"Linked SBS instrument {codigo_sbs} to existing "
            f"entity_id={entity_id} via ISIN {isin}."
        )

    # Always store codigo_sbs on the entity (new or existing)
    upsert_entity_identifier(
        conn, entity_id, "codigo_sbs", codigo_sbs, "sbs", is_primary=True
    )
    # ISIN stored as source="internal" - universal standard identifier
    if isin:
        upsert_entity_identifier(
            conn, entity_id, "isin", isin, "internal", is_primary=False
        )

    return _register_series(
        conn, entity_id, fields, run_date, domain, source, frequency
    )


def _register_x_isin_instrument(
    conn,
    row,
    fields: list[str],
    run_date: date,
    domain: str,
    source: str,
    frequency: str,
) -> int:
    """
    Registers an X-ISIN instrument as a SEPARATE entity.

    X-ISIN variants cannot share an entity with their parent because
    series_registry requires unique (entity_id, field, source) and
    both variants report the same fields from source='sbs'.

    Attempts parent ISIN prefix lookup for Bloomberg enrichment linkage.
    Stores parent_entity_id as a reference identifier if found.
    Flags unresolved cases with a warning.
    """
    codigo_sbs       = str(row["codigo_sbs"]).strip()
    isin_x           = _clean(row.get("isin"))
    tipo_instrumento = _clean(row.get("tipo_instrumento"))

    # Always create a new entity for this X-ISIN variant
    internal_code = f"SBS_{codigo_sbs}"
    entity_id = get_or_create_entity_id(
        conn,
        ticker=internal_code,
        entity_type="security",
        name=_derive_name(row),
    )

    # dim_security skeleton
    conn.execute(
        """
        INSERT INTO dim_security (entity_id, security_type)
        VALUES (?, ?)
        ON CONFLICT (entity_id) DO NOTHING
        """,
        (entity_id, tipo_instrumento),
    )

    # Store X-ISIN and codigo_sbs
    upsert_entity_identifier(
        conn, entity_id, "codigo_sbs", codigo_sbs, "sbs", is_primary=True
    )
    if isin_x:
        upsert_entity_identifier(
            conn, entity_id, "isin_x", isin_x, "sbs", is_primary=False
        )

    # Attempt parent resolution via ISIN prefix (first 11 chars)
    if isin_x:
        prefix        = isin_x[:-1]   # strip the X
        parent_entity = _resolve_by_isin_prefix(conn, prefix)

        if parent_entity:
            # Store parent entity reference for Bloomberg enrichment
            # This is NOT a merge - just a hint for enrichment pipelines
            upsert_entity_identifier(
                conn, entity_id,
                id_type="isin_prefix",
                id_value=prefix,
                source="sbs",
                is_primary=False,
            )
            conn.execute(
                """
                INSERT INTO dim_entity_identifiers
                    (entity_id, id_type, id_value, source, is_primary)
                VALUES (?, 'parent_entity_id', ?, 'sbs', 0)
                ON CONFLICT (entity_id, id_type, source) DO UPDATE SET
                    id_value = excluded.id_value
                """,
                (entity_id, str(parent_entity)),
            )
            logger.debug(
                f"X-ISIN {isin_x} (entity_id={entity_id}) linked to "
                f"parent entity_id={parent_entity} via prefix {prefix}."
            )
        else:
            # Store prefix only - flag for manual review
            upsert_entity_identifier(
                conn, entity_id,
                id_type="isin_prefix",
                id_value=prefix,
                source="sbs",
                is_primary=False,
            )
            logger.warning(
                f"X-ISIN {isin_x} (codigo_sbs={codigo_sbs}): "
                f"no parent entity found for prefix '{prefix}'. "
                f"Bloomberg enrichment will not be linked until "
                f"parent instrument is onboarded. "
                f"entity_id={entity_id} flagged for manual review."
            )

    return _register_series(
        conn, entity_id, fields, run_date, domain, source, frequency
    )


# ---- FX registration -------------------------------------------

def _register_fx(
    raw_df: pd.DataFrame,
    fields: list[str],
    run_date: date,
    domain: str,
    source: str,
    frequency: str,
) -> int:
    """
    Registers FX currency pairs from tipo_cambio file.
    No ISIN, no X-ISIN logic - pairs are identified by
    (moneda_nocional, moneda_contraparte, fuente).
    """
    with get_connection() as conn:
        existing_codes = _get_existing_sbs_codes(conn)

    pairs = (
        raw_df[["moneda_nocional", "moneda_contraparte", "fuente"]]
        .drop_duplicates()
        .dropna(subset=["moneda_nocional", "moneda_contraparte", "fuente"])
    )

    registered = 0

    with get_connection() as conn:
        for _, row in pairs.iterrows():
            mn  = str(row["moneda_nocional"]).strip()
            mc  = str(row["moneda_contraparte"]).strip()
            src = str(row["fuente"]).strip()

            codigo_sbs    = f"FX_{mn}_{mc}_{src}"
            internal_code = f"SBS_{codigo_sbs}"

            if codigo_sbs in existing_codes:
                continue

            entity_id = get_or_create_entity_id(
                conn,
                ticker=internal_code,
                entity_type="security",
                name=f"{mn}/{mc} ({src})",
            )

            upsert_entity_identifier(
                conn, entity_id, "codigo_sbs", codigo_sbs, "sbs", True
            )
            for id_type, id_value in [
                ("moneda_nocional",    mn),
                ("moneda_contraparte", mc),
                ("fuente",             src),
            ]:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO dim_entity_identifiers
                        (entity_id, id_type, id_value, source, is_primary)
                    VALUES (?, ?, ?, 'sbs', 0)
                    """,
                    (entity_id, id_type, id_value),
                )

            conn.execute(
                """
                INSERT INTO dim_security (entity_id, security_type)
                VALUES (?, 'fx')
                ON CONFLICT (entity_id) DO NOTHING
                """,
                (entity_id,),
            )

            sec = conn.execute(
                "SELECT security_id FROM dim_security WHERE entity_id = ?",
                (entity_id,),
            ).fetchone()
            if sec:
                conn.execute(
                    """
                    INSERT INTO dim_security_fx
                        (security_id, base_currency, quote_currency, pair, fx_type)
                    VALUES (?, ?, ?, ?, 'spot')
                    ON CONFLICT (security_id) DO NOTHING
                    """,
                    (sec["security_id"], mn, mc, f"{mn}/{mc}"),
                )

            registered += _register_series(
                conn, entity_id, fields, run_date, domain, source, frequency
            )

    logger.info(f"registry [tipo_cambio]: {registered} new series registered.")
    return registered


# ---- Shared helpers --------------------------------------------

def _register_series(
    conn,
    entity_id: int,
    fields: list[str],
    run_date: date,
    domain: str,
    source: str,
    frequency: str,
) -> int:
    """Inserts series_registry rows for each field. Returns count inserted."""
    inserted = 0
    for field in fields:
        conn.execute(
            """
            INSERT INTO series_registry (
                entity_id, field, domain, source, frequency,
                default_start_date, status
            ) VALUES (?, ?, ?, ?, ?, ?, 'backfill-pending')
            ON CONFLICT (entity_id, field, source) DO NOTHING
            """,
            (entity_id, field, domain, source, frequency, run_date.isoformat()),
        )
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            inserted += 1
    return inserted


def _get_existing_sbs_codes(conn) -> set[str]:
    """Returns all codigo_sbs values already registered."""
    rows = conn.execute(
        """
        SELECT id_value FROM dim_entity_identifiers
        WHERE id_type = 'codigo_sbs' AND source = 'sbs'
        """
    ).fetchall()
    return {r["id_value"] for r in rows}


def _resolve_by_isin_prefix(conn, prefix: str) -> Optional[int]:
    """
    Finds entity_id whose ISIN starts with the given 11-char prefix.
    Returns entity_id if exactly one match found, None otherwise.
    Multiple matches are logged as a warning.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT entity_id FROM dim_entity_identifiers
        WHERE id_type = 'isin'
          AND source  = 'internal'
          AND substr(id_value, 1, 11) = ?
        """,
        (prefix,),
    ).fetchall()

    if len(rows) == 1:
        return rows[0]["entity_id"]
    if len(rows) > 1:
        logger.warning(
            f"ISIN prefix '{prefix}' matched {len(rows)} entities: "
            f"{[r['entity_id'] for r in rows]}. Cannot resolve uniquely."
        )
    return None


def _is_x_isin(val) -> bool:
    """Returns True if val is an SBS X-ISIN (12 chars ending in X)."""
    if val is None:
        return False
    s = str(val).strip().upper()
    return len(s) == 12 and s.endswith("X")


def _derive_name(row) -> Optional[str]:
    """Derives a human-readable name from available row attributes."""
    emisor = _clean(row.get("emisor"))
    tipo   = _clean(row.get("tipo_instrumento"))
    isin   = _clean(row.get("isin"))
    parts  = [p for p in [emisor, tipo, isin] if p]
    return " | ".join(parts) if parts else None


def _clean(val) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none", "") else None