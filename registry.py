# src/pipeline/prices/sbs/registry.py
# ---------------------------------------------------------------
# Shared SBS universe registration logic.
# Called as Step 1 in each SBS pipeline run() before _classify().
#
# Discovers new instruments from the raw file DataFrame,
# diffs against already-registered codigo_sbs values in the DB,
# and registers new instruments directly into:
#   dim_entity
#   dim_entity_identifiers  (codigo_sbs + isin)
#   dim_security            (skeleton only)
#   series_registry         (one row per field, status=backfill-pending)
#
# Bypasses series.csv entirely - SBS universe is discovered
# from the regulator files, not manually curated.
#
# created_at in series_registry records when each instrument
# first appeared in the SBS files, providing the audit trail.
# ---------------------------------------------------------------

import logging
from datetime import date
from typing import Optional

import pandas as pd

from src.db.session import get_connection
from src.db.queries import get_or_create_entity_id, upsert_entity_identifier

logger = logging.getLogger(__name__)


# ---- Field sets per file type ---------------------------------
# Maps file type name to the series fields that should be
# registered in series_registry for each instrument.

FILE_TYPE_FIELDS = {
    "vector_completo": ["PX_LAST"],
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

    raw_df:    raw DataFrame from read_raw() in extract.py.
               Must contain at minimum: codigo_sbs, isin (optional),
               tipo_instrumento (optional).
               For tipo_cambio: moneda_nocional, moneda_contraparte, fuente.

    file_type: one of vector_completo, rf_local, rf_exterior, tipo_cambio.
    run_date:  used as default_start_date for newly registered series.

    Returns number of new series registered.
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


# ---- Securities registration (vector_completo, rf_local, rf_exterior) --

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
    Registers bond/equity securities from SBS files.
    One entity per codigo_sbs, multiple series per entity (one per field).
    """
    # Get already-registered codigo_sbs values
    with get_connection() as conn:
        existing = _get_existing_sbs_codes(conn)

    # Unique instruments in file
    instruments = (
        raw_df[["codigo_sbs", "isin", "tipo_instrumento"]]
        .drop_duplicates(subset=["codigo_sbs"])
        .dropna(subset=["codigo_sbs"])
    )

    new_instruments = instruments[
        ~instruments["codigo_sbs"].astype(str).isin(existing)
    ]

    if new_instruments.empty:
        logger.info(
            f"registry [{file_type}]: no new instruments found "
            f"({len(instruments)} already registered)."
        )
        return 0

    logger.info(
        f"registry [{file_type}]: {len(new_instruments)} new instruments "
        f"found in file. Registering..."
    )

    registered = 0

    with get_connection() as conn:
        for _, row in new_instruments.iterrows():
            codigo_sbs      = str(row["codigo_sbs"]).strip()
            isin            = _clean(row.get("isin"))
            tipo_instrumento = _clean(row.get("tipo_instrumento"))

            # Internal code: SBS_{codigo_sbs}
            internal_code = f"SBS_{codigo_sbs}"

            # 1. dim_entity
            entity_id = get_or_create_entity_id(
                conn,
                ticker=internal_code,
                entity_type="security",
                name=interno_name(row),
            )

            # 2. dim_entity_identifiers
            upsert_entity_identifier(
                conn, entity_id,
                id_type="codigo_sbs",
                id_value=codigo_sbs,
                source="sbs",
                is_primary=True,
            )
            if isin:
                upsert_entity_identifier(
                    conn, entity_id,
                    id_type="isin",
                    id_value=isin,
                    source="sbs",
                    is_primary=False,
                )

            # 3. dim_security skeleton
            conn.execute(
                """
                INSERT INTO dim_security (entity_id, security_type)
                VALUES (?, ?)
                ON CONFLICT (entity_id) DO NOTHING
                """,
                (entity_id, tipo_instrumento),
            )

            # 4. series_registry - one row per field
            for field in fields:
                conn.execute(
                    """
                    INSERT INTO series_registry (
                        entity_id, field, domain, source, frequency,
                        default_start_date, status
                    ) VALUES (?, ?, ?, ?, ?, ?, 'backfill-pending')
                    ON CONFLICT (entity_id, field, source) DO NOTHING
                    """,
                    (
                        entity_id, field, domain, source, frequency,
                        run_date.isoformat(),
                    ),
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    registered += 1

            logger.debug(
                f"Registered: {internal_code} | codigo_sbs={codigo_sbs} "
                f"| isin={isin} | {len(fields)} series"
            )

    logger.info(
        f"registry [{file_type}]: {len(new_instruments)} new instruments, "
        f"{registered} new series registered."
    )
    return registered


# ---- FX registration (tipo_cambio) -----------------------------

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
    Internal code: SBS_FX_{moneda_nocional}_{moneda_contraparte}_{fuente}
    One entity per pair+source, multiple series per entity.
    """
    with get_connection() as conn:
        existing = _get_existing_sbs_codes(conn)

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

            # Unique codigo for this pair+source
            codigo_sbs    = f"FX_{mn}_{mc}_{src}"
            internal_code = f"SBS_{codigo_sbs}"

            if codigo_sbs in existing:
                continue

            # 1. dim_entity
            entity_id = get_or_create_entity_id(
                conn,
                ticker=internal_code,
                entity_type="security",
                name=f"{mn}/{mc} ({src})",
            )

            # 2. dim_entity_identifiers
            upsert_entity_identifier(
                conn, entity_id,
                id_type="codigo_sbs",
                id_value=codigo_sbs,
                source="sbs",
                is_primary=True,
            )

            # Store pair components for pipeline lookup
            conn.execute(
                """
                INSERT OR IGNORE INTO dim_entity_identifiers
                    (entity_id, id_type, id_value, source, is_primary)
                VALUES (?, 'moneda_nocional',   ?, 'sbs', 0)
                """,
                (entity_id, mn),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO dim_entity_identifiers
                    (entity_id, id_type, id_value, source, is_primary)
                VALUES (?, 'moneda_contraparte', ?, 'sbs', 0)
                """,
                (entity_id, mc),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO dim_entity_identifiers
                    (entity_id, id_type, id_value, source, is_primary)
                VALUES (?, 'fuente',              ?, 'sbs', 0)
                """,
                (entity_id, src),
            )

            # 3. dim_security skeleton
            conn.execute(
                """
                INSERT INTO dim_security (entity_id, security_type)
                VALUES (?, 'fx')
                ON CONFLICT (entity_id) DO NOTHING
                """,
                (entity_id,),
            )

            # 4. dim_security_fx
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

            # 5. series_registry - one row per field
            for field in fields:
                # Store pair metadata on series for lookup in transform
                conn.execute(
                    """
                    INSERT INTO series_registry (
                        entity_id, field, domain, source, frequency,
                        default_start_date, status
                    ) VALUES (?, ?, ?, ?, ?, ?, 'backfill-pending')
                    ON CONFLICT (entity_id, field, source) DO NOTHING
                    """,
                    (
                        entity_id, field, domain, source, frequency,
                        run_date.isoformat(),
                    ),
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    registered += 1

            logger.debug(f"Registered FX: {internal_code} | {len(fields)} series")

    logger.info(
        f"registry [tipo_cambio]: {registered} new series registered."
    )
    return registered


# ---- Helpers ---------------------------------------------------

def _get_existing_sbs_codes(conn) -> set[str]:
    """Returns set of all codigo_sbs values already registered."""
    rows = conn.execute(
        """
        SELECT id_value FROM dim_entity_identifiers
        WHERE id_type = 'codigo_sbs' AND source = 'sbs'
        """
    ).fetchall()
    return {r["id_value"] for r in rows}


def interno_name(row) -> Optional[str]:
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
