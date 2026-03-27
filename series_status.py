# src/pipeline/dim/bloomberg/security/loaders/series_status.py
# ---------------------------------------------------------------
# Updates series_registry status based on Bloomberg MARKET_STATUS
# and EXCH_MARKET_STATUS fields retrieved during dim enrichment.
#
# Status vocabulary:
#   backfill-pending  registered, history not yet loaded
#   active            trading normally, pipeline running
#   suspended         temporarily halted (exchange or issuer)
#   inactive          no longer trading for any reason
#   error-hold        pipeline failed, manual review needed
#
# Resolution logic:
#   1. MARKET_STATUS present and not in _FALLBACK_TRIGGER
#      -> use MARKET_STATUS_MAP (authoritative)
#   2. MARKET_STATUS missing or PRNA
#      -> fall back to EXCH_MARKET_STATUS_MAP
#   3. Both missing or unrecognised
#      -> skip, log warning, leave status unchanged
#
# Rules:
#   - Never overwrites terminal states (inactive, error-hold)
#   - Never flips backfill-pending to active (fact pipeline owns that)
#   - suspended is reversible: Bloomberg ACTV flips back to active
#   - All changes logged with source field for audit
# ---------------------------------------------------------------

import logging
import pandas as pd

logger = logging.getLogger(__name__)


# ---- Status maps -----------------------------------------------

# Primary: Bloomberg MARKET_STATUS field
MARKET_STATUS_MAP = {
    "ACTV": "active",
    "TRDG": "active",       # some exchanges use TRDG instead of ACTV
    "OTCM": "active",       # OTC market active
    "SSPD": "suspended",    # suspended by issuer
    "ESUS": "suspended",    # suspended by exchange
    "SUSB": "suspended",    # suspended due to bankruptcy filing
    "DLST": "inactive",     # delisted
    "ACQD": "inactive",     # acquired / taken private
    "EXPI": "inactive",     # expired (futures, options)
    "DEFX": "inactive",     # defaulted
    "DQLF": "inactive",     # disqualified
    "PRNA": "inactive",     # price not available - no trading activity
    "NACT": "inactive",     # not active
    "PUBA": "inactive",     # public announcement (M&A, treat as inactive)
    "ESTP": "inactive",     # exchange stopped permanently
}

# Fallback: Bloomberg EXCH_MARKET_STATUS field
# Used when MARKET_STATUS is missing or PRNA
# Covers futures, FX, and instruments where MARKET_STATUS is absent
EXCH_MARKET_STATUS_MAP = {
    "ACTV": "active",
    "CLSD": "suspended",    # market closed temporarily
    "HALT": "suspended",    # trading halt
    "SSPD": "suspended",
    "INAC": "inactive",
}

# MARKET_STATUS values that trigger fallback to EXCH_MARKET_STATUS
_FALLBACK_TRIGGER: set[str] = {"PRNA"}

# Status values that enrichment pipeline is never allowed to overwrite
_TERMINAL_STATES: set[str] = {"inactive", "error-hold"}

# Protected transitions: {from_status: {set of disallowed new_status}}
# backfill-pending -> active is owned exclusively by the fact pipeline
_PROTECTED_TRANSITIONS: dict[str, set[str]] = {
    "backfill-pending": {"active"},
}


# ---- Public loader ---------------------------------------------

def load_series_status_from_enrichment(conn, df: pd.DataFrame) -> None:
    """
    Updates series_registry.status for all series belonging to
    entities in df.

    df columns:
        entity_id        int
        bloomberg_status str   raw Bloomberg field value used
        registry_status  str   mapped registry status
        resolved_from    str   which Bloomberg field resolved the status
    """
    if df.empty:
        logger.info("series_status: no status updates from enrichment.")
        return

    updated = 0
    skipped = 0

    for _, row in df.iterrows():
        new_status = row["registry_status"]

        series_rows = conn.execute(
            """
            SELECT series_id, status
            FROM   series_registry
            WHERE  entity_id = ?
            """,
            (int(row["entity_id"]),),
        ).fetchall()

        for sr in series_rows:
            current_status = sr["status"]

            # Never overwrite terminal states
            if current_status in _TERMINAL_STATES:
                skipped += 1
                continue

            # Never make protected transitions
            if new_status in _PROTECTED_TRANSITIONS.get(current_status, set()):
                skipped += 1
                continue

            # No change needed
            if current_status == new_status:
                skipped += 1
                continue

            conn.execute(
                """
                UPDATE series_registry
                SET    status     = ?,
                       updated_at = datetime('now')
                WHERE  series_id  = ?
                """,
                (new_status, sr["series_id"]),
            )
            logger.info(
                f"series_id={sr['series_id']} "
                f"entity_id={int(row['entity_id'])}: "
                f"{current_status} -> {new_status} "
                f"(Bloomberg {row['resolved_from']}="
                f"{row['bloomberg_status']})"
            )
            updated += 1

    logger.info(f"series_status: {updated} updated, {skipped} skipped.")


# ---- Internal helpers ------------------------------------------

def _clean(val) -> str | None:
    """Strips and uppercases a Bloomberg field value. Returns None if empty."""
    if val is None:
        return None
    s = str(val).strip().upper()
    return s if s and s not in ("NAN", "NONE", "") else None
