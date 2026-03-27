# src/pipeline/dim/bloomberg/security/loaders/series_status.py
# ---------------------------------------------------------------
# Updates series_registry status based on Bloomberg MARKET_STATUS
# field retrieved during dim enrichment BDP call.
#
# Status lifecycle:
#   backfill-pending  pipeline registered, history not yet loaded
#   active            trading normally, pipeline running
#   suspended         temporarily halted by exchange
#   inactive          no longer trading for any reason
#   error-hold        pipeline failed, manual review needed
#
# Rules:
#   - Never overwrites terminal states (inactive, error-hold)
#   - Never flips backfill-pending to active (pipeline owns that)
#   - suspended is reversible: Bloomberg ACTV flips it back to active
#   - All changes are logged for audit
# ---------------------------------------------------------------

import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Maps Bloomberg MARKET_STATUS field values to series_registry status
MARKET_STATUS_MAP = {
    "ACTV": "active",       # actively traded
    "SSPD": "suspended",    # suspended by exchange - temporary
    "ESUS": "suspended",    # exchange suspended - temporary
    "DLST": "inactive",     # delisted
    "ACQD": "inactive",     # acquired / taken private
    "EXPI": "inactive",     # expired (futures, options)
    "DEFX": "inactive",     # defaulted
    "DQLF": "inactive",     # disqualified
}

# States that the enrichment pipeline is never allowed to overwrite
_TERMINAL_STATES = {"inactive", "error-hold"}

# States that enrichment is never allowed to set directly
# (backfill-pending -> active is owned by the fact pipeline)
_PROTECTED_TRANSITIONS = {
    "backfill-pending": {"active"},
}


def load_series_status_from_enrichment(conn, df: pd.DataFrame) -> None:
    """
    Updates series_registry.status for all series belonging to
    entities in df, based on Bloomberg MARKET_STATUS values.

    df columns: [entity_id, bloomberg_status, registry_status]
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
            FROM series_registry
            WHERE entity_id = ?
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
                f"series_id={sr['series_id']} entity_id={int(row['entity_id'])}: "
                f"{current_status} -> {new_status} "
                f"(Bloomberg MARKET_STATUS={row['bloomberg_status']})"
            )
            updated += 1

    logger.info(f"series_status: {updated} updated, {skipped} skipped.")
