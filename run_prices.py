# scripts/run_prices.py
# ---------------------------------------------------------------
# Scheduler entry point for fact prices pipelines.
# Dispatches to vendor/file-type specific price pipelines.
#
# Usage:
#   # Bloomberg daily prices (incremental)
#   python scripts/run_prices.py --source bloomberg
#
#   # Bloomberg for a specific date
#   python scripts/run_prices.py --source bloomberg --date 2026-03-10
#
#   # Bloomberg backfill all pending
#   python scripts/run_prices.py --source bloomberg --backfill
#
#   # SBS all file types (incremental)
#   python scripts/run_prices.py --source sbs
#
#   # SBS specific file type only
#   python scripts/run_prices.py --source sbs --file-type rf_local
#
#   # SBS backfill all pending, specific file type
#   python scripts/run_prices.py --source sbs --file-type rf_local --backfill
#
#   # SBS backfill all file types
#   python scripts/run_prices.py --source sbs --backfill
# ---------------------------------------------------------------

import argparse
import importlib
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils.logging import setup_logging

logger = logging.getLogger(__name__)

# Registry of SBS sub-pipelines
SBS_PIPELINES = {
    "vector_completo": "src.pipeline.prices.sbs.vector_completo.run",
    "rf_local":        "src.pipeline.prices.sbs.rf_local.run",
    "rf_exterior":     "src.pipeline.prices.sbs.rf_exterior.run",
    "tipo_cambio":     "src.pipeline.prices.sbs.tipo_cambio.run",
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the prices fact pipeline.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="Run date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--source",
        type=str,
        default="bloomberg",
        choices=["bloomberg", "refinitiv", "sbs"],
        help="Data source to run. Defaults to bloomberg.",
    )
    parser.add_argument(
        "--file-type",
        type=str,
        default=None,
        dest="file_type",
        choices=list(SBS_PIPELINES.keys()),
        help=(
            "SBS file type to run. Only valid when --source sbs.\n"
            f"Options: {', '.join(SBS_PIPELINES.keys())}.\n"
            "Runs all SBS file types if omitted."
        ),
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        default=False,
        help="Run in backfill mode: processes all backfill-pending series.",
    )

    args = parser.parse_args()

    # Validate --file-type is only used with --source sbs
    if args.file_type and args.source != "sbs":
        parser.error("--file-type is only valid when --source sbs.")

    setup_logging("run_prices")

    series_override: Optional[list[dict]] = None

    # ---- Bloomberg ---------------------------------------------
    if args.source == "bloomberg":
        from src.configs.machine_config import assert_bloomberg
        assert_bloomberg()

        if args.backfill:
            from src.db.session import get_connection
            from src.db.queries import get_backfill_pending_series
            with get_connection() as conn:
                series_override = get_backfill_pending_series(
                    conn, domain="prices", source="bloomberg"
                )

        from src.pipeline.prices.bloomberg.run import run
        run(run_date=args.date, series_override=series_override)

    # ---- Refinitiv ---------------------------------------------
    elif args.source == "refinitiv":
        from src.configs.machine_config import assert_bloomberg
        assert_bloomberg()

        if args.backfill:
            from src.db.session import get_connection
            from src.db.queries import get_backfill_pending_series
            with get_connection() as conn:
                series_override = get_backfill_pending_series(
                    conn, domain="prices", source="refinitiv"
                )

        from src.pipeline.prices.refinitiv.run import run
        run(run_date=args.date, series_override=series_override)

    # ---- SBS ---------------------------------------------------
    elif args.source == "sbs":
        if args.backfill:
            from src.db.session import get_connection
            from src.db.queries import get_backfill_pending_series
            with get_connection() as conn:
                series_override = get_backfill_pending_series(
                    conn, domain="prices", source="sbs"
                )

        # Scope to one file type or run all
        to_run = (
            {args.file_type: SBS_PIPELINES[args.file_type]}
            if args.file_type
            else SBS_PIPELINES
        )

        for file_type, module_path in to_run.items():
            logger.info(f"--- SBS pipeline: {file_type} ---")
            mod = importlib.import_module(module_path)
            mod.run(run_date=args.date, series_override=series_override)


if __name__ == "__main__":
    main()
