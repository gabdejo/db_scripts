# scripts/backfill_sbs_prices.py
# ---------------------------------------------------------------
# File-driven backfill for SBS price pipelines.
# Iterates through all available raw files oldest to newest
# and runs the ingestion pipeline for each date.
#
# Unlike Bloomberg backfill which calls an API with a date range,
# SBS backfill reads local files already acquired by the scraper.
# No API calls are made - purely file-to-DB ingestion.
#
# Usage:
#   # Backfill all file types
#   python scripts/backfill_sbs_prices.py
#
#   # Backfill specific file type only
#   python scripts/backfill_sbs_prices.py --file-type rf_local
#
#   # Backfill multiple specific file types
#   python scripts/backfill_sbs_prices.py --file-type rf_local rf_exterior
#
#   # Backfill within a date range
#   python scripts/backfill_sbs_prices.py --start 2024-01-01 --end 2024-12-31
#
#   # Dry run: show available dates without loading
#   python scripts/backfill_sbs_prices.py --file-type vector_completo --dry-run
#
#   # Skip dates already fully loaded (default behaviour)
#   python scripts/backfill_sbs_prices.py --file-type rf_local
#
#   # Force reload even for dates already loaded
#   python scripts/backfill_sbs_prices.py --file-type rf_local --force
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
from src.utils.paths import RAW_DIR

logger = logging.getLogger(__name__)

# Maps file type to its raw subdirectory under data/raw/manual/sbs/
FILE_TYPE_SUBDIR = {
    "vector_completo": "vector_completo",
    "rf_local":        "rf_local",
    "rf_exterior":     "rf_exterior",
    "tipo_cambio":     "tipo_cambio",
}

# Maps file type to its pipeline module
FILE_TYPE_MODULE = {
    "vector_completo": "src.pipeline.prices.sbs.vector_completo.run",
    "rf_local":        "src.pipeline.prices.sbs.rf_local.run",
    "rf_exterior":     "src.pipeline.prices.sbs.rf_exterior.run",
    "tipo_cambio":     "src.pipeline.prices.sbs.tipo_cambio.run",
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "File-driven backfill for SBS price pipelines. "
            "Iterates available raw files oldest to newest."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--file-type",
        nargs="+",
        default=None,
        dest="file_types",
        choices=list(FILE_TYPE_SUBDIR.keys()),
        help=(
            "File type(s) to backfill. Runs all if omitted.\n"
            f"Options: {', '.join(FILE_TYPE_SUBDIR.keys())}"
        ),
    )
    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        default=None,
        help="Only process files on or after this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=None,
        help="Only process files on or before this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        dest="dry_run",
        help="Show available dates without running any pipelines.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help=(
            "Process all dates even if series are already loaded. "
            "Default: skip dates where fact_prices already has rows "
            "for all series of that file type."
        ),
    )

    args       = parser.parse_args()
    file_types = args.file_types or list(FILE_TYPE_SUBDIR.keys())

    setup_logging("backfill_sbs_prices")
    logger.info(
        f"=== SBS backfill | file_types={file_types} | "
        f"start={args.start} | end={args.end} | "
        f"dry_run={args.dry_run} | force={args.force} ==="
    )

    for file_type in file_types:
        _backfill_file_type(
            file_type=file_type,
            start=args.start,
            end=args.end,
            dry_run=args.dry_run,
            force=args.force,
        )

    logger.info("=== SBS backfill complete ===")


# ---- Per file type backfill ------------------------------------

def _backfill_file_type(
    file_type: str,
    start: Optional[date],
    end: Optional[date],
    dry_run: bool,
    force: bool,
) -> None:
    logger.info(f"--- Backfill: {file_type} ---")

    # Discover available dates from filesystem
    available_dates = _discover_dates(file_type, start, end)

    if not available_dates:
        logger.warning(
            f"{file_type}: no raw files found in "
            f"{RAW_DIR / 'manual' / 'sbs' / FILE_TYPE_SUBDIR[file_type]}."
        )
        return

    logger.info(f"{file_type}: {len(available_dates)} files available.")

    if dry_run:
        logger.info(f"{file_type}: dry run - dates that would be processed:")
        for d in available_dates:
            logger.info(f"  {d}")
        return

    # Filter out already-loaded dates unless --force
    if not force:
        dates_to_process = _filter_already_loaded(file_type, available_dates)
        skipped = len(available_dates) - len(dates_to_process)
        if skipped:
            logger.info(
                f"{file_type}: skipping {skipped} dates already loaded. "
                f"Use --force to reload."
            )
    else:
        dates_to_process = available_dates

    if not dates_to_process:
        logger.info(f"{file_type}: all available dates already loaded.")
        return

    logger.info(
        f"{file_type}: processing {len(dates_to_process)} dates "
        f"({dates_to_process[0]} to {dates_to_process[-1]})."
    )

    # Load pipeline module
    mod = importlib.import_module(FILE_TYPE_MODULE[file_type])

    succeeded = []
    failed    = []

    for run_date in dates_to_process:
        try:
            logger.info(f"{file_type}: processing {run_date}...")
            mod.run(run_date=run_date)
            succeeded.append(run_date)
        except Exception as e:
            logger.error(
                f"{file_type}: failed for {run_date}: {e}",
                exc_info=True,
            )
            failed.append(run_date)
            # Continue to next date rather than aborting entire backfill

    logger.info(
        f"{file_type}: {len(succeeded)} succeeded, {len(failed)} failed."
    )
    if failed:
        logger.warning(
            f"{file_type}: failed dates: {[str(d) for d in failed]}. "
            f"Re-run with --start {min(failed)} --end {max(failed)} "
            f"--file-type {file_type} to retry."
        )


# ---- Filesystem helpers ----------------------------------------

def _discover_dates(
    file_type: str,
    start: Optional[date],
    end: Optional[date],
) -> list[date]:
    """
    Scans data/raw/manual/sbs/{subdomain}/ across all year subdirs
    and extracts dates from YYYYMMDD_ prefixed filenames.
    Returns sorted list of dates within the optional range.
    """
    subdir = RAW_DIR / "manual" / "sbs" / FILE_TYPE_SUBDIR[file_type]

    if not subdir.exists():
        return []

    dates = []
    for year_dir in sorted(subdir.iterdir()):
        if not year_dir.is_dir():
            continue
        for f in sorted(year_dir.glob("*.xls*")):
            stem = f.stem
            if len(stem) >= 8 and stem[:8].isdigit():
                try:
                    d = date(
                        int(stem[:4]),
                        int(stem[4:6]),
                        int(stem[6:8]),
                    )
                    if start and d < start:
                        continue
                    if end and d > end:
                        continue
                    dates.append(d)
                except ValueError:
                    continue

    return sorted(set(dates))


def _filter_already_loaded(
    file_type: str,
    available_dates: list[date],
) -> list[date]:
    """
    Returns dates from available_dates that have not yet been
    fully loaded into fact_prices.
    A date is considered loaded if fact_prices has at least one row
    for that reference_date from source='sbs'.
    """
    if not available_dates:
        return []

    from src.db.session import get_connection

    with get_connection() as conn:
        # Get all reference_dates already loaded from sbs
        rows = conn.execute(
            """
            SELECT DISTINCT reference_date
            FROM fact_prices
            WHERE source = 'sbs'
            """
        ).fetchall()
        loaded_dates = {r["reference_date"] for r in rows}

    return [
        d for d in available_dates
        if d.isoformat() not in loaded_dates
    ]


if __name__ == "__main__":
    main()
