# scripts/acquire/acquire_sbs_bulk.py
# ---------------------------------------------------------------
# Bulk SBS acquisition for initial or catch-up loads.
# Single browser session: logs in once, queries portal for
# eligible dates, diffs against raw/, downloads what is missing.
#
# Usage:
#   # Full history from 2020 to today
#   python scripts/acquire/acquire_sbs_bulk.py --start 2020-01-01
#
#   # Scoped date range
#   python scripts/acquire/acquire_sbs_bulk.py --start 2024-01-01 --end 2024-12-31
#
#   # Specific file types only
#   python scripts/acquire/acquire_sbs_bulk.py --start 2020-01-01 --file-types tasa_activa_mn tipo_cambio
#
#   # Custom retry settings
#   python scripts/acquire/acquire_sbs_bulk.py --start 2020-01-01 --max-retries 5 --retry-delay 60
# ---------------------------------------------------------------

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.scrapers.sbs import acquire_bulk, SBS_FILES
from src.utils.logging import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    known_types = [f["name"] for f in SBS_FILES]

    parser = argparse.ArgumentParser(
        description="Bulk SBS acquisition across a date range.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        required=True,
        help="Start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=None,
        help="End date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--file-types",
        nargs="+",
        default=None,
        dest="file_types",
        help=(
            "File types to download. Downloads all if omitted.
"
            f"Options: {', '.join(known_types)}"
        ),
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=180,
        help="Seconds to wait for manual login. Default: 180.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        dest="max_retries",
        help="Max download attempts per file before giving up. Default: 3.",
    )
    parser.add_argument(
        "--retry-delay",
        type=int,
        default=30,
        dest="retry_delay",
        help="Seconds between retry attempts. Default: 30.",
    )

    args     = parser.parse_args()
    end_date = args.end or date.today()

    setup_logging("acquire_sbs_bulk")

    logger.info(
        f"=== SBS bulk acquisition | "
        f"{args.start} to {end_date} | "
        f"file_types={args.file_types or 'all'} ==="
    )

    results = acquire_bulk(
        start_date=args.start,
        end_date=end_date,
        file_types=args.file_types,
        timeout_seconds=args.timeout,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay,
    )

    succeeded = results["succeeded"]
    failed    = results["failed"]
    skipped   = results["skipped"]

    logger.info(
        f"Summary: {len(succeeded)} succeeded, "
        f"{len(failed)} failed, {len(skipped)} skipped."
    )

    if failed:
        failed_sorted = sorted(failed)
        logger.error(
            f"{len(failed)} dates failed. Re-run with:
"
            f"python scripts/acquire/acquire_sbs_bulk.py "
            f"--start {failed_sorted[0]} --end {failed_sorted[-1]}"
            + (f" --file-types {' '.join(args.file_types)}" if args.file_types else "")
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
