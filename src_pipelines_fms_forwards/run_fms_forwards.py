# scripts/run_fms_forwards.py
# ---------------------------------------------------------------
# CLI entry point for the FMS forwards pipeline.
#
# Thin shell: parses argparse, sets up logging, dispatches to the
# run() wrappers in src/pipeline/positions/fms/forwards/run.py.
#
# Usage:
#   python scripts/run_fms_forwards.py --start-date 2026-06-22 --end-date 2026-06-22
#   python scripts/run_fms_forwards.py --start-date 2026-06-01 --end-date 2026-06-30
#   python scripts/run_fms_forwards.py --from-stg --batch-id fms_forwards_20260622_081532
#   python scripts/run_fms_forwards.py --start-date 2025-01-01 --end-date 2025-12-31 --force
#
# Flags:
#   --start-date YYYY-MM-DD    required unless --from-stg
#   --end-date YYYY-MM-DD      required unless --from-stg
#   --from-stg                 skip FMS, rebuild fact from staging
#   --batch-id STR             required with --from-stg
#   --force                    override MAX_RANGE_DAYS guard in extract
# ---------------------------------------------------------------

import argparse
import logging
from datetime import date

from src.pipeline.positions.fms.forwards.run import run_full, run_from_stg


def main() -> None:
    args = _parse_args()

    if args.from_stg:
        run_from_stg(batch_id=args.batch_id)
    else:
        run_full(
            start_date=args.start_date,
            end_date=args.end_date,
            force=args.force,
        )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run the FMS forwards pipeline (extract, stage, fact-load)."
    )
    p.add_argument("--start-date", type=date.fromisoformat)
    p.add_argument("--end-date", type=date.fromisoformat)
    p.add_argument("--from-stg", action="store_true",
                   help="skip FMS extract, rebuild fact from existing staging batch")
    p.add_argument("--batch-id", type=str,
                   help="required with --from-stg")
    p.add_argument("--force", action="store_true",
                   help="override MAX_RANGE_DAYS guard in extract (allows large backfills)")

    args = p.parse_args()

    if args.from_stg:
        if not args.batch_id:
            p.error("--from-stg requires --batch-id")
    else:
        if not (args.start_date and args.end_date):
            p.error("--start-date and --end-date are required unless --from-stg")

    return args


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    main()
