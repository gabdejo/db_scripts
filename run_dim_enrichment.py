# scripts/run_dim_enrichment.py
# ---------------------------------------------------------------
# Generic entry point for dim enrichment pipelines.
# Dispatches to vendor/domain-specific enrichment pipelines
# via the registry in src/db/bootstrap.run_dim_enrichment().
#
# Run weekly (Sunday night via scheduler_central.py) or
# on-demand after new securities are onboarded.
#
# Usage:
#   # Enrich all vendors and all domains
#   python scripts/run_dim_enrichment.py
#
#   # Enrich bloomberg securities only
#   python scripts/run_dim_enrichment.py --vendor bloomberg --domain security
#
#   # Enrich all bloomberg dims
#   python scripts/run_dim_enrichment.py --vendor bloomberg
#
#   # Enrich all security dims across all vendors
#   python scripts/run_dim_enrichment.py --domain security
# ---------------------------------------------------------------

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.configs.machine_config import assert_bloomberg
from src.utils.logging import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run dim enrichment pipelines.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--vendor",
        type=str,
        default=None,
        help=(
            "Filter by vendor (e.g. bloomberg, refinitiv). "
            "Runs all vendors if omitted."
        ),
    )
    parser.add_argument(
        "--domain",
        type=str,
        default=None,
        help=(
            "Filter by domain (e.g. security). "
            "Runs all domains if omitted."
        ),
    )

    args = parser.parse_args()
    setup_logging("run_dim_enrichment")

    # All dim enrichment pipelines currently require Bloomberg access.
    # If non-Bloomberg vendors are added later, move this guard
    # inside the vendor-specific branch.
    assert_bloomberg()

    logger.info(
        f"=== Dim enrichment | "
        f"vendor={args.vendor or 'all'} | "
        f"domain={args.domain or 'all'} ==="
    )

    from src.db.bootstrap import run_dim_enrichment
    run_dim_enrichment(vendor=args.vendor, domain=args.domain)


if __name__ == "__main__":
    main()
