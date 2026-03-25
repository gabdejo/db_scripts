# scripts/scheduler_scraper.py
# ---------------------------------------------------------------
# Scheduler for acquisition machines (colleague or Gabriel's PC).
# Runs SBS file acquisition only.
# Ingestion runs on the central PC via scheduler_central.py.
#
# Key calendar logic:
#   - Uses prev_reporting_day from calendar_sbs to determine
#     which date to acquire. This handles:
#       Monday       -> acquires Friday
#       Tuesday-Fri  -> acquires previous day
#       After holiday -> acquires last reporting day
#   - A reporting day is any day NYSE or XLIM was open.
#     The SBS portal reports data whenever either exchange is open.
#   - CronTrigger runs Mon-Fri only since PC is off on weekends.
#     Friday data is correctly acquired on Monday morning via
#     prev_reporting_day.
#
# Requires: pip install apscheduler exchange_calendars
#
# Usage:
#   python scripts/scheduler_scraper.py
#
# To run as a background Windows service:
#   nssm install MarketDataScraper python scripts/scheduler_scraper.py
#   nssm start MarketDataScraper
# ---------------------------------------------------------------

import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from src.configs.machine_config import assert_scraper, scraper_enabled, timezone
from src.utils.logging import setup_logging

setup_logging("scheduler_scraper")
logger = logging.getLogger(__name__)

# Fail immediately with a clear message if run on wrong machine
assert_scraper()

TZ = timezone()
scheduler = BlockingScheduler(timezone=TZ)


# ---- Job definitions -------------------------------------------

def job_acquire_sbs():
    """
    Semi-automated SBS daily acquisition.

    Determines run_date as the previous SBS reporting day:
      - Monday morning   -> Friday (last reporting day before weekend)
      - Tuesday-Friday   -> previous calendar day
      - After local holiday -> last day either exchange was open

    Opens Chromium browser for manual image pad login, then
    automatically downloads all files for run_date.
    Files land on network share for central PC ingestion at 09:10.
    """
    from src.calendars.calendar_sbs import prev_reporting_day, is_reporting_day

    today    = date.today()
    run_date = prev_reporting_day(today)

    logger.info(
        f"--- job: acquire sbs | today={today} | "
        f"acquiring reporting day={run_date} ---"
    )

    # Sanity check - should always pass since prev_reporting_day
    # guarantees a valid reporting day
    if not is_reporting_day(run_date):
        logger.error(
            f"{run_date} is not an SBS reporting day. "
            f"This should not happen - check calendar_sbs logic."
        )
        return

    from src.scrapers.sbs import acquire_all
    results = acquire_all(run_date=run_date)

    success_count = sum(1 for ok in results.values() if ok)
    total         = len(results)

    logger.info(f"SBS acquisition: {success_count}/{total} files downloaded.")

    failed = [name for name, ok in results.items() if not ok]
    if failed:
        logger.error(
            f"SBS acquisition failed for: {failed}. "
            f"Central PC ingestion may use stale data. "
            f"Re-run manually: python scripts/acquire/acquire_sbs.py "
            f"--date {run_date}"
        )


# ---- Schedule definitions --------------------------------------

# SBS acquisition - Mon-Fri at 08:00 local time.
# PC is off on weekends so no Saturday job needed.
# Monday correctly acquires Friday via prev_reporting_day.
# Timed to complete before central PC check_sbs at 09:00
# and ingestion at 09:10.
scheduler.add_job(
    job_acquire_sbs,
    CronTrigger(day_of_week="mon-fri", hour=8, minute=0),
    id="acquire_sbs",
    name="SBS daily acquisition",
    misfire_grace_time=3600,    # run up to 1h late if machine was slow to boot
    coalesce=True,              # if somehow fired twice, run once
    max_instances=1,            # never open two browser sessions simultaneously
)


# ---- Entry point -----------------------------------------------

if __name__ == "__main__":
    logger.info("=== Scraper scheduler started ===")
    logger.info(f"Timezone:        {TZ}")
    logger.info(f"Scraper enabled: {scraper_enabled()}")
    logger.info(f"Registered jobs: {[j.id for j in scheduler.get_jobs()]}")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("=== Scraper scheduler stopped ===")
        scheduler.shutdown()
