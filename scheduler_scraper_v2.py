# scripts/scheduler_scraper.py
# ---------------------------------------------------------------
# Scheduler for acquisition machines.
# Based on session context: runs on the Bloomberg workstation
# which is always on during market hours. Both bloomberg_enabled
# and scraper_enabled are true on that machine.
#
# Runs SBS file acquisition only.
# Ingestion runs on the i7 PostgreSQL server via scheduler_central.py.
#
# Key calendar logic:
#   - Uses prev_reporting_day from calendar_sbs to determine
#     which date to acquire. This handles:
#       Monday       -> acquires Friday
#       Tuesday-Fri  -> acquires previous calendar day
#       After holiday -> acquires last day either exchange was open
#   - A reporting day is any day NYSE or XLIM was open.
#     The SBS portal reports data whenever either exchange is open.
#   - CronTrigger runs Mon-Fri only since machine is off on weekends.
#     Friday data is correctly acquired on Monday morning via
#     prev_reporting_day.
#
# Beep behaviour:
#   - A repeating beep starts when the job fires to alert the
#     operator that the browser is opening and image pad login
#     is required.
#   - Beeping stops automatically when acquisition completes,
#     whether successful, partial, or timed out on login.
#   - Beep runs in a daemon thread so it never outlives the
#     scheduler process.
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
import threading
import time
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


# ---- Beep helper -----------------------------------------------

def _beep_until_stopped(stop_event: threading.Event) -> None:
    """
    Plays a repeating beep until stop_event is set.
    Runs in a daemon thread - dies with the scheduler process.
    Checks stop_event every 100ms during pauses so it stops
    promptly when acquisition completes.
    """
    import winsound
    while not stop_event.is_set():
        winsound.Beep(880, 300)
        # Pause between beeps in 100ms increments to stay responsive
        for _ in range(7):
            if stop_event.is_set():
                return
            time.sleep(0.1)


# ---- Job definitions -------------------------------------------

def job_acquire_sbs():
    """
    Semi-automated SBS daily acquisition.

    Determines run_date as the previous SBS reporting day:
      - Monday morning   -> Friday (last reporting day before weekend)
      - Tuesday-Friday   -> previous calendar day
      - After local holiday -> last day either exchange was open

    Starts a repeating beep to alert the operator that the browser
    is opening and image pad login is required. Beep stops
    automatically when acquisition completes in any outcome.

    Downloads all files for run_date via Selenium.
    Files land on the network share for i7 server ingestion at 09:10.
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

    # Start beeping to alert operator before browser opens
    stop_event  = threading.Event()
    beep_thread = threading.Thread(
        target=_beep_until_stopped,
        args=(stop_event,),
        daemon=True,
        name="sbs_beep",
    )
    beep_thread.start()
    logger.info("Beep started - browser opening, image pad login required.")

    try:
        from src.scrapers.sbs import acquire_day
        results = acquire_day(run_date=run_date)

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

    except Exception as e:
        logger.error(
            f"SBS acquisition error: {e}",
            exc_info=True,
        )

    finally:
        # Always stop beeping regardless of outcome:
        # successful download, partial failure, login timeout, or exception
        stop_event.set()
        beep_thread.join(timeout=1)
        logger.info("Beep stopped.")


# ---- Schedule definitions --------------------------------------

# SBS acquisition - Mon-Fri at 08:00 local time.
# Machine is off on weekends so no Saturday job needed.
# Monday correctly acquires Friday via prev_reporting_day.
# Timed to complete before i7 server check_sbs at 09:00
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
