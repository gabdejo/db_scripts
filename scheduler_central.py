# scripts/scheduler_central.py
# ---------------------------------------------------------------
# Scheduler for the central PC.
# Runs all Bloomberg ingestion pipelines and SBS ingestion.
# Self-configures based on machine_config.local.yaml:
#   - bloomberg_enabled must be true (asserted at startup)
#   - if scraper_enabled is also true, runs SBS acquisition
#     locally and skips check_sbs
#   - if scraper_enabled is false, runs check_sbs before
#     ingestion to gate on remote files arriving
#
# Requires: pip install apscheduler exchange_calendars
#
# Usage:
#   python scripts/scheduler_central.py
#
# To run as a background Windows service:
#   nssm install MarketDataCentral python scripts/scheduler_central.py
#   nssm start MarketDataCentral
# ---------------------------------------------------------------

import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from src.configs.machine_config import (
    assert_bloomberg,
    bloomberg_enabled,
    scraper_enabled,
    timezone,
)
from src.utils.logging import setup_logging

setup_logging("scheduler_central")
logger = logging.getLogger(__name__)

# Fail immediately with a clear message if run on wrong machine
assert_bloomberg()

TZ = timezone()
scheduler = BlockingScheduler(timezone=TZ)


# ---- Calendar helpers ------------------------------------------

def _is_first_business_day_of_month(d: date, exchange: str = "nyse") -> bool:
    """
    Returns True if d is the first trading session of its month.
    Guards monthly jobs against APScheduler firing on day=1
    when it falls on a weekend or holiday.
    """
    if exchange == "nyse":
        from src.calendars.calendar_nyse import first_business_day_of_month
    else:
        from src.calendars.calendar_xlim import first_business_day_of_month
    return d == first_business_day_of_month(d.year, d.month)


def _is_first_business_day_after_quarter_close(
    d: date,
    lag_days: int = 0,
) -> bool:
    """
    Returns True if d is the first NYSE session after the most
    recently closed quarter + lag_days.
    Guards quarterly jobs against firing on the wrong day.
    """
    from src.calendars.calendar_nyse import first_business_day_after_quarter_close
    month   = d.month
    quarter = (month - 1) // 3 + 1
    # Identify the quarter that most recently closed relative to d
    prev_quarter = quarter - 1 if quarter > 1 else 4
    prev_year    = d.year if quarter > 1 else d.year - 1
    expected = first_business_day_after_quarter_close(
        prev_year, prev_quarter, lag_days=lag_days
    )
    return d == expected


# ---- Job definitions -------------------------------------------

def job_prices_bloomberg():
    """
    Daily Bloomberg prices ingestion.
    Calendar check (is_business_day) is inside run() itself -
    skips non-NYSE trading days without logging noise here.
    """
    logger.info("--- job: prices bloomberg ---")
    from src.pipeline.prices.bloomberg.run import run
    run(run_date=date.today())


def job_macro_bloomberg_monthly():
    """
    Monthly Bloomberg macro ingestion.
    APScheduler fires on day=1 of every month. The internal
    guard checks the actual first NYSE business day to handle
    cases where the 1st is a weekend or holiday.
    """
    logger.info("--- job: macro bloomberg monthly ---")
    today = date.today()
    if not _is_first_business_day_of_month(today, exchange="nyse"):
        logger.info(
            f"{today} is not the first NYSE business day of the month. "
            f"Skipping macro monthly."
        )
        return
    from src.pipeline.macro.bloomberg.run import run
    run(run_date=today, frequency="monthly")


def job_macro_bloomberg_quarterly():
    """
    Quarterly Bloomberg macro ingestion.
    Fires on 1st of Jan/Apr/Jul/Oct. Internal guard checks the
    actual first NYSE session after quarter close + 5 day lag
    to give Bloomberg time to publish revised data.
    """
    logger.info("--- job: macro bloomberg quarterly ---")
    today = date.today()
    if not _is_first_business_day_after_quarter_close(today, lag_days=5):
        logger.info(
            f"{today} is not the first NYSE business day after "
            f"quarter close + 5 days. Skipping macro quarterly."
        )
        return
    from src.pipeline.macro.bloomberg.run import run
    run(run_date=today, frequency="quarterly")


def job_fundamentals_bloomberg():
    """
    Quarterly Bloomberg fundamentals ingestion.
    Offset ~2 weeks from macro quarterly to allow Bloomberg
    time to publish full earnings data.
    No extra calendar guard - runs on whichever business day
    the 15th falls closest to.
    """
    logger.info("--- job: fundamentals bloomberg ---")
    from src.pipeline.fundamentals.bloomberg.run import run
    run(run_date=date.today())


def job_dim_enrichment():
    """
    Weekly Bloomberg dim enrichment.
    No calendar guard - runs Sunday night regardless of exchange.
    """
    logger.info("--- job: dim enrichment bloomberg ---")
    from src.db.bootstrap import run_dim_enrichment
    run_dim_enrichment(vendor="bloomberg", domain="security")


def job_acquire_sbs():
    """
    SBS acquisition - only registered if scraper_enabled on this machine.
    Uses prev_reporting_day so Monday correctly acquires Friday,
    and days after local holidays acquire the last reporting day.
    Reporting day = NYSE or XLIM open.
    """
    logger.info("--- job: acquire sbs (local) ---")
    from src.calendars.calendar_sbs import prev_reporting_day
    run_date = prev_reporting_day(date.today())
    logger.info(f"Acquiring SBS data for reporting day: {run_date}")

    from src.scrapers.sbs import acquire_all
    results = acquire_all(run_date=run_date)

    failed = [name for name, ok in results.items() if not ok]
    if failed:
        logger.error(
            f"SBS acquisition failed for: {failed}. "
            f"Ingestion may use stale data. "
            f"Re-run manually: python scripts/acquire/acquire_sbs.py "
            f"--date {run_date}"
        )


def job_check_sbs():
    """
    SBS file check - only registered if scraper runs on remote machine.
    Verifies prev_reporting_day files arrived on network share.
    Raises RuntimeError on missing files so APScheduler logs the
    failure and ingestion window passes without running.
    """
    logger.info("--- job: check sbs ---")
    from src.calendars.calendar_sbs import prev_reporting_day
    from src.scrapers.sbs import find_latest_file, SBS_FILES

    run_date    = prev_reporting_day(date.today())
    date_prefix = run_date.strftime("%Y%m%d")
    logger.info(f"Checking SBS files for reporting day: {run_date}")

    missing = []
    for f in SBS_FILES:
        path = find_latest_file(f["subdomain"], run_date)
        if path is None or path.stem[:8] != date_prefix:
            missing.append(f["name"])
        else:
            logger.info(f"  {f['name']}: OK ({path.name})")

    if missing:
        raise RuntimeError(
            f"SBS files missing for {run_date}: {missing}. "
            f"Ingestion will not run until files arrive."
        )
    logger.info(f"All SBS files present for {run_date}.")


def job_ingest_sbs():
    """
    SBS ingestion.
    Uses prev_reporting_day consistent with acquisition and check.
    Verifies the date is an SBS reporting day before running
    (NYSE or XLIM open).
    """
    logger.info("--- job: ingest sbs ---")
    from src.calendars.calendar_sbs import prev_reporting_day, is_reporting_day
    run_date = prev_reporting_day(date.today())

    if not is_reporting_day(run_date):
        logger.info(
            f"{run_date} is not an SBS reporting day "
            f"(neither NYSE nor XLIM open). Skipping ingestion."
        )
        return

    from src.pipeline.macro.sbs.run import run
    run(run_date=run_date)


# ---- Schedule definitions --------------------------------------

# Bloomberg prices - after NYSE close Mon-Fri
# Calendar check inside run() handles non-trading days
scheduler.add_job(
    job_prices_bloomberg,
    CronTrigger(
        day_of_week="mon-fri",
        hour=18,
        minute=30,
        timezone="America/New_York",
    ),
    id="prices_bloomberg",
    name="Bloomberg daily prices",
    misfire_grace_time=3600,
    coalesce=True,
    max_instances=1,
)

# Bloomberg macro monthly - fire on 1st of each month
# Job guards on actual first NYSE business day internally
scheduler.add_job(
    job_macro_bloomberg_monthly,
    CronTrigger(day=1, hour=9, minute=0),
    id="macro_bloomberg_monthly",
    name="Bloomberg macro monthly",
    misfire_grace_time=86400,
    coalesce=True,
    max_instances=1,
)

# Bloomberg macro quarterly - fire on 1st of Jan/Apr/Jul/Oct
# Job guards on actual first NYSE session after quarter close + 5d
scheduler.add_job(
    job_macro_bloomberg_quarterly,
    CronTrigger(month="1,4,7,10", day=1, hour=9, minute=30),
    id="macro_bloomberg_quarterly",
    name="Bloomberg macro quarterly",
    misfire_grace_time=86400,
    coalesce=True,
    max_instances=1,
)

# Bloomberg fundamentals - 15th of month after quarter close
# Offset from macro to allow full earnings publication
scheduler.add_job(
    job_fundamentals_bloomberg,
    CronTrigger(month="1,4,7,10", day=15, hour=9, minute=0),
    id="fundamentals_bloomberg",
    name="Bloomberg fundamentals quarterly",
    misfire_grace_time=86400,
    coalesce=True,
    max_instances=1,
)

# Dim enrichment - weekly Sunday night
scheduler.add_job(
    job_dim_enrichment,
    CronTrigger(day_of_week="sun", hour=22, minute=0),
    id="dim_enrichment",
    name="Bloomberg dim enrichment",
    misfire_grace_time=3600,
    coalesce=True,
    max_instances=1,
)

# SBS jobs - self-configures based on machine_config.local.yaml
if scraper_enabled():
    # Acquisition runs on this machine
    # acquire at 08:00, ingest at 09:10 with no check needed
    scheduler.add_job(
        job_acquire_sbs,
        CronTrigger(day_of_week="mon-fri", hour=8, minute=0),
        id="acquire_sbs",
        name="SBS acquisition (local)",
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
    )
else:
    # Acquisition runs on a remote machine
    # check at 09:00, ingest at 09:10
    # check_sbs raises RuntimeError on missing files so
    # ingest window passes without running on bad days
    scheduler.add_job(
        job_check_sbs,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=0),
        id="check_sbs",
        name="SBS file check",
        misfire_grace_time=1800,
        coalesce=True,
        max_instances=1,
    )

# Ingest always registered regardless of local/remote acquisition
scheduler.add_job(
    job_ingest_sbs,
    CronTrigger(day_of_week="mon-fri", hour=9, minute=10),
    id="ingest_sbs",
    name="SBS ingestion",
    misfire_grace_time=1800,
    coalesce=True,
    max_instances=1,
)


# ---- Entry point -----------------------------------------------

if __name__ == "__main__":
    logger.info("=== Central scheduler started ===")
    logger.info(f"Timezone:                    {TZ}")
    logger.info(f"Bloomberg enabled:           {bloomberg_enabled()}")
    logger.info(f"Scraper enabled (local acq): {scraper_enabled()}")
    logger.info(
        f"Registered jobs:             "
        f"{[j.id for j in scheduler.get_jobs()]}"
    )
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("=== Central scheduler stopped ===")
        scheduler.shutdown()
