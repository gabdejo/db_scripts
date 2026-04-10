# src/scrapers/sbs.py
# ---------------------------------------------------------------
# SBS portal acquisition logic.
#
# Public API:
#
#   acquire_day(run_date, file_types, timeout, max_retries, retry_delay)
#       Acquires files for a single reporting day.
#       Used by the daily scheduler job.
#       Does NOT query the portal for eligible dates - goes straight
#       to download. Lean and robust for daily use.
#
#   acquire_range(start_date, end_date, file_types, timeout, max_retries, retry_delay)
#       Acquires missing files across a date range.
#       Single login session. Queries portal select element for
#       eligible dates, diffs against raw/, downloads missing.
#       Used for initial bulk loads and catch-up runs.
#
#   find_latest_file(subdomain, run_date)
#       Filesystem utility - no browser interaction.
#       Used by check_sbs.py and ingestion pipeline extract.py.
#
# Shared internals:
#   _download_day   retry loop over all files for one date
#   _download_file  single file download via Selenium
# ---------------------------------------------------------------

import logging
import time
from datetime import date
from pathlib import Path
from typing import Optional

from src.utils.paths import RAW_DIR

logger = logging.getLogger(__name__)

SBS_BASE_URL  = "https://www.sbs.gob.pe"
SBS_LOGIN_URL = f"{SBS_BASE_URL}/login"

SBS_FILES = [
    {
        "name":      "tasa_activa_mn",
        "subdomain": "rates",
        "url_path":  "/app/stats/tasa-activa-mn",
        "filename":  "tasa_activa_mn.xls",
    },
    {
        "name":      "tasa_activa_me",
        "subdomain": "rates",
        "url_path":  "/app/stats/tasa-activa-me",
        "filename":  "tasa_activa_me.xls",
    },
    {
        "name":      "tasa_pasiva_mn",
        "subdomain": "rates",
        "url_path":  "/app/stats/tasa-pasiva-mn",
        "filename":  "tasa_pasiva_mn.xls",
    },
    {
        "name":      "tasa_pasiva_me",
        "subdomain": "rates",
        "url_path":  "/app/stats/tasa-pasiva-me",
        "filename":  "tasa_pasiva_me.xls",
    },
    {
        "name":      "tipo_cambio",
        "subdomain": "exchange",
        "url_path":  "/app/stats/tipo-cambio",
        "filename":  "tipo_cambio.xls",
    },
    {
        "name":      "spread",
        "subdomain": "rates",
        "url_path":  "/app/stats/spread",
        "filename":  "spread.xls",
    },
]


# ---- Public entry points ---------------------------------------

def acquire_day(
    run_date: date,
    file_types: Optional[list[str]] = None,
    timeout_seconds: int = 180,
    max_retries: int = 3,
    retry_delay: int = 30,
) -> dict[str, bool]:
    """
    Acquires all files for a single SBS reporting day.
    Used by the daily scheduler job.

    Does NOT query the portal for eligible dates - goes straight
    to download for run_date. This keeps the daily job lean and
    avoids a fragile portal parse on every routine run.

    Returns dict {file_name: True|False} per file attempted.
    """
    _assert_selenium()

    files_to_download = _resolve_file_types(file_types)
    if not files_to_download:
        return {}

    logger.info(
        f"=== SBS acquire_day | date={run_date} | "
        f"files={[f['name'] for f in files_to_download]} ==="
    )

    from src.configs.machine_config import chromedriver_path
    driver = _build_driver(chromedriver_path())

    results = {}

    try:
        driver.get(SBS_LOGIN_URL)
        logger.info(f"Waiting up to {timeout_seconds}s for manual login.")

        if not _wait_for_login(driver, timeout_seconds):
            logger.error("Login not detected. Aborting.")
            return {f["name"]: False for f in files_to_download}

        logger.info("Login detected. Downloading files.")

        day_results = _download_day(
            driver=driver,
            run_date=run_date,
            files_to_download=files_to_download,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )

        # Convert to {name: bool} for daily caller convenience
        results = {
            name: (status in ("ok", "skipped"))
            for name, status in day_results.items()
        }

    finally:
        driver.quit()

    success_count = sum(1 for ok in results.values() if ok)
    logger.info(
        f"acquire_day complete: {success_count}/{len(results)} files OK."
    )
    return results


def acquire_range(
    start_date: date,
    end_date: date,
    file_types: Optional[list[str]] = None,
    timeout_seconds: int = 180,
    max_retries: int = 3,
    retry_delay: int = 30,
) -> dict[str, list[date]]:
    """
    Acquires missing SBS files across a date range.
    Opens a single browser session for the entire range.

    Steps:
      1. Login once (manual image pad)
      2. Query portal select element for eligible dates in range
      3. Diff eligible dates against data/raw/ filesystem
      4. Download missing files with per-file retry logic

    Returns:
        {
            "succeeded": [date, ...],  all files downloaded OK
            "failed":    [date, ...],  at least one file failed
            "skipped":   [date, ...],  all files already in raw/
        }
    """
    _assert_selenium()

    files_to_download = _resolve_file_types(file_types)
    if not files_to_download:
        return {"succeeded": [], "failed": [], "skipped": []}

    logger.info(
        f"=== SBS acquire_range | "
        f"{start_date} to {end_date} | "
        f"files={[f['name'] for f in files_to_download]} | "
        f"max_retries={max_retries} ==="
    )

    from src.configs.machine_config import chromedriver_path
    driver = _build_driver(chromedriver_path())

    succeeded = []
    failed    = []
    skipped   = []

    try:
        # Step 1: login once
        driver.get(SBS_LOGIN_URL)
        logger.info(f"Waiting up to {timeout_seconds}s for manual login.")

        if not _wait_for_login(driver, timeout_seconds):
            logger.error("Login not detected. Aborting.")
            return {"succeeded": [], "failed": [], "skipped": []}

        logger.info("Login detected.")

        # Step 2: query portal for eligible dates in range
        # acquire_day skips this step - it is only needed for range runs
        # where we do not know upfront which dates the portal has data for
        logger.info("Querying portal for available dates.")
        eligible_dates = _get_eligible_dates(driver, start_date, end_date)

        if not eligible_dates:
            logger.warning("No eligible dates found in portal for given range.")
            return {"succeeded": [], "failed": [], "skipped": []}

        logger.info(f"{len(eligible_dates)} eligible dates found in portal.")

        # Step 3: diff against filesystem
        dates_to_download = _diff_against_raw(eligible_dates, files_to_download)
        already_present   = [d for d in eligible_dates if d not in dates_to_download]

        if already_present:
            skipped.extend(already_present)
            logger.info(f"{len(already_present)} dates already in raw/ (skipped).")

        if not dates_to_download:
            logger.info("All eligible dates already present. Nothing to download.")
            return {"succeeded": [], "failed": [], "skipped": skipped}

        logger.info(f"{len(dates_to_download)} dates to download.")

        # Step 4: download with retry
        for run_date in dates_to_download:
            day_results = _download_day(
                driver=driver,
                run_date=run_date,
                files_to_download=files_to_download,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )

            statuses = list(day_results.values())
            if all(s == "skipped" for s in statuses):
                skipped.append(run_date)
            elif any(s == "failed" for s in statuses):
                failed.append(run_date)
                logger.warning(
                    f"{run_date}: partial failure - "
                    f"{[k for k, v in day_results.items() if v == 'failed']}"
                )
            else:
                succeeded.append(run_date)
                logger.info(f"{run_date}: all files OK.")

    finally:
        driver.quit()

    logger.info(
        f"acquire_range complete: {len(succeeded)} succeeded, "
        f"{len(failed)} failed, {len(skipped)} skipped."
    )
    if failed:
        logger.warning(
            f"Failed dates: {[str(d) for d in sorted(failed)]}."
        )

    return {"succeeded": succeeded, "failed": failed, "skipped": skipped}


def find_latest_file(subdomain: str, run_date: date) -> Optional[Path]:
    """
    Finds the most recent .xls file in data/raw/manual/sbs/{subdomain}/{year}/
    up to and including run_date.
    Used by check_sbs.py and ingestion pipeline extract.py.
    No browser interaction.
    """
    search_dir = RAW_DIR / "manual" / "sbs" / subdomain / str(run_date.year)
    if not search_dir.exists():
        logger.warning(f"SBS raw dir does not exist: {search_dir}")
        return None

    files = sorted(search_dir.glob("*.xls*"), reverse=True)
    if not files:
        logger.warning(f"No SBS files found in {search_dir}")
        return None

    cutoff = run_date.strftime("%Y%m%d")
    target = next(
        (f for f in files if f.stem[:8] <= cutoff),
        None,
    )

    if target is None:
        logger.warning(f"No SBS file on or before {run_date} in {search_dir}")
    else:
        logger.info(f"Found SBS file: {target.name}")

    return target


# ---- Shared core: per-day download with retry ------------------

def _download_day(
    driver,
    run_date: date,
    files_to_download: list[dict],
    max_retries: int,
    retry_delay: int,
) -> dict[str, str]:
    """
    Downloads all files for a single day with per-file retry logic.
    Shared by both acquire_day and acquire_range.

    Returns dict {file_name: "ok" | "skipped" | "failed"} per file.
    "skipped" means the file already existed on disk before attempting.
    """
    date_prefix = run_date.strftime("%Y%m%d")
    results     = {}

    for file_cfg in files_to_download:
        output_path = _output_path(file_cfg, date_prefix, run_date)

        if output_path.exists():
            results[file_cfg["name"]] = "skipped"
            logger.info(f"{file_cfg['name']} {run_date}: already exists, skipping.")
            continue

        success = False
        for attempt in range(1, max_retries + 1):
            success = _download_file(driver, file_cfg, date_prefix, run_date)
            if success:
                break
            if attempt < max_retries:
                logger.warning(
                    f"{file_cfg['name']} {run_date}: "
                    f"attempt {attempt}/{max_retries} failed, "
                    f"retrying in {retry_delay}s..."
                )
                time.sleep(retry_delay)
            else:
                logger.error(
                    f"{file_cfg['name']} {run_date}: "
                    f"all {max_retries} attempts failed."
                )

        results[file_cfg["name"]] = "ok" if success else "failed"

    return results


# ---- Internal: browser helpers ---------------------------------

def _build_driver(driver_path: Optional[str]):
    """
    Builds a Selenium Chrome driver using the chromedriver path
    from machine_config.local.yaml.
    Suppresses DevTools and logging noise.
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    import os

    options = Options()
    options.add_argument("--log-level=3")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_experimental_option("useAutomationExtension", False)

    service = Service(
        executable_path=driver_path,
        log_path=os.devnull,
    )

    return webdriver.Chrome(service=service, options=options)


def _wait_for_login(driver, timeout_seconds: int) -> bool:
    """
    Polls every 3s for a post-login page element.
    Adjust selector to match the actual SBS portal.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException

    poll_interval = 3
    elapsed       = 0

    while elapsed < timeout_seconds:
        try:
            WebDriverWait(driver, poll_interval).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "#main-menu")  # ← replace with actual selector
                )
            )
            return True
        except TimeoutException:
            elapsed   += poll_interval
            remaining  = timeout_seconds - elapsed
            if remaining > 0:
                logger.info(f"Waiting for login... {remaining}s remaining.")

    return False


def _get_eligible_dates(
    driver,
    start_date: date,
    end_date: date,
) -> list[date]:
    """
    Reads the date select element from the portal within the active session.
    Returns dates available in the portal within [start_date, end_date].
    Only called by acquire_range - acquire_day skips this entirely.
    Adjust selector and date parsing to match the actual SBS portal.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait, Select
    from selenium.webdriver.support import expected_conditions as EC

    try:
        driver.get(f"{SBS_BASE_URL}/app/stats/tasa-activa-mn")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "select#fecha")  # ← replace with actual selector
            )
        )

        select_el     = Select(driver.find_element(By.CSS_SELECTOR, "select#fecha"))
        option_values = [opt.get_attribute("value") for opt in select_el.options]

        eligible = []
        for val in option_values:
            if not val:
                continue
            try:
                d = date.fromisoformat(val)  # ← adjust if portal uses DD/MM/YYYY
                if start_date <= d <= end_date:
                    eligible.append(d)
            except ValueError:
                continue

        logger.info(
            f"Portal has {len(eligible)} eligible dates "
            f"between {start_date} and {end_date}."
        )
        return sorted(eligible)

    except Exception as e:
        logger.error(
            f"Failed to read eligible dates from portal: {e}",
            exc_info=True,
        )
        return []


def _download_file(
    driver,
    file_cfg: dict,
    date_prefix: str,
    run_date: date,
) -> bool:
    """
    Navigates to a file download page and saves to data/raw/.
    Returns True on success, False on failure.
    Adjust click selector to match actual SBS download trigger.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import shutil

    output_path = _output_path(file_cfg, date_prefix, run_date)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        return True

    try:
        url = f"{SBS_BASE_URL}{file_cfg['url_path']}"
        logger.info(f"Downloading {file_cfg['name']} | {run_date} | {url}")

        driver.get(url)
        WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "a.download-link")  # ← replace with actual selector
            )
        ).click()

        downloaded = _wait_for_download(file_cfg["filename"], timeout=60)
        if downloaded:
            shutil.move(str(downloaded), str(output_path))
            logger.info(f"{file_cfg['name']}: saved to {output_path.name}")
            return True
        else:
            logger.error(f"{file_cfg['name']}: download timed out.")
            return False

    except Exception as e:
        logger.error(
            f"{file_cfg['name']} {run_date}: download failed - {e}",
            exc_info=True,
        )
        return False


def _wait_for_download(filename: str, timeout: int = 60) -> Optional[Path]:
    """
    Waits for a file to appear in the system Downloads folder.
    Returns the Path once the file is fully written, None on timeout.
    """
    import os
    downloads_dir = Path(os.path.expandvars("%USERPROFILE%")) / "Downloads"
    target        = downloads_dir / filename
    elapsed       = 0

    while elapsed < timeout:
        if (
            target.exists()
            and not (downloads_dir / f"{filename}.crdownload").exists()
        ):
            return target
        time.sleep(1)
        elapsed += 1

    return None


# ---- Internal: utilities ---------------------------------------

def _resolve_file_types(file_types: Optional[list[str]]) -> list[dict]:
    """Returns subset of SBS_FILES matching file_types, or all if None."""
    if file_types is None:
        return SBS_FILES

    known_names = {f["name"] for f in SBS_FILES}
    unknown     = [ft for ft in file_types if ft not in known_names]
    if unknown:
        logger.warning(
            f"Unrecognised file types (ignored): {unknown}. "
            f"Known: {sorted(known_names)}"
        )

    resolved = [f for f in SBS_FILES if f["name"] in file_types]
    if not resolved:
        logger.warning("No valid file types matched. Nothing to download.")
    return resolved


def _diff_against_raw(
    eligible_dates: list[date],
    files_to_download: list[dict],
) -> list[date]:
    """
    Returns dates where at least one expected file is missing from raw/.
    A date is complete only when ALL expected files exist.
    """
    missing = []
    for d in eligible_dates:
        date_prefix = d.strftime("%Y%m%d")
        all_present = all(
            _output_path(f, date_prefix, d).exists()
            for f in files_to_download
        )
        if not all_present:
            missing.append(d)
    return missing


def _output_path(file_cfg: dict, date_prefix: str, run_date: date) -> Path:
    """Returns the expected output path for a given file and date."""
    return (
        RAW_DIR
        / "manual"
        / "sbs"
        / file_cfg["subdomain"]
        / str(run_date.year)
        / f"{date_prefix}_{file_cfg['filename']}"
    )


def _assert_selenium() -> None:
    """Raises ImportError with install instructions if selenium missing."""
    try:
        import selenium
    except ImportError:
        raise ImportError(
            "selenium is not installed. Run: pip install selenium"
        )
