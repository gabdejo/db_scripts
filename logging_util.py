# src/utils/logging.py
# ---------------------------------------------------------------
# Centralised logging setup for all entry points.
# Called once at the top of every script's main() before any
# pipeline code runs.
#
# Design decisions explained inline below.
# ---------------------------------------------------------------

import logging
import sys
from datetime import date
from pathlib import Path


def setup_logging(name: str, level: int = logging.INFO) -> None:
    """
    Configures the root logger with two handlers:
      - StreamHandler  : writes to stdout (visible in terminal / NSSM logs)
      - FileHandler    : writes to data/logs/etl/{name}_YYYYMMDD.log

    name:  identifies the script/pipeline, used as the log filename stem.
           e.g. "bootstrap", "run_prices", "acquire_sbs"
    level: default INFO. Pass logging.DEBUG for troubleshooting.

    All modules in the project use logging.getLogger(__name__) which
    propagates to the root logger configured here. No per-module
    handler setup needed.
    """
    # Resolve log directory from paths module.
    # Imported here (not at module top) to avoid circular imports
    # when logging.py is imported early in bootstrap.
    from src.utils.paths import LOGS_DIR

    log_dir = LOGS_DIR
    log_dir.mkdir(parents=True, exist_ok=True)

    today    = date.today().strftime("%Y%m%d")
    log_file = log_dir / f"{name}_{today}.log"

    # Format: timestamp | level | module | message
    # %(name)s resolves to the logger name which is the full
    # dotted module path when modules use getLogger(__name__).
    # This tells you exactly which file a message came from.
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    # Root logger - all child loggers propagate here by default
    root = logging.getLogger()
    root.setLevel(level)

    # Guard: avoid adding duplicate handlers if setup_logging is
    # called more than once in the same process (e.g. in tests).
    if root.handlers:
        root.handlers.clear()

    # Handler 1: stdout
    # Using stdout rather than stderr so that NSSM and Windows
    # Task Scheduler capture output cleanly without mixing with
    # Python tracebacks which go to stderr.
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    # Handler 2: daily rotating file
    # One file per script per day: run_prices_20260310.log
    # No RotatingFileHandler needed - daily cadence means files
    # stay small. Old files accumulate in logs/etl/ and can be
    # archived or deleted on a schedule.
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # Silence noisy third-party loggers that propagate to root.
    # These produce irrelevant output at INFO level.
    _silence = [
        "urllib3",          # HTTP connection pool noise
        "selenium",         # WebDriver session management
        "WDM",              # webdriver-manager download messages
        "playwright",       # browser automation internals
        "apscheduler",      # job scheduler heartbeat messages
        "exchange_calendars", # calendar data loading
        "filelock",         # used by exchange_calendars
    ]
    for lib in _silence:
        logging.getLogger(lib).setLevel(logging.WARNING)

    # Log the startup line from the root logger using the script
    # name so the first line of every log file is self-identifying.
    logging.getLogger(name).info(
        f"Logging initialised | script={name} | "
        f"file={log_file.name} | level={logging.getLevelName(level)}"
    )
