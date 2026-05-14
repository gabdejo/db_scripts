# src/vendors/fms.py
# ---------------------------------------------------------------
# FMS (SQL Server) vendor adapter.
#
# FMS is a shared, stressed transactional system. Two rules
# follow from that:
#   1. Always go through call_sproc — never inline ad-hoc SQL.
#      Sprocs are the contract; ad-hoc queries break it and
#      bypass DBA-controlled execution plans.
#   2. Every connection has explicit login + query timeouts so
#      a slow FMS doesn't hang the scheduler indefinitely.
#
# Connection settings live in machine_config under an `fms` block:
#
#   fms:
#     enabled: true
#     driver: "ODBC Driver 17 for SQL Server"
#     server: "fmsdb.internal"
#     database: "FMS_PROD"
#     auth: "windows"        # windows | sql
#     username: null
#     password: null
#     query_timeout_s: 120
#     login_timeout_s: 10
#
# assert_fms() mirrors assert_bloomberg / assert_scraper /
# assert_automated_scraper. Pipelines call it at run() top.
# ---------------------------------------------------------------

import logging
import time
from contextlib import contextmanager
from typing import Any, Iterator

import pyodbc

from src.configs.machine_config import load_machine_config

logger = logging.getLogger(__name__)


class FMSConfigError(RuntimeError):
    pass


def assert_fms() -> None:
    """Refuse to run on machines where FMS access is disabled."""
    cfg = load_machine_config()
    fms = cfg.get("fms") or {}
    if not fms.get("enabled"):
        raise FMSConfigError(
            "fms.enabled is False or missing in machine_config; "
            "FMS pipelines refuse to run on this machine"
        )


def _build_conn_str() -> tuple[str, int, int]:
    """Returns (conn_str, query_timeout_s, login_timeout_s)."""
    cfg = load_machine_config()
    fms = cfg.get("fms") or {}

    required = ("driver", "server", "database", "auth")
    missing = [k for k in required if not fms.get(k)]
    if missing:
        raise FMSConfigError(f"machine_config.fms missing keys: {missing}")

    parts = [
        f"DRIVER={{{fms['driver']}}}",
        f"SERVER={fms['server']}",
        f"DATABASE={fms['database']}",
    ]

    auth = fms["auth"]
    if auth == "windows":
        parts.append("Trusted_Connection=yes")
    elif auth == "sql":
        if not fms.get("username") or not fms.get("password"):
            raise FMSConfigError("fms.auth=sql requires username and password")
        parts.append(f"UID={fms['username']}")
        parts.append(f"PWD={fms['password']}")
    else:
        raise FMSConfigError(f"unknown fms.auth: {auth!r}")

    conn_str      = ";".join(parts) + ";"
    query_timeout = int(fms.get("query_timeout_s", 120))
    login_timeout = int(fms.get("login_timeout_s", 10))
    return conn_str, query_timeout, login_timeout


@contextmanager
def fms_connection() -> Iterator[pyodbc.Connection]:
    """
    Context-managed read-only connection to FMS. Always closes,
    even on exception. Login + query timeouts set from config.
    """
    assert_fms()
    conn_str, query_timeout, login_timeout = _build_conn_str()
    logger.debug(f"opening FMS connection (login_timeout={login_timeout}s)")
    conn = pyodbc.connect(conn_str, timeout=login_timeout, readonly=True)
    conn.timeout = query_timeout
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            logger.warning("error closing FMS connection", exc_info=True)


def call_sproc(
    sproc_name: str,
    params: tuple[Any, ...] = (),
    *,
    max_retries: int = 2,
    backoff_s: float = 5.0,
) -> list[dict]:
    """
    Execute an FMS stored procedure and return rows as list[dict].

    Retries transient errors (timeouts, deadlocks, dropped conns)
    with linear backoff. Does NOT retry logical errors (bad params,
    permission denied) — those bubble immediately.
    """
    placeholders = ",".join("?" * len(params)) if params else ""
    sql = f"EXEC {sproc_name} {placeholders}".strip()

    attempt = 0
    while True:
        attempt += 1
        try:
            with fms_connection() as conn:
                cur = conn.cursor()
                t0 = time.monotonic()
                logger.info(f"calling sproc {sproc_name} (attempt {attempt})")
                cur.execute(sql, params)
                cols = [c[0] for c in cur.description] if cur.description else []
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                elapsed = time.monotonic() - t0
                logger.info(
                    f"sproc {sproc_name}: {len(rows)} rows in {elapsed:.1f}s"
                )
                return rows
        except pyodbc.OperationalError as e:
            if attempt > max_retries:
                logger.error(
                    f"sproc {sproc_name} failed after {attempt} attempts: {e}"
                )
                raise
            wait = backoff_s * attempt
            logger.warning(
                f"sproc {sproc_name} transient failure (attempt {attempt}): {e}; "
                f"retrying in {wait:.1f}s"
            )
            time.sleep(wait)
        except pyodbc.Error:
            logger.exception(f"sproc {sproc_name} non-retryable error")
            raise
