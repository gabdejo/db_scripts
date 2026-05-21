# src/vendors/fms.py
# ---------------------------------------------------------------
# FMS (SQL Server) vendor adapter.
#
# FMS is a shared, stressed transactional system. Two rules
# follow from that:
#   1. Always go through call_sproc - never inline ad-hoc SQL.
#      Sprocs are the contract; ad-hoc queries break it and
#      bypass DBA-controlled execution plans.
#   2. Every connection has explicit login + query timeouts so
#      a slow FMS doesn't hang the scheduler indefinitely.
#
# Auth is Windows (Trusted), so the connection string is pure
# non-secret topology - see get_fms_connection_string() below for
# where it lives and how to relocate it to config later.
#
# "Should this machine run FMS ingestion on a schedule" is a
# separate, machine-specific concern - that belongs in
# machine_config (e.g. fms_ingestion_enabled), NOT here. This
# module only knows how to talk to FMS, not whether it should.
#
# Public sprocs (existing on FMS):
#   sp_GetPositions     daily holdings per account
#   sp_GetPortfolios    portfolio metadata
#   sp_GetTransactions  trade-level data
# ---------------------------------------------------------------

import logging
import time
from contextlib import contextmanager
from typing import Any, Iterator

import pyodbc

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------
# Connection topology.
#
# FMS uses Windows (Trusted) auth, so this string carries no
# secret - it's just server + database coordinates, identical on
# every machine. It lives here as a constant for now.
#
# If the topology ever moves (DB migration, a read replica stood
# up to take load off the transactional instance, prod/DR
# failover), relocate the value to a committed configs/fms.yaml
# and change ONLY the body of get_fms_connection_string() to read
# from config_loader. Nothing that calls the accessor needs to
# change - that's the whole point of going through a function.
# ---------------------------------------------------------------
FMS_CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=fmsdb.internal;"          # TODO: confirm actual server name
    "DATABASE=FMS_PROD;"              # TODO: confirm actual database name
    "Trusted_Connection=yes;"
)

# Timeouts (seconds) - kept conservative because FMS is shared.
LOGIN_TIMEOUT_S = 10
QUERY_TIMEOUT_S = 120


def get_fms_connection_string() -> str:
    """
    Single read-point for the FMS connection string.

    Today this returns the module constant. To move the value into
    config later, swap the body to read from config_loader and
    leave every caller untouched.
    """
    conn_str = FMS_CONNECTION_STRING
    if not conn_str or not conn_str.strip():
        raise RuntimeError("FMS connection string is empty - check fms.py")
    return conn_str


@contextmanager
def get_fms_connection() -> Iterator[pyodbc.Connection]:
    """
    Context-managed read-only connection to FMS. Always closes,
    even on exception. Login + query timeouts set from constants
    above; FMS is shared, so we never block the scheduler.
    """
    conn_str = get_fms_connection_string()
    logger.debug(f"opening FMS connection (login_timeout={LOGIN_TIMEOUT_S}s)")
    conn = pyodbc.connect(conn_str, timeout=LOGIN_TIMEOUT_S, readonly=True)
    conn.timeout = QUERY_TIMEOUT_S
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
    permission denied) - those bubble immediately.
    """
    placeholders = ",".join("?" * len(params)) if params else ""
    sql = f"EXEC {sproc_name} {placeholders}".strip()

    attempt = 0
    while True:
        attempt += 1
        try:
            with get_fms_connection() as conn:
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
