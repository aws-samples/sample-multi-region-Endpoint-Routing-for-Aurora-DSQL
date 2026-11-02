# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
db.py

Data-access layer for the pet store API. Reuses the parent repo's
DSQLConnectionManager so the Lambda rides the same client-side multi-Region
routing and failover as the rest of this sample.

Built for the load + failover demo:
  - A module-level connection is cached and reused across warm Lambda
    invocations (new connections are expensive on DSQL: IAM token + TLS).
  - Every query runs through _run(), which on a connection-level failure --
    what a Region interruption looks like -- discards the dead connection and
    retries once. The retry calls get_connection() again, which re-runs the
    manager's endpoint selection, so the retry lands on a healthy Region. That
    is the "seamless failover" the demo shows.

Aurora DSQL compatibility baked into the SQL:
  - Primary keys are client-generated UUIDs (DSQL has no SERIAL/sequences).
  - No foreign keys (DSQL does not support them).
  - Each row records the Region it was written through plus an updated_at
    timestamp -- the signal the switchover / observability demo visualizes.
"""

import os
import time
import uuid
import random
import logging
from datetime import datetime, timezone

import boto3
import psycopg2
from psycopg2 import OperationalError, InterfaceError, DatabaseError

from dsql_connection_manager import DSQLConnectionManager

# Aurora DSQL uses optimistic concurrency control (OCC): overlapping concurrent
# transactions are rejected at COMMIT with a serialization failure
# (SQLSTATE 40001, surfaced as "OC000 change conflicts with another
# transaction"). The documented fix is to retry the transaction with backoff +
# jitter -- it is NOT a connection problem and must NOT trigger a Region reroute.
OCC_SQLSTATE = "40001"
_OCC_MAX_RETRIES = int(os.environ.get("OCC_MAX_RETRIES", "5"))
_OCC_BASE_DELAY = float(os.environ.get("OCC_BASE_DELAY_SECONDS", "0.02"))  # 20ms

logger = logging.getLogger()

DATABASE = os.environ.get("DSQL_DATABASE", "postgres")
DB_USER = os.environ.get("DSQL_USER", "admin")
_CONFIG_FILE = os.environ.get("DSQL_CONFIG_FILE", "dsql_config.json")

# Failover demo: an SSM parameter names a Region to treat as "interrupted".
# The Lambda drops that Region from endpoint selection so traffic reroutes to
# the other Region. The value is cached briefly to avoid an SSM read per call.
_INTERRUPT_PARAM = os.environ.get("INTERRUPT_PARAM_NAME", "")
_INTERRUPT_TTL = float(os.environ.get("INTERRUPT_TTL_SECONDS", "5"))
_ssm = None
_interrupt_cache = {"value": "none", "ts": 0.0}

# Reused across warm invocations within one container.
_manager = None
_conn = None
_conn_region = None  # region of the current cached connection
_conn_flag = "none"  # interrupt-flag value the current connection was opened under

# Connection-level errors that mean "this endpoint is gone" -> reroute.
_RETRYABLE = (OperationalError, InterfaceError)


def _get_manager():
    global _manager
    if _manager is None:
        _manager = DSQLConnectionManager(config_file=_CONFIG_FILE)
    return _manager


def _interrupted_region() -> str:
    """Return the Region currently marked interrupted ('none' if not set).

    Reads the SSM flag, cached for _INTERRUPT_TTL seconds. Fails open: any
    error reading SSM leaves routing unchanged (returns 'none').
    """
    if not _INTERRUPT_PARAM:
        return "none"
    now = time.time()
    if now - _interrupt_cache["ts"] < _INTERRUPT_TTL:
        return _interrupt_cache["value"]
    global _ssm
    try:
        if _ssm is None:
            _ssm = boto3.client("ssm")
        resp = _ssm.get_parameter(Name=_INTERRUPT_PARAM)
        value = (resp["Parameter"]["Value"] or "none").strip()
    except Exception:  # noqa: BLE001 - fail open, never block traffic on SSM
        value = "none"
    _interrupt_cache["value"] = value
    _interrupt_cache["ts"] = now
    return value


def _get_connection():
    """Return a healthy cached connection, honoring the interrupt flag.

    If the Region currently serving our cached connection is the interrupted
    one, drop it so we reconnect elsewhere. When (re)connecting, hide the
    interrupted Region's endpoints from the manager so its latency selection
    picks the surviving Region.
    """
    global _conn, _conn_region, _conn_flag
    interrupted = _interrupted_region()

    # If the interrupt flag changed since this connection was opened, drop it so
    # the next connect re-selects under the new state. This makes failover
    # (interrupt -> other Region) AND failback (restore -> preferred Region)
    # both take effect, rather than sticking to whichever Region we first hit.
    if _conn is not None and interrupted != _conn_flag:
        _discard_connection()

    if _conn is None or _conn.closed:
        mgr = _get_manager()
        all_endpoints = mgr.endpoints
        if interrupted and interrupted != "none":
            survivors = [e for e in all_endpoints if e.get("region") != interrupted]
            # Only hide the interrupted Region if at least one endpoint remains.
            use = survivors if survivors else all_endpoints
        else:
            use = all_endpoints
        mgr.endpoints = use
        try:
            _conn = mgr.get_connection(DATABASE, DB_USER)
        finally:
            mgr.endpoints = all_endpoints  # always restore the full list
        _conn_region = _current_region(_conn)
        _conn_flag = interrupted  # remember the flag this connection was opened under
    return _conn


def _discard_connection():
    global _conn, _conn_region
    if _conn is not None:
        try:
            _conn.close()
        except Exception:  # noqa: BLE001 - closing a broken conn can throw
            pass
        _conn = None
    _conn_region = None


def _current_region(conn) -> str:
    """Which Region the active connection landed in, for stamping rows."""
    try:
        host = conn.get_dsn_parameters().get("host")
    except Exception:  # noqa: BLE001
        host = None
    for endpoint in _get_manager().endpoints:
        if endpoint.get("hostname") == host:
            return endpoint.get("region", os.environ.get("AWS_REGION", "unknown"))
    return os.environ.get("AWS_REGION", "unknown")


def _is_occ_conflict(exc) -> bool:
    """True if the exception is a DSQL/Postgres OCC serialization failure (40001)."""
    return getattr(exc, "pgcode", None) == OCC_SQLSTATE


def _rollback_quietly(conn, write: bool):
    if write and conn is not None:
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass


def _run(fn, *, write: bool):
    """Run fn(conn, region) with three distinct failure handlings:

      1. OCC conflict (SQLSTATE 40001) -- DSQL rejected the commit because a
         concurrent transaction touched the same data. Retry the SAME
         transaction on the SAME connection, with exponential backoff + jitter
         (per AWS DSQL guidance). This is NOT a connection problem.
      2. Connection-level error (OperationalError/InterfaceError) -- the
         endpoint is gone. Discard the connection and reconnect, which
         re-selects a healthy endpoint (failover). One reroute attempt.
      3. Any other DatabaseError (bad SQL, constraint) -- roll back and surface.

    Returns (result, region).
    """
    occ_attempts = 0
    reroute_used = False

    while True:
        conn = _get_connection()
        region = _current_region(conn)
        try:
            result = fn(conn, region)
            if write:
                conn.commit()
            else:
                # psycopg2 opens a transaction implicitly. End read transactions
                # too, otherwise the cached connection keeps reading from an old
                # DSQL snapshot and misses rows committed by other connections.
                conn.rollback()
            return result, region

        except psycopg2.Error as e:
            # Branch on SQLSTATE first. DSQL surfaces an OCC conflict as an
            # OperationalError (SQLSTATE 40001), NOT a plain DatabaseError, so we
            # must check the code before deciding it is a connection problem --
            # otherwise OCC conflicts get wrongly treated as a Region failover.
            _rollback_quietly(conn, write)

            # 1. OCC conflict -> retry SAME transaction, SAME connection.
            if _is_occ_conflict(e):
                occ_attempts += 1
                if occ_attempts > _OCC_MAX_RETRIES:
                    logger.warning(
                        "OCC conflict (region=%s): exhausted %d retries; surfacing",
                        region, _OCC_MAX_RETRIES,
                    )
                    raise
                delay = _OCC_BASE_DELAY * (2 ** (occ_attempts - 1))
                delay += random.uniform(0, _OCC_BASE_DELAY)  # jitter
                logger.info(
                    "OCC conflict (region=%s), retry %d/%d after %.3fs",
                    region, occ_attempts, _OCC_MAX_RETRIES, delay,
                )
                time.sleep(delay)
                continue  # same connection, retry the transaction

            # 2. Genuine connection-level failure -> reroute (failover), once.
            if isinstance(e, _RETRYABLE):
                _discard_connection()
                if reroute_used:
                    logger.warning(
                        "DB connection error (region=%s): %s; reroute already tried, surfacing",
                        region, e,
                    )
                    raise
                reroute_used = True
                logger.warning(
                    "DB connection error (region=%s): %s; rerouting to another Region",
                    region, e,
                )
                continue  # reconnect re-selects a healthy endpoint

            # 3. Any other DB error (bad SQL, constraint) -- surface it.
            raise


SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS pets (
    pet_id       UUID PRIMARY KEY,
    pet_type     VARCHAR(64)  NOT NULL,
    pet_name     VARCHAR(128) NOT NULL,
    pet_age      INTEGER      NOT NULL DEFAULT 0,
    write_region VARCHAR(32)  NOT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL
);
"""


def init_schema():
    """Create the pets table if it does not already exist."""
    def _do(conn, _region):
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DDL)
        return None
    _run(_do, write=True)


def create_pet(pet_type: str, pet_name: str, pet_age: int) -> dict:
    """Insert a pet; returns the stored row including the Region it landed in."""
    pet_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    def _do(conn, _region):
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pets (pet_id, pet_type, pet_name, pet_age, write_region, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (pet_id, pet_type, pet_name, pet_age, _region, now),
            )
        return None

    _, region = _run(_do, write=True)
    return {
        "petId": pet_id,
        "petType": pet_type,
        "petName": pet_name,
        "petAge": pet_age,
        "writeRegion": region,
        "updatedAt": now.isoformat(),
    }


def update_pet(pet_id: str, pet_type=None, pet_name=None, pet_age=None):
    """Update mutable fields of a pet. Returns the updated row, or None if not found.

    Only the provided fields change; each update re-stamps write_region and
    updated_at so the row reflects the Region that served the latest write.
    """
    now = datetime.now(timezone.utc)

    def _do(conn, region):
        sets = ["write_region = %s", "updated_at = %s"]
        params = [region, now]
        if pet_type is not None:
            sets.append("pet_type = %s"); params.append(pet_type)
        if pet_name is not None:
            sets.append("pet_name = %s"); params.append(pet_name)
        if pet_age is not None:
            sets.append("pet_age = %s"); params.append(pet_age)
        params.append(pet_id)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pets SET " + ", ".join(sets) + " WHERE pet_id = %s "
                "RETURNING pet_id, pet_type, pet_name, pet_age, write_region, updated_at",
                params,
            )
            return cur.fetchone()

    row, _ = _run(_do, write=True)
    return _row_to_pet(row) if row else None


def delete_pet(pet_id: str) -> bool:
    """Delete a pet by id. Returns True if a row was deleted, False if not found."""
    def _do(conn, _region):
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pets WHERE pet_id = %s", (pet_id,))
            return cur.rowcount

    rowcount, _ = _run(_do, write=True)
    return rowcount > 0


def get_pet(pet_id: str):
    """Return a single pet by id, or None if not found."""
    def _do(conn, _region):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pet_id, pet_type, pet_name, pet_age, write_region, updated_at "
                "FROM pets WHERE pet_id = %s",
                (pet_id,),
            )
            return cur.fetchone()

    row, _ = _run(_do, write=False)
    return _row_to_pet(row) if row else None


def list_pets(limit: int = 50):
    """Return up to `limit` pets, newest first."""
    limit = max(1, min(limit, 50))

    def _do(conn, _region):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pet_id, pet_type, pet_name, pet_age, write_region, updated_at "
                "FROM pets ORDER BY updated_at DESC LIMIT %s",
                (limit,),
            )
            return cur.fetchall()

    rows, _ = _run(_do, write=False)
    return [_row_to_pet(r) for r in rows]


def _row_to_pet(row) -> dict:
    return {
        "petId": str(row[0]),
        "petType": row[1],
        "petName": row[2],
        "petAge": row[3],
        "writeRegion": row[4],
        "updatedAt": row[5].isoformat() if row[5] else None,
    }
