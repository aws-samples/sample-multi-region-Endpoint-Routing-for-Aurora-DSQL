#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
cleanup.py

Authoritative wipe of the pet store `pets` table. Connects straight to Aurora
DSQL via the repo's DSQLConnectionManager (NOT through the public API), so it
can clear everything -- load-test leftovers and any pre-existing demo rows --
without exposing a destructive "delete all" route on the API.

This is intentionally kept off the public API: a delete-everything capability
belongs with an operator running it locally, not reachable through CloudFront.

Usage (from the repo root, with dsql_config.json present):
  python loadtest/cleanup.py            # show the current row count only
  python loadtest/cleanup.py --delete   # delete all rows (prints count first)

Env: DSQL_CONFIG_FILE (default dsql_config.json), DSQL_DATABASE (postgres),
     DSQL_USER (admin).
"""

import argparse
import os
import sys

# Reuse the pet store data layer so routing/failover and auth are identical.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "petstore-api", "app"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import db  # noqa: E402  (path set up above)


def count_rows() -> int:
    def _do(conn, _region):
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pets")
            return cur.fetchone()[0]
    n, _ = db._run(_do, write=False)
    return n


def delete_all() -> int:
    def _do(conn, _region):
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pets")
            return cur.rowcount
    n, region = db._run(_do, write=True)
    return n


def main() -> None:
    parser = argparse.ArgumentParser(description="Wipe the pet store pets table (off-API).")
    parser.add_argument("--delete", action="store_true",
                        help="Actually delete all rows. Without this flag, only the count is shown.")
    args = parser.parse_args()

    before = count_rows()
    print(f"pets table currently has {before} row(s)")

    if not args.delete:
        print("Dry run. Re-run with --delete to remove all rows.")
        return

    if before == 0:
        print("Nothing to delete.")
        return

    deleted = delete_all()
    after = count_rows()
    print(f"Deleted {deleted} row(s). pets table now has {after} row(s).")
    if after != 0:
        print("WARNING: table is not empty after delete.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
