#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# interrupt.sh
#
# Wrapper around interrupt.py that runs it with a Python that has boto3.
# Prefers the repo's venv; falls back to python3 on PATH. Forwards all args.
#
#   failover/interrupt.sh status
#   failover/interrupt.sh interrupt --region us-east-1
#   failover/interrupt.sh restore
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"

# Pick a Python that has boto3: venv first, then python3, then python.
PYBIN=""
for cand in "$REPO_ROOT/venv/bin/python" python3 python; do
  if command -v "$cand" >/dev/null 2>&1 && "$cand" -c "import boto3" >/dev/null 2>&1; then
    PYBIN="$cand"
    break
  fi
done

if [[ -z "$PYBIN" ]]; then
  echo "ERROR: no Python with boto3 found." >&2
  echo "Install it (e.g. 'pip install boto3'), or create the repo venv:" >&2
  echo "  python3 -m venv venv && ./venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

exec "$PYBIN" "$HERE/interrupt.py" "$@"
