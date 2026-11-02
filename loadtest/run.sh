#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# run.sh
#
# Launches the pet store k6 load test with k6's built-in live web dashboard.
# Open http://127.0.0.1:5665 while it runs to watch RPS, latency, errors, and
# the per-Region write breakdown update in real time. A self-contained HTML
# report is also written at the end.
#
# Usage:
#   loadtest/run.sh [--base-url URL] [--vus N] [--rpm N] [--wait-ms MS]
#                   [--duration D] [--report FILE] [--no-cleanup]
#
# Load models (pick one):
#   --rpm N      fixed request RATE: N requests/min, held steady regardless of
#                latency. Each request = one full CRUD iteration (~6 HTTP calls).
#   --vus N      fixed CONCURRENCY: N virtual users looping (default, 10 VUs).
#                Used when --rpm is omitted.
#   --wait-ms MS think-time between iterations (default 200).
#   --duration D how long to run, e.g. 2m (default 1m).
#
# Examples:
#   loadtest/run.sh --vus 25 --duration 5m
#   loadtest/run.sh --rpm 600 --duration 3m          # 600 req/min (~10/sec)
#   loadtest/run.sh --rpm 1200 --wait-ms 0 --duration 2m
#
# If --base-url (or the BASE_URL env var) is not set, the CloudFront URL is read
# from the CFDistribution output of the petstore-edge stack in us-east-1
# (override with EDGE_STACK / EDGE_REGION).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
SCRIPT="$HERE/petstore_load.js"

BASE_URL="${BASE_URL:-}"
VUS="10"
DURATION="1m"
RPM="0"       # >0 => fixed request-rate model (requests/min); 0 => VUS model
WAIT_MS="200" # think-time between iterations, in ms
REPORT="$HERE/report.html"
CLEANUP=1   # wipe the table after the run by default; --no-cleanup to skip

usage() { grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url) BASE_URL="$2"; shift 2 ;;
    --vus)      VUS="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --rpm)      RPM="$2"; shift 2 ;;
    --wait-ms)  WAIT_MS="$2"; shift 2 ;;
    --report)   REPORT="$2"; shift 2 ;;
    --no-cleanup) CLEANUP=0; shift ;;
    -h|--help)  usage 0 ;;
    *) echo "Unknown arg: $1" >&2; usage 1 ;;
  esac
done

command -v k6 >/dev/null || {
  echo "ERROR: k6 is not installed. Install it with:" >&2
  echo "  macOS:  brew install k6" >&2
  echo "  other:  https://grafana.com/docs/k6/latest/set-up/install-k6/" >&2
  exit 1
}

# Default the base URL from the CloudFront stack output if not given.
EDGE_STACK="${EDGE_STACK:-petstore-edge}"
EDGE_REGION="${EDGE_REGION:-us-east-1}"
if [[ -z "$BASE_URL" ]] && command -v aws >/dev/null; then
  BASE_URL="$(aws cloudformation describe-stacks --region "$EDGE_REGION" \
    --stack-name "$EDGE_STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='CFDistribution'].OutputValue" \
    --output text 2>/dev/null || true)"
  [[ "$BASE_URL" == "None" ]] && BASE_URL=""
fi

if [[ -z "$BASE_URL" ]]; then
  echo "ERROR: no base URL. Pass --base-url https://xxxx.cloudfront.net, or deploy" >&2
  echo "       the '$EDGE_STACK' stack in $EDGE_REGION (override with EDGE_STACK / EDGE_REGION)." >&2
  exit 1
fi

echo "==> Target:    $BASE_URL"
if [[ "$RPM" -gt 0 ]]; then
  echo "==> Load:      rate model -- $RPM requests/min for $DURATION"
else
  echo "==> Load:      concurrency model -- $VUS VUs for $DURATION"
fi
echo "==> Wait:      ${WAIT_MS}ms think-time between iterations"
echo "==> Dashboard: http://127.0.0.1:5665  (opens live while running)"
echo "==> Report:    $REPORT (written at the end)"
echo

# K6_WEB_DASHBOARD serves the live dashboard; the _EXPORT writes a static HTML
# snapshot at the end so the results can be shared after the run.
# Don't let a failed threshold abort before cleanup runs.
set +e
K6_WEB_DASHBOARD=true \
K6_WEB_DASHBOARD_EXPORT="$REPORT" \
BASE_URL="$BASE_URL" \
VUS="$VUS" \
DURATION="$DURATION" \
RPM="$RPM" \
WAIT_MS="$WAIT_MS" \
  k6 run "$SCRIPT"
K6_EXIT=$?
set -e

# Guarantee the table is empty after the run (the per-iteration self-delete can
# leave a straggler if the duration cuts an iteration mid-cycle).
if [[ "$CLEANUP" -eq 1 ]]; then
  echo
  echo "==> Cleaning up: removing any remaining pets"
  CLEANUP_PY="$HERE/cleanup.py"
  PYBIN="$REPO_ROOT/venv/bin/python"
  [[ -x "$PYBIN" ]] || PYBIN="python3"
  DSQL_CONFIG_FILE="$REPO_ROOT/dsql_config.json" AWS_REGION="${AWS_REGION:-us-east-1}" \
    "$PYBIN" "$CLEANUP_PY" --delete 2>&1 | grep -v " - INFO - " || true
else
  echo
  echo "==> Skipping cleanup (--no-cleanup). Use loadtest/cleanup.py --delete to wipe later."
fi

exit "$K6_EXIT"
