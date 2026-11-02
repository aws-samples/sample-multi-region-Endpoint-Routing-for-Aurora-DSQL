#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# build_and_upload.sh
#
# Packages the pet store Lambda as a zip + a psycopg2 layer (Option 1: no
# Docker, no ECR) and uploads both to S3, then prints the parameter values for
# the backend CloudFormation stack.
#
# The psycopg2 layer is built for the Lambda runtime (manylinux x86_64) using
# pip's --platform flag, so it works from macOS/arm64 without a container.
#
# Run from the repository root (the directory with dsql_connection_manager.py
# and dsql_config.json):
#
#   petstore-api/build_and_upload.sh --bucket <s3-bucket> [--region us-east-1]
#
set -euo pipefail

BUCKET=""
REGION="${AWS_REGION:-us-east-1}"
PREFIX="petstore-api"
PY_RUNTIME="python3.12"

usage() { grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket) BUCKET="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --prefix) PREFIX="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "Unknown arg: $1" >&2; usage 1 ;;
  esac
done

[[ -z "$BUCKET" ]] && { echo "ERROR: --bucket is required" >&2; usage 1; }
[[ -f dsql_connection_manager.py ]] || { echo "ERROR: run from the repo root (dsql_connection_manager.py not found)" >&2; exit 1; }
[[ -f dsql_config.json ]] || { echo "ERROR: dsql_config.json not found at repo root; generate it first" >&2; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# ---------------------------------------------------------------------------
# 1. Function zip: handler + db + parent connection manager + endpoint config
# ---------------------------------------------------------------------------
echo "==> Building function zip"
FN_DIR="$WORK/function"
mkdir -p "$FN_DIR"
cp petstore-api/app/handler.py "$FN_DIR/"
cp petstore-api/app/db.py "$FN_DIR/"
cp dsql_connection_manager.py "$FN_DIR/"
cp dsql_config.json "$FN_DIR/"
( cd "$FN_DIR" && zip -q -r "$WORK/petstore-function.zip" . )

# ---------------------------------------------------------------------------
# 2. psycopg2 layer, built for the Lambda runtime (manylinux x86_64).
#    Lambda layers expect libraries under python/.
# ---------------------------------------------------------------------------
echo "==> Building psycopg2 layer for the Lambda runtime"
LAYER_DIR="$WORK/layer/python"
mkdir -p "$LAYER_DIR"
pip install \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  --target "$LAYER_DIR" \
  "psycopg2-binary>=2.9.0" >/dev/null
( cd "$WORK/layer" && zip -q -r "$WORK/psycopg2-layer.zip" python )

# ---------------------------------------------------------------------------
# 3. Upload both to S3
# ---------------------------------------------------------------------------
FN_KEY="$PREFIX/petstore-function.zip"
LAYER_KEY="$PREFIX/psycopg2-layer.zip"
echo "==> Uploading to s3://$BUCKET/$PREFIX/"
aws s3 cp "$WORK/petstore-function.zip" "s3://$BUCKET/$FN_KEY" --region "$REGION"
aws s3 cp "$WORK/psycopg2-layer.zip" "s3://$BUCKET/$LAYER_KEY" --region "$REGION"

cat <<EOF

Done. Pass these to the backend stack:

  CodeS3Bucket=$BUCKET
  CodeS3Key=$FN_KEY
  Psycopg2LayerS3Key=$LAYER_KEY
EOF
