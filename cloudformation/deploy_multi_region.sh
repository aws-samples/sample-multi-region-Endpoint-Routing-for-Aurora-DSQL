#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# deploy_multi_region.sh
#
# Provisions a peered Amazon Aurora DSQL multi-Region cluster pair using the
# mr-cluster.yaml CloudFormation template, then writes a dsql_config.json that
# the client-side routing code (dsql_connection_manager.py /
# hybrid_failover_approach.py) can consume directly.
#
# Workflow (per the AWS docs, this is a two-phase process):
#   Phase 1  Create a cluster in each of the two linked Regions, independently.
#   Phase 2  Re-apply the template to both Regions with BOTH cluster ARNs set,
#            which establishes the peer relationship.
#   Phase 3  Read stack outputs and generate dsql_config.json.
#
# This script is non-destructive: it only creates/updates stacks. It never
# deletes clusters or disables deletion protection.
#
# Usage:
#   ./deploy_multi_region.sh \
#       --region-1 us-east-1 \
#       --region-2 us-east-2 \
#       --witness-region us-west-2 \
#       [--stack-name MRCluster] \
#       [--config-out ../dsql_config.json]
#
set -euo pipefail

STACK_NAME="MRCluster"
CONFIG_OUT="../dsql_config.json"
REGION_1=""
REGION_2=""
WITNESS_REGION=""
TEMPLATE_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/mr-cluster.yaml"

usage() {
  grep '^#' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region-1)        REGION_1="$2"; shift 2 ;;
    --region-2)        REGION_2="$2"; shift 2 ;;
    --witness-region)  WITNESS_REGION="$2"; shift 2 ;;
    --stack-name)      STACK_NAME="$2"; shift 2 ;;
    --config-out)      CONFIG_OUT="$2"; shift 2 ;;
    -h|--help)         usage 0 ;;
    *) echo "Unknown argument: $1" >&2; usage 1 ;;
  esac
done

if [[ -z "$REGION_1" || -z "$REGION_2" || -z "$WITNESS_REGION" ]]; then
  echo "ERROR: --region-1, --region-2 and --witness-region are all required." >&2
  usage 1
fi

if [[ "$REGION_1" == "$REGION_2" ]]; then
  echo "ERROR: --region-1 and --region-2 must be different Regions." >&2
  exit 1
fi

if [[ "$WITNESS_REGION" == "$REGION_1" || "$WITNESS_REGION" == "$REGION_2" ]]; then
  echo "ERROR: --witness-region must differ from both cluster Regions." >&2
  exit 1
fi

echo "==> Caller identity:"
aws sts get-caller-identity --output table

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

deploy_stack() {
  # $1 region, $2 extra "ParameterKey=...,ParameterValue=..." args (optional)
  local region="$1"; shift
  local extra_params=("$@")

  echo "==> Deploying stack '$STACK_NAME' in $region"
  aws cloudformation deploy \
    --region "$region" \
    --stack-name "$STACK_NAME" \
    --template-file "$TEMPLATE_FILE" \
    --no-fail-on-empty-changeset \
    --parameter-overrides \
      "WitnessRegion=$WITNESS_REGION" \
      ${extra_params[@]+"${extra_params[@]}"}
}

get_output() {
  # $1 region, $2 output key -> prints the output value
  local region="$1" key="$2"
  aws cloudformation describe-stacks \
    --region "$region" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$key'].OutputValue" \
    --output text
}

wait_active() {
  # $1 region, $2 cluster identifier - poll until cluster status is ACTIVE
  local region="$1" cluster_id="$2"
  echo "==> Waiting for cluster $cluster_id in $region to become ACTIVE"
  for _ in $(seq 1 60); do
    local status
    status="$(aws dsql get-cluster --region "$region" --identifier "$cluster_id" \
      --query 'status' --output text 2>/dev/null || echo UNKNOWN)"
    echo "    status=$status"
    if [[ "$status" == "ACTIVE" ]]; then
      return 0
    fi
    sleep 10
  done
  echo "ERROR: cluster $cluster_id in $region did not reach ACTIVE in time." >&2
  return 1
}

# ---------------------------------------------------------------------------
# Phase 1 - create a cluster in each Region (no peer ARNs yet)
# ---------------------------------------------------------------------------
echo
echo "############ PHASE 1: create clusters ############"
deploy_stack "$REGION_1"
deploy_stack "$REGION_2"

ARN_1="$(get_output "$REGION_1" ClusterArn)"
ARN_2="$(get_output "$REGION_2" ClusterArn)"
ID_1="$(get_output "$REGION_1" ClusterIdentifier)"
ID_2="$(get_output "$REGION_2" ClusterIdentifier)"
HOST_1="$(get_output "$REGION_1" ClusterEndpoint)"
HOST_2="$(get_output "$REGION_2" ClusterEndpoint)"

echo "    $REGION_1 -> id=$ID_1 arn=$ARN_1"
echo "    $REGION_2 -> id=$ID_2 arn=$ARN_2"

# ---------------------------------------------------------------------------
# Phase 2 - re-apply with both ARNs to establish peering
# ---------------------------------------------------------------------------
echo
echo "############ PHASE 2: establish peering ############"
PEER_LIST="$ARN_1,$ARN_2"
deploy_stack "$REGION_1" "PeerClusterArns=$PEER_LIST"
deploy_stack "$REGION_2" "PeerClusterArns=$PEER_LIST"

wait_active "$REGION_1" "$ID_1"
wait_active "$REGION_2" "$ID_2"

# ---------------------------------------------------------------------------
# Phase 3 - generate dsql_config.json for the routing code
# ---------------------------------------------------------------------------
echo
echo "############ PHASE 3: generate dsql_config.json ############"
python3 "$(dirname "$TEMPLATE_FILE")/generate_config.py" \
  --endpoint "cluster_id=$ID_1,region=$REGION_1,hostname=$HOST_1,priority=1" \
  --endpoint "cluster_id=$ID_2,region=$REGION_2,hostname=$HOST_2,priority=2" \
  --out "$CONFIG_OUT"

echo
echo "Done. Multi-Region DSQL cluster is peered and $CONFIG_OUT is ready."
echo "Point the routing code at it, e.g.:"
echo "  DSQLConnectionManager(config_file=\"$CONFIG_OUT\")"
