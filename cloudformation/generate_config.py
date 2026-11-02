#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
generate_config.py

Writes a dsql_config.json file in the exact shape consumed by the client-side
routing code in this repo (dsql_connection_manager.py and
hybrid_failover_approach.py).

It can be driven two ways:

1. Directly from --endpoint arguments (how deploy_multi_region.sh calls it):

     python3 generate_config.py \
       --endpoint "cluster_id=abc,region=us-east-1,hostname=abc.dsql.us-east-1.on.aws,priority=1" \
       --endpoint "cluster_id=def,region=us-east-2,hostname=def.dsql.us-east-2.on.aws,priority=2" \
       --out ../dsql_config.json

2. From deployed CloudFormation stacks, by reading the stack outputs itself
   (requires boto3 and AWS credentials):

     python3 generate_config.py \
       --from-stacks \
       --stack-name MRCluster \
       --region us-east-1 --region us-east-2 \
       --out ../dsql_config.json

The output matches dsql_config.json: a top-level "endpoints" list plus a
"connection_settings" block. Each endpoint carries cluster_id, region,
hostname, port and priority. If a hostname is not supplied it is derived as
"{cluster_id}.dsql.{region}.on.aws", which is the DSQL endpoint convention.
"""

import argparse
import json
import sys

DEFAULT_CONNECTION_SETTINGS = {
    "connect_timeout": 5,
    "application_name": "dsql-latency-router",
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3,
}


def derive_hostname(cluster_id: str, region: str) -> str:
    return f"{cluster_id}.dsql.{region}.on.aws"


def parse_endpoint_arg(raw: str, index: int) -> dict:
    """Parse a 'key=value,key=value' endpoint string into an endpoint dict."""
    fields = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair:
            continue
        if "=" not in pair:
            raise ValueError(f"Malformed endpoint field '{pair}' (expected key=value)")
        key, value = pair.split("=", 1)
        fields[key.strip()] = value.strip()

    if "cluster_id" not in fields or "region" not in fields:
        raise ValueError(
            f"Endpoint '{raw}' must include at least cluster_id and region"
        )

    cluster_id = fields["cluster_id"]
    region = fields["region"]
    hostname = fields.get("hostname") or derive_hostname(cluster_id, region)
    port = int(fields.get("port", 5432))
    priority = int(fields.get("priority", index + 1))

    endpoint = {
        "cluster_id": cluster_id,
        "region": region,
        "hostname": hostname,
        "port": port,
        "priority": priority,
    }
    # Preserve an optional Route 53 health check id if one was passed.
    if fields.get("health_check_id"):
        endpoint["health_check_id"] = fields["health_check_id"]
    return endpoint


def endpoints_from_stacks(stack_name: str, regions: list) -> list:
    """Read ClusterIdentifier/ClusterEndpoint outputs from deployed stacks."""
    try:
        import boto3
    except ImportError:
        sys.exit("boto3 is required for --from-stacks. Run: pip install -r ../requirements.txt")

    endpoints = []
    for index, region in enumerate(regions):
        cfn = boto3.client("cloudformation", region_name=region)
        stacks = cfn.describe_stacks(StackName=stack_name)["Stacks"]
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stacks[0].get("Outputs", [])}

        cluster_id = outputs.get("ClusterIdentifier")
        hostname = outputs.get("ClusterEndpoint") or (
            derive_hostname(cluster_id, region) if cluster_id else None
        )
        if not cluster_id or not hostname:
            sys.exit(f"Stack '{stack_name}' in {region} is missing required outputs.")

        endpoints.append(
            {
                "cluster_id": cluster_id,
                "region": region,
                "hostname": hostname,
                "port": 5432,
                "priority": index + 1,
            }
        )
    return endpoints


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate dsql_config.json for the DSQL routing code.")
    parser.add_argument(
        "--endpoint",
        action="append",
        default=[],
        metavar="KEY=VALUE,...",
        help="An endpoint spec, e.g. cluster_id=abc,region=us-east-1,hostname=...,priority=1. Repeatable.",
    )
    parser.add_argument("--from-stacks", action="store_true", help="Read endpoints from deployed CloudFormation stacks.")
    parser.add_argument("--stack-name", default="MRCluster", help="Stack name to read (with --from-stacks).")
    parser.add_argument("--region", action="append", default=[], help="A Region to read (with --from-stacks). Repeatable.")
    parser.add_argument("--out", default="dsql_config.json", help="Output path for the generated config.")
    parser.add_argument("--app-name", default=None, help="Override connection_settings.application_name.")
    args = parser.parse_args()

    if args.from_stacks:
        if not args.region:
            parser.error("--from-stacks requires at least one --region")
        endpoints = endpoints_from_stacks(args.stack_name, args.region)
    else:
        if not args.endpoint:
            parser.error("Provide --endpoint arguments or use --from-stacks")
        endpoints = [parse_endpoint_arg(raw, i) for i, raw in enumerate(args.endpoint)]

    connection_settings = dict(DEFAULT_CONNECTION_SETTINGS)
    if args.app_name:
        connection_settings["application_name"] = args.app_name

    config = {"endpoints": endpoints, "connection_settings": connection_settings}

    with open(args.out, "w") as f:
        json.dump(config, f, indent=2)
        f.write("\n")

    print(f"Wrote {args.out} with {len(endpoints)} endpoint(s):")
    for ep in endpoints:
        print(f"  - {ep['hostname']} ({ep['region']}, priority {ep['priority']})")


if __name__ == "__main__":
    main()
