#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
interrupt.py

Failover demo control. Flips an SSM parameter that the pet store Lambda reads
to drop a Region from its DSQL endpoint selection -- simulating that Region
becoming unavailable, so traffic fails over to the surviving Region.

This runs with the operator's AWS credentials (not through the public API), so
the "take a Region out" capability stays off the customer-facing surface.

The Lambda caches the flag for a few seconds (INTERRUPT_TTL_SECONDS, default 5),
so a flip takes effect within that window.

Usage:
  failover/interrupt.py status
  failover/interrupt.py interrupt --region us-east-1
  failover/interrupt.py restore

Options:
  --region R     Region to interrupt (with the 'interrupt' command)
  --param NAME   SSM parameter name (default /petstore/interrupt-region)
  --ssm-region R AWS Region where the SSM parameter lives (default us-east-1)
"""

import argparse
import sys

import boto3

VALID_REGIONS = {"us-east-1", "us-east-2"}


def get_flag(ssm, name: str) -> str:
    try:
        return ssm.get_parameter(Name=name)["Parameter"]["Value"]
    except ssm.exceptions.ParameterNotFound:
        return "(not found)"


def set_flag(ssm, name: str, value: str) -> None:
    ssm.put_parameter(Name=name, Value=value, Type="String", Overwrite=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pet store failover interrupt control.")
    parser.add_argument("command", choices=["status", "interrupt", "restore"])
    parser.add_argument("--region", metavar="REGION",
                        help="Region to interrupt (for the 'interrupt' command), e.g. us-east-1")
    parser.add_argument("--param", default="/petstore/interrupt-region",
                        help="SSM parameter name (default /petstore/interrupt-region)")
    parser.add_argument("--ssm-region", default="us-east-1",
                        help="AWS Region where the SSM parameter lives (default us-east-1)")
    args = parser.parse_args()

    ssm = boto3.client("ssm", region_name=args.ssm_region)

    if args.command == "status":
        print(f"interrupt flag ({args.param}) = {get_flag(ssm, args.param)}")
        return

    if args.command == "restore":
        set_flag(ssm, args.param, "none")
        print(f"restored: {args.param} = none (all Regions serving)")
        return

    # interrupt
    region = args.region
    if not region:
        parser.error("interrupt requires --region <aws-region>, e.g. --region us-east-1")
    if region not in VALID_REGIONS:
        parser.error(f"--region must be one of {sorted(VALID_REGIONS)}")
    set_flag(ssm, args.param, region)
    print(f"interrupted: {args.param} = {region}")
    print("traffic will fail over to the other Region within the Lambda's cache TTL (~5s).")


if __name__ == "__main__":
    main()
