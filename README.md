# Multi-Region Endpoint Routing for Amazon Aurora DSQL

This repository has two parts:

1. **A connection manager library** (Python) that routes Amazon Aurora DSQL connections from the client side. It selects the lowest-latency healthy endpoint and fails over to another endpoint when one is unavailable.
2. **A multi-Region demo** built on that library. It provisions a multi-Region Aurora DSQL cluster, runs a sample API against it, drives load, and interrupts a Region to show traffic moving to the surviving Region.

## Contents

- [Repository layout](#repository-layout)
- [Connection manager library](#connection-manager-library)
- [Multi-Region demo](#multi-region-demo)
  - [Architecture](#architecture)
  - [Prerequisites](#prerequisites)
  - [Step 1: Provision the cluster](#step-1-provision-the-cluster)
  - [Step 2: Deploy the API and CloudFront](#step-2-deploy-the-api-and-cloudfront)
  - [Step 3: Smoke test the API](#step-3-smoke-test-the-api)
  - [Step 4: Run the load test](#step-4-run-the-load-test)
  - [Step 5: Interrupt a Region](#step-5-interrupt-a-region)
  - [Step 6: Clean up test data](#step-6-clean-up-test-data)
  - [Teardown](#teardown)
  - [How the demo works](#how-the-demo-works)
  - [Reference](#reference)
  - [Troubleshooting](#troubleshooting)
- [Important notes](#important-notes)
- [License](#license)

## Repository layout

| Path | Purpose |
|---|---|
| [`dsql_connection_manager.py`](dsql_connection_manager.py) | Connection manager: TCP health checks and lowest-latency routing |
| [`hybrid_failover_approach.py`](hybrid_failover_approach.py) | Connection manager variant that uses Route 53 health checks |
| [`test_failover.py`](test_failover.py), [`test_route53_failover.py`](test_route53_failover.py) | Failover tests for the library |
| [`dsql_config.json`](dsql_config.json) | Endpoint configuration read by the library and the demo |
| [`cloudformation/`](cloudformation/) | Demo step 1: [`mr-cluster.yaml`](cloudformation/mr-cluster.yaml), [`deploy_multi_region.sh`](cloudformation/deploy_multi_region.sh), [`generate_config.py`](cloudformation/generate_config.py) |
| [`petstore-api/`](petstore-api/) | Demo step 2: [`template.yaml`](petstore-api/template.yaml) (API stack), [`edge/apiprotection.yml`](petstore-api/edge/apiprotection.yml) (CloudFront stack), [`build_and_upload.sh`](petstore-api/build_and_upload.sh), and the Lambda code in [`app/`](petstore-api/app/) |
| [`loadtest/`](loadtest/) | Demo step 4: [`run.sh`](loadtest/run.sh), [`petstore_load.js`](loadtest/petstore_load.js) (k6 scenario), [`cleanup.py`](loadtest/cleanup.py), and the customer-facing [`LOAD_TEST_GUIDE.md`](loadtest/LOAD_TEST_GUIDE.md) |
| [`failover/`](failover/) | Demo step 5: [`interrupt.sh`](failover/interrupt.sh) and [`interrupt.py`](failover/interrupt.py) |

---

## Connection manager library

### Features

- **Client-based routing**: routes database connections to the best endpoint from the client side
- **Automatic endpoint selection**: connects to the DSQL endpoint with the lowest latency
- **Failover support**: tries alternative endpoints if the selected endpoint is unavailable
- **Health checking**: checks endpoint health with direct TCP checks or Route 53 health checks
- **IAM authentication**: uses AWS IAM authentication with DSQL admin auth tokens

### Requirements

- Python 3.8+
- AWS credentials with DSQL access permissions ([IAM for Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/security-iam.html))
- Network access to the DSQL endpoints
- boto3 1.37.24 or later (includes DSQL support)
- For Route 53 health checks: permissions to create and manage Route 53 health checks

### Installation

```bash
git clone https://github.com/aws-samples/sample-multi-region-Endpoint-Routing-for-Aurora-DSQL.git
cd sample-multi-region-Endpoint-Routing-for-Aurora-DSQL
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

The library uses the AWS SDK default credential provider chain. See [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

### Basic usage

```python
from dsql_connection_manager import DSQLConnectionManager

manager = DSQLConnectionManager()           # reads dsql_config.json
conn = manager.get_connection("postgres", "admin")

with conn.cursor() as cursor:
    cursor.execute("SELECT version();")
    print(f"Database version: {cursor.fetchone()[0]}")

conn.close()
```

To pass endpoints directly instead of reading `dsql_config.json`:

```python
endpoints = [
    {"cluster_id": "<cluster-id-1>", "region": "us-east-1",
     "hostname": "<cluster-id-1>.dsql.us-east-1.on.aws", "port": 5432},
    {"cluster_id": "<cluster-id-2>", "region": "us-east-2",
     "hostname": "<cluster-id-2>.dsql.us-east-2.on.aws", "port": 5432},
]
manager = DSQLConnectionManager(endpoints=endpoints)
```

Or load a specific configuration file:

```python
manager = DSQLConnectionManager(config_file="dsql_config_with_healthchecks.json")
```

The configuration file has an `endpoints` list (`cluster_id`, `region`, `hostname`, `port`, and optionally `priority` and `health_check_id`) and a `connection_settings` block. See [`dsql_config_with_healthchecks.json`](dsql_config_with_healthchecks.json) for an example. Step 1 of the demo generates this file for you.

### Route 53 health checks

The hybrid manager uses Route 53 health checks for endpoint health instead of direct TCP checks.

1. Create a configuration file with your endpoints (like `dsql_config_with_healthchecks.json`).
2. Create the health checks. This writes a `health_check_id` for each endpoint into the file:
   ```bash
   python hybrid_failover_approach.py --setup --config dsql_config_with_healthchecks.json
   ```
3. Use the hybrid manager:
   ```python
   from hybrid_failover_approach import DSQLHybridConnectionManager

   manager = DSQLHybridConnectionManager(
       config_file="dsql_config_with_healthchecks.json",
       health_check_ttl=60,        # cache health results for 60 seconds
       latency_test_timeout=2.0,   # latency test timeout in seconds
       latency_test_retries=3,     # latency test retries
       connection_timeout=5,       # database connection timeout in seconds
   )
   conn = manager.get_connection("postgres", "admin")
   ```

### Testing the library

```bash
# Basic connectivity
python hybrid_failover_approach.py --test --config dsql_config_with_healthchecks.json --database postgres --user admin

# Failover with Route 53 health checks
python test_route53_failover.py --config dsql_config_with_healthchecks.json --database postgres --user admin
```

The Route 53 failover test connects to the best endpoint, disables that endpoint's health check, waits for the status to propagate, confirms the connection moves to another endpoint, then re-enables the health check and confirms the restored endpoint is reachable.

### Logging

The library logs endpoint health, latency, and connection attempts:

```
Endpoint latency comparison:
  1. <cluster-id-1>.dsql.us-east-1.on.aws - Latency: 0.055231s, Priority: 1, Region: us-east-1
  2. <cluster-id-2>.dsql.us-east-2.on.aws - Latency: 0.058111s, Priority: 2, Region: us-east-2
Selected best endpoint: <cluster-id-1>.dsql.us-east-1.on.aws (latency: 0.055231s)
```

Set the level with `logging.basicConfig(level=logging.INFO)` (or `logging.DEBUG`).

---

## Multi-Region demo

Work through steps 1 to 6 in order; each step uses what the previous one produced. Run all commands from the repository root unless a step says otherwise. The demo creates billable AWS resources until you complete [Teardown](#teardown).

### Architecture

```
Client -> Amazon CloudFront (adds a secret x-origin-verify header)
       -> Amazon API Gateway (REST; a Lambda authorizer verifies the header)
       -> AWS Lambda (pet store CRUD, Python)
       -> connection manager (lowest-latency routing, retry, reroute)
       -> Aurora DSQL multi-Region cluster (us-east-1 and us-east-2, witness us-west-2)
```

An Aurora DSQL multi-Region cluster is a single logical database with a writable endpoint in each of two linked Regions, plus a witness Region. Writes are committed synchronously across the Regions, so both endpoints are strongly consistent. See [Resilience in Amazon Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/disaster-recovery-resiliency.html).

### Prerequisites

- The library [installation](#installation) above, with the venv active.
- AWS CLI with credentials that can create Aurora DSQL multi-Region clusters, CloudFormation stacks, Lambda functions, API Gateway, CloudFront, S3 buckets, IAM roles, and SSM parameters. Multi-Region DSQL needs `dsql:CreateCluster`, `dsql:TagResource`, `dsql:PutMultiRegionProperties`, `dsql:AddPeerCluster`, and `dsql:PutWitnessRegion` (checked in both the cluster Region and the witness Region).
- `pip` and `zip` for packaging the Lambda (no Docker needed).
- [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) for the load test (`brew install k6` on macOS).

```bash
aws sts get-caller-identity   # confirm you are in the intended account
```

### Step 1: Provision the cluster

Files: [`cloudformation/`](cloudformation/)

```bash
cd cloudformation
./deploy_multi_region.sh --region-1 us-east-1 --region-2 us-east-2 --witness-region us-west-2
cd ..
```

The script deploys [`mr-cluster.yaml`](cloudformation/mr-cluster.yaml) in two phases, as Aurora DSQL requires ([Configuring multi-Region clusters with CloudFormation](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/mr-cluster-setup.html)):

1. Create a cluster in each Region, both pointing at the witness Region.
2. Redeploy both stacks with both cluster ARNs to peer them.

It then waits for both clusters to reach `ACTIVE` and writes `dsql_config.json` at the repository root. This takes several minutes. Check the result:

```bash
cat dsql_config.json   # should show real cluster IDs and hostnames
```

### Step 2: Deploy the API and CloudFront

Files: [`petstore-api/`](petstore-api/)

**2a. Load the schema.** This creates the `pets` table from [`app/schema.sql`](petstore-api/app/schema.sql):

```bash
python -c "import sys; sys.path.insert(0, 'petstore-api/app'); import db; db.init_schema()"
```

**2b. Create a deploy bucket and upload the Lambda package.** [`build_and_upload.sh`](petstore-api/build_and_upload.sh) zips the function (handler, data layer, connection manager, and `dsql_config.json`) and builds a psycopg2 layer for the Lambda runtime:

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGION=us-east-1
BUCKET=petstore-api-deploy-$ACCOUNT-$REGION

aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

petstore-api/build_and_upload.sh --bucket "$BUCKET" --region "$REGION"
```

**2c. Create the origin secret and deploy the API stack.** CloudFront and the API stack must use the same secret. Keep it in a local file, which `.gitignore` excludes:

```bash
openssl rand -hex 32 > petstore-api/.origin-secret && chmod 600 petstore-api/.origin-secret
ORIGIN_SECRET="$(cat petstore-api/.origin-secret)"

aws cloudformation deploy --region "$REGION" --stack-name petstore-api \
  --template-file petstore-api/template.yaml --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    CodeS3Bucket="$BUCKET" \
    CodeS3Key=petstore-api/petstore-function.zip \
    Psycopg2LayerS3Key=petstore-api/psycopg2-layer.zip \
    OriginVerifySecret="$ORIGIN_SECRET" \
    DsqlClusterArnPattern="arn:aws:dsql:*:${ACCOUNT}:cluster/*"

APIGW_ENDPOINT=$(aws cloudformation describe-stacks --region "$REGION" --stack-name petstore-api \
  --query "Stacks[0].Outputs[?OutputKey=='ApiGatewayEndpoint'].OutputValue" --output text)
```

**2d. Deploy the CloudFront stack** with the same secret:

```bash
aws cloudformation deploy --region us-east-1 --stack-name petstore-edge \
  --template-file petstore-api/edge/apiprotection.yml \
  --parameter-overrides APIGWEndpoint="$APIGW_ENDPOINT" OriginVerifySecret="$ORIGIN_SECRET"

CF_URL=$(aws cloudformation describe-stacks --region us-east-1 --stack-name petstore-edge \
  --query "Stacks[0].Outputs[?OutputKey=='CFDistribution'].OutputValue" --output text)
echo "$CF_URL"
```

Creating the distribution takes several minutes and can outlast the CLI wait. If the command times out, check the stack status with `aws cloudformation describe-stacks` before rerunning.

**Redeploying code changes.** If you change `handler.py` or `db.py`, rerun `build_and_upload.sh`. CloudFormation does not detect new code at an unchanged S3 key, so update the function directly:

```bash
FN=$(aws cloudformation describe-stack-resources --region us-east-1 --stack-name petstore-api \
  --query "StackResources[?LogicalResourceId=='PetStoreFunction'].PhysicalResourceId" --output text)
aws lambda update-function-code --region us-east-1 --function-name "$FN" \
  --s3-bucket "$BUCKET" --s3-key petstore-api/petstore-function.zip
aws lambda wait function-updated --region us-east-1 --function-name "$FN"
```

### Step 3: Smoke test the API

```bash
# Through CloudFront: expect 201 and a writeRegion field
curl -s -X POST "$CF_URL/pets" -H 'Content-Type: application/json' \
  -d '{"petType":"dog","petName":"Rex","petAge":3}'
curl -s "$CF_URL/pets"

# Directly to API Gateway, without the secret header: expect 401
curl -s -o /dev/null -w '%{http_code}\n' "https://$APIGW_ENDPOINT/prod/pets"
```

### Step 4: Run the load test

Files: [`loadtest/`](loadtest/). Full guide: [`LOAD_TEST_GUIDE.md`](loadtest/LOAD_TEST_GUIDE.md)

```bash
loadtest/run.sh --rpm 600 --duration 5m      # fixed request rate
loadtest/run.sh --vus 25 --wait-ms 500 --duration 5m   # fixed concurrency with think-time
```

- [`run.sh`](loadtest/run.sh) reads the CloudFront URL from the `petstore-edge` stack, so `--base-url` is optional.
- Each iteration of [`petstore_load.js`](loadtest/petstore_load.js) runs a full create, read, update, read, delete cycle (about six HTTP calls), so `--rpm 600` is about 3,600 HTTP calls per minute.
- Open `http://127.0.0.1:5665` for the live k6 dashboard (latency, throughput, errors).
- The terminal summary shows writes per Region and the CRUD operation mix:
  ```
  === Writes by DSQL Region ===
    us-east-1     1840
    us-east-2     0
  ```
- Under normal load all writes land in us-east-1. The Lambda runs in us-east-1, so that endpoint always has the lowest latency. Step 5 moves traffic to us-east-2.
- The k6 process stays running while a browser tab is open on the dashboard. Close the tab or press Ctrl-C to finish; the summary and data cleanup then run.

See [`LOAD_TEST_GUIDE.md`](loadtest/LOAD_TEST_GUIDE.md) for every flag, how to read the dashboard, and what a healthy run looks like.

### Step 5: Interrupt a Region

Files: [`failover/`](failover/)

While a load test from step 4 is running, in a second terminal:

```bash
failover/interrupt.sh status
failover/interrupt.sh interrupt --region us-east-1   # writes move to us-east-2 within about 5 to 10 seconds
failover/interrupt.sh restore                        # writes return to us-east-1
```

The final load-test summary shows writes split between us-east-1 and us-east-2, with writes continuing to succeed through the switch. [`interrupt.sh`](failover/interrupt.sh) runs [`interrupt.py`](failover/interrupt.py) with a Python that has boto3 (the repository venv if present). It needs `ssm:GetParameter` and `ssm:PutParameter` on `/petstore/interrupt-region`. See [How the demo works](#how-the-demo-works) for the mechanism.

### Step 6: Clean up test data

`run.sh` removes all test data at the end of each run (pass `--no-cleanup` to keep it). To check or clear the table yourself:

```bash
python loadtest/cleanup.py            # show the row count
python loadtest/cleanup.py --delete   # delete all rows
```

[`cleanup.py`](loadtest/cleanup.py) connects to DSQL directly, so the API has no delete-everything endpoint.

### Teardown

Tear down in reverse order. Every step here permanently deletes resources, so confirm you are in the intended account first.

**T1. Reset the interrupt flag** (optional; the SSM parameter is deleted with the API stack):

```bash
failover/interrupt.sh restore
```

**T2. Delete the CloudFront stack.** Disabling and deleting the distribution can take a while:

```bash
aws cloudformation delete-stack --region us-east-1 --stack-name petstore-edge
aws cloudformation wait stack-delete-complete --region us-east-1 --stack-name petstore-edge
```

**T3. Delete the API stack.** This removes the Lambda functions, API Gateway, IAM roles, log groups, and the SSM parameter:

```bash
aws cloudformation delete-stack --region us-east-1 --stack-name petstore-api
aws cloudformation wait stack-delete-complete --region us-east-1 --stack-name petstore-api
```

**T4. Delete the clusters.** Deletion protection is on by default, so turn it off in both Regions, then delete both stacks. For an existing stack, `aws cloudformation deploy` keeps any parameters you do not override, so the peering stays in place and only deletion protection changes:

```bash
cd cloudformation
for R in us-east-1 us-east-2; do
  aws cloudformation deploy --region $R --stack-name MRCluster --template-file mr-cluster.yaml \
    --parameter-overrides WitnessRegion=us-west-2 DeletionProtection=false
done
aws cloudformation delete-stack --region us-east-1 --stack-name MRCluster
aws cloudformation delete-stack --region us-east-2 --stack-name MRCluster
cd ..
```

Confirm with `aws dsql list-clusters --region us-east-1` and `--region us-east-2`.

**T5. Empty and delete the retained S3 buckets.** The CloudFront access-log bucket and the deploy bucket from step 2b are kept when their stacks are deleted. Find both:

```bash
aws s3api list-buckets --output text \
  --query "Buckets[?starts_with(Name,'petstore-edge-s3bucketaccesslogs-') || starts_with(Name,'petstore-api-deploy-')].Name"
```

Then empty and delete each one:

```bash
aws s3 rb s3://<bucket-name> --force
```

`--force` deletes the current objects and then the bucket. If you enabled versioning on a bucket, `rb --force` cannot remove old object versions; delete them first with `aws s3api delete-objects` using the output of `aws s3api list-object-versions`.

**T6. Local cleanup.** Delete `petstore-api/.origin-secret`, and restore the placeholder `dsql_config.json` (`git checkout dsql_config.json`) so live endpoints are not committed.

### How the demo works

**Cluster provisioning.** Aurora DSQL peers multi-Region clusters in two phases: each cluster is created with `MultiRegionProperties.WitnessRegion`, then both are updated with both cluster ARNs in `MultiRegionProperties.Clusters`. [`deploy_multi_region.sh`](cloudformation/deploy_multi_region.sh) runs both phases and [`generate_config.py`](cloudformation/generate_config.py) writes `dsql_config.json` from the stack outputs.

**CloudFront-only access.** CloudFront adds a secret `x-origin-verify` header to every request it sends to API Gateway. A REQUEST Lambda authorizer compares it in constant time and denies the request if it does not match or if the secret is not configured. The pet store handler checks the same header again. The secret is a `NoEcho` CloudFormation parameter and is never stored in the templates. The API has no WAF and no end-user authentication; it is a public sample API, and the header only controls that requests come through CloudFront.

**Data layer.** [`petstore-api/app/db.py`](petstore-api/app/db.py) uses the connection manager from Lambda. It reuses one connection per Lambda container across invocations and ends every transaction, reads included (commit after writes, rollback after reads). Aurora DSQL uses snapshot isolation, so a read transaction left open on a reused connection keeps an old snapshot and does not see rows other connections commit afterwards. It handles three kinds of failure:

- Optimistic-concurrency conflicts (SQLSTATE 40001, OC000): retried on the same connection with exponential backoff and jitter, as described in [Concurrency control in Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html).
- Connection errors: the connection is dropped and reopened, which selects a healthy endpoint again.
- Other database errors: returned to the caller.

Each row records `write_region` and `updated_at`, which is how the load test reports writes per Region.

**Region interruption.** [`interrupt.py`](failover/interrupt.py) sets the SSM parameter `/petstore/interrupt-region` to `none`, `us-east-1`, or `us-east-2`. The Lambda reads it (cached for about 5 seconds), leaves the named Region out of endpoint selection, and reopens its connection whenever the value changes, so traffic moves on interrupt and returns on restore. This is an application-level interruption: the cluster itself stays up, which makes the demo quick and fully reversible. The read fails open, so if SSM is unreachable, routing is unchanged.

### Reference

**Cluster template parameters** ([`mr-cluster.yaml`](cloudformation/mr-cluster.yaml)):

| Parameter | Default | Notes |
|---|---|---|
| `WitnessRegion` | none (required) | Must differ from the deploy Region; same value in both stacks |
| `PeerClusterArns` | `''` | Both cluster ARNs, comma-separated; set only in the peering phase |
| `DeletionProtection` | `true` | Set to `false` before deleting the stack |
| `ProjectTag` | `dsql-multi-region-routing-demo` | Value of the `Project` tag |

Outputs: `ClusterIdentifier`, `ClusterEndpoint`, `ClusterArn`, `ClusterStatus`, `VpcEndpointServiceName`.

**API stack parameters** ([`template.yaml`](petstore-api/template.yaml)): `CodeS3Bucket`, `CodeS3Key`, `Psycopg2LayerS3Key`, `OriginVerifySecret`, `OriginVerifyHeaderName` (default `x-origin-verify`), `DsqlClusterArnPattern` (narrow to your cluster ARNs for least privilege), `InterruptParameterName` (default `/petstore/interrupt-region`). Outputs: `ApiGatewayEndpoint`, `ApiInvokeUrlDirect`, `StageName`.

**CloudFront stack parameters** ([`edge/apiprotection.yml`](petstore-api/edge/apiprotection.yml)): `APIGWEndpoint`, `OriginPath` (default `/prod`), `OriginVerifySecret`, `OriginVerifyHeaderName`. Outputs: `CFDistribution`, `S3BucketAccessLogs`.

**API endpoints** (through the CloudFront URL):

| Method | Path | Body | Returns |
|---|---|---|---|
| POST | `/pets` | `{petType, petName, petAge?}` | 201 with the created pet |
| GET | `/pets` | `?limit=` (1 to 50, default 50) | 200 `{pets: [...]}` |
| GET | `/pets/{petId}` | none | 200 pet, or 404 |
| PUT | `/pets/{petId}` | any of `{petType, petName, petAge}` | 200 updated pet, or 404 |
| DELETE | `/pets/{petId}` | none | 204, or 404 |

**Load test flags** ([`run.sh`](loadtest/run.sh)): `--rpm`, `--vus`, `--wait-ms`, `--duration`, `--base-url`, `--report`, `--no-cleanup`. Details in [`LOAD_TEST_GUIDE.md`](loadtest/LOAD_TEST_GUIDE.md).

**Interrupt commands** ([`interrupt.sh`](failover/interrupt.sh)): `status`, `interrupt --region us-east-1|us-east-2`, `restore`. Options: `--param`, `--ssm-region`.

### Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `ModuleNotFoundError: No module named 'boto3'` | The script ran with a Python that lacks boto3. Activate the venv, or use `failover/interrupt.sh` instead of `interrupt.py`. |
| Load test never shows us-east-2 | Expected under normal load (lowest latency is us-east-1). Run step 5 to move traffic. |
| `run.sh` does not exit | A browser tab is open on the k6 dashboard. Close it or press Ctrl-C. |
| `error` entries in the summary | An `error` is a create or update that did not return 2xx. Check the pet store Lambda's CloudWatch logs: `OCC conflict ... exhausted` means sustained contention on the same rows (raise the `OCC_MAX_RETRIES` environment variable, default 5), and `Unhandled error` is a 500 with the cause logged. A short dip right after `interrupt` is the in-flight connection rerouting. |
| Code change has no effect after redeploy | CloudFormation did not refresh the Lambda code. See "Redeploying code changes" in [step 2](#step-2-deploy-the-api-and-cloudfront). |
| CloudFront returns 403 right after a deploy | The distribution is still propagating. Retry after a minute. |
| `AccessDenied` from `interrupt.sh` | Your credentials need `ssm:GetParameter` and `ssm:PutParameter` on `/petstore/interrupt-region`. |
| Stack delete fails on the cluster | Deletion protection is still on. See teardown step T4. |
| `petstore-api` fails with `The policy failed legacy parsing` | In zsh, `$ACCOUNT:cluster` loses `:c` (zsh treats it as a modifier). Use `${ACCOUNT}:cluster`, delete the `ROLLBACK_COMPLETE` stack, and redeploy. |

---

## Important notes

- The library requires boto3 1.37.24 or later.
- The DSQL endpoints must be reachable from where the code runs, and your credentials must be allowed to generate DSQL admin auth tokens.
- The demo uses synthetic data and is not production-ready. For production use, add error handling, monitoring, and least-privilege IAM suited to your environment.

## License

This project is licensed under the MIT No Attribution (MIT-0) License. See the [LICENSE](LICENSE) file.
