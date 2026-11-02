<!--
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
-->
# Running the Load Test and Visualizing Results

This guide walks you through driving load at the multi-Region Aurora DSQL pet
store and watching the results live. You control the request rate (or
concurrency) and the wait time between requests, and a live browser dashboard
shows throughput, latency, errors, and which Aurora DSQL Region is serving
writes.

The test exercises a realistic mix of operations (Create, Read, Update, Delete)
and removes all test data at the end automatically.

---

## 1. Prerequisites

1. Install k6 (the open-source load generator). It is a single binary.

   - macOS: `brew install k6`
   - Windows / Linux: https://grafana.com/docs/k6/latest/set-up/install-k6/

2. Have the application's CloudFront URL. The scripts default to the deployed
   one; you can also pass it explicitly with `--base-url`.

No AWS credentials are needed to run the load test itself — it calls the public
CloudFront endpoint. (Credentials are only needed for the automatic cleanup
step, which clears test data from the database at the end.)

---

## 2. Run a load test

From the repository root:

```bash
loadtest/run.sh --vus 10 --duration 2m
```

While it runs, open the live dashboard in your browser:

```
http://127.0.0.1:5665
```

When the run finishes, a shareable report is written to
`loadtest/report.html`, and all test data is removed from the database.

---

## 3. Controlling load: rate vs. concurrency

There are two ways to describe load. Pick whichever matches how you think about
it.

### Option A — Request rate (requests per minute)

Use `--rpm` to hold a **fixed request rate**, regardless of how fast the API
responds. This is the most intuitive "how much traffic" control.

```bash
# 600 requests per minute (~10 per second) for 3 minutes
loadtest/run.sh --rpm 600 --duration 3m

# 1200 requests per minute, no pause between requests
loadtest/run.sh --rpm 1200 --wait-ms 0 --duration 2m
```

### Option B — Concurrency (number of simultaneous users)

Use `--vus` (virtual users) to simulate **N users hitting the API at once**.
The actual request rate then depends on how fast each request completes.

```bash
# 25 concurrent users for 5 minutes
loadtest/run.sh --vus 25 --duration 5m
```

If you pass both, `--rpm` wins (the rate model takes over and `--vus` just sizes
the worker pool).

### Controlling wait time (think-time)

`--wait-ms` sets the pause between iterations, in milliseconds. This simulates
users pausing between actions.

```bash
# 10 users, pausing 1 second between each CRUD cycle
loadtest/run.sh --vus 10 --wait-ms 1000 --duration 2m

# no pause -- push as hard as the rate/concurrency allows
loadtest/run.sh --rpm 1200 --wait-ms 0 --duration 2m
```

### All options

| Flag           | Meaning                                              | Default |
|----------------|------------------------------------------------------|---------|
| `--rpm N`      | Fixed request rate: N requests per minute            | off     |
| `--vus N`      | Fixed concurrency: N simultaneous virtual users      | 10      |
| `--wait-ms MS` | Pause between iterations, in milliseconds            | 200     |
| `--duration D` | How long to run (e.g. `30s`, `2m`, `1h`)             | 1m      |
| `--base-url U` | CloudFront URL to target                             | read from the `petstore-edge` stack |
| `--report F`   | Where to write the HTML report                       | loadtest/report.html |
| `--no-cleanup` | Keep test data after the run (skip the auto-wipe)    | cleanup on |

> One "request" in `--rpm` terms is one full CRUD cycle (Create → Read → Update
> → Read → Delete), which is about six HTTP calls. So `--rpm 600` drives roughly
> 3,600 HTTP calls per minute.

---

## 4. Reading the dashboard

Open `http://127.0.0.1:5665` during the run. Key things to watch:

- **Writes by DSQL Region** (`writes_by_region`) — how many writes each Region
  served, split as `us-east-1` vs `us-east-2`. This is the signal that shifts
  during a Region failover.
- **http_req_duration** — end-user latency. The p95 line should stay under 2s.
- **write_success** — the share of writes that succeeded (target > 95%).
- **CRUD operation mix** — counts of create / read / update / delete.

At the end of the run, the terminal also prints a compact summary:

```
=== Writes by DSQL Region ===
  us-east-1     1840
  us-east-2     97
=== CRUD operation mix ===
  create  463
  read    926
  update  463
  delete  463
```

---

## 5. What a healthy run looks like

- `write_success` stays above 95% (ideally 100%).
- Latency stays steady; no climbing error count.
- Writes concentrate in the Region nearest the application (expected — routing
  picks the lowest-latency healthy endpoint).

> During normal load, writes stay in one Region. To see traffic move to the
> other Region, interrupt the active one — see **section 6** below.

---

## 6. Interrupting a Region (failover demo)

This is the main event: with load flowing, you interrupt the active Region and
watch the application reroute writes to the surviving Region, live, with no
errors.

### Why you need this step

The application routes each database connection to the lowest-latency healthy
endpoint. Because the API runs in **us-east-1**, that endpoint is always
closest, so **every write lands in us-east-1 no matter how high you set `--rpm`
or `--vus`**. Raising the load does not move traffic to us-east-2. The other
Region takes over only when the active Region is interrupted — which is exactly
what this section does.

### One-time prerequisite

The interrupt control (`failover/interrupt.sh`) changes an AWS Systems Manager
(SSM) parameter, so it needs AWS credentials with permission to read and write
that parameter. Configure credentials the usual way (`aws configure`, a named
profile, or environment variables) before running it. This control is run by an
operator — it is intentionally **not** exposed on the public API, so a customer
hitting the API can never take a Region offline.

> The `.sh` wrapper runs the tool with a Python that has `boto3` (the repo's
> `venv` if present, otherwise `python3`). If you prefer, you can call the
> underlying `failover/interrupt.py` directly, but only with a Python that has
> `boto3` installed.

### Step by step

1. **Start load.** In one terminal, start a sustained run and confirm the
   summary shows writes served by **us-east-1**:

   ```bash
   loadtest/run.sh --rpm 600 --duration 5m
   ```

2. **Check the current state** (optional) in a second terminal:

   ```bash
   failover/interrupt.sh status
   # interrupt flag (/petstore/interrupt-region) = none
   ```

3. **Interrupt us-east-1:**

   ```bash
   failover/interrupt.sh interrupt --region us-east-1
   ```

   Within about 5 seconds, new writes shift to **us-east-2** while
   `write_success` stays high — the app reroutes seamlessly, no errors. Because
   Aurora DSQL is active-active with strong consistency, the rerouted writes are
   immediately consistent; nothing is lost. Watch the terminal summary (or the
   `writes_by_region` counts) move from us-east-1 to us-east-2.

4. **Restore normal routing** and watch writes return to us-east-1:

   ```bash
   failover/interrupt.sh restore
   ```

### Commands reference

| Command                                        | Effect                                      |
|------------------------------------------------|---------------------------------------------|
| `failover/interrupt.sh status`                 | Show the current flag (`none` / a Region)   |
| `failover/interrupt.sh interrupt --region us-east-1` | Drop us-east-1 → traffic fails over to us-east-2 |
| `failover/interrupt.sh interrupt --region us-east-2` | Drop us-east-2 → traffic fails over to us-east-1 |
| `failover/interrupt.sh restore`                | Clear the flag → normal routing resumes     |

### What to expect / troubleshooting

- **It takes a few seconds.** The Lambda caches the flag (~5s), so the switch is
  not instantaneous. Give it up to ~5–10 seconds.
- **You see the shift in the terminal summary, not as a dashboard chart.** The
  per-Region split (`writes_by_region`) prints in the end-of-run summary; the
  built-in k6 dashboard panels show latency, throughput, and errors.
- **`write_success` should stay high throughout.** A brief dip as in-flight
  connections to the interrupted Region drop and reconnect is normal; sustained
  errors are not.
- **AccessDenied from the CLI** means your AWS credentials lack
  `ssm:GetParameter` / `ssm:PutParameter` on `/petstore/interrupt-region`.

See [How the demo works](../README.md#how-the-demo-works) in the repository README for how the mechanism works.

---

## 7. Cleaning up test data

By default, `run.sh` removes all test data from the database when the run
finishes — you do not need to do anything.

- To keep data for inspection, add `--no-cleanup`.
- To wipe the table manually at any time:

  ```bash
  python loadtest/cleanup.py            # show how many rows exist
  python loadtest/cleanup.py --delete   # remove all rows
  ```

  `cleanup.py` talks directly to the database (not through the public API), so
  there is no destructive "delete everything" endpoint exposed on the API.

---

## 8. Quick reference

```bash
# --- Load ---
# Light smoke test
loadtest/run.sh --rpm 60 --duration 1m

# Steady moderate load
loadtest/run.sh --rpm 600 --duration 5m

# Concurrency-style load
loadtest/run.sh --vus 25 --wait-ms 500 --duration 5m

# Heavy, no think-time
loadtest/run.sh --rpm 1800 --wait-ms 0 --duration 3m

# Keep the data after the run
loadtest/run.sh --vus 10 --duration 2m --no-cleanup

# --- Failover (run in a second terminal while load is flowing) ---
failover/interrupt.sh status                       # current state
failover/interrupt.sh interrupt --region us-east-1 # fail over to us-east-2
failover/interrupt.sh restore                      # back to normal

# --- Cleanup (manual) ---
python loadtest/cleanup.py --delete
```
