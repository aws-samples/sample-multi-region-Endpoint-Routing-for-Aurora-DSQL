// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
//
// petstore_load.js
//
// k6 load scenario for the DSQL-backed pet store, driven through CloudFront.
// Each iteration exercises a full CRUD lifecycle -- Create, Read, Update, Read,
// Delete -- so under steady load the table sees a realistic mix of operations
// and each iteration cleans up after itself (the pet it created is deleted at
// the end of the same iteration).
//
// Writes carry writeRegion (which Aurora DSQL Region served them); the script
// records a per-Region counter + latency/success trends tagged by Region, so
// k6's live web dashboard shows the Region mix shifting during an interruption.
//
// Teardown: as a safety net, any ids that were created but not yet deleted
// (e.g. an iteration interrupted mid-cycle) are deleted in teardown(). For a
// guaranteed full wipe of the table -- including pre-existing data -- use
// loadtest/cleanup.py, which goes straight to DSQL (off the public API).
//
// Run via run.sh (enables the web dashboard), or directly:
//   BASE_URL=https://xxxx.cloudfront.net k6 run loadtest/petstore_load.js
//
// Tunables (env vars):
//   BASE_URL   CloudFront URL (required)
//   DURATION   how long to run, e.g. 2m (default 1m)
//   RPM        requests per minute -- fixed RATE model (constant-arrival-rate).
//              When >0 this overrides the VUS model. Note each iteration makes
//              ~6 HTTP calls (create/read/update/read/delete + a read), so one
//              "request" here = one full CRUD iteration.
//   VUS        concurrent virtual users -- fixed CONCURRENCY model (default 10).
//              Used when RPM is unset/0. With RPM set, acts as the VU pool size.
//   WAIT_MS    think-time between iterations in ms (default 200).

import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Trend, Rate } from "k6/metrics";

const BASE_URL = (__ENV.BASE_URL || "").replace(/\/$/, "");
const VUS = parseInt(__ENV.VUS || "10", 10);
const DURATION = __ENV.DURATION || "1m";
// RPM: if set (>0), drive a fixed request rate (requests per minute) using the
// constant-arrival-rate executor, independent of latency. If unset, use the
// concurrency model (VUS constant virtual users).
const RPM = parseInt(__ENV.RPM || "0", 10);
// WAIT_MS: think-time between iterations, in milliseconds (default 200).
const WAIT_MS = parseInt(__ENV.WAIT_MS || "200", 10);

if (!BASE_URL) {
  throw new Error("BASE_URL is required, e.g. BASE_URL=https://xxxx.cloudfront.net");
}

// Custom metrics. writes_by_region is tagged by Region so the dashboard breaks
// writes down as writes_by_region{region:us-east-1} vs {region:us-east-2}.
const writesByRegion = new Counter("writes_by_region");
const writeLatency = new Trend("write_latency_ms", true);
const writeSuccess = new Rate("write_success");
const opCount = new Counter("crud_ops"); // tagged by op: create/read/update/delete

// Tracks ids created but not yet deleted, so teardown can sweep leftovers.
// (Module scope is per-VU in k6; the array is handed to teardown via the
// setup/handleSummary data channel is not available for live state, so each
// iteration deletes its own pet. This array only catches an iteration that
// errors before its own delete.)
const leftover = [];

const PET_TYPES = ["dog", "cat", "bird", "rabbit", "hamster"];
const PET_NAMES = ["Rex", "Whiskers", "Tweety", "Thumper", "Nibbles", "Luna", "Max", "Bella"];
function pick(a) { return a[Math.floor(Math.random() * a.length)]; }

// Pick the load model: a fixed request RATE (RPM) or fixed CONCURRENCY (VUS).
const crudScenario = RPM > 0
  ? {
      // constant-arrival-rate holds a steady rate regardless of response time.
      executor: "constant-arrival-rate",
      rate: RPM,
      timeUnit: "1m",            // RPM = iterations per minute
      duration: DURATION,
      preAllocatedVUs: Math.max(VUS, Math.ceil(RPM / 60) * 2),
      maxVUs: Math.max(VUS * 5, Math.ceil(RPM / 60) * 10),
    }
  : { executor: "constant-vus", vus: VUS, duration: DURATION };

export const options = {
  scenarios: {
    crud: crudScenario,
  },
  thresholds: {
    write_success: ["rate>0.95"],
    "http_req_duration{expected_response:true}": ["p(95)<2000"],
    // Always-true thresholds exist only to FORCE k6 to materialize the tagged
    // submetrics so they show in the dashboard and summary.
    "writes_by_region{region:us-east-1}": ["count>=0"],
    "writes_by_region{region:us-east-2}": ["count>=0"],
    "writes_by_region{region:error}": ["count>=0"],
    "writes_by_region{region:unknown}": ["count>=0"],
    "crud_ops{op:create}": ["count>=0"],
    "crud_ops{op:read}": ["count>=0"],
    "crud_ops{op:update}": ["count>=0"],
    "crud_ops{op:delete}": ["count>=0"],
  },
};

const JSON_HEADERS = { headers: { "Content-Type": "application/json" } };

function recordWrite(res) {
  const ok = res.status >= 200 && res.status < 300;
  writeSuccess.add(ok);
  let region = "error";
  if (ok && res.body) {
    try { region = JSON.parse(res.body).writeRegion || "unknown"; }
    catch (e) { region = "unparseable"; }
    writeLatency.add(res.timings.duration, { region: region });
  }
  writesByRegion.add(1, { region: region });
  return region;
}

export default function () {
  // --- CREATE ---
  const createBody = JSON.stringify({
    petType: pick(PET_TYPES),
    petName: pick(PET_NAMES) + "-" + Math.floor(Math.random() * 1000000),
    petAge: Math.floor(Math.random() * 15),
  });
  const created = http.post(`${BASE_URL}/pets`, createBody, { ...JSON_HEADERS, tags: { name: "create_pet" } });
  const createOk = check(created, { "create 201": (r) => r.status === 201 });
  opCount.add(1, { op: "create" });
  recordWrite(created);

  let id = null;
  if (createOk) {
    try { id = JSON.parse(created.body).petId; } catch (e) { id = null; }
  }
  if (!id) { sleep(WAIT_MS / 1000); return; }  // nothing to do the rest of the cycle on
  leftover.push(id);

  // --- READ ---
  const got = http.get(`${BASE_URL}/pets/${id}`, { tags: { name: "get_pet" } });
  check(got, { "get 200": (r) => r.status === 200 });
  opCount.add(1, { op: "read" });

  // --- UPDATE ---
  const updBody = JSON.stringify({ petAge: Math.floor(Math.random() * 20) });
  const updated = http.put(`${BASE_URL}/pets/${id}`, updBody, { ...JSON_HEADERS, tags: { name: "update_pet" } });
  check(updated, { "update 200": (r) => r.status === 200 });
  opCount.add(1, { op: "update" });
  recordWrite(updated);

  // --- READ again ---
  const got2 = http.get(`${BASE_URL}/pets/${id}`, { tags: { name: "get_pet_2" } });
  check(got2, { "get-after-update 200": (r) => r.status === 200 });
  opCount.add(1, { op: "read" });

  // --- DELETE (self-cleanup) ---
  const del = http.del(`${BASE_URL}/pets/${id}`, null, { tags: { name: "delete_pet" } });
  const delOk = check(del, { "delete 204": (r) => r.status === 204 });
  opCount.add(1, { op: "delete" });
  if (delOk) {
    const i = leftover.indexOf(id);
    if (i >= 0) leftover.splice(i, 1);
  }

  sleep(WAIT_MS / 1000);
}

// Safety-net sweep: delete any ids this VU created but didn't manage to delete
// (e.g. an iteration that errored mid-cycle). Each VU runs teardown's logic via
// its own leftover array is NOT shared, so this only covers the final VU's
// leftovers; cleanup.py is the authoritative full wipe.
export function teardown() {
  for (const id of leftover) {
    http.del(`${BASE_URL}/pets/${id}`);
  }
}

export function handleSummary(data) {
  let lines = "\n=== Writes by DSQL Region ===\n";
  const subs = Object.keys(data.metrics).filter((k) => k.startsWith("writes_by_region{"));
  for (const k of subs.sort()) {
    const region = k.replace("writes_by_region{region:", "").replace("}", "");
    lines += `  ${region.padEnd(14)}${data.metrics[k].values.count || 0}\n`;
  }
  lines += "\n=== CRUD operation mix ===\n";
  for (const op of ["create", "read", "update", "delete"]) {
    const k = `crud_ops{op:${op}}`;
    if (data.metrics[k]) lines += `  ${op.padEnd(8)}${data.metrics[k].values.count || 0}\n`;
  }
  return { stdout: lines + "\n" };
}
