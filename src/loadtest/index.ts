// Load test for POST /events. Three scenarios, each ending in a reconciliation
// against MongoDB (the only source of truth that matters):
//
//   steady : open-loop constant rate. Is the happy path fast and loss-free?
//   burst  : N events as fast as possible by many clients that RETRY on 503 (like
//            reconnecting devices), with concurrent duplicate sends of the same uid
//            and a late replay of already-acked uids. This is the one that hurts.
//   ramp   : open-loop, rate stepped up until the service pushes back (503/errors).
//
// Why open-loop for steady/ramp: a closed-loop client slows down when the server
// does, hiding overload ("coordinated omission"). Open-loop keeps offering load.
//
// Vocabulary used in the report:
//   sent         unique uids the generator tried to deliver
//   attempts     HTTP requests made (>= sent: retries and duplicate sends)
//   acknowledged unique uids that received at least one 200
//   stored       documents in Mongo for this run
//   lost         acknowledged but NOT in Mongo (must be 0)
//   duplicates   uids with more than one document (must be 0)
import { parseArgs } from "node:util";
import { mkdirSync, writeFileSync } from "node:fs";
import { Pool, request } from "undici";
import { MongoClient } from "mongodb";

const { values: args, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    target: { type: "string", default: process.env.TARGET_URL ?? "http://localhost:8080" },
    "mongo-uri": { type: "string", default: process.env.MONGO_URI ?? "mongodb://localhost:27017" },
    db: { type: "string", default: "pipeline" },
    collection: { type: "string", default: "events" },
    rate: { type: "string", default: "500" }, // steady: events/sec
    duration: { type: "string", default: "30" }, // steady: seconds
    events: { type: "string", default: "100000" }, // burst: unique events
    concurrency: { type: "string", default: "500" }, // burst: parallel clients / pool size
    "dup-ratio": { type: "string", default: "0.2" }, // burst: fraction also sent twice concurrently
    "replay-ratio": { type: "string", default: "0.1" }, // burst: fraction of acked uids replayed afterwards
    "start-rate": { type: "string", default: "500" }, // ramp
    "step-rate": { type: "string", default: "500" },
    "step-secs": { type: "string", default: "10" },
    "max-rate": { type: "string", default: "20000" },
    "max-inflight": { type: "string", default: "2000" }, // open-loop client safety cap
  },
});

const scenario = positionals[0];
const num = (k: keyof typeof args) => Number(args[k]);
const target = String(args.target);
const runId = `r${Date.now().toString(36)}`; // uid prefix => every run is isolated and queryable
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ---------- client ----------
const pool = new Pool(target, {
  connections: Math.max(num("concurrency"), 256),
  pipelining: 1,
  headersTimeout: 15_000,
  bodyTimeout: 15_000,
});

interface Counters {
  attempts: number;
  ok: number; // 200
  overloaded: number; // 503
  otherHttp: number;
  netErr: number; // timeouts/resets: the client does NOT know if the event was stored
  latencies: number[];
}
const newCounters = (): Counters => ({ attempts: 0, ok: 0, overloaded: 0, otherHttp: 0, netErr: 0, latencies: [] });
const acked = new Set<string>(); // ground truth of what the service promised us

function makeEvent(i: number): Record<string, unknown> {
  const uid = `${runId}-${i}`;
  // ~30% of events look like a reconnecting device flushing an old backlog.
  const age = Math.random() < 0.3 ? Math.floor(Math.random() * 10 * 60_000) : 0;
  return {
    uid,
    site_name: "Plant 3 - North Yard",
    camera_name: `Loading Dock Cam ${String(i % 40).padStart(2, "0")}`,
    uc_type: "Man_Down",
    usecase_objects: [["person", true, true]],
    severity: "Low",
    count: 1,
    media_link: `https://media.example.com/images/frame_${i}.jpeg`,
    video_url: `https://media.example.com/clips/alert_${i}.mp4`,
    additional_info: { class_name: "person", duration_seconds: i % 60 },
    time_created: new Date(Date.now() - age).toISOString(),
    status: "Logged",
  };
}

type Outcome = { acked: boolean; retryAfterMs: number };

async function sendOnce(ev: Record<string, unknown>, c: Counters): Promise<Outcome> {
  const t0 = performance.now();
  c.attempts++;
  try {
    const res = await pool.request({
      path: "/events",
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(ev),
    });
          await res.body.text();
       if (res.statusCode === 200) {
         // Only successful acks count towards latency: instant 503s would drag the
         // percentiles down and make an overloaded service look fast.
         c.latencies.push(performance.now() - t0);
         c.ok++;
      acked.add(ev.uid as string);
      return { acked: true, retryAfterMs: 0 };
    }
    if (res.statusCode === 503) c.overloaded++;
    else c.otherHttp++;
    const ra = Number(res.headers["retry-after"]);
    return { acked: false, retryAfterMs: Number.isFinite(ra) ? ra * 1000 : 0 };
  } catch {
    c.netErr++;
    return { acked: false, retryAfterMs: 0 };
  }
}

// A device that is not acked keeps the event and resends the SAME uid, honouring
// Retry-After (with jitter so a whole fleet doesn't retry in lock-step).
async function sendWithRetry(ev: Record<string, unknown>, c: Counters, maxAttempts = 60): Promise<boolean> {
  for (let a = 0; a < maxAttempts; a++) {
    const o = await sendOnce(ev, c);
    if (o.acked) return true;
    const base = o.retryAfterMs || 200 * 2 ** Math.min(a, 5);
    await sleep(base * (0.5 + Math.random()));
  }
  return false;
}

// ---------- scenarios ----------
async function runSteady(rate: number, seconds: number, c: Counters, firstIndex: number): Promise<number> {
  const maxInflight = num("max-inflight");
  const start = performance.now();
  let sent = 0;
  let inflight = 0;
  while ((performance.now() - start) / 1000 < seconds) {
    const due = Math.floor(((performance.now() - start) / 1000) * rate);
    // If the client itself saturates (inflight cap) we fall behind schedule instead of
    // queueing unboundedly in the load generator; the report shows achieved vs target.
    while (sent < due && inflight < maxInflight) {
      const ev = makeEvent(firstIndex + sent++);
      inflight++;
      void sendOnce(ev, c).finally(() => inflight--);
    }
    await sleep(2);
  }
  while (inflight > 0) await sleep(10);
  return sent;
}

async function runBurst(c: Counters): Promise<{ sent: number; replays: number }> {
  const n = num("events");
  const dupRatio = Number(args["dup-ratio"]);
  let next = 0;
  const client = async () => {
    for (;;) {
      const i = next++;
      if (i >= n) return;
      const ev = makeEvent(i);
      const jobs = [sendWithRetry(ev, c)];
      // Same uid in flight twice at once: the race the unique index must win.
      if (Math.random() < dupRatio) jobs.push(sendWithRetry(ev, c));
      await Promise.all(jobs);
    }
  };
  await Promise.all(Array.from({ length: num("concurrency") }, client));

  // Late replays: devices resending uids we already acked. Must be acked again, add no rows.
  const replayN = Math.floor(n * Number(args["replay-ratio"]));
  let replays = 0;
  await Promise.all(
    Array.from({ length: num("concurrency") }, async () => {
      while (replays < replayN) {
        replays++;
        await sendWithRetry(makeEvent(Math.floor(Math.random() * n)), c);
      }
    }),
  );
  return { sent: n, replays: replayN };
}

interface Step { rate: number; achieved: number; sent: number; ok: number; overloaded: number; netErr: number; p99: number }

async function runRamp(allCounters: Counters[]): Promise<{ sent: number; steps: Step[]; pushbackRate: number | null }> {
  const steps: Step[] = [];
  let index = 0;
  let pushbackRate: number | null = null;
  for (let rate = num("start-rate"); rate <= num("max-rate"); rate += num("step-rate")) {
    const c = newCounters();
    allCounters.push(c);
    const t0 = performance.now();
    const sent = await runSteady(rate, num("step-secs"), c, index);
    index += sent;
    const secs = (performance.now() - t0) / 1000;
    const step = { rate, achieved: Math.round(c.ok / secs), sent, ok: c.ok, overloaded: c.overloaded, netErr: c.netErr, p99: pct(c.latencies, 99) };
    steps.push(step);
    console.log(`  step ${rate}/s: ok=${c.ok} 503=${c.overloaded} err=${c.netErr} p99=${step.p99.toFixed(0)}ms`);
    // "Pushing back" = more than 1% of requests refused or failed in this step.
    if ((c.overloaded + c.netErr) / Math.max(c.attempts, 1) > 0.01) {
      pushbackRate = rate;
      break;
    }
  }
  return { sent: index, steps, pushbackRate };
}

// ---------- stats, reconciliation ----------
function pct(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const s = [...values].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.max(0, Math.ceil((p / 100) * s.length) - 1))]; // nearest-rank
}

const sum = (cs: Counters[]): Counters => {
  const t = newCounters();
  for (const c of cs) {
    t.attempts += c.attempts; t.ok += c.ok; t.overloaded += c.overloaded; t.otherHttp += c.otherHttp; t.netErr += c.netErr;
    for (const l of c.latencies) t.latencies.push(l);
  }
  return t;
};

// Poll the service so peak memory/queue depth are observed from outside, too.
const peak = { rss: 0, pending: 0 };
let lastStats: Record<string, number> = {};
const poller = setInterval(async () => {
  try {
    const res = await request(`${target}/stats`);
    lastStats = (await res.body.json()) as Record<string, number>;
    peak.rss = Math.max(peak.rss, lastStats.rss_bytes ?? 0);
    peak.pending = Math.max(peak.pending, lastStats.pending_events ?? 0);
  } catch { /* service may be restarting; ignore */ }
}, 500);

async function reconcile() {
  const mongo = new MongoClient(String(args["mongo-uri"]));
  await mongo.connect();
  const col = mongo.db(String(args.db)).collection(String(args.collection));
  // _id == uid, and every uid of this run shares the "<runId>-" prefix, so a range scan
  // on the _id index isolates the run ('.' sorts right after '-').
  const range = { _id: { $gte: `${runId}-`, $lt: `${runId}.` } as never };

  const stored = new Set<string>();
  const lags: number[] = [];
  for await (const d of col.find(range, { projection: { pipeline_lag_ms: 1 }, batchSize: 10_000 })) {
    stored.add(d._id as unknown as string);
    if (typeof d.pipeline_lag_ms === "number") lags.push(d.pipeline_lag_ms);
  }
  let lost = 0;
  for (const uid of acked) if (!stored.has(uid)) lost++;

  // Independent check on the uid FIELD (not just _id): any uid with >1 document?
  const dups = await col
    .aggregate([{ $match: range }, { $group: { _id: "$uid", n: { $sum: 1 } } }, { $match: { n: { $gt: 1 } } }, { $count: "n" }])
    .toArray();
  await mongo.close();
  return { stored: stored.size, lost, duplicates: (dups[0]?.n as number | undefined) ?? 0, lags, ackedNotStoredIds: lost };
}

// ---------- main ----------
async function main() {
  if (!["steady", "burst", "ramp"].includes(scenario)) {
    console.error("usage: loadtest <steady|burst|ramp> [--rate N --duration S | --events N --concurrency C | --start-rate ... --max-rate ...]");
    process.exit(2);
  }
  console.log(`run ${runId}: ${scenario} -> ${target}`);
  const t0 = performance.now();
  const counters: Counters[] = [];
  let sent = 0;
  let extra: Record<string, unknown> = {};

  if (scenario === "steady") {
    const c = newCounters();
    counters.push(c);
    sent = await runSteady(num("rate"), num("duration"), c, 0);
  } else if (scenario === "burst") {
    const c = newCounters();
    counters.push(c);
    const r = await runBurst(c);
    sent = r.sent;
    extra = { replays: r.replays };
  } else {
    const r = await runRamp(counters);
    sent = r.sent;
    extra = { steps: r.steps, first_pushback_rate: r.pushbackRate };
  }
  const seconds = (performance.now() - t0) / 1000;
  clearInterval(poller);
  await pool.close();

  const total = sum(counters);
  const rec = await reconcile();
  const final = await request(`${target}/stats`).then((r) => r.body.json() as Promise<Record<string, number>>).catch(() => lastStats);

  const report = {
    run_id: runId,
    scenario,
    seconds: Number(seconds.toFixed(1)),
    sent,
    attempts: total.attempts,
    acknowledged: acked.size,
    rejected_503: total.overloaded,
    network_errors: total.netErr,
    stored: rec.stored,
    duplicates: rec.duplicates,
    lost: rec.lost,
    stored_but_never_acked: rec.stored - (acked.size - rec.lost),
    sustained_acked_per_sec: Math.round(acked.size / seconds),
    client_ack_latency_ms_successful_only: { p50: pct(total.latencies, 50), p95: pct(total.latencies, 95), p99: pct(total.latencies, 99) },
    pipeline_lag_ms: { p50: pct(rec.lags, 50), p95: pct(rec.lags, 95), p99: pct(rec.lags, 99), max: rec.lags.reduce((m, v) => (v > m ? v : m), 0) },
    peak_service_rss_mb_polled: Number((peak.rss / 1048576).toFixed(1)),
    peak_service_rss_mb_kernel: Number(((final.max_rss_bytes ?? 0) / 1048576).toFixed(1)),
    peak_pending_events: peak.pending,
    avg_batch_size: Number((final.avg_batch_size ?? 0).toFixed(1)),
    ...extra,
    verification_queries: [
      `db.${args.collection}.countDocuments({_id:{$gte:"${runId}-",$lt:"${runId}."}})`,
      `db.${args.collection}.aggregate([{$match:{_id:{$gte:"${runId}-",$lt:"${runId}."}}},{$group:{_id:"$uid",n:{$sum:1}}},{$match:{n:{$gt:1}}},{$count:"duplicate_uids"}])`,
    ],
  };

  mkdirSync("results", { recursive: true });
  writeFileSync(`results/${runId}.json`, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));

  // Non-zero exit makes loss/duplication fail CI-style runs loudly.
  if (rec.lost > 0 || rec.duplicates > 0) {
    console.error("FAIL: lost or duplicated events detected");
    process.exit(1);
  }
  process.exit(0);
}
void main();

