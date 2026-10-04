# High-Throughput Event Ingestion Pipeline

Fastify HTTP API -> bounded in-memory queue -> worker threads (enrich + batched insert) -> MongoDB.
The client gets `200` only after MongoDB has durably written the event.

## 1. Run

Needs Docker + Docker Compose.

```bash
docker compose up --build -d      # local MongoDB 7 + service on :8080
curl localhost:8080/stats         # live counters
docker compose down               # add -v to wipe the data
```

Load tests (each prints a JSON report, reconciles against MongoDB, exits non-zero on loss/duplicates):

```bash
docker compose run --rm loadtest steady --rate 500 --duration 60
docker compose run --rm loadtest burst  --events 200000 --concurrency 500 --dup-ratio 0.2
docker compose run --rm loadtest ramp   --start-rate 2000 --step-rate 2000 --step-secs 10 --max-rate 30000
```

Shutdown/crash test (PowerShell): `.\scripts\shutdown-test.ps1 -Signal SIGTERM` or `-Signal SIGKILL`.

MongoDB Atlas instead of local: copy `.env.example` to `.env`, set `MONGO_URI`, then add
`-f docker-compose.atlas.yml` to the commands above. Tunables are env vars (`WORKERS`, `BATCH_MAX`, `MAX_PENDING`, see `src/config.ts`).

API: `POST /events` -> `200 {"status":"durable"}` (stored; also returned for an already-stored uid),
`503` + `Retry-After` (not stored, retry the same event), `400` (missing/invalid `uid`).

## 2. Design

**Stages.** (1) *Ingest*: Fastify validates `uid`, stamps `time_received`, checks capacity.
(2) *Queue*: bounded in-memory array of not-yet-acknowledged events. (3) *Process + Persist*: a worker thread computes
`score` and `transport_lag_ms`, stamps `time_persisted`/`pipeline_lag_ms`, and writes with `insertMany` (unordered).
(4) *Ack*: when the worker reports success, the waiting HTTP requests get `200`.

**Why an in-memory queue is safe.** The queue only holds events that have *not* been acknowledged. The ack is tied to the
durable write, not to enqueueing, so losing the queue in a crash loses nothing we promised. Trade-off: ack latency includes
batching + the database commit. A durable log (Kafka/Redis) would ack faster but adds a second system to prove correct.

**Concurrency.** 4 worker threads (default), each with its own Mongo client and one batch in flight. Batches are up to 500
events: a free worker takes whatever is queued (waiting at most 5 ms for a partial batch), so batches grow when load is high.
Sizing was by default choice plus one experiment: on Atlas, going from 4 to 16 workers did not raise throughput (70 -> 78 acks/s).

**5.1 No silent loss: where an event could be lost, and what closes it.**
| Process dies... | Acked? | Result |
|---|---|---|
| while parsing / queued / in a worker before the write commits | no | client has no 200, resends |
| after MongoDB commits but before the 200 is sent | no | row exists; resend hits the duplicate key and gets 200 |
| after the 200 was sent | yes | already journaled (`w:majority, j:true`) before the 200 existed |

A worker crash puts its batch back in the queue. If writes keep failing, clients get 503, never a false 200.

**5.2 One row per uid.** `_id` is set to `uid`, so MongoDB's unique `_id` index enforces it, even for concurrent copies
and copies in the same batch. A duplicate-key error (E11000) means the row already exists, so it counts as success.
The moment a duplicate could appear is a resend after a lost response or a concurrent send; the unique index closes it.

**5.3 Backpressure.** New events are refused when 20,000 events or 64 MB are pending. The client sees `503` with
`Retry-After: 1`; a request waiting over 10 s for its write also gets 503. Memory stays bounded and refusals are explicit.

**5.4 Clean shutdown (SIGTERM).** New requests get 503 and `/readyz` flips, partial batches flush, the server stops
listening, the service waits until every accepted event is acked, closes the Mongo clients and exits 0.

**5.5 Parallelism.** Enrichment and writing run in multiple worker threads, off the HTTP thread.

**Bottleneck.**
- *Local MongoDB:* acks plateaued around 11-12k/s while the p99 latency jumped from 23 ms (at 10k/s) to 643 ms (at 12k/s).
  Pipeline lag stayed under 40 ms, so the workers were not the limit. I suspect the single HTTP thread or the load generator
  sharing the same machine; I did not confirm which.
- *Atlas:* found via `GET /stats` during a burst: `avg_write_ms` was about 3.3 s per batch with 0 failed batches, so the
  workers were waiting on the database. Throughput was about 70-100 events/s regardless of worker count, so the database
  was the limit, and backpressure (503) handled the overload. I did not identify the cluster tier.

**10x throughput.** Several service replicas behind a load balancer; a sharded MongoDB replica set (hashed `_id`);
batched POSTs from devices; and, if ack latency matters, a durable log in front of the database.

## 3. Results

"Local" = MongoDB container (single node). "Atlas" = remote cluster. Sent/Acked/Stored are unique uids.
Lag = `pipeline_lag_ms` in ms (p50/p95/p99/max). Every run: duplicates 0, lost 0.

| Run | DB | Sent | Acked | Stored | Throughput (acked/s) | Lag | Peak RSS |
|---|---|---|---|---|---|---|---|
| steady 500/s, 60 s | local | 29,999 | 29,999 | 29,999 | 500 | 5 / 178 / 1420 / 2017 | 180 MB |
| burst 200k, 20% concurrent dup sends, 10% replays | local | 200,000 | 200,000 | 200,000 | 6,774 | 4 / 10 / 16 / 228 | 210 MB |
| burst 60k + SIGTERM mid-run | local | 60,000 | 60,000 | 60,000 | 3,810 | 4 / 7 / 8 / 20 | 189 MB |
| burst 60k + SIGKILL mid-run | local | 60,000 | 60,000 | 60,000 | 4,079 | 4 / 6 / 8 / 19 | 188 MB |
| ramp 2k -> 30k/s | local | 1,773,988 | 1,773,988 | 1,773,988 | ~10.8k avg | 4 / 8 / 11 / 38 | 214 MB |
| burst 50k, queue limit 500 | Atlas | 50,000 | 41,516 | 42,441 | 70 | 933 / 2490 / 4274 / 5384 | 212 MB |
| ramp 20 -> 140/s, queue limit 200 | Atlas | 3,196 | 2,988 | 2,988 | 72 | 83 / 757 / 947 / 1077 | 171 MB |

Notes:
- **Backpressure:** the Atlas 50k burst had 1,031,324 requests refused with 503; the queue stayed capped and memory flat.
  Clients retried 60 times (~1 min) and then gave up, so Acked < Sent. That is un-acked, not lost.
  The Atlas ramp reached pushback at 140/s: 3,196 sent = 2,988 acked + 208 refused.
- **Stored > Acked (925 in the Atlas 50k burst):** written, but the 200 never reached the client (likely the 10 s ack timeout).
  Harmless: a resend is a duplicate-key hit.
- **SIGTERM:** logs show the drain, exit code 0. **SIGKILL:** exit code 137, service restarted, clients retried. Both: lost 0.
- `pipeline_lag_ms` excludes the final commit time (see assumptions), so on Atlas it understates what clients wait.

**Reconciliation.** Each run's script reads MongoDB directly and compares it to the uids the service acknowledged:
`Acked <= Stored <= Sent`, lost = acked uids missing from MongoDB = 0. By hand (replace `<runId>`, printed in every report):
```js
db.events.countDocuments({ _id: { $gte: "<runId>-", $lt: "<runId>." } })          // = Stored
db.events.aggregate([ { $match: { _id: { $gte: "<runId>-", $lt: "<runId>." } } },
  { $group: { _id: "$uid", n: { $sum: 1 } } }, { $match: { n: { $gt: 1 } } }, { $count: "duplicate_uids" } ])  // no rows
```

## 4. Assumptions and gaps

**Assumptions**
- `uid` (non-empty string, up to 256 chars) is required. All other fields are stored untouched.
- An unparseable `time_created` is accepted and `transport_lag_ms` is stored as `null`.
- `time_persisted` must be inside the document, so it is stamped just before the insert; `pipeline_lag_ms` excludes the commit.
- `score` = `round(Date.now()/7, 3)`, a BSON double, computed in the worker's processing step.
- First write wins: a resend with different fields does not overwrite the stored event.
- One event per request; request bodies are limited to 64 KB. No authentication or TLS.
- Clients retry the same uid on 503 (the load test does this).
- Local MongoDB is a single node, so `w:majority` adds no replication there; Atlas is a replica set.

**Libraries.** `fastify` (fast HTTP, body limit, graceful close), `mongodb` (official driver, bulk writes, write concern),
`undici` (fast HTTP client for the load test), `typescript` (build), Node `worker_threads` (parallelism, no extra dependency).

