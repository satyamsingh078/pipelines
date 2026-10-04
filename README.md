# High-Throughput Event Ingestion Pipeline

An HTTP service that absorbs bursty detection events from edge devices, enriches them in parallel, and writes them to MongoDB in batches. A client receives `200` only after its event is durably stored.

```text
device --POST /events--> Fastify (ingest) --> bounded queue --> worker threads --> MongoDB
                              ^                                  (enrich + insertMany)
                              |                                         |
                              +------------- 200 after the write -------+
```

## Run

Requirements: Docker and Docker Compose.

```bash
docker compose up --build -d     # local MongoDB 7 + the service on port 8080
curl localhost:8080/stats        # live counters (PowerShell: curl.exe -s localhost:8080/stats)
docker compose down              # add -v to delete the stored data
```

### Load tests

Each run prints a JSON report, reconciles it against MongoDB, and exits non-zero if any event is lost or duplicated.

```bash
docker compose run --rm loadtest steady --rate 500 --duration 60
docker compose run --rm loadtest burst  --events 200000 --concurrency 500 --dup-ratio 0.2
docker compose run --rm loadtest ramp   --start-rate 2000 --step-rate 2000 --step-secs 10 --max-rate 30000
```

- `steady`: constant rate (open-loop, so a slow server cannot hide overload).
- `burst`: many clients that retry on 503, 20% of uids sent twice at the same time, then 10% late replays of acknowledged uids.
- `ramp`: raises the rate step by step until more than 1% of requests are refused.

### Shutdown and crash test (PowerShell)

```powershell
.\scripts\shutdown-test.ps1 -Signal SIGTERM   # graceful drain, expect exit code 0
.\scripts\shutdown-test.ps1 -Signal SIGKILL   # hard crash, container restarts
```

### MongoDB Atlas instead of the local database

1. Copy `.env.example` to `.env` and put your connection string in `MONGO_URI`.
2. Allow your IP in Atlas Network Access.
3. Add `-f docker-compose.atlas.yml` to the commands above, for example `docker compose -f docker-compose.atlas.yml up --build -d`.

### Configuration

Set as environment variables (see `src/config.ts`): `WORKERS` (4), `BATCH_MAX` (500), `BATCH_LINGER_MS` (5), `MAX_PENDING` (20000), `MAX_PENDING_BYTES` (64 MB), `ACK_TIMEOUT_MS` (10000).

### API

| Request | Response | Meaning |
|---|---|---|
| `POST /events` | `200 {"status":"durable"}` | Stored. Also returned for an already-stored uid. |
| `POST /events` | `503` + `Retry-After` | Not stored (overloaded, shutting down, or write failed). Retry the same event. |
| `POST /events` | `400` | Missing or invalid `uid`. |
| `GET /readyz` | `200` / `503` | Ready / draining. |
| `GET /stats` | JSON | Queue depth, batches, rejections, memory. |

## Design

### Stages

1. **Ingest** (main thread): validates `uid`, stamps `time_received`, checks capacity.
2. **Queue**: a bounded in-memory array of events that have not been acknowledged yet.
3. **Process and persist** (worker thread): computes `score` and `transport_lag_ms`, stamps `time_persisted` and `pipeline_lag_ms`, then writes with an unordered `insertMany`.
4. **Ack**: when the worker reports success, the waiting HTTP requests get `200`.

**Why an in-memory queue is safe.** It only holds events that have not been acknowledged. The ack is tied to the durable write, not to enqueueing, so losing the queue in a crash loses nothing that was promised. The cost is that ack latency includes batching and the database commit. A durable log (Kafka or Redis Streams) would ack faster but adds a second system to prove correct.

### Concurrency and sizing

- 4 worker threads by default. Each has its own MongoDB client and one batch in flight.
- A free worker takes whatever is queued, up to 500 events, waiting at most 5 ms for a partial batch. Under load the queue fills while workers are busy, so batches grow on their own.
- Sizing came from defaults plus one experiment: on Atlas, raising workers from 4 to 16 did not raise throughput (70 to 78 acks/s).

### No silent loss

| If the process dies... | Acknowledged? | Outcome |
|---|---|---|
| while parsing, queued, or in a worker before the write commits | no | The client has no 200 and resends. |
| after MongoDB commits but before the 200 is sent | no | The row exists. The resend hits the duplicate key and gets 200. |
| after the 200 was sent | yes | Already journaled (`w:majority`, `j:true`) before the 200 existed. |

If a worker thread crashes, its batch goes back to the front of the queue. If writes keep failing, clients get 503, never a false 200.

### Exactly one row per uid

`_id` is set to `uid`, so MongoDB's unique `_id` index enforces uniqueness, including for concurrent copies and copies in the same batch. A duplicate-key error (E11000) means the row already exists, so it counts as success. A duplicate can only appear through a resend after a lost response or a concurrent send, and the unique index closes both.

### Backpressure

New events are refused when 20,000 events or 64 MB are pending. The client sees `503` with `Retry-After: 1`. A request that waits more than 10 s for its write also gets 503. Memory stays bounded and refusal is explicit.

### Clean shutdown (SIGTERM)

New requests get 503 and `/readyz` flips to 503. Partial batches flush, the server stops listening, and the service waits until every accepted event is acknowledged. Then it closes the MongoDB clients and exits with code 0. A 45 s watchdog forces exit if MongoDB hangs.

### Bottleneck

- **Local MongoDB:** acks plateaued around 11-12k per second. The p99 latency rose from 23 ms (at 10k/s offered) to 643 ms (at 12k/s) while pipeline lag stayed under 40 ms, so the workers were not the limit. I suspect the single HTTP thread, or the load generator sharing the machine, but I did not confirm which.
- **Atlas:** found through `GET /stats` during a burst. `avg_write_ms` was about 3.3 s per batch with no failed batches, so workers were waiting on the database. Throughput stayed at about 70-100 events/s regardless of worker count. Backpressure (503) handled the overload.

### Scaling to 10x

Run several service replicas behind a load balancer, use a sharded MongoDB replica set (hashed `_id`), let devices send batched POSTs, and put a durable log in front if ack latency matters.

## Results

"Local" is a single-node MongoDB container. "Atlas" is a remote cluster. Sent, Acked and Stored are unique uids. Lag is `pipeline_lag_ms` in ms as p50 / p95 / p99 / max. **Every run had 0 duplicates and 0 lost events.**

| Run | DB | Sent | Acked | Stored | Acked per s | Lag | Peak RSS |
|---|---|---|---|---|---|---|---|
| steady 500/s, 60 s | local | 29,999 | 29,999 | 29,999 | 500 | 5 / 178 / 1420 / 2017 | 180 MB |
| burst, 20% dup sends, 10% replays | local | 200,000 | 200,000 | 200,000 | 6,774 | 4 / 10 / 16 / 228 | 210 MB |
| burst + SIGTERM mid-run | local | 60,000 | 60,000 | 60,000 | 3,810 | 4 / 7 / 8 / 20 | 189 MB |
| burst + SIGKILL mid-run | local | 60,000 | 60,000 | 60,000 | 4,079 | 4 / 6 / 8 / 19 | 188 MB |
| ramp 2k to 30k/s | local | 1,773,988 | 1,773,988 | 1,773,988 | about 10.8k | 4 / 8 / 11 / 38 | 214 MB |
| burst, queue limit 500 | Atlas | 50,000 | 41,516 | 42,441 | 70 | 933 / 2490 / 4274 / 5384 | 212 MB |
| ramp 20 to 140/s, queue limit 200 | Atlas | 3,196 | 2,988 | 2,988 | 72 | 83 / 757 / 947 / 1077 | 171 MB |

Notes:

- **Backpressure:** the Atlas burst had 1,031,324 requests refused with 503 while the queue stayed capped and memory stayed flat. Clients retry about 60 times (roughly a minute) and then give up, so Acked is below Sent. Those events are un-acknowledged, not lost. The Atlas ramp reached pushback at 140/s: 3,196 sent = 2,988 acked + 208 refused.
- **Stored above Acked (925 in the Atlas burst):** the event was written but the 200 never reached the client, most likely the 10 s ack timeout. This is harmless because a resend is a duplicate-key hit.
- **SIGTERM:** the logs show the drain and exit code 0. **SIGKILL:** exit code 137, the service restarted, and clients retried.
- `pipeline_lag_ms` excludes the final commit time (see Assumptions), so on Atlas it understates what clients actually wait.

### Reconciliation

Each run's script reads MongoDB and compares it with the uids the service acknowledged. The identity checked is `Acked <= Stored <= Sent`, and lost (acknowledged but missing from MongoDB) is 0. To repeat it by hand, replace `<runId>` with the id printed in the report:

```js
// number of stored documents for the run (equals "Stored")
db.events.countDocuments({ _id: { $gte: "<runId>-", $lt: "<runId>." } })

// uids with more than one document (must return no rows)
db.events.aggregate([
  { $match: { _id: { $gte: "<runId>-", $lt: "<runId>." } } },
  { $group: { _id: "$uid", n: { $sum: 1 } } },
  { $match: { n: { $gt: 1 } } },
  { $count: "duplicate_uids" }
])
```

## Assumptions and gaps

### Assumptions

- `uid` (a non-empty string of at most 256 characters) is required. All other fields are stored untouched.
- An unparseable `time_created` is accepted and `transport_lag_ms` is stored as `null`.
- `time_persisted` has to be inside the document, so it is stamped just before the insert. `pipeline_lag_ms` therefore excludes the commit.
- `score` is `round(Date.now() / 7, 3)` stored as a BSON double, computed in the worker's processing step.
- First write wins: a resend with different fields does not overwrite the stored event.
- One event per request. Request bodies are limited to 64 KB. There is no authentication or TLS.
- Clients retry the same uid after a 503 (the load test does).
- The local MongoDB is a single node, so `w:majority` adds no replication there. Atlas is a replica set.

### Libraries

- `fastify`: fast HTTP with a body limit and graceful close.
- `mongodb`: official driver with bulk writes and write concern.
- `undici`: fast HTTP client for the load test.
- `typescript`: build step.
- Node `worker_threads`: parallelism without extra dependencies.


## Project layout

```text
src/server.ts          HTTP endpoints, acknowledgement, shutdown
src/pipeline.ts        admission control, batching, dispatch, worker supervision
src/worker.ts          enrichment and batched insert (worker thread)
src/config.ts          settings
src/loadtest/index.ts  load test and reconciliation
scripts/shutdown-test.ps1   SIGTERM / SIGKILL test
docker-compose.yml          local MongoDB + service
docker-compose.atlas.yml    Atlas variant
```
