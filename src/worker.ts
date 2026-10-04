// Worker thread: PROCESS (enrich) + PERSIST (batched insert) for one batch at a time.
//
// Why threads and not just async I/O in the main thread: enrichment, BSON
// serialisation and driver bookkeeping are CPU work. Keeping them off the HTTP
// thread means a slow Mongo round-trip or a big batch never stalls request parsing,
// and it gives real parallelism (requirement 5.5), not just concurrent waiting.
import { parentPort, workerData } from "node:worker_threads";
import { MongoClient, MongoBulkWriteError, Double, type Document } from "mongodb";
import type { WorkerData } from "./config.js";
import type { BatchRequest, WorkerRequest, WorkerResponse } from "./types.js";

const cfg = workerData as WorkerData;
const port = parentPort!;
const send = (m: WorkerResponse) => port.postMessage(m);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// w:majority + j:true is the heart of requirement 5.1: insertMany only resolves
// once the write is in the on-disk journal (and on a majority of replica members
// if there are any). We ack the HTTP client only after this resolves.
const client = new MongoClient(cfg.mongoUri, {
  maxPoolSize: cfg.poolSize,
  writeConcern: { w: "majority", j: true },
  serverSelectionTimeoutMS: 5_000,
});
const col = client.db(cfg.dbName).collection(cfg.collection);

// PROCESS stage. `score` is computed here (not at ingest, not at write) as required.
function enrich(ev: Document, timeReceived: number): Document {
  const score = Math.round((Date.now() / 7) * 1000) / 1000;
  const created = typeof ev.time_created === "string" ? Date.parse(ev.time_created) : NaN;
  return {
    ...ev, // device fields stored untouched (opaque)
    // _id = uid: Mongo's mandatory unique _id index IS the dedupe constraint
    // (requirement 5.2), with no second index to maintain on the hot write path.
    // `uid` is still stored as a normal field because the device sent it.
    _id: ev.uid,
    time_received: new Date(timeReceived),
    // Double() forces BSON double even when the value happens to be integral.
    score: new Double(score),
    // null (not NaN/0) when the device sent an unparseable time_created: we
    // don't invent data, and we don't reject an event over an opaque field.
    transport_lag_ms: Number.isNaN(created) ? null : timeReceived - created,
  };
}

// insertMany unordered: one duplicate must not stop the rest of the batch.
// E11000 on a uid means "that row already exists durably", which is exactly what
// the caller needs to hear, so it counts as success. Any OTHER error rethrows.
async function insertIgnoringDuplicates(docs: Document[]): Promise<number> {
  try {
    await col.insertMany(docs, { ordered: false });
    return 0;
  } catch (e) {
    if (e instanceof MongoBulkWriteError) {
      const errs = Array.isArray(e.writeErrors) ? e.writeErrors : [e.writeErrors];
      const onlyDuplicates = errs.length > 0 && errs.every((w) => w.code === 11000);
      // writeConcernError means the docs may NOT be durable: not a success.
      if (onlyDuplicates && !e.writeConcernError) return errs.length;
    }
    throw e;
  }
}

// PERSIST stage with retry. Retrying the WHOLE batch is safe because the unique
// _id makes every write idempotent. That also closes the "network error after
// Mongo committed" window: the retry just hits E11000 and is treated as success.
async function persist(docs: Document[]): Promise<number> {
  for (let attempt = 1; ; attempt++) {
    // Stamped immediately before each attempt: these fields live inside the doc,
    // so they cannot be known after the write. Lag therefore excludes the
    // final Mongo commit time (documented in README assumptions).
    const now = Date.now();
    const persisted = new Date(now);
    for (const d of docs) {
      d.time_persisted = persisted;
      d.pipeline_lag_ms = now - (d.time_received as Date).getTime();
    }
    try {
      return await insertIgnoringDuplicates(docs);
    } catch (e) {
      if (attempt >= cfg.maxWriteAttempts) throw e;
      await sleep(Math.min(100 * 2 ** attempt, 2_000));
    }
  }
}

async function handleBatch(msg: BatchRequest): Promise<void> {
  const t0 = Date.now();
  try {
    const docs = msg.events.map((e) => enrich(e.event, e.timeReceived));
    const duplicates = await persist(docs);
    send({ type: "done", batchId: msg.batchId, duplicates, writeMs: Date.now() - t0 });
  } catch (e) {
    // After retries are exhausted we report failure; main thread answers the
    // waiting HTTP requests with 503 (never acked, so nothing promised is lost).
    send({ type: "failed", batchId: msg.batchId, error: e instanceof Error ? e.message : String(e) });
  }
}

port.on("message", (msg: WorkerRequest) => {
  if (msg.type === "close") {
    // Main only sends this after every accepted event was acked or failed.
    void client.close().then(() => port.close());
    return;
  }
  void handleBatch(msg);
});

await client.connect();
send({ type: "ready" });
