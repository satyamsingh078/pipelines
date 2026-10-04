// All tunables in one place, overridable by env so the load test can sweep them
// without rebuilding. Defaults are the values the README's results were run with.
const int = (key: string, dflt: number): number => {
  const raw = process.env[key];
  if (raw === undefined) return dflt;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n) || n <= 0) throw new Error(`Invalid ${key}=${raw}`);
  return n;
};

export const config = {
  port: int("PORT", 8080),
  mongoUri: process.env.MONGO_URI ?? "mongodb://localhost:27017",
  dbName: process.env.MONGO_DB ?? "pipeline",
  collection: process.env.MONGO_COLLECTION ?? "events",

  // Worker threads. Each owns its own Mongo client, so enrichment + BSON
  // serialisation + network I/O run truly in parallel with the HTTP thread.
  workers: int("WORKERS", 4),
  // Upper bound on documents per insertMany. Batches grow towards this under
  // load (all workers busy -> queue builds -> next batch is bigger).
  batchMax: int("BATCH_MAX", 500),
  // At low load, how long the first queued event waits for company. This is
  // latency we add to every ack when traffic is light, so keep it small.
  batchLingerMs: int("BATCH_LINGER_MS", 5),

  // Backpressure limits. Memory is bounded by BOTH: a count cap and a byte cap.
  maxPending: int("MAX_PENDING", 20_000),
  maxPendingBytes: int("MAX_PENDING_BYTES", 64 * 1024 * 1024),
  // Events are ~1KB; 64KB is generous and stops one client from burning the byte budget.
  maxBodyBytes: int("MAX_BODY_BYTES", 64 * 1024),

  // How long an HTTP request may wait for its durable write before we give up
  // and answer 503 (the event may still land; a retry is harmless, see README).
  ackTimeoutMs: int("ACK_TIMEOUT_MS", 10_000),
  maxWriteAttempts: int("MAX_WRITE_ATTEMPTS", 5),
  mongoPoolPerWorker: int("MONGO_POOL_PER_WORKER", 2),

  retryAfterSec: int("RETRY_AFTER_SEC", 1),
  shutdownTimeoutMs: int("SHUTDOWN_TIMEOUT_MS", 45_000),
};

// Plain-data subset handed to worker threads (they cannot share the module state).
export interface WorkerData {
  mongoUri: string;
  dbName: string;
  collection: string;
  poolSize: number;
  maxWriteAttempts: number;
}

export const workerData: WorkerData = {
  mongoUri: config.mongoUri,
  dbName: config.dbName,
  collection: config.collection,
  poolSize: config.mongoPoolPerWorker,
  maxWriteAttempts: config.maxWriteAttempts,
};
