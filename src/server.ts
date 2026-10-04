// INGEST stage: validate, stamp time_received, apply backpressure, and answer 200
// only once the event is durable in MongoDB.
import Fastify from "fastify";
import { config } from "./config.js";
import { Pipeline } from "./pipeline.js";

const pipeline = new Pipeline();
await pipeline.start(); // fail fast (and before listening) if Mongo is unreachable

// forceCloseConnections:'idle' lets app.close() drop idle keep-alive sockets
// instead of waiting for devices to hang up, so shutdown isn't held hostage.
const app = Fastify({ bodyLimit: config.maxBodyBytes, forceCloseConnections: "idle" });

// 503 + Retry-After is our one overload/shutdown signal. Chosen over 429 because
// 429 means "THIS client is too fast", whereas here the whole service is saturated.
function reject503(reply: import("fastify").FastifyReply, error: string) {
  reply.header("retry-after", String(config.retryAfterSec));
  if (pipeline.isDraining()) reply.header("connection", "close");
  return reply.code(503).send({ error, retry_after_ms: config.retryAfterSec * 1000 });
}

app.post("/events", async (req, reply) => {
  const body = req.body;
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    return reply.code(400).send({ error: "body must be a JSON object" });
  }
  const event = body as Record<string, unknown>;
  // uid is the only field we interpret: it is the dedupe key. Everything else is opaque.
  if (typeof event.uid !== "string" || event.uid.length === 0 || event.uid.length > 256) {
    return reply.code(400).send({ error: "uid must be a non-empty string (<=256 chars)" });
  }

  const timeReceived = Date.now(); // "when your service accepted the event"
  const bytes = Number(req.headers["content-length"]) || 1024;

  const verdict = pipeline.admit(bytes);
  if (verdict !== "ok") return reject503(reply, verdict === "full" ? "overloaded" : "shutting_down");

  let timer: NodeJS.Timeout | undefined;
  try {
    const timeout = new Promise<never>((_, rej) => {
      timer = setTimeout(() => rej(new Error("ack_timeout")), config.ackTimeoutMs);
    });
    await Promise.race([pipeline.submit(event, timeReceived, bytes), timeout]);
    return reply.code(200).send({ uid: event.uid, status: "durable" });
  } catch {
    // Not acked => the client keeps responsibility and retries. If the write
    // later lands anyway, the retry becomes a harmless E11000.
    return reject503(reply, "not_acknowledged");
  } finally {
    clearTimeout(timer);
  }
});

app.get("/healthz", async () => ({ ok: true })); // liveness: process is up
app.get("/readyz", async (_req, reply) =>
  pipeline.isDraining() ? reply.code(503).send({ ready: false }) : { ready: true },
);

// Used by the load test to record peak memory and queue depth.
app.get("/stats", async () => {
  const mu = process.memoryUsage();
  return {
    ...pipeline.snapshot(),
    rss_bytes: mu.rss,
    heap_used_bytes: mu.heapUsed,
    max_rss_bytes: process.resourceUsage().maxRSS * 1024, // kernel-tracked peak, in KB on Linux
  };
});

let shuttingDown = false;
async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[shutdown] ${signal}: stop accepting, draining`);
  // Safety net: if Mongo hangs we still exit. Unacked events are the clients' to retry.
  setTimeout(() => {
    console.error("[shutdown] timeout, forcing exit");
    process.exit(1);
  }, config.shutdownTimeoutMs).unref();

  pipeline.beginDrain(); // 1. new requests -> 503; flush partial batches now
  const closing = app.close(); // 2. close the listening socket; in-flight requests keep running
  await pipeline.drained(); // 3. every accepted event is acked (or failed -> 503)
  await closing; //    4. their HTTP responses are fully written
  await pipeline.closeWorkers(); // 5. close Mongo clients
  console.log("[shutdown] drained cleanly");
  process.exit(0);
}
process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));

await app.listen({ port: config.port, host: "0.0.0.0" });
console.log(`[server] listening on :${config.port} workers=${config.workers} batchMax=${config.batchMax}`);
