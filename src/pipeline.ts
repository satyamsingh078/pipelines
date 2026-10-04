// Main-thread side of the pipeline: admission control, batching, dispatch to
// worker threads, and ack bookkeeping.
//
// Stages:  HTTP handler --submit()--> queue --pump()--> worker thread (enrich+insert)
//          HTTP handler <--resolve()-- batch result <-- worker thread
//
// The queue is an in-process bounded buffer. That is safe ONLY because nothing is
// acknowledged until the worker reports a durable write; the buffer holds
// not-yet-acknowledged work, so losing it in a crash loses nothing we promised.
import { once } from "node:events";
import { Worker } from "node:worker_threads";
import { config, workerData } from "./config.js";
import type { RawEvent, WorkerResponse } from "./types.js";

interface Item {
  event: RawEvent;
  timeReceived: number;
  bytes: number;
  enqueuedAt: number;
  resolve: () => void;
  reject: (e: Error) => void;
}

interface Slot {
  worker: Worker;
  ready: boolean;
  busy: boolean; // one batch in flight per worker: simple, and bounds memory per worker
}

export type Admission = "ok" | "full" | "draining";

export class Pipeline {
  private queue: Item[] = [];
  private pendingCount = 0; // queued + in flight (not yet acked/failed)
  private pendingBytes = 0;
  private slots: Slot[] = [];
  private inflight = new Map<number, { items: Item[]; slot: Slot }>();
  private nextBatchId = 1;
  private lingerTimer: NodeJS.Timeout | null = null;
  private draining = false;
  private closing = false;
  private drainWaiters: Array<() => void> = [];

  readonly stats = {
    accepted: 0,
    rejectedFull: 0,
    rejectedDraining: 0,
    storedEvents: 0, // acked via worker success (includes duplicates that already existed)
    duplicateKeyHits: 0,
    batches: 0,
    failedBatches: 0,
    workerRestarts: 0,
    writeMsTotal: 0,
  };

  async start(): Promise<void> {
    await Promise.all(Array.from({ length: config.workers }, () => this.spawn()));
  }

  private spawn(): Promise<void> {
    const slot: Slot = {
      worker: new Worker(new URL("./worker.js", import.meta.url), { workerData }),
      ready: false,
      busy: false,
    };
    this.slots.push(slot);
    return new Promise<void>((resolve, reject) => {
      slot.worker.on("message", (m: WorkerResponse) => {
        if (m.type === "ready") {
          slot.ready = true;
          resolve();
          this.pump();
        } else {
          this.onBatchResult(slot, m);
        }
      });
      slot.worker.on("error", (err) => {
        console.error("[worker] error:", err);
        if (!slot.ready) reject(err); // startup failure (e.g. Mongo unreachable) -> fail fast
      });
      slot.worker.on("exit", () => this.onWorkerExit(slot));
    });
  }

  // A worker died mid-batch. Its batch is NOT lost: those events were never acked,
  // so we put them back at the FRONT of the queue and a (re)spawned worker retries.
  // If the dead worker had already committed some of them, the retry hits E11000
  // and is treated as success.
  private onWorkerExit(slot: Slot): void {
    this.slots = this.slots.filter((s) => s !== slot);
    for (const [id, b] of this.inflight) {
      if (b.slot === slot) {
        this.inflight.delete(id);
        this.queue.unshift(...b.items);
      }
    }
    if (!this.closing && slot.ready) {
      this.stats.workerRestarts++;
      console.error("[pipeline] worker exited unexpectedly; respawning");
      setTimeout(() => void this.spawn().catch((e) => console.error("[pipeline] respawn failed", e)), 500);
    }
    this.pump();
  }

  // Admission control: the ONLY place backpressure is decided. Cheap and synchronous,
  // so overload costs us almost nothing per rejected request.
  admit(bytes: number): Admission {
    if (this.draining) {
      this.stats.rejectedDraining++;
      return "draining";
    }
    if (this.pendingCount >= config.maxPending || this.pendingBytes + bytes > config.maxPendingBytes) {
      this.stats.rejectedFull++;
      return "full";
    }
    return "ok";
  }

  // Caller must have just received "ok" from admit() with no await in between.
  // The returned promise resolves only when the event is durably in MongoDB.
  submit(event: RawEvent, timeReceived: number, bytes: number): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.queue.push({ event, timeReceived, bytes, enqueuedAt: Date.now(), resolve, reject });
      this.pendingCount++;
      this.pendingBytes += bytes;
      this.stats.accepted++;
      // Only pump if it could do something new: a linger timer already covers the partial batch.
      if (!this.lingerTimer || this.queue.length >= config.batchMax) this.pump();
    });
  }

  // Greedy adaptive batching: while workers are free we dispatch (after the linger
  // for small batches); when all are busy events accumulate, so batches grow on
  // their own exactly when throughput matters.
  private pump(): void {
    if (this.lingerTimer) {
      clearTimeout(this.lingerTimer);
      this.lingerTimer = null;
    }
    while (this.queue.length > 0) {
      const slot = this.slots.find((s) => s.ready && !s.busy);
      if (!slot) return; // a worker finishing will call pump() again
      const age = Date.now() - this.queue[0].enqueuedAt;
      const full = this.queue.length >= config.batchMax;
      if (!full && !this.draining && age < config.batchLingerMs) {
        this.lingerTimer = setTimeout(() => {
          this.lingerTimer = null;
          this.pump();
        }, config.batchLingerMs - age);
        return;
      }
      this.dispatch(slot);
    }
  }

  private dispatch(slot: Slot): void {
    const items = this.queue.splice(0, config.batchMax);
    const batchId = this.nextBatchId++;
    slot.busy = true;
    this.inflight.set(batchId, { items, slot });
    slot.worker.postMessage({
      type: "batch",
      batchId,
      events: items.map((i) => ({ event: i.event, timeReceived: i.timeReceived })),
    });
  }

  private onBatchResult(slot: Slot, m: Exclude<WorkerResponse, { type: "ready" }>): void {
    const b = this.inflight.get(m.batchId);
    slot.busy = false;
    if (!b) return this.pump(); // already requeued by onWorkerExit
    this.inflight.delete(m.batchId);
    this.stats.batches++;
    if (m.type === "done") {
      this.stats.storedEvents += b.items.length;
      this.stats.duplicateKeyHits += m.duplicates;
      this.stats.writeMsTotal += m.writeMs;
      for (const i of b.items) i.resolve(); // <- THE acknowledgement moment
    } else {
      this.stats.failedBatches++;
      console.error(`[pipeline] batch ${m.batchId} failed after retries: ${m.error}`);
      for (const i of b.items) i.reject(new Error("write_failed"));
    }
    this.release(b.items);
    this.pump();
  }

  private release(items: Item[]): void {
    for (const i of items) this.pendingBytes -= i.bytes;
    this.pendingCount -= items.length;
    if (this.pendingCount === 0) {
      for (const w of this.drainWaiters.splice(0)) w();
    }
  }

  // Shutdown step 1: refuse new work and flush partial batches immediately.
  beginDrain(): void {
    this.draining = true;
    this.pump();
  }

  // Shutdown step 2: resolves when every accepted event was acked or failed.
  drained(): Promise<void> {
    return this.pendingCount === 0 ? Promise.resolve() : new Promise((r) => this.drainWaiters.push(r));
  }

  // Shutdown step 3: workers close their Mongo clients and exit.
  async closeWorkers(): Promise<void> {
    this.closing = true;
    const exits = this.slots.map((s) => once(s.worker, "exit"));
    for (const s of this.slots) s.worker.postMessage({ type: "close" });
    const timeout = new Promise((r) => setTimeout(r, 10_000));
    await Promise.race([Promise.all(exits), timeout]);
    await Promise.all(this.slots.map((s) => s.worker.terminate())); // no-op if already gone
  }

  isDraining(): boolean {
    return this.draining;
  }

  snapshot() {
    return {
      pending_events: this.pendingCount,
      pending_bytes: this.pendingBytes,
      queued_events: this.queue.length,
      inflight_batches: this.inflight.size,
      workers: this.slots.length,
      draining: this.draining,
      ...this.stats,
      avg_write_ms: this.stats.batches ? this.stats.writeMsTotal / this.stats.batches : 0,
      avg_batch_size: this.stats.batches ? this.stats.storedEvents / this.stats.batches : 0,
    };
  }
}
