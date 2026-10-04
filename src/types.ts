// Messages crossing the main-thread <-> worker boundary. Only plain data
// travels (structured clone); callbacks stay in the main thread, keyed by batchId.

export type RawEvent = Record<string, unknown>;

export interface BatchRequest {
  type: "batch";
  batchId: number;
  events: { event: RawEvent; timeReceived: number }[];
}
export interface CloseRequest {
  type: "close";
}
export type WorkerRequest = BatchRequest | CloseRequest;

export type WorkerResponse =
  | { type: "ready" }
  | { type: "done"; batchId: number; duplicates: number; writeMs: number }
  | { type: "failed"; batchId: number; error: string };
