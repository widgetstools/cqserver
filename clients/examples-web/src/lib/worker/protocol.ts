/**
 * Typed message protocol shared by the main thread and the
 * cqserver SharedWorker. Both directions are JSON-serialisable
 * (no transferables yet — Phase 2 keeps everything plain Row).
 *
 * The worker is the source of truth: it owns the WebSocket, the
 * `Client`, every reference-counted subscription, and the coalesce
 * buffer. The main thread holds React hooks that mirror per-port
 * state derived from `ServerMsg`s.
 */
export type Row = Record<string, unknown>;

export type ConnectionStatus =
  | 'connecting'
  | 'snapshotting'
  | 'live'
  | 'disconnected'
  | 'error';

/** Main → Worker. */
export type ClientMsg =
  | { kind: 'hello'; tabId: string }
  | {
      kind: 'subscribe';
      subId: string;
      topic: string;
      filter?: string;
      /** Inline SQL — mutually exclusive with `filter`. */
      sql?: string;
    }
  | { kind: 'unsubscribe'; subId: string }
  | {
      /**
       * One-shot SQL fetch (JOINs / derived tables). Unlike `subscribe`,
       * this routes through the server's one-shot SOW path and streams no
       * live deltas. The hub replies with `snapshot` chunks tagged with
       * `reqId` (used as `subId`) and a terminating `more:false`, or an
       * `error` tagged with `reqId`.
       */
      kind: 'sow';
      reqId: string;
      topic: string;
      sql: string;
    }
  | { kind: 'ping' };

/** Worker → Main. */
export type ServerMsg =
  | { kind: 'hello-ack'; sharedWorker: boolean }
  | { kind: 'connected' }
  | { kind: 'disconnected'; reason?: string }
  | { kind: 'status'; subId: string; status: ConnectionStatus }
  | {
      kind: 'snapshot';
      subId: string;
      /** This chunk of the SOW. */
      chunk: Row[];
      /** False on the final chunk; true while more chunks are pending. */
      more: boolean;
    }
  | {
      kind: 'delta';
      subId: string;
      /** Coalesced over the last ~50ms. Lists are stable per message. */
      add: Row[];
      update: Row[];
      remove: Row[];
    }
  | { kind: 'error'; subId?: string; message: string }
  | { kind: 'pong' };

/** Chunk size for progressive SOW delivery — keeps each postMessage cheap. */
export const SOW_CHUNK_ROWS = 500;

/** Coalesce window for live deltas (ms). Matches the legacy COALESCE_MS. */
export const COALESCE_MS = 50;
