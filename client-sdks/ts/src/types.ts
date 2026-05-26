export type DeltaKind = 'add' | 'update' | 'remove' | 'oof';

export interface Delta {
  deltaType: DeltaKind;
  subId: string;
  sequence?: number;
  data: Record<string, unknown>;
}

export class ClientError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ClientError';
  }
}

/** Wire shape mirrors the Rust `CqMessage` struct's serde renames. */
export interface CqMessage {
  c: string;
  cid?: string;
  t?: string;
  sid?: string;
  f?: string;
  o?: string;
  a?: string;
  s?: string;
  r?: string;
  d?: unknown;
  n?: number;
  ts?: number;
  dt?: string;
  seq?: number;
  bm?: number;
  /**
   * Inline raw SQL. When set on `sow` / `sow_and_subscribe`, the
   * server evaluates this as the verbatim query (overriding any
   * filter/options-derived SQL) — required for aggregate /
   * GROUP BY queries that don't fit the projection-only options
   * string. The FROM table name is rewritten server-side so the
   * caller can write `FROM positions` (or anything) freely.
   */
  sql?: string;
  /** Q2 — `publish_batch` payload (array of records). */
  bat?: Array<Record<string, unknown>>;
  /** Q2 — `publish_batch` ack: assigned per-row sequences. */
  seqs?: number[];
}
