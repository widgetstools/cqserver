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
}
