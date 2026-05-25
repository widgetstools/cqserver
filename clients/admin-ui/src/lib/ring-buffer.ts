/**
 * Fixed-size ring buffer of numeric samples. Used by sparklines and
 * per-second rate derivation from successive cumulative counters.
 *
 * Push order is FIFO; `toArray()` returns oldest-first for chart libs.
 */
export class RingBuffer {
  private buf: number[];
  private idx = 0;
  private filled = false;

  constructor(public readonly capacity: number) {
    this.buf = new Array<number>(capacity);
  }

  push(v: number): void {
    this.buf[this.idx] = v;
    this.idx = (this.idx + 1) % this.capacity;
    if (this.idx === 0) this.filled = true;
  }

  size(): number {
    return this.filled ? this.capacity : this.idx;
  }

  /** Latest pushed value, or `undefined` if empty. */
  last(): number | undefined {
    if (!this.filled && this.idx === 0) return undefined;
    const i = (this.idx - 1 + this.capacity) % this.capacity;
    return this.buf[i];
  }

  /** Oldest-first copy of the populated portion. */
  toArray(): number[] {
    if (!this.filled) return this.buf.slice(0, this.idx);
    return [
      ...this.buf.slice(this.idx),
      ...this.buf.slice(0, this.idx),
    ];
  }

  /** Derive per-second rate from two cumulative samples N seconds apart. */
  rateFromCumulative(intervalSec: number): number {
    const arr = this.toArray();
    if (arr.length < 2 || intervalSec <= 0) return 0;
    const span = arr.length - 1;
    const delta = arr[arr.length - 1] - arr[0];
    return delta / (span * intervalSec);
  }
}
