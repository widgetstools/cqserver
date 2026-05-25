// Mulberry32 — a small, fast, fully-deterministic PRNG. We seed it
// once from a known constant so every load produces the same data
// across reloads, screenshots, and CI — important for examples.

export class Rng {
  private s: number;
  constructor(seed: number) {
    this.s = seed >>> 0;
  }
  next(): number {
    let t = (this.s += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }
  /** Uniform float in [lo, hi). */
  uniform(lo: number, hi: number): number {
    return lo + (hi - lo) * this.next();
  }
  /** Integer in [lo, hi]. */
  int(lo: number, hi: number): number {
    return Math.floor(this.uniform(lo, hi + 1));
  }
  /** Pick one element. */
  pick<T>(arr: readonly T[]): T {
    return arr[Math.floor(this.next() * arr.length)]!;
  }
  /** Standard normal via Box-Muller. */
  normal(mean = 0, std = 1): number {
    const u1 = Math.max(this.next(), Number.EPSILON);
    const u2 = this.next();
    return mean + std * Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
  }
  /** Returns true with probability p. */
  chance(p: number): boolean {
    return this.next() < p;
  }
}
