import { useEffect, useRef, useState } from 'react';
import { RingBuffer } from './ring-buffer';

/**
 * Push a new sample into a ring buffer on every render where `value`
 * changes (typically every TanStack Query poll). Returns a stable
 * snapshot array suitable for sparkline rendering.
 *
 * `capacity` is the number of historical samples retained.
 */
export function useSeries(value: number | undefined, capacity = 60): number[] {
  const bufRef = useRef<RingBuffer | null>(null);
  if (bufRef.current == null) bufRef.current = new RingBuffer(capacity);
  const [, force] = useState(0);

  useEffect(() => {
    if (value == null || !Number.isFinite(value)) return;
    bufRef.current!.push(value);
    force((n) => n + 1);
  }, [value]);

  return bufRef.current!.toArray();
}

/**
 * For Prometheus cumulative counters: push raw counter samples, then
 * return both the cumulative series and a derived per-second rate
 * series suitable for sparklines.
 */
export function useCumulativeSeries(
  value: number | undefined,
  intervalSec: number,
  capacity = 60,
): { cumulative: number[]; ratePerSec: number } {
  const bufRef = useRef<RingBuffer | null>(null);
  if (bufRef.current == null) bufRef.current = new RingBuffer(capacity);
  const [, force] = useState(0);

  useEffect(() => {
    if (value == null || !Number.isFinite(value)) return;
    bufRef.current!.push(value);
    force((n) => n + 1);
  }, [value]);

  return {
    cumulative: bufRef.current!.toArray(),
    ratePerSec: bufRef.current!.rateFromCumulative(intervalSec),
  };
}
