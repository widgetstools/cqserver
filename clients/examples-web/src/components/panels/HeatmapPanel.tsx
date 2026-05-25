import { useEffect, useMemo, useRef, useState } from 'react';
import { PanelChrome } from './PanelChrome';
import { Badge } from '@/components/ui/badge';
import { fmtPct, fmtCcy } from '@/lib/format';

export interface HeatmapDatum {
  row: string;   // y-axis label
  col: string;   // x-axis label
  value: number; // % change in [-something, +something]
  weight?: number;
}

interface HeatmapPanelProps {
  title: string;
  data: HeatmapDatum[];
  ticking?: boolean;
  /** Returns extra info to show in the cell tooltip. */
  tooltipExtra?: (d: HeatmapDatum) => string;
}

function bucketOf(v: number): -3 | -2 | -1 | 0 | 1 | 2 | 3 {
  if (v <= -3) return -3;
  if (v <= -1.5) return -2;
  if (v <= -0.3) return -1;
  if (v < 0.3) return 0;
  if (v < 1.5) return 1;
  if (v < 3) return 2;
  return 3;
}

/**
 * HeatmapPanel — follows its `data` prop directly. When ticking is
 * enabled and `data` changes, we diff previous-vs-current buckets
 * per cell and stamp a flash timestamp on cells that crossed a
 * bucket boundary. The visual fade between background colors comes
 * from the 600ms CSS transition on `.heatmap-cell`; the flash
 * outline (`.flash`) is added for ~650ms after a bucket crossing.
 */
export function HeatmapPanel({ title, data, ticking = false, tooltipExtra }: HeatmapPanelProps) {
  const [tick, setTick] = useState(0);
  const prevBucketsRef = useRef<Map<string, number>>(new Map());
  const flashRef = useRef<Map<string, number>>(new Map());

  const { rows, cols } = useMemo(() => {
    const r = Array.from(new Set(data.map((d) => d.row)));
    const c = Array.from(new Set(data.map((d) => d.col)));
    return { rows: r, cols: c };
  }, [data]);

  // Whenever `data` changes, diff bucket-of each cell against the
  // previous snapshot. Cells that crossed a bucket get a flash mark.
  useEffect(() => {
    if (!ticking) {
      prevBucketsRef.current = new Map(data.map((d) => [`${d.row}::${d.col}`, bucketOf(d.value)]));
      return;
    }
    const now = performance.now();
    const prev = prevBucketsRef.current;
    const next = new Map<string, number>();
    for (const d of data) {
      const k = `${d.row}::${d.col}`;
      const b = bucketOf(d.value);
      next.set(k, b);
      const old = prev.get(k);
      if (old !== undefined && old !== b) flashRef.current.set(k, now);
    }
    prevBucketsRef.current = next;
    setTick((t) => t + 1);
  }, [data, ticking]);

  const flatMin = useMemo(() => Math.min(...data.map((d) => d.value)), [data]);
  const flatMax = useMemo(() => Math.max(...data.map((d) => d.value)), [data]);

  const now = performance.now();

  return (
    <PanelChrome
      title={title}
      right={
        <div className="flex items-center gap-2">
          {ticking ? <Badge variant="signal" className="!text-[9px]">LIVE</Badge> : null}
          <span className="font-mono text-[10px] text-muted-foreground">
            {fmtPct(flatMin)} · {fmtPct(flatMax)}
          </span>
          <span className="font-mono text-[9px] text-muted-foreground">t={tick}</span>
        </div>
      }
    >
      <div className="p-3 h-full overflow-auto">
        <table className="border-collapse" style={{ tableLayout: 'fixed' }}>
          <thead>
            <tr>
              <th className="w-[110px] text-left text-[9.5px] font-mono uppercase tracking-[0.1em] text-muted-foreground p-0 pb-2 pl-1">
                Sector ↓ · Region →
              </th>
              {cols.map((c) => (
                <th
                  key={c}
                  className="w-[78px] text-[9.5px] font-mono uppercase tracking-[0.08em] text-muted-foreground p-0 pb-2 text-center"
                >
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r}>
                <td className="text-[10.5px] font-medium pr-2 text-foreground truncate" style={{ maxWidth: 110 }}>
                  {r}
                </td>
                {cols.map((c) => {
                  const k = `${r}::${c}`;
                  const d = data.find((x) => x.row === r && x.col === c);
                  if (!d) {
                    return (
                      <td key={c} className="p-0">
                        <div className="heatmap-cell" data-bucket="0" style={{ height: 42, width: 76, margin: 1 }}>
                          —
                        </div>
                      </td>
                    );
                  }
                  const b = bucketOf(d.value);
                  const lastFlash = flashRef.current.get(k) ?? -Infinity;
                  const isFlashing = now - lastFlash < 650;
                  const tip = `${d.row} · ${d.col}: ${fmtPct(d.value)}${
                    tooltipExtra ? ` · ${tooltipExtra(d)}` : ''
                  }`;
                  return (
                    <td key={c} className="p-0">
                      <div
                        className={`heatmap-cell ${isFlashing ? 'flash' : ''}`}
                        data-bucket={b}
                        title={tip}
                        style={{ height: 42, width: 76, margin: 1 }}
                      >
                        {d.value >= 0 ? '+' : ''}{d.value.toFixed(2)}
                      </div>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
        <div className="mt-4 flex items-center gap-2 text-[10px] font-mono text-muted-foreground">
          <span className="tracked-tight">Scale</span>
          <div className="flex items-center gap-[2px]">
            {([-3, -2, -1, 0, 1, 2, 3] as const).map((b) => (
              <div
                key={b}
                className="heatmap-cell"
                data-bucket={b}
                style={{ width: 28, height: 16, fontSize: 9 }}
              >
                {b === 0 ? '≈0' : b < 0 ? `${b}` : `+${b}`}
              </div>
            ))}
          </div>
          <span className="ml-2">% intraday return · diverging</span>
          <span className="ml-auto">
            min {fmtCcy(flatMin * 1_000_000, '', 0)} · max {fmtCcy(flatMax * 1_000_000, '', 0)}
          </span>
        </div>
      </div>
    </PanelChrome>
  );
}
