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

export function HeatmapPanel({ title, data, ticking = false, tooltipExtra }: HeatmapPanelProps) {
  const [tick, setTick] = useState(0);
  const flashRef = useRef<Map<string, number>>(new Map());

  const { rows, cols, byKey } = useMemo(() => {
    const r = Array.from(new Set(data.map((d) => d.row)));
    const c = Array.from(new Set(data.map((d) => d.col)));
    const m = new Map<string, HeatmapDatum>();
    for (const d of data) m.set(`${d.row}${d.col}`, d);
    return { rows: r, cols: c, byKey: m };
  }, [data]);

  // Synthesize live ticks: each interval, mutate a random subset of
  // cells by a small Δ. The cell's data-bucket flips and the 600ms
  // background transition kicks in. We also mark cells that switched
  // buckets so the .flash outline animates.
  const [live, setLive] = useState(data);
  useEffect(() => {
    if (!ticking) {
      setLive(data);
      return;
    }
    setLive(data);
    const id = setInterval(() => {
      setLive((prev) => {
        const next = prev.slice();
        const flashes: string[] = [];
        const n = Math.max(3, Math.floor(prev.length * 0.18));
        for (let i = 0; i < n; i++) {
          const ix = Math.floor(Math.random() * next.length);
          const cur = next[ix]!;
          const delta = (Math.random() - 0.48) * 0.6;
          const before = bucketOf(cur.value);
          const updated = { ...cur, value: Math.max(-6, Math.min(6, cur.value + delta)) };
          const after = bucketOf(updated.value);
          next[ix] = updated;
          if (before !== after) flashes.push(`${updated.row}${updated.col}`);
        }
        if (flashes.length) {
          const now = performance.now();
          for (const k of flashes) flashRef.current.set(k, now);
        }
        return next;
      });
      setTick((t) => t + 1);
    }, 900);
    return () => clearInterval(id);
  }, [data, ticking]);

  const flatMin = useMemo(() => Math.min(...live.map((d) => d.value)), [live]);
  const flatMax = useMemo(() => Math.max(...live.map((d) => d.value)), [live]);

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
            {rows.map((r) => {
              const usedCols = byKey;
              void usedCols;
              return (
                <tr key={r}>
                  <td className="text-[10.5px] font-medium pr-2 text-foreground truncate" style={{ maxWidth: 110 }}>
                    {r}
                  </td>
                  {cols.map((c) => {
                    const k = `${r}${c}`;
                    const d = live.find((x) => x.row === r && x.col === c);
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
              );
            })}
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
