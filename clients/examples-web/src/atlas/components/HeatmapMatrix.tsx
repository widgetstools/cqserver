/**
 * HeatmapMatrix — sector × region color-encoded matrix. Each cell is
 * coloured by its value's sign + magnitude (amber for gains, red for
 * losses; intensity ∝ |value| / max). Ticks land via `subscribeDeltas`
 * so each cell pulses on change.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import type { SubscriptionHandle, Row } from '@/lib/use-subscription';
import { cellValuesEqual } from '../aggridLive';
import { useAtlasTheme } from '../theme/ThemeContext';

/** Heat cell RGB — light mode uses a more saturated ramp than dark. */
const HEAT_RGB = {
  dark: { pos: '255, 87, 34', neg: '255, 96, 98' },
  light: { pos: '234, 88, 12', neg: '255, 59, 72' },
} as const;

interface HeatmapMatrixProps {
  title?: string;
  status?: string;
  liveSubscription: SubscriptionHandle;
  /** Field on each row that names the cell's sector (Y axis). */
  rowKey: string;
  /** Field on each row that names the cell's region (X axis). */
  colKey: string;
  /** Field whose value drives the cell colour. */
  valueKey: string;
  /** Optional field for the secondary label inside each cell. */
  countKey?: string;
}

/** Cell pulse duration (ms) — kept in sync with AG Grid flash timing. */
const HEAT_FLASH_MS = 300;

interface Cell {
  row: string;
  col: string;
  value: number;
  count: number;
}

export function HeatmapMatrix({
  title,
  status,
  liveSubscription,
  rowKey,
  colKey,
  valueKey,
  countKey,
}: HeatmapMatrixProps) {
  const { theme } = useAtlasTheme();
  const heatRgb = HEAT_RGB[theme];
  // Live snapshot — pulled from the worker-backed Sub via the same
  // pattern DataTable uses (seed once from getSnapshot, then merge
  // deltas in place).
  const [cells, setCells] = useState<Map<string, Cell>>(() => new Map());
  const flashRef = useRef<Map<string, number>>(new Map());
  const [flashTick, setFlashTick] = useState(0);
  const subRef = useRef<SubscriptionHandle | null>(null);

  useEffect(() => {
    // Identity change → reset.
    if (subRef.current !== liveSubscription) {
      subRef.current = liveSubscription;
      setCells(new Map());
      flashRef.current.clear();
    }
    const ingest = (rows: Row[], mark: boolean) => {
      let flashed = false;
      setCells((prev) => {
        const next = new Map(prev);
        for (const r of rows) {
          const row = String(r[rowKey] ?? '');
          const col = String(r[colKey] ?? '');
          if (!row || !col) continue;
          const key = `${row}|${col}`;
          const value = Number(r[valueKey] ?? 0);
          const count = countKey ? Number(r[countKey] ?? 0) : 0;
          const existing = prev.get(key);
          const changed =
            !existing ||
            !cellValuesEqual(existing.value, value) ||
            !cellValuesEqual(existing.count, count);
          next.set(key, { row, col, value, count });
          if (mark && changed) {
            flashRef.current.set(key, Date.now());
            flashed = true;
          }
        }
        return next;
      });
      if (flashed) setFlashTick((t) => t + 1);
    };

    // Initial seed.
    const snap = liveSubscription.getSnapshot();
    if (snap.length > 0) ingest(snap, false);

    const offChunks = liveSubscription.subscribeSnapshotChunks((chunk) => ingest(chunk, false));
    const offDeltas = liveSubscription.subscribeDeltas((b) => {
      ingest(b.add, true);
      ingest(b.update, true);
      if (b.remove.length > 0) {
        setCells((prev) => {
          const next = new Map(prev);
          for (const r of b.remove) {
            const row = String(r[rowKey] ?? '');
            const col = String(r[colKey] ?? '');
            next.delete(`${row}|${col}`);
          }
          return next;
        });
      }
    });
    return () => {
      offChunks();
      offDeltas();
    };
  }, [liveSubscription, rowKey, colKey, valueKey, countKey]);

  // Decay the flash highlights — re-render every 200ms so faded cells
  // settle back to their base colour.
  useEffect(() => {
    const id = setInterval(() => setFlashTick((t) => t + 1), 250);
    return () => clearInterval(id);
  }, []);

  const { rowLabels, colLabels, max } = useMemo(() => {
    const rs = new Set<string>();
    const cs = new Set<string>();
    let m = 0;
    for (const c of cells.values()) {
      rs.add(c.row);
      cs.add(c.col);
      const abs = Math.abs(c.value);
      if (abs > m) m = abs;
    }
    return {
      rowLabels: Array.from(rs).sort(),
      colLabels: Array.from(cs).sort(),
      max: m || 1,
    };
  }, [cells]);

  // flashTick is read so React re-runs this on each tick; we don't
  // actually use the value.
  void flashTick;

  const now = Date.now();

  return (
    <div
      style={{
        position: 'relative',
        zIndex: 1,
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        minHeight: 0,
        padding: '18px 24px 12px',
      }}
    >
      {(title || status) && (
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            paddingBottom: 12,
          }}
        >
          {title ? (
            <div style={{ fontSize: 11, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)' }}>{title}</div>
          ) : (
            <div />
          )}
          {status ? <div style={{ fontSize: 10, color: 'var(--atlas-fg-faint)' }}>{status}</div> : null}
        </div>
      )}
      <div style={{ flex: 1, minHeight: 280, overflow: 'auto' }}>
        <table
          style={{
            borderCollapse: 'separate',
            borderSpacing: 2,
            fontSize: 10,
            fontFamily: 'var(--atlas-font)',
            color: 'var(--atlas-fg)',
          }}
        >
          <thead>
            <tr>
              <th
                style={{
                  position: 'sticky',
                  left: 0,
                  background: 'var(--atlas-ink)',
                  textAlign: 'right',
                  padding: '6px 10px',
                  fontWeight: 500,
                  color: 'var(--atlas-fg-faint)',
                  letterSpacing: '.18em',
                }}
              />
              {colLabels.map((c) => (
                <th
                  key={c}
                  style={{
                    padding: '6px 8px',
                    fontWeight: 500,
                    color: 'var(--atlas-fg-faint)',
                    letterSpacing: '.14em',
                    textAlign: 'center',
                    minWidth: 80,
                  }}
                >
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rowLabels.map((r) => (
              <tr key={r}>
                <th
                  style={{
                    position: 'sticky',
                    left: 0,
                    background: 'var(--atlas-ink)',
                    textAlign: 'right',
                    padding: '6px 12px 6px 0',
                    fontWeight: 500,
                    color: 'var(--atlas-fg)',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {r}
                </th>
                {colLabels.map((c) => {
                  const cell = cells.get(`${r}|${c}`);
                  if (!cell) {
                    return (
                      <td
                        key={c}
                        style={{
                          background: 'var(--atlas-cell-empty)',
                          padding: '8px',
                          minWidth: 80,
                          height: 38,
                        }}
                      />
                    );
                  }
                  const intensity = Math.min(1, Math.abs(cell.value) / max);
                  const baseMin = theme === 'light' ? 0.16 : 0.12;
                  const baseSpan = theme === 'light' ? 0.62 : 0.55;
                  const baseColor = cell.value >= 0
                    ? `rgba(${heatRgb.pos}, ${(baseMin + intensity * baseSpan).toFixed(3)})`
                    : `rgba(${heatRgb.neg}, ${(baseMin + intensity * baseSpan).toFixed(3)})`;
                  const flashedAt = flashRef.current.get(`${r}|${c}`) ?? 0;
                  const sinceFlash = now - flashedAt;
                  const flashing = sinceFlash < HEAT_FLASH_MS;
                  const flashColor = flashing
                    ? `rgba(${heatRgb.pos}, ${(0.88 - sinceFlash / HEAT_FLASH_MS * 0.88).toFixed(3)})`
                    : baseColor;
                  return (
                    <td
                      key={c}
                      title={`${r} · ${c}\n${valueKey} = ${cell.value.toExponential(2)}${countKey ? `\n${countKey} = ${cell.count}` : ''}`}
                      style={{
                        background: flashing ? flashColor : baseColor,
                        padding: '6px 8px',
                        minWidth: 80,
                        height: 38,
                        textAlign: 'right',
                        fontFeatureSettings: '"tnum"',
                        transition: flashing ? undefined : `background ${HEAT_FLASH_MS}ms ease`,
                        color: intensity > 0.4 ? 'var(--atlas-on-heat)' : 'var(--atlas-fg)',
                      }}
                    >
                      {fmtCellValue(cell.value)}
                      {countKey && cell.count > 0 ? (
                        <div style={{ fontSize: 8.5, opacity: 0.7, marginTop: 2 }}>n={cell.count}</div>
                      ) : null}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function fmtCellValue(n: number): string {
  if (!Number.isFinite(n)) return '—';
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `${n >= 0 ? '' : '−'}${(abs / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${n >= 0 ? '' : '−'}${(abs / 1_000).toFixed(1)}k`;
  if (abs >= 1) return n.toFixed(2);
  return n.toFixed(3);
}
