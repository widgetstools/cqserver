/**
 * BookBars — single-axis horizontal bar chart for per-book PnL.
 *
 * Bars extend right from a left anchor; the colour signals sign
 * (amber for positive contribution, red for negative). Sorted descending
 * by value so the top of the list is the biggest contributor.
 *
 * Used by PulseChapter to surface /v_pnl_by_book contributions alongside
 * the sector ladder. Animates the same 600ms width transition so deltas
 * from cqserver settle smoothly.
 */
import { useMemo } from 'react';
import type { Row } from '@/lib/use-subscription';

interface BookBarsProps {
  title: string;
  rows: Row[];
  labelKey: string;
  valueKey: string;
  format: (n: number) => string;
}

export function BookBars({
  title,
  rows,
  labelKey,
  valueKey,
  format,
}: BookBarsProps) {
  const bars = useMemo(() => {
    return rows
      .map((r) => ({
        label: String(r[labelKey] ?? ''),
        value: Number(r[valueKey] ?? 0),
      }))
      .filter((b) => b.label)
      .sort((a, b) => b.value - a.value);
  }, [rows, labelKey, valueKey]);

  const max = useMemo(
    () => Math.max(1, ...bars.map((b) => Math.abs(b.value))),
    [bars],
  );

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        minHeight: 0,
        padding: '14px 18px',
        flex: 1,
      }}
    >
      <div
        style={{
          fontSize: 11,
          letterSpacing: '.22em',
          color: 'var(--atlas-fg-dim)',
          paddingBottom: 10,
        }}
      >
        {title}
      </div>
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          gap: 7,
          overflow: 'auto',
          flex: 1,
          minHeight: 0,
        }}
      >
        {bars.length === 0 ? (
          <div style={{ fontSize: 10, color: 'var(--atlas-fg-faint)' }}>—</div>
        ) : (
          bars.map((b) => {
            const pct = Math.abs(b.value) / max;
            const positive = b.value >= 0;
            return (
              <div
                key={b.label}
                style={{ display: 'flex', alignItems: 'center', gap: 10 }}
              >
                <span
                  style={{
                    fontSize: 10.5,
                    color: 'var(--atlas-fg)',
                    width: 130,
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {b.label}
                </span>
                <div
                  style={{
                    flex: 1,
                    height: 6,
                    background: 'var(--atlas-surface)',
                    overflow: 'hidden',
                  }}
                >
                  <div
                    style={{
                      height: '100%',
                      width: `${pct * 100}%`,
                      background: positive
                        ? 'rgba(244, 165, 43, .65)'
                        : 'rgba(255, 96, 98, .65)',
                      transition: 'width 600ms ease',
                    }}
                  />
                </div>
                <span
                  style={{
                    fontSize: 10.5,
                    fontFeatureSettings: '"tnum"',
                    width: 84,
                    textAlign: 'right',
                    color: positive ? 'var(--atlas-amber)' : 'var(--atlas-neg)',
                  }}
                >
                  {format(b.value)}
                </span>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
