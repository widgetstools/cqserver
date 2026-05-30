/**
 * SectorLadder — ranked horizontal bar list with a centred axis.
 *
 * Reads an aggregate view (one row per band) and renders each band as
 * a bar that grows right of the midline when positive (amber) and left
 * of the midline when negative (red). Rows sorted descending by value
 * and capped at `limit`, mirroring the legacy ex01 sector PnL ladder.
 *
 * Bar widths animate at 600ms so live updates from cqserver settle
 * visibly rather than snap.
 */
import { useMemo } from 'react';
import type { Row } from '@/lib/use-subscription';

interface SectorLadderProps {
  title: string;
  rows: Row[];
  /** Field on each row that names the band (e.g. `issuer_sector`). */
  labelKey: string;
  /** Field whose number drives the bar length + direction. */
  valueKey: string;
  /** Cap on rendered rows (sorted desc by value first). */
  limit?: number;
  /** Right-side value formatter, e.g. `fmtSignedMillions`. */
  format: (n: number) => string;
}

export function SectorLadder({
  title,
  rows,
  labelKey,
  valueKey,
  limit = 14,
  format,
}: SectorLadderProps) {
  const bars = useMemo(() => {
    return rows
      .map((r) => ({
        label: String(r[labelKey] ?? ''),
        value: Number(r[valueKey] ?? 0),
      }))
      .filter((b) => b.label)
      .sort((a, b) => b.value - a.value)
      .slice(0, limit);
  }, [rows, labelKey, valueKey, limit]);

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
        borderBottom: '1px solid var(--atlas-rule)',
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
          gap: 5,
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
                    width: 110,
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
                    height: 8,
                    display: 'flex',
                    background: 'var(--atlas-surface)',
                    borderLeft: '1px solid var(--atlas-rule-soft)',
                    borderRight: '1px solid var(--atlas-rule-soft)',
                  }}
                >
                  <div
                    style={{
                      width: '50%',
                      display: 'flex',
                      justifyContent: 'flex-end',
                    }}
                  >
                    {!positive && (
                      <div
                        style={{
                          height: '100%',
                          width: `${pct * 100}%`,
                          background: 'var(--atlas-neg-bar)',
                          transition: 'width 600ms ease',
                        }}
                      />
                    )}
                  </div>
                  <div style={{ width: '50%', display: 'flex' }}>
                    {positive && (
                      <div
                        style={{
                          height: '100%',
                          width: `${pct * 100}%`,
                          background: 'var(--atlas-pos-bar)',
                          transition: 'width 600ms ease',
                        }}
                      />
                    )}
                  </div>
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
