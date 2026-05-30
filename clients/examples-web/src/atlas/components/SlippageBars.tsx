/**
 * SlippageBars — horizontal bar list of (venue × algo) buckets ranked
 * by absolute slippage. Positive slip (we paid more than arrival, a
 * loss) renders right of the centred axis in red; negative slip
 * (favourable execution) renders left in amber. The trade count for
 * each bucket is annotated alongside the bps value so the eye can
 * weigh small-but-loud buckets against big-but-quiet ones.
 *
 * Used by SlipChapter beside the data grid to give the chapter the
 * "slippage-vs-trade-count" visual the legacy spec asked for.
 */
import { useMemo } from 'react';
import type { Row } from '@/lib/use-subscription';

interface SlippageBarsProps {
  title: string;
  rows: Row[];
  /** Field that names each bucket — typically `execution_venue`. */
  venueKey: string;
  /** Secondary key on each bucket — typically `execution_algo`. */
  algoKey: string;
  /** Field whose number is the bps slippage. */
  slipKey: string;
  /** Field whose number is the trade count per bucket. */
  countKey: string;
  limit?: number;
}

interface SlipRow {
  label: string;
  slip: number;
  count: number;
}

export function SlippageBars({
  title,
  rows,
  venueKey,
  algoKey,
  slipKey,
  countKey,
  limit = 16,
}: SlippageBarsProps) {
  const buckets = useMemo<SlipRow[]>(() => {
    return rows
      .map((r) => {
        const venue = String(r[venueKey] ?? '');
        const algo = String(r[algoKey] ?? '');
        return {
          label: venue && algo ? `${venue} · ${algo}` : venue || algo,
          slip: Number(r[slipKey] ?? 0),
          count: Number(r[countKey] ?? 0),
        };
      })
      .filter((b) => b.label && Number.isFinite(b.slip))
      .sort((a, b) => Math.abs(b.slip) - Math.abs(a.slip))
      .slice(0, limit);
  }, [rows, venueKey, algoKey, slipKey, countKey, limit]);

  const maxSlip = useMemo(
    () => Math.max(1, ...buckets.map((b) => Math.abs(b.slip))),
    [buckets],
  );

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
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          paddingBottom: 12,
        }}
      >
        <div style={{ fontSize: 11, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)' }}>{title}</div>
        <div style={{ fontSize: 10, color: 'var(--atlas-fg-faint)' }}>
          {buckets.length} of {rows.length} buckets · sorted by |slip|
        </div>
      </div>
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          gap: 6,
          overflow: 'auto',
          flex: 1,
          minHeight: 0,
        }}
      >
        {buckets.length === 0 ? (
          <div style={{ fontSize: 10, color: 'var(--atlas-fg-faint)' }}>—</div>
        ) : (
          buckets.map((b) => {
            const pct = Math.abs(b.slip) / maxSlip;
            const negative = b.slip > 0;
            return (
              <div
                key={b.label}
                style={{ display: 'flex', alignItems: 'center', gap: 10 }}
              >
                <span
                  style={{
                    fontSize: 10.5,
                    color: 'var(--atlas-fg)',
                    width: 170,
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
                  <div style={{ width: '50%', display: 'flex', justifyContent: 'flex-end' }}>
                    {!negative && (
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
                  <div style={{ width: '50%', display: 'flex' }}>
                    {negative && (
                      <div
                        style={{
                          height: '100%',
                          width: `${pct * 100}%`,
                          background: 'var(--atlas-neg-bar-strong)',
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
                    width: 78,
                    textAlign: 'right',
                    color: negative ? 'var(--atlas-neg)' : 'var(--atlas-amber)',
                  }}
                >
                  {b.slip.toFixed(1)} bps
                </span>
                <span
                  style={{
                    fontSize: 9.5,
                    fontFeatureSettings: '"tnum"',
                    width: 84,
                    textAlign: 'right',
                    color: 'var(--atlas-fg-faint)',
                  }}
                >
                  n={b.count.toLocaleString('en-US')}
                </span>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
