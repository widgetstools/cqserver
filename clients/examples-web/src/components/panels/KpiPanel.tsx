import { PanelChrome } from './PanelChrome';
import { fmtCcy, fmtSigned, fmtPct } from '@/lib/format';
import { cn } from '@/lib/utils';

export interface Kpi {
  label: string;
  value: number;
  /** Display kind. */
  kind: 'ccy' | 'pct' | 'signed-ccy' | 'count';
  /** Optional delta vs prev period (raw value, same units). */
  delta?: number;
  /** Optional sub-line (e.g. "since open"). */
  sub?: string;
}

interface KpiPanelProps {
  title: string;
  kpis: Kpi[];
  /** Layout — columns × rows in the grid. */
  cols?: number;
}

function valueStr(k: Kpi): string {
  switch (k.kind) {
    case 'ccy': return fmtCcy(k.value, 'USD', 0);
    case 'pct': return fmtPct(k.value, 2);
    case 'signed-ccy': return fmtSigned(k.value);
    case 'count': return k.value.toLocaleString();
  }
}

function deltaStr(k: Kpi): string | null {
  if (k.delta == null) return null;
  switch (k.kind) {
    case 'ccy':
    case 'signed-ccy': return fmtSigned(k.delta);
    case 'pct': return `${k.delta >= 0 ? '+' : ''}${k.delta.toFixed(2)}%`;
    case 'count': return k.delta >= 0 ? `+${k.delta}` : `${k.delta}`;
  }
}

export function KpiPanel({ title, kpis, cols = 2 }: KpiPanelProps) {
  return (
    <PanelChrome title={title}>
      <div
        className="p-3 grid gap-3"
        style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}
      >
        {kpis.map((k, i) => {
          const d = deltaStr(k);
          const up = (k.delta ?? 0) >= 0;
          return (
            <div key={i} className="kpi fade-up" style={{ animationDelay: `${i * 40}ms` }}>
              <div className="kpi-label">{k.label}</div>
              <div className="kpi-value">{valueStr(k)}</div>
              {d ? (
                <div className={cn('kpi-delta', up ? 'up' : 'down')}>
                  {up ? '▲' : '▼'} {d}{k.sub ? <span className="ml-1 text-muted-foreground/80">· {k.sub}</span> : null}
                </div>
              ) : k.sub ? (
                <div className="text-[10.5px] font-mono text-muted-foreground/80 mt-0.5">{k.sub}</div>
              ) : null}
            </div>
          );
        })}
      </div>
    </PanelChrome>
  );
}
