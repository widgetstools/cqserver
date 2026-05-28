export interface Kpi {
  label: string;
  value: string;
  caption?: string;
  /** Apply amber colour to the value when true (default false). */
  emphasis?: boolean;
}

interface KpiStripProps {
  kpis: readonly Kpi[];
}

export function KpiStrip({ kpis }: KpiStripProps) {
  return (
    <div
      style={{
        position: 'relative',
        zIndex: 1,
        display: 'grid',
        gridTemplateColumns: `repeat(${kpis.length}, 1fr)`,
        borderBottom: '1px solid var(--atlas-rule)',
      }}
    >
      {kpis.map((k, i) => (
        <div
          key={k.label}
          style={{
            padding: '14px 16px',
            borderRight: i < kpis.length - 1 ? '1px solid var(--atlas-rule)' : 'none',
          }}
        >
          <div style={{ fontSize: 9, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)' }}>{k.label}</div>
          <div
            style={{
              fontSize: 22,
              fontWeight: 600,
              marginTop: 6,
              fontFeatureSettings: '"tnum"',
              color: k.emphasis ? 'var(--atlas-amber)' : 'var(--atlas-fg)',
            }}
          >
            {k.value}
          </div>
          {k.caption ? (
            <div style={{ fontSize: 10, color: 'var(--atlas-fg-faint)', marginTop: 4 }}>{k.caption}</div>
          ) : null}
        </div>
      ))}
    </div>
  );
}
