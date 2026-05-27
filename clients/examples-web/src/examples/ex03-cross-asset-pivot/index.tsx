import { useMemo, useState } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { GridPanel } from '@/components/panels/GridPanel';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { useFilteredSubscription } from '@/lib/use-filtered-subscription';
import { POSITION_COLUMNS } from '@/lib/schema/positions';
import { buildColDefs, defaultPositionView } from '@/lib/grid-cols';
import { fmtCcy, fmtSigned } from '@/lib/format';
import { DOCS_BY_ID } from '@/docs';
import { cn } from '@/lib/utils';

type Measure = 'market_value_usd' | 'unrealized_pnl_usd' | 'var_1d_95' | 'exposure_gross';

const MEASURES: { id: Measure; label: string; signed: boolean }[] = [
  { id: 'market_value_usd',   label: 'Market Value USD', signed: true },
  { id: 'unrealized_pnl_usd', label: 'Unrealized PnL',   signed: true },
  { id: 'var_1d_95',          label: 'VaR 1d 95%',       signed: false },
  { id: 'exposure_gross',     label: 'Gross Exposure',   signed: false },
];

interface PivotCell {
  row: string;
  col: string;
  v: number;
  n: number;
}

/**
 * Arrange long-form aggregate rows from /v_cross_asset_pivot into the
 * 2D grid the table expects. This is presentation only — every number
 * already came from cqserver's incremental aggregation, this just
 * picks the selected measure column and indexes by (row, col).
 */
function arrangeViewIntoPivot(
  rows: Record<string, unknown>[],
  measure: Measure,
): {
  rows: string[];
  cols: string[];
  cells: Map<string, PivotCell>;
  rowTotals: Map<string, number>;
  colTotals: Map<string, number>;
  grand: number;
} {
  const rowSet = new Set<string>();
  const colSet = new Set<string>();
  const cells = new Map<string, PivotCell>();
  const rowT = new Map<string, number>();
  const colT = new Map<string, number>();
  let grand = 0;

  for (const r of rows) {
    const rk = String(r.asset_class ?? '—');
    const ck = String(r.currency ?? '—');
    const vRaw = r[measure];
    const v = typeof vRaw === 'number' ? vRaw : 0;
    const n = typeof r.n_positions === 'number' ? r.n_positions : 0;
    rowSet.add(rk);
    colSet.add(ck);
    cells.set(`${rk}::${ck}`, { row: rk, col: ck, v, n });
    rowT.set(rk, (rowT.get(rk) ?? 0) + v);
    colT.set(ck, (colT.get(ck) ?? 0) + v);
    grand += v;
  }
  return {
    rows: Array.from(rowSet).sort(),
    cols: Array.from(colSet).sort(),
    cells,
    rowTotals: rowT,
    colTotals: colT,
    grand,
  };
}

export function CrossAssetPivotCanvas() {
  // The pivot table reads from a long-form aggregate view —
  // /v_cross_asset_pivot — that re-materializes whenever any
  // position's measure column changes. The 2D arrangement on the
  // client is presentation, not aggregation.
  const pivotSub = useFilteredSubscription('/v_cross_asset_pivot', null);
  const colDefs = useMemo(() => buildColDefs(POSITION_COLUMNS), []);
  const visible = useMemo(() => defaultPositionView(), []);

  const [measure, setMeasure] = useState<Measure>('market_value_usd');
  const [selected, setSelected] = useState<{ row?: string; col?: string }>({});

  const pivot = useMemo(
    () => arrangeViewIntoPivot(pivotSub.rows as Record<string, unknown>[], measure),
    [pivotSub.rows, measure],
  );
  const measureDef = MEASURES.find((m) => m.id === measure)!;

  // Max-abs for the heatmap fill scale.
  const maxAbs = useMemo(() => {
    let m = 0;
    for (const c of pivot.cells.values()) m = Math.max(m, Math.abs(c.v));
    return m || 1;
  }, [pivot.cells]);

  // Drill-through is a server-side filter over /positions. The
  // predicate composes from the pivot's selected (row, col); empty
  // selection means no predicate and a full /positions stream. Each
  // selection change tears down the prior sub and opens a new one,
  // so cqserver's filter compiler is doing the work — no React
  // post-filtering, no full-table scan on the client.
  const drillFilter = useMemo<string | null>(() => {
    const clauses: string[] = [];
    if (selected.row) clauses.push(`asset_class = '${selected.row.replace(/'/g, "''")}'`);
    if (selected.col) clauses.push(`currency = '${selected.col.replace(/'/g, "''")}'`);
    return clauses.length ? clauses.join(' AND ') : null;
  }, [selected]);
  const drillSub = useFilteredSubscription('/positions', drillFilter);
  const drillthrough = drillSub.rows;

  const pivotSql = `-- Pivot source (declared in cqserver.toml)
SELECT asset_class, currency,
       SUM(market_value_usd)   AS market_value_usd,
       SUM(unrealized_pnl_usd) AS unrealized_pnl_usd,
       SUM(var_1d_95)          AS var_1d_95,
       SUM(exposure_gross)     AS exposure_gross,
       COUNT(*)                AS n_positions
FROM positions
GROUP BY asset_class, currency;

-- Drill-through (per-component server filter on /positions)
SELECT *
FROM positions
${drillFilter ? `WHERE ${drillFilter}` : '-- (no filter — full stream)'};

-- Active measure on the pivot table: ${measure}`;

  const panels: DockPanelSpec[] = [
    {
      id: 'pivot',
      title: 'Pivot · asset_class × currency',
      render: () => (
        <PanelChrome
          title="Pivot · asset_class × currency"
          right={
            <div className="flex items-center gap-1.5">
              <span className="atlas-eyebrow !text-[9px]">Measure</span>
              <select
                value={measure}
                onChange={(e) => setMeasure(e.target.value as Measure)}
                className="text-[10px] font-mono bg-card border border-border h-5 px-1 rounded-sm text-foreground focus:outline-none focus:ring-1 focus:ring-ring"
              >
                {MEASURES.map((m) => (
                  <option key={m.id} value={m.id}>{m.label}</option>
                ))}
              </select>
            </div>
          }
        >
          <div className="p-3 overflow-auto">
            <table className="border-collapse text-[11px]">
              <thead>
                <tr>
                  <th className="text-left p-1.5 tracked-tight font-mono uppercase text-muted-foreground border-b border-border">
                    Asset Class ↓ \ Currency →
                  </th>
                  {pivot.cols.map((c) => (
                    <th
                      key={c}
                      onClick={() => setSelected((s) => ({ ...s, col: s.col === c ? undefined : c }))}
                      className={cn(
                        'text-right p-1.5 font-mono uppercase tracking-[0.06em] cursor-pointer border-b border-border min-w-[90px]',
                        selected.col === c ? 'text-signal' : 'text-muted-foreground hover:text-foreground',
                      )}
                    >
                      {c}
                    </th>
                  ))}
                  <th className="text-right p-1.5 tracked-tight font-mono uppercase text-foreground border-b border-border min-w-[110px]">
                    TOTAL
                  </th>
                </tr>
              </thead>
              <tbody>
                {pivot.rows.map((r) => (
                  <tr key={r}>
                    <td
                      onClick={() => setSelected((s) => ({ ...s, row: s.row === r ? undefined : r }))}
                      className={cn(
                        'p-1.5 cursor-pointer border-b border-data-grid-line',
                        selected.row === r ? 'text-signal font-semibold' : 'text-foreground',
                      )}
                    >
                      {r}
                    </td>
                    {pivot.cols.map((c) => {
                      const k = `${r}::${c}`;
                      const cell = pivot.cells.get(k);
                      const v = cell?.v ?? 0;
                      const n = cell?.n ?? 0;
                      const ratio = Math.abs(v) / maxAbs;
                      const bg = v === 0
                        ? 'transparent'
                        : v > 0
                          ? `color-mix(in oklab, var(--ok) ${(ratio * 65).toFixed(0)}%, transparent)`
                          : `color-mix(in oklab, var(--err) ${(ratio * 65).toFixed(0)}%, transparent)`;
                      const isSel = selected.row === r && selected.col === c;
                      return (
                        <td
                          key={c}
                          onClick={() => setSelected({ row: r, col: c })}
                          className={cn(
                            'p-1.5 text-right font-mono tabular cursor-pointer border-b border-data-grid-line',
                            isSel ? 'ring-1 ring-inset ring-signal' : '',
                          )}
                          style={{ backgroundColor: bg, transition: 'background-color 600ms ease' }}
                          title={`${r} · ${c}: ${fmtSigned(v)} · ${n} rows`}
                        >
                          {n === 0 ? <span className="text-muted-foreground">·</span> : (
                            <>
                              <div>{measureDef.signed ? fmtSigned(v) : fmtCcy(v, '', 0)}</div>
                              <div className="text-[8.5px] text-muted-foreground">{n}</div>
                            </>
                          )}
                        </td>
                      );
                    })}
                    <td className="p-1.5 text-right font-mono tabular border-b border-data-grid-line font-semibold">
                      {fmtSigned(pivot.rowTotals.get(r) ?? 0)}
                    </td>
                  </tr>
                ))}
                <tr className="bg-muted">
                  <td className="p-1.5 tracked-tight font-mono uppercase">TOTAL</td>
                  {pivot.cols.map((c) => (
                    <td key={c} className="p-1.5 text-right font-mono tabular font-semibold">
                      {fmtSigned(pivot.colTotals.get(c) ?? 0)}
                    </td>
                  ))}
                  <td className="p-1.5 text-right font-mono tabular font-bold text-foreground">
                    {fmtSigned(pivot.grand)}
                  </td>
                </tr>
              </tbody>
            </table>
            <div className="mt-3 atlas-eyebrow">Selected · {selected.row ?? '* '}× {selected.col ?? '*'}</div>
          </div>
        </PanelChrome>
      ),
    },
    {
      id: 'detail',
      title: `Drill-through · ${drillthrough.length} positions`,
      render: () => (
        <GridPanel
          title="Drill-through Positions"
          colDefs={colDefs}
          visible={visible}
          getRowId={(r) => r.position_id as string}
          liveSubscription={drillSub}
        />
      ),
    },
    {
      id: 'sql',
      title: 'SQL · pivot',
      pin: 'right',
      render: () => <SqlPanel title="Pivot SQL" value={pivotSql} readOnly planSummary={`PIVOT · ${pivot.rows.length} × ${pivot.cols.length} = ${pivot.cells.size} cells`} />,
    },
    {
      id: 'notes',
      title: 'Help · ex03.md',
      pin: 'right',
      render: () => <MarkdownPanel title="Help · ex03.md" filename="ex03.md" source={DOCS_BY_ID['cross-asset-pivot']} />,
    },
  ];

  const layout: DockLayoutStep[] = [
    { id: 'pivot' },
    { id: 'sql', relativeTo: 'pivot', direction: 'right' },
    { id: 'detail', relativeTo: 'pivot', direction: 'below' },
    { id: 'notes', relativeTo: 'detail', direction: 'right' },
  ];

  return <DockSurface panels={panels} layout={layout} />;
}
