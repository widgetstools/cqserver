import { useMemo, useState } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { GridPanel } from '@/components/panels/GridPanel';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { getPositions } from '@/lib/data-gen';
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

function buildPivot(positions: Record<string, unknown>[], measure: Measure): {
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

  for (const p of positions) {
    const r = String(p.asset_class ?? '—');
    const c = String(p.currency ?? '—');
    const vRaw = p[measure];
    const v = typeof vRaw === 'number' ? vRaw : 0;
    rowSet.add(r);
    colSet.add(c);
    const k = `${r}::${c}`;
    const cur = cells.get(k);
    if (cur) {
      cur.v += v;
      cur.n += 1;
    } else {
      cells.set(k, { row: r, col: c, v, n: 1 });
    }
    rowT.set(r, (rowT.get(r) ?? 0) + v);
    colT.set(c, (colT.get(c) ?? 0) + v);
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
  const positions = useMemo(() => getPositions(), []);
  const colDefs = useMemo(() => buildColDefs(POSITION_COLUMNS), []);
  const visible = useMemo(() => defaultPositionView(), []);

  const [measure, setMeasure] = useState<Measure>('market_value_usd');
  const [selected, setSelected] = useState<{ row?: string; col?: string }>({});

  const pivot = useMemo(() => buildPivot(positions as Record<string, unknown>[], measure), [positions, measure]);
  const measureDef = MEASURES.find((m) => m.id === measure)!;

  // Max-abs for the heatmap fill scale.
  const maxAbs = useMemo(() => {
    let m = 0;
    for (const c of pivot.cells.values()) m = Math.max(m, Math.abs(c.v));
    return m || 1;
  }, [pivot.cells]);

  const drillthrough = useMemo(() => {
    if (!selected.row && !selected.col) return positions;
    return positions.filter((p) => {
      if (selected.row && p.asset_class !== selected.row) return false;
      if (selected.col && p.currency !== selected.col) return false;
      return true;
    });
  }, [positions, selected]);

  const pivotSql = `SELECT asset_class, currency,
       SUM(${measure}) AS measure,
       COUNT(*)       AS n
FROM positions
${selected.row || selected.col ? `WHERE 1=1${selected.row ? ` AND asset_class = '${selected.row}'` : ''}${selected.col ? ` AND currency = '${selected.col}'` : ''}` : ''}
GROUP BY asset_class, currency
PIVOT (currency);`;

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
          rows={drillthrough as Record<string, unknown>[]}
          colDefs={colDefs}
          visible={visible}
        />
      ),
    },
    {
      id: 'sql',
      title: 'Pivot SQL',
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
