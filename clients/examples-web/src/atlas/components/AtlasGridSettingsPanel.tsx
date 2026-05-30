/**
 * Custom AG Grid side-bar panel — column visibility, grouping, quick actions.
 * Scrollable body with sticky section headers so long schemas stay usable.
 */
import { useCallback, useEffect, useMemo, useState } from 'react';
import type { Column, GridApi } from 'ag-grid-community';

export interface AtlasGridSettingsPanelProps {
  api: GridApi;
}

type ColumnGroupId =
  | 'identity'
  | 'classification'
  | 'market'
  | 'pnl'
  | 'risk'
  | 'time'
  | 'other';

const GROUP_META: Record<
  ColumnGroupId,
  { label: string; hint: string }
> = {
  identity: { label: 'Identity & keys', hint: 'ids · book · symbol · status' },
  classification: { label: 'Classification', hint: 'sector · asset · region · ccy' },
  market: { label: 'Market & size', hint: 'price · qty · exposure · notional' },
  pnl: { label: 'PnL', hint: 'day · ytd · realized · unrealized' },
  risk: { label: 'Risk', hint: 'var · dv01 · greek · util' },
  time: { label: 'Time', hint: 'dates · timestamps' },
  other: { label: 'Other', hint: 'remaining fields' },
};

const GROUP_ORDER: ColumnGroupId[] = [
  'identity',
  'classification',
  'market',
  'pnl',
  'risk',
  'time',
  'other',
];

function columnGroup(field: string): ColumnGroupId {
  const f = field.toLowerCase();
  if (
    /(^|_)id$|^id$|position_id|trade_id|book|symbol|cusip|trader|status|compliance|key/.test(f)
  ) {
    return 'identity';
  }
  if (/sector|asset|region|currency|country|industry|class|issuer/.test(f)) {
    return 'classification';
  }
  if (/price|quantity|qty|market_value|exposure|notional|mv|volume|size|rate/.test(f)) {
    return 'market';
  }
  if (/pnl|profit|loss|day_|ytd|mtd|realized|unrealized|return/.test(f)) {
    return 'pnl';
  }
  if (/var|dv01|delta|beta|gamma|vega|util|risk|stress|limit/.test(f)) {
    return 'risk';
  }
  if (/date|time|timestamp|created|updated|asof|settle/.test(f)) {
    return 'time';
  }
  return 'other';
}

function columnLabel(col: Column): string {
  const def = col.getColDef();
  return String(def.headerName ?? def.field ?? col.getColId());
}

function columnField(col: Column): string {
  return col.getColDef().field ?? col.getColId();
}

interface ColumnEntry {
  col: Column;
  field: string;
  label: string;
  visible: boolean;
}

export function AtlasGridSettingsPanel({ api }: AtlasGridSettingsPanelProps) {
  const [query, setQuery] = useState('');
  const [collapsed, setCollapsed] = useState<Partial<Record<ColumnGroupId, boolean>>>({});
  const [version, bump] = useState(0);
  const refresh = useCallback(() => bump((n) => n + 1), []);

  useEffect(() => {
    if (!api) return;
    const events = [
      'columnVisible',
      'columnMoved',
      'columnPinned',
      'newColumnsLoaded',
      'gridColumnsChanged',
    ] as const;
    for (const event of events) api.addEventListener(event, refresh);
    return () => {
      for (const event of events) api.removeEventListener(event, refresh);
    };
  }, [api, refresh]);

  const entries = useMemo<ColumnEntry[]>(() => {
    if (!api) return [];
    return (api.getColumns() ?? [])
      .map((col) => ({
        col,
        field: columnField(col),
        label: columnLabel(col),
        visible: col.isVisible(),
      }))
      .filter((e) => e.field.length > 0);
  }, [api, version]);

  const needle = query.trim().toLowerCase();

  const filtered = useMemo(() => {
    if (!needle) return entries;
    return entries.filter(
      (e) =>
        e.label.toLowerCase().includes(needle) ||
        e.field.toLowerCase().includes(needle),
    );
  }, [entries, needle]);

  const grouped = useMemo(() => {
    const map = new Map<ColumnGroupId, ColumnEntry[]>();
    for (const id of GROUP_ORDER) map.set(id, []);
    for (const entry of filtered) {
      const bucket = map.get(columnGroup(entry.field)) ?? map.get('other')!;
      bucket.push(entry);
    }
    for (const [, list] of map) {
      list.sort((a, b) => a.label.localeCompare(b.label));
    }
    return GROUP_ORDER.map((id) => ({ id, items: map.get(id) ?? [] })).filter(
      (g) => g.items.length > 0,
    );
  }, [filtered]);

  const visibleCount = entries.filter((e) => e.visible).length;

  const setVisible = (fields: string[], visible: boolean) => {
    if (fields.length === 0) return;
    api.setColumnsVisible(fields, visible);
  };

  const toggleGroup = (id: ColumnGroupId) => {
    setCollapsed((prev) => ({ ...prev, [id]: !prev[id] }));
  };

  return (
    <div className="atlas-grid-settings">
      <div className="atlas-grid-settings__header">
        <div className="atlas-grid-settings__title">Grid settings</div>
        <div className="atlas-grid-settings__meta">
          {visibleCount} / {entries.length} visible
        </div>
        <input
          type="search"
          className="atlas-grid-settings__search"
          placeholder="Filter columns…"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <div className="atlas-grid-settings__actions">
          <button type="button" onClick={() => setVisible(entries.map((e) => e.field), true)}>
            Show all
          </button>
          <button type="button" onClick={() => setVisible(entries.map((e) => e.field), false)}>
            Hide all
          </button>
          <button
            type="button"
            onClick={() => {
              api.resetColumnState();
              refresh();
            }}
          >
            Reset layout
          </button>
        </div>
      </div>

      <div className="atlas-grid-settings__scroll">
        {grouped.length === 0 ? (
          <div className="atlas-grid-settings__empty">
            {entries.length === 0 ? 'No columns yet — run a query or wait for data.' : 'No columns match your filter.'}
          </div>
        ) : (
          grouped.map(({ id, items }) => {
            const isCollapsed = collapsed[id] === true;
            const shown = items.filter((i) => i.visible).length;
            return (
              <section key={id} className="atlas-grid-settings__section">
                <button
                  type="button"
                  className="atlas-grid-settings__section-head"
                  onClick={() => toggleGroup(id)}
                >
                  <span className="atlas-grid-settings__section-chevron">{isCollapsed ? '▸' : '▾'}</span>
                  <span className="atlas-grid-settings__section-label">{GROUP_META[id].label}</span>
                  <span className="atlas-grid-settings__section-count">
                    {shown}/{items.length}
                  </span>
                </button>
                {!isCollapsed ? (
                  <>
                    <div className="atlas-grid-settings__section-hint">{GROUP_META[id].hint}</div>
                    <div className="atlas-grid-settings__section-actions">
                      <button type="button" onClick={() => setVisible(items.map((i) => i.field), true)}>
                        All
                      </button>
                      <button type="button" onClick={() => setVisible(items.map((i) => i.field), false)}>
                        None
                      </button>
                    </div>
                    <ul className="atlas-grid-settings__list">
                      {items.map((item) => (
                        <li key={item.field}>
                          <label className="atlas-grid-settings__row">
                            <input
                              type="checkbox"
                              checked={item.visible}
                              onChange={(e) => setVisible([item.field], e.target.checked)}
                            />
                            <span className="atlas-grid-settings__row-label" title={item.field}>
                              {item.label}
                            </span>
                            <span className="atlas-grid-settings__row-field">{item.field}</span>
                          </label>
                        </li>
                      ))}
                    </ul>
                  </>
                ) : null}
              </section>
            );
          })
        )}
      </div>

      <div className="atlas-grid-settings__footer">
        <span>Drag headers to reorder · pin from column menu</span>
      </div>
    </div>
  );
}
