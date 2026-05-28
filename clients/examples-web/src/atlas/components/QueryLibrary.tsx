/**
 * QueryLibrary — left rail catalog for Chapter 08. Groups the global
 * query library by feature (Joins / Filters / Aggregations / Pivots /
 * Views / Window Functions); search filters in place. Click an entry
 * to select it — selection drives the SQL editor on the right.
 */
import { useMemo, useState } from 'react';
import {
  QUERIES,
  FEATURE_LABEL,
  FEATURE_ORDER,
  type QueryEntry,
  type QueryFeature,
} from '../scopes/query';

interface QueryLibraryProps {
  selectedId: string;
  onSelect: (q: QueryEntry) => void;
}

export function QueryLibrary({ selectedId, onSelect }: QueryLibraryProps) {
  const [filter, setFilter] = useState('');
  const filtered = useMemo<QueryEntry[]>(() => {
    const needle = filter.trim().toLowerCase();
    if (!needle) return QUERIES;
    return QUERIES.filter((q) =>
      q.title.toLowerCase().includes(needle) ||
      q.synopsis.toLowerCase().includes(needle) ||
      q.sql.toLowerCase().includes(needle),
    );
  }, [filter]);

  const groups = useMemo(() => {
    const byFeature = new Map<QueryFeature, QueryEntry[]>();
    for (const q of filtered) {
      const arr = byFeature.get(q.feature) ?? [];
      arr.push(q);
      byFeature.set(q.feature, arr);
    }
    return FEATURE_ORDER
      .map((f) => ({ feature: f, entries: byFeature.get(f) ?? [] }))
      .filter((g) => g.entries.length > 0);
  }, [filtered]);

  return (
    <aside
      style={{
        position: 'relative',
        zIndex: 1,
        width: 280,
        minWidth: 280,
        display: 'flex',
        flexDirection: 'column',
        borderRight: '1px solid var(--atlas-rule)',
        minHeight: 0,
      }}
    >
      <div
        style={{
          padding: '14px 16px 10px',
          borderBottom: '1px solid var(--atlas-rule)',
        }}
      >
        <div
          style={{
            fontSize: 10,
            letterSpacing: '.22em',
            color: 'var(--atlas-fg-dim)',
            paddingBottom: 8,
          }}
        >
          QUERY LIBRARY · {QUERIES.length}
        </div>
        <input
          type="text"
          placeholder="search…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          style={{
            width: '100%',
            background: 'transparent',
            border: '1px solid var(--atlas-rule)',
            color: 'var(--atlas-fg)',
            fontFamily: 'var(--atlas-font)',
            fontSize: 11,
            padding: '6px 8px',
            outline: 'none',
          }}
        />
      </div>
      <div style={{ flex: 1, overflow: 'auto', padding: '8px 0' }}>
        {groups.map((g) => (
          <div key={g.feature} style={{ padding: '6px 0' }}>
            <div
              style={{
                padding: '6px 16px',
                fontSize: 10,
                letterSpacing: '.18em',
                color: 'var(--atlas-amber)',
              }}
            >
              {FEATURE_LABEL[g.feature]}
            </div>
            {g.entries.map((q) => {
              const selected = q.id === selectedId;
              return (
                <button
                  key={q.id}
                  onClick={() => onSelect(q)}
                  style={{
                    display: 'block',
                    width: '100%',
                    textAlign: 'left',
                    background: selected ? 'var(--atlas-amber-soft)' : 'transparent',
                    border: 'none',
                    borderLeft: selected
                      ? '2px solid var(--atlas-amber)'
                      : '2px solid transparent',
                    padding: '6px 14px',
                    color: selected ? 'var(--atlas-amber)' : 'var(--atlas-fg)',
                    fontFamily: 'var(--atlas-font)',
                    fontSize: 11,
                    cursor: 'pointer',
                  }}
                  title={q.synopsis}
                >
                  {q.title}
                </button>
              );
            })}
          </div>
        ))}
        {groups.length === 0 && (
          <div
            style={{
              padding: '12px 16px',
              fontSize: 10,
              color: 'var(--atlas-fg-faint)',
            }}
          >
            no matches
          </div>
        )}
      </div>
    </aside>
  );
}
