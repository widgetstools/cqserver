import { useMemo, useState } from 'react';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { Badge } from '@/components/ui/badge';
import { ChevronDown, ChevronRight, RefreshCw, Table2, Layers } from 'lucide-react';
import { useCatalog } from '@/lib/use-catalog';
import type { CatalogEntry } from '@/lib/catalog';

interface CatalogPanelProps {
  /** Insert a token (table name sans slash, or a column name) into the
   *  SQL editor at the caller's discretion. */
  onInsert: (token: string) => void;
}

/** Strip the leading slash so a catalog name like `/positions` inserts
 *  as the bare `positions` the builder's FROM clause expects. */
function tableToken(name: string): string {
  return name.replace(/^\//, '');
}

export function CatalogPanel({ onInsert }: CatalogPanelProps) {
  const { entries, loading, error, refresh } = useCatalog();
  const [open, setOpen] = useState<Set<string>>(new Set());

  const { topics, views } = useMemo(() => {
    const topics: CatalogEntry[] = [];
    const views: CatalogEntry[] = [];
    for (const e of entries) (e.kind === 'view' ? views : topics).push(e);
    const byName = (a: CatalogEntry, b: CatalogEntry) => a.name.localeCompare(b.name);
    topics.sort(byName);
    views.sort(byName);
    return { topics, views };
  }, [entries]);

  const toggle = (name: string) =>
    setOpen((s) => {
      const n = new Set(s);
      if (n.has(name)) n.delete(name);
      else n.add(name);
      return n;
    });

  const renderEntry = (e: CatalogEntry) => {
    const isOpen = open.has(e.name);
    return (
      <div key={e.name} className="mb-0.5">
        <div className="flex items-center gap-1 px-2 py-1 group">
          <button
            onClick={() => toggle(e.name)}
            className="text-muted-foreground hover:text-foreground"
            aria-label={isOpen ? 'collapse' : 'expand'}
          >
            {isOpen ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
          </button>
          <button
            onClick={() => onInsert(tableToken(e.name))}
            className="flex-1 flex items-center gap-1.5 text-left text-[11.5px] font-medium text-foreground hover:text-accent-foreground"
            title={`Insert ${tableToken(e.name)}`}
          >
            {e.kind === 'view' ? <Layers size={11} /> : <Table2 size={11} />}
            <span className="truncate">{tableToken(e.name)}</span>
            <span className="ml-auto font-mono text-[9px] text-muted-foreground">
              {e.columns.length}
            </span>
          </button>
        </div>
        {isOpen ? (
          <div className="ml-5 border-l border-border pl-2">
            {e.columns.map((c) => (
              <button
                key={c.name}
                onClick={() => onInsert(c.name)}
                className="w-full flex items-center gap-1.5 px-1 py-0.5 text-left text-[11px] text-muted-foreground hover:text-accent-foreground hover:bg-accent rounded-sm"
                title={`Insert ${c.name}`}
              >
                <span className="truncate">{c.name}</span>
                <span className="ml-auto font-mono text-[9px] text-muted-foreground/70">
                  {c.type}
                </span>
              </button>
            ))}
          </div>
        ) : null}
      </div>
    );
  };

  return (
    <PanelChrome
      title="Schema Catalog"
      right={
        <button
          onClick={refresh}
          className="flex items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground"
          title="Refresh catalog"
        >
          <RefreshCw size={11} />
          refresh
        </button>
      }
    >
      <div className="overflow-y-auto py-1 h-full">
        {error ? (
          <div className="p-3 text-[11px]">
            <Badge variant="err" className="!text-[9px]">unavailable</Badge>
            <p className="text-muted-foreground mt-2 leading-relaxed">
              Couldn't reach the cqserver admin catalog ({error}). Is cqserver
              running and the dev proxy pointing at its admin port?
            </p>
          </div>
        ) : loading ? (
          <div className="p-3 text-[11px] text-muted-foreground">Loading catalog…</div>
        ) : (
          <>
            <div className="px-3 pt-1 pb-0.5 text-[10px] font-mono uppercase tracking-[0.1em] text-muted-foreground">
              Topics <span className="ml-1">{topics.length}</span>
            </div>
            {topics.map(renderEntry)}
            <div className="px-3 pt-2 pb-0.5 text-[10px] font-mono uppercase tracking-[0.1em] text-muted-foreground">
              Views <span className="ml-1">{views.length}</span>
            </div>
            {views.map(renderEntry)}
          </>
        )}
      </div>
    </PanelChrome>
  );
}
