import { useMemo, useState } from 'react';
import { EXAMPLES, type ExampleEntry } from '@/examples/registry';
import type { ExampleId } from '@/examples/shared';
import { Input } from '@/components/ui/input';
import { Search } from 'lucide-react';

interface AtlasIndexProps {
  active: ExampleId;
  onSelect: (id: ExampleId) => void;
}

const CATEGORY_ORDER: ExampleEntry['category'][] = ['live', 'analytics', 'reference', 'lab'];
const CATEGORY_LABEL: Record<ExampleEntry['category'], string> = {
  live: 'Live',
  analytics: 'Analytics',
  reference: 'Reference',
  lab: 'Lab',
};

export function AtlasIndex({ active, onSelect }: AtlasIndexProps) {
  const [q, setQ] = useState('');

  const matches = useMemo(() => {
    if (!q.trim()) return EXAMPLES;
    const needle = q.toLowerCase();
    return EXAMPLES.filter(
      (e) =>
        e.title.toLowerCase().includes(needle) ||
        e.synopsis.toLowerCase().includes(needle) ||
        e.features.some((f) => f.includes(needle)) ||
        e.id.includes(needle),
    );
  }, [q]);

  return (
    <aside className="w-[260px] shrink-0 border-r border-border bg-card flex flex-col">
      <div className="px-4 py-4 border-b border-border">
        <div className="atlas-eyebrow">
          <span className="dot">●</span> ATLAS · {EXAMPLES.length} EXAMPLES
        </div>
        <div className="text-[11px] text-muted-foreground mt-1.5 leading-snug">
          A field guide of cqserver patterns over a 200-column
          positions + trades dataset.
        </div>
      </div>

      <div className="px-3 py-2 border-b border-border">
        <div className="relative">
          <Search
            size={11}
            className="absolute left-2 top-1/2 -translate-y-1/2 text-muted-foreground"
          />
          <Input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Filter examples or features…"
            className="pl-6 h-7 text-[11.5px]"
          />
        </div>
      </div>

      <div className="flex-1 overflow-y-auto py-2">
        {CATEGORY_ORDER.map((cat) => {
          const group = matches.filter((e) => e.category === cat);
          if (!group.length) return null;
          return (
            <div key={cat} className="mb-2">
              <div className="px-4 pb-1.5 pt-2 atlas-eyebrow !text-[9.5px]">
                {CATEGORY_LABEL[cat]} · {group.length}
              </div>
              <div>
                {group.map((e) => (
                  <div
                    key={e.id}
                    className="atlas-row"
                    data-active={e.id === active}
                    onClick={() => onSelect(e.id)}
                  >
                    <span className="atlas-row-num">
                      {e.ord.toString().padStart(2, '0')}
                    </span>
                    <div className="min-w-0">
                      <div className="atlas-row-label truncate">
                        {e.title.replace(/^.+— /, '')}
                      </div>
                      <div className="atlas-row-sub truncate">{e.synopsis}</div>
                      <div className="atlas-row-tags">
                        {e.features.slice(0, 4).map((f) => (
                          <span key={f} className="feature-tag" data-kind={f}>
                            {f}
                          </span>
                        ))}
                      </div>
                    </div>
                    <e.icon size={12} className="text-muted-foreground" />
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>

      <div className="border-t border-border px-4 py-2.5 flex items-center justify-between">
        <span className="atlas-eyebrow">v0.1.0</span>
        <span className="font-mono text-[9.5px] text-muted-foreground">
          cqserver · ATLAS
        </span>
      </div>
    </aside>
  );
}
