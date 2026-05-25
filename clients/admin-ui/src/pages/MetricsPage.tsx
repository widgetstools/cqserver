import { useQuery } from '@tanstack/react-query';
import { useEffect, useMemo, useRef, useState } from 'react';
import { Pin, PinOff, RefreshCw, Search, Sigma, X } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Sparkline } from '@/components/monitor/Sparkline';
import { adminApi, parsePrometheus, type ParsedMetric } from '@/lib/admin';
import { RingBuffer } from '@/lib/ring-buffer';
import { cn, formatCount, hashStr } from '@/lib/utils';

const POLL_MS = 2_000;
const HISTORY_LEN = 60;
const STORAGE_KEY = 'cqserver-admin-pinned-metrics';

interface PinnedKey {
  /** Stable identifier — metric name + sorted-label-pairs string. */
  id: string;
  name: string;
  labels: Record<string, string>;
}

/** Build a stable `(name, labels) → id` so the same series keeps its
 *  ring-buffer across renders. */
function seriesKey(m: ParsedMetric): string {
  const sortedLabels = Object.entries(m.labels)
    .sort(([a], [b]) => (a < b ? -1 : 1))
    .map(([k, v]) => `${k}=${v}`)
    .join(',');
  return `${m.name}{${sortedLabels}}`;
}

function loadPinned(): PinnedKey[] {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr)) return [];
    return arr;
  } catch {
    return [];
  }
}

export function MetricsPage() {
  const [query, setQuery] = useState('');
  const [pinned, setPinned] = useState<PinnedKey[]>(loadPinned);

  useEffect(() => {
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(pinned));
    } catch {
      // ignore quota / disabled localStorage
    }
  }, [pinned]);

  // Per-series ring buffers. Persist across polls; cleaned up when a
  // metric stops appearing in /metrics.
  const buffers = useRef<Map<string, RingBuffer>>(new Map());
  const [tick, setTick] = useState(0);

  const metricsText = useQuery({
    queryKey: ['metrics'],
    queryFn: adminApi.metricsText,
    refetchInterval: POLL_MS,
  });

  const parsed = useMemo<ParsedMetric[]>(
    () => (metricsText.data ? parsePrometheus(metricsText.data) : []),
    [metricsText.data],
  );

  // Push every observed series into its buffer.
  useEffect(() => {
    if (parsed.length === 0) return;
    const seen = new Set<string>();
    for (const m of parsed) {
      const k = seriesKey(m);
      seen.add(k);
      let buf = buffers.current.get(k);
      if (!buf) {
        buf = new RingBuffer(HISTORY_LEN);
        buffers.current.set(k, buf);
      }
      buf.push(m.value);
    }
    setTick((t) => t + 1);
  }, [parsed]);

  // Index metrics by name → series list for grouping.
  const byName = useMemo(() => {
    const m = new Map<string, ParsedMetric[]>();
    for (const p of parsed) {
      const arr = m.get(p.name);
      if (arr) arr.push(p);
      else m.set(p.name, [p]);
    }
    return m;
  }, [parsed]);

  // Filtered list of metric names.
  const filteredNames = useMemo(() => {
    const names = Array.from(byName.keys()).sort();
    if (!query) return names;
    const q = query.toLowerCase();
    return names.filter((n) => n.toLowerCase().includes(q));
  }, [byName, query]);

  const pinnedSeries = useMemo<Array<{ key: PinnedKey; current: ParsedMetric | undefined }>>(() => {
    return pinned.map((p) => {
      const series = byName.get(p.name) ?? [];
      const current = series.find((s) => seriesKey(s) === p.id);
      return { key: p, current };
    });
  }, [pinned, byName]);

  const togglePin = (m: ParsedMetric) => {
    const id = seriesKey(m);
    setPinned((cur) => {
      if (cur.some((p) => p.id === id)) {
        return cur.filter((p) => p.id !== id);
      }
      return [...cur, { id, name: m.name, labels: m.labels }];
    });
  };

  const isPinned = (m: ParsedMetric) => pinned.some((p) => p.id === seriesKey(m));

  return (
    <div className="px-6 py-5 max-w-[1400px] mx-auto">
      <div className="flex items-baseline justify-between mb-4">
        <div>
          <h1 className="text-[18px] font-semibold tracking-tight leading-none flex items-center gap-2">
            <Sigma size={16} className="text-primary" />
            Metrics
          </h1>
          <p className="text-[11.5px] text-muted-foreground mt-1.5">
            Live Prometheus series. Pin series to the sparkline grid below;
            pins persist across reloads. Use Grafana for PromQL, queries, alerts.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Search
              size={12}
              className="absolute left-2 top-1/2 -translate-y-1/2 text-muted-foreground"
            />
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Filter metrics…"
              className="h-7 w-56 pl-7 pr-2 rounded-md border border-border bg-input text-[12px] font-mono text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
            />
          </div>
          <Button
            variant="secondary"
            size="sm"
            onClick={() => metricsText.refetch()}
            disabled={metricsText.isFetching}
          >
            <RefreshCw size={11} className={metricsText.isFetching ? 'animate-spin' : ''} />
            Refresh
          </Button>
        </div>
      </div>

      {/* Pinned grid */}
      {pinned.length > 0 ? (
        <div className="mb-5">
          <div className="text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground font-medium mb-2 flex items-center gap-1.5">
            <Pin size={10} />
            Pinned ({pinned.length})
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2.5">
            {pinnedSeries.map(({ key, current }) => {
              const buf = buffers.current.get(key.id);
              const series = buf?.toArray() ?? [];
              const labelPairs = Object.entries(key.labels);
              return (
                <Card key={key.id}>
                  <CardHeader className="flex flex-row items-start justify-between pb-2 border-b border-border gap-2">
                    <CardTitle
                      className="font-mono text-[11px] text-foreground normal-case tracking-normal break-all"
                      title={key.id}
                    >
                      {key.name}
                    </CardTitle>
                    <button
                      onClick={() => setPinned((cur) => cur.filter((p) => p.id !== key.id))}
                      title="Unpin"
                      className="text-muted-foreground hover:text-foreground"
                    >
                      <X size={12} />
                    </button>
                  </CardHeader>
                  <CardContent className="pt-3">
                    <div className="font-mono tabular text-[22px] font-semibold leading-none mb-2">
                      {current ? formatCompact(current.value) : '—'}
                    </div>
                    <Sparkline
                      data={series}
                      width={320}
                      height={36}
                      stroke={tintForMetric(key.name)}
                      className="w-full"
                    />
                    {labelPairs.length > 0 ? (
                      <div className="mt-2 flex flex-wrap gap-1">
                        {labelPairs.map(([k, v]) => (
                          <Badge key={k} variant="muted" className="text-[10px] normal-case tracking-normal">
                            <span className="text-muted-foreground/70">{k}=</span>
                            <span className="font-mono">{v}</span>
                          </Badge>
                        ))}
                      </div>
                    ) : null}
                  </CardContent>
                </Card>
              );
            })}
          </div>
        </div>
      ) : null}

      {/* All metrics list */}
      <Card>
        <CardHeader className="flex flex-row items-baseline justify-between pb-2 border-b border-border">
          <CardTitle>
            All series ({formatCount(filteredNames.length)} metric{filteredNames.length === 1 ? '' : 's'},{' '}
            {formatCount(parsed.length)} series)
          </CardTitle>
          <span className="text-[11px] text-muted-foreground font-mono">polling 2s</span>
        </CardHeader>
        <CardContent className="p-0">
          {filteredNames.length === 0 ? (
            <div className="py-10 text-center text-[12px] text-muted-foreground">
              {metricsText.isLoading
                ? 'Loading metrics…'
                : query
                ? `No metric matches "${query}".`
                : 'No metrics emitted yet.'}
            </div>
          ) : (
            <ul>
              {filteredNames.map((name) => {
                const series = byName.get(name) ?? [];
                return (
                  <li key={name} className="border-b border-border last:border-0">
                    <details>
                      <summary className="flex items-baseline justify-between gap-3 px-4 py-2 cursor-pointer hover:bg-accent/30 select-none">
                        <code className="font-mono text-[12px] text-foreground">{name}</code>
                        <span className="font-mono tabular text-[11px] text-muted-foreground shrink-0">
                          {series.length} series
                        </span>
                      </summary>
                      <div className="px-4 pb-3 pt-1">
                        <table className="w-full text-[11.5px]">
                          <tbody>
                            {series.map((s) => {
                              const k = seriesKey(s);
                              const pinnedNow = isPinned(s);
                              return (
                                <tr
                                  key={k}
                                  className="border-t border-border hover:bg-accent/30"
                                >
                                  <td className="py-1.5 pr-3 font-mono text-muted-foreground break-all">
                                    {Object.entries(s.labels).length === 0
                                      ? '·'
                                      : Object.entries(s.labels)
                                          .map(([lk, v]) => `${lk}=${v}`)
                                          .join(' · ')}
                                  </td>
                                  <td className="py-1.5 pr-3 text-right font-mono tabular text-foreground w-28">
                                    {formatCompact(s.value)}
                                  </td>
                                  <td className="py-1.5 w-8 text-right">
                                    <button
                                      onClick={() => togglePin(s)}
                                      className={cn(
                                        'inline-flex items-center justify-center h-5 w-5 rounded-sm transition-colors',
                                        pinnedNow
                                          ? 'text-primary hover:text-foreground'
                                          : 'text-muted-foreground/60 hover:text-primary',
                                      )}
                                      title={pinnedNow ? 'Unpin' : 'Pin to grid above'}
                                    >
                                      {pinnedNow ? <PinOff size={11} /> : <Pin size={11} />}
                                    </button>
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    </details>
                  </li>
                );
              })}
            </ul>
          )}
        </CardContent>
      </Card>

      {/* Hidden marker so React keeps re-rendering on tick. */}
      <span hidden>{tick}</span>
    </div>
  );
}

function formatCompact(v: number): string {
  if (!Number.isFinite(v)) return '—';
  const abs = Math.abs(v);
  if (abs >= 1e9) return `${(v / 1e9).toFixed(2)}G`;
  if (abs >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
  if (Number.isInteger(v)) return formatCount(v);
  return v.toFixed(3);
}

/** Pick a tint based on the metric name hash so different pinned
 *  metrics use distinguishable sparkline colors. */
function tintForMetric(name: string): string {
  const palette = [
    'var(--primary)',
    'var(--ok)',
    'var(--warn)',
    'var(--err)',
    'var(--info)',
  ];
  return palette[Math.abs(hashStr(name)) % palette.length];
}
