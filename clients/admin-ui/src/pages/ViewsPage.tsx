import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import { ArrowUpRight, Eye, Plus, RefreshCw } from 'lucide-react';
import { Link } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { adminApi, type TopicInfo, type ViewInfo } from '@/lib/admin';
import { formatCount } from '@/lib/utils';

export function ViewsPage() {
  const [selected, setSelected] = useState<string | null>(null);

  const views = useQuery({
    queryKey: ['views'],
    queryFn: adminApi.views,
    refetchInterval: 10_000,
  });
  const topics = useQuery({
    queryKey: ['topics'],
    queryFn: adminApi.topics,
    refetchInterval: 5_000,
  });

  const list: ViewInfo[] = views.data ?? [];
  const topicByName: Map<string, TopicInfo> = useMemo(() => {
    const m = new Map<string, TopicInfo>();
    for (const t of topics.data ?? []) m.set(t.name, t);
    return m;
  }, [topics.data]);

  const activeView = useMemo(() => {
    if (!selected) return list[0] ?? null;
    return list.find((v) => v.name === selected) ?? null;
  }, [list, selected]);

  return (
    <div className="px-6 py-5 max-w-[1400px] mx-auto">
      <div className="flex items-baseline justify-between mb-4">
        <div>
          <h1 className="text-[18px] font-semibold tracking-tight leading-none flex items-center gap-2">
            <Eye size={16} className="text-primary" />
            Views
          </h1>
          <p className="text-[11.5px] text-muted-foreground mt-1.5">
            Materialized continuous queries. Each view is a topic whose
            rows are the aggregate output, maintained incrementally as
            its source mutates.
          </p>
        </div>
        <Button
          variant="secondary"
          size="sm"
          onClick={() => {
            views.refetch();
            topics.refetch();
          }}
          disabled={views.isFetching}
        >
          <RefreshCw size={11} className={views.isFetching ? 'animate-spin' : ''} />
          Refresh
        </Button>
      </div>

      <CreateViewForm />

      {list.length === 0 ? (
        <Card>
          <CardContent className="py-10 text-center text-[12px] text-muted-foreground">
            {views.isLoading
              ? 'Loading views…'
              : 'No views yet. Use "New view" above to create one, or declare a [[views]] block in cqserver.toml.'}
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-12 gap-2.5">
          {/* List */}
          <Card className="col-span-12 lg:col-span-5">
            <CardHeader className="pb-2 border-b border-border">
              <CardTitle>Registered views ({formatCount(list.length)})</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              <ul>
                {list.map((v) => {
                  const t = topicByName.get(v.name);
                  const isActive = activeView?.name === v.name;
                  return (
                    <li key={v.name}>
                      <button
                        onClick={() => setSelected(v.name)}
                        className={`w-full text-left flex items-baseline justify-between gap-3 px-4 py-2.5 border-b border-border last:border-0 transition-colors ${
                          isActive ? 'bg-info-muted' : 'hover:bg-accent/50'
                        }`}
                      >
                        <div className="flex flex-col gap-0.5 min-w-0">
                          <span className="font-mono text-[12.5px] text-foreground truncate">
                            {v.name}
                          </span>
                          <span className="text-[10.5px] text-muted-foreground">
                            from <code className="font-mono">{v.source}</code>
                          </span>
                        </div>
                        <span className="font-mono tabular text-[12px] text-muted-foreground shrink-0">
                          {t ? formatCount(t.rowCount) : '—'} rows
                        </span>
                      </button>
                    </li>
                  );
                })}
              </ul>
            </CardContent>
          </Card>

          {/* Detail */}
          <Card className="col-span-12 lg:col-span-7">
            {activeView ? (
              <ViewDetail view={activeView} stats={topicByName.get(activeView.name)} />
            ) : (
              <CardContent className="py-10 text-center text-[12px] text-muted-foreground">
                Select a view on the left.
              </CardContent>
            )}
          </Card>
        </div>
      )}
    </div>
  );
}

function ViewDetail({ view, stats }: { view: ViewInfo; stats: TopicInfo | undefined }) {
  return (
    <>
      <CardHeader className="pb-2 border-b border-border">
        <CardTitle className="font-mono text-[13px] text-foreground normal-case tracking-normal">
          {view.name}
        </CardTitle>
      </CardHeader>
      <CardContent className="pt-3">
        <dl className="grid grid-cols-4 gap-y-2 mb-4">
          <DetailCell label="Source">
            <Link
              to={`/topics?focus=${encodeURIComponent(view.source)}`}
              className="font-mono text-primary hover:underline inline-flex items-center gap-0.5"
            >
              {view.source} <ArrowUpRight size={10} />
            </Link>
          </DetailCell>
          <DetailCell label="Rows">
            <span className="font-mono tabular">
              {stats ? formatCount(stats.rowCount) : '—'}
            </span>
          </DetailCell>
          <DetailCell label="Capacity">
            <span className="font-mono tabular text-muted-foreground">
              {formatCount(view.initial_capacity)}
            </span>
          </DetailCell>
          <DetailCell label="Subs">
            <span className="font-mono tabular text-muted-foreground">
              {stats ? formatCount(stats.subscriptions) : '—'}
            </span>
          </DetailCell>
        </dl>

        <div className="text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground font-medium mb-1.5">
          SQL
        </div>
        <pre className="font-mono text-[11.5px] leading-relaxed bg-muted text-foreground rounded-md border border-border p-3 overflow-x-auto whitespace-pre-wrap break-words">
          {view.sql}
        </pre>
      </CardContent>
    </>
  );
}

function CreateViewForm() {
  const qc = useQueryClient();
  const catalog = useQuery({
    queryKey: ['catalog'],
    queryFn: adminApi.catalog,
    refetchInterval: 10_000,
  });
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [source, setSource] = useState('');
  const [sql, setSql] = useState('');

  const sources = useMemo(
    () =>
      (catalog.data ?? [])
        .filter((e) => e.kind === 'topic')
        .map((e) => e.name)
        .sort(),
    [catalog.data],
  );

  const create = useMutation({
    mutationFn: () => adminApi.createView({ name: name.trim(), source, sql }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['views'] });
      qc.invalidateQueries({ queryKey: ['catalog'] });
      setName('');
      setSql('');
    },
  });

  const canSubmit = !!name.trim() && !!source && !!sql.trim() && !create.isPending;
  const inputCls =
    'h-8 px-2 rounded-md border border-border bg-input font-mono text-[12.5px] text-foreground focus:outline-none focus:ring-1 focus:ring-ring';

  if (!open) {
    return (
      <Button variant="secondary" size="sm" className="mb-3" onClick={() => setOpen(true)}>
        <Plus size={11} /> New view
      </Button>
    );
  }

  return (
    <Card className="mb-3">
      <CardHeader className="pb-2 border-b border-border flex flex-row items-center justify-between">
        <CardTitle>Create view</CardTitle>
        <Button variant="ghost" size="sm" onClick={() => setOpen(false)}>
          Cancel
        </Button>
      </CardHeader>
      <CardContent className="pt-3 space-y-2.5">
        <div className="grid grid-cols-2 gap-2.5">
          <label className="flex flex-col gap-1 text-[11px] text-muted-foreground">
            View name
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="/v_my_view"
              className={inputCls}
            />
          </label>
          <label className="flex flex-col gap-1 text-[11px] text-muted-foreground">
            Source topic
            <select value={source} onChange={(e) => setSource(e.target.value)} className={inputCls}>
              <option value="">Select a source…</option>
              {sources.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </label>
        </div>
        <label className="flex flex-col gap-1 text-[11px] text-muted-foreground">
          SQL — aggregate query; FROM is interpreted as the source topic
          <textarea
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            rows={5}
            spellCheck={false}
            placeholder={'SELECT issuer_sector, COUNT(*) AS n\nFROM positions\nGROUP BY issuer_sector'}
            className="px-2 py-1.5 rounded-md border border-border bg-input font-mono text-[12px] text-foreground focus:outline-none focus:ring-1 focus:ring-ring"
          />
        </label>
        <div className="flex items-center gap-2">
          <Button size="sm" onClick={() => create.mutate()} disabled={!canSubmit}>
            {create.isPending ? 'Creating…' : 'Create view'}
          </Button>
          {create.isError ? (
            <span className="text-[11.5px] text-err">{(create.error as Error).message}</span>
          ) : null}
          {create.isSuccess ? <span className="text-[11.5px] text-ok">Created ✓</span> : null}
        </div>
      </CardContent>
    </Card>
  );
}

function DetailCell({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-[0.1em] text-muted-foreground mb-0.5">
        {label}
      </dt>
      <dd className="text-[12.5px]">{children}</dd>
    </div>
  );
}
