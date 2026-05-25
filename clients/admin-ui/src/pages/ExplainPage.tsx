import { useMutation, useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import {
  AlertTriangle,
  Beaker,
  CheckCircle2,
  Database,
  HelpCircle,
  Play,
} from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { adminApi, type ExplainResponse } from '@/lib/admin';
import { formatBytes, formatCount } from '@/lib/utils';

const EXAMPLE_SQL = `SELECT book, sector,
       SUM(qty)      AS total_qty,
       SUM(notional) AS total_notional,
       COUNT(*)      AS trades
FROM "/trades"
GROUP BY book, sector`;

export function ExplainPage() {
  const [topic, setTopic] = useState('');
  const [sql, setSql] = useState(EXAMPLE_SQL);

  const topics = useQuery({
    queryKey: ['topics'],
    queryFn: adminApi.topics,
    refetchInterval: 30_000,
  });

  const explain = useMutation({
    mutationFn: ({ topic, sql }: { topic: string; sql: string }) =>
      adminApi.explain(topic, sql),
  });

  const onRun = () => {
    if (!topic) return;
    explain.mutate({ topic, sql });
  };

  // When topics load, default the dropdown to /trades or the first
  // topic available.
  const topicList = topics.data ?? [];
  if (!topic && topicList.length > 0) {
    const preferred =
      topicList.find((t) => t.name === '/trades') ?? topicList[0];
    queueMicrotask(() => setTopic(preferred.name));
  }

  return (
    <div className="px-6 py-5 max-w-[1400px] mx-auto">
      <div className="mb-4">
        <h1 className="text-[18px] font-semibold tracking-tight leading-none flex items-center gap-2">
          <Beaker size={16} className="text-primary" />
          Query Explain
        </h1>
        <p className="text-[11.5px] text-muted-foreground mt-1.5">
          Estimate a query's cost before subscribing. Calls{' '}
          <code className="font-mono text-foreground">POST /admin/explain</code>
          ; uses the same cost estimator the subscribe-time guardrails (G3)
          enforce.
        </p>
      </div>

      <div className="grid grid-cols-12 gap-2.5">
        {/* Form */}
        <Card className="col-span-12 lg:col-span-7">
          <CardHeader className="pb-2 border-b border-border">
            <CardTitle>Query</CardTitle>
          </CardHeader>
          <CardContent className="pt-3 space-y-3">
            <div>
              <label className="block text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground font-medium mb-1.5">
                Topic
              </label>
              <select
                value={topic}
                onChange={(e) => setTopic(e.target.value)}
                className="w-full h-8 px-2 rounded-md border border-border bg-input font-mono text-[12.5px] text-foreground focus:outline-none focus:ring-1 focus:ring-ring"
              >
                {topicList.length === 0 ? (
                  <option value="">{topics.isLoading ? 'Loading…' : 'No topics'}</option>
                ) : (
                  topicList.map((t) => (
                    <option key={t.name} value={t.name}>
                      {t.name} ({formatCount(t.rowCount)} rows)
                    </option>
                  ))
                )}
              </select>
            </div>
            <div>
              <label className="block text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground font-medium mb-1.5">
                SQL
              </label>
              <textarea
                value={sql}
                onChange={(e) => setSql(e.target.value)}
                rows={9}
                spellCheck={false}
                className="w-full px-3 py-2 rounded-md border border-border bg-input font-mono text-[12px] text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring resize-y"
              />
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="default"
                size="sm"
                onClick={onRun}
                disabled={!topic || !sql.trim() || explain.isPending}
              >
                <Play size={11} />
                {explain.isPending ? 'Estimating…' : 'Estimate cost'}
              </Button>
              <span className="text-[11px] text-muted-foreground">
                Read-only; does not subscribe or touch encoder state.
              </span>
            </div>
          </CardContent>
        </Card>

        {/* Result */}
        <Card className="col-span-12 lg:col-span-5">
          <CardHeader className="pb-2 border-b border-border">
            <CardTitle>Cost estimate</CardTitle>
          </CardHeader>
          <CardContent className="pt-3">
            {explain.isError ? (
              <ErrorBanner message={`${explain.error}`} />
            ) : explain.data ? (
              <EstimateView est={explain.data} />
            ) : (
              <div className="text-[12px] text-muted-foreground py-6 text-center flex flex-col items-center gap-1.5">
                <Database size={20} className="text-muted-foreground/40" />
                Run an estimate to see rows, bytes, confidence,
                <br />
                used indexes, and any assumptions.
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function EstimateView({ est }: { est: ExplainResponse }) {
  const confTone =
    est.confidence === 'high' ? 'ok' : est.confidence === 'medium' ? 'warn' : 'muted';

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-2 gap-2.5">
        <Stat
          label="Result rows"
          value={formatCount(est.estimated_result_rows)}
          tone="info"
        />
        <Stat
          label="Result bytes"
          value={formatBytes(est.estimated_result_bytes)}
          tone="info"
        />
        <Stat label="Source rows" value={formatCount(est.estimated_source_rows)} />
        <Stat
          label="Join fanout"
          value={
            est.estimated_join_fanout_avg != null
              ? est.estimated_join_fanout_avg.toFixed(2) + '×'
              : '—'
          }
        />
      </div>

      <div className="flex items-center gap-2 pt-1">
        <span className="text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground">
          Confidence
        </span>
        <Badge variant={confTone}>
          {est.confidence === 'high' ? (
            <CheckCircle2 size={9} />
          ) : (
            <AlertTriangle size={9} />
          )}
          {est.confidence}
        </Badge>
      </div>

      {est.used_indexes.length > 0 ? (
        <div>
          <div className="text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground font-medium mb-1.5">
            Indexes consulted
          </div>
          <div className="flex flex-wrap gap-1">
            {est.used_indexes.map((idx) => (
              <Badge key={idx} variant="primary" className="font-mono normal-case">
                {idx}
              </Badge>
            ))}
          </div>
        </div>
      ) : (
        <div className="text-[11px] text-muted-foreground flex items-center gap-1.5">
          <HelpCircle size={11} className="text-warn" />
          No indexes consulted — estimate used the unknown-selectivity heuristic.
        </div>
      )}

      {est.assumptions.length > 0 ? (
        <div>
          <div className="text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground font-medium mb-1.5">
            Assumptions
          </div>
          <ul className="space-y-1">
            {est.assumptions.map((a, i) => (
              <li
                key={i}
                className="text-[11.5px] text-muted-foreground border-l-2 border-warn/50 pl-2"
              >
                {a}
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
}

function Stat({
  label,
  value,
  tone = 'muted',
}: {
  label: string;
  value: string;
  tone?: 'muted' | 'info' | 'warn';
}) {
  const cls =
    tone === 'info' ? 'text-foreground' : tone === 'warn' ? 'text-warn' : 'text-muted-foreground';
  return (
    <div className="rounded-md border border-border bg-card px-3 py-2.5">
      <div className="text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground font-medium mb-1">
        {label}
      </div>
      <div className={`font-mono tabular text-[18px] font-semibold leading-none ${cls}`}>
        {value}
      </div>
    </div>
  );
}

function ErrorBanner({ message }: { message: string }) {
  return (
    <div className="flex items-start gap-2 rounded-md border border-err/30 bg-err-muted px-3 py-2.5 text-[12px]">
      <AlertTriangle size={14} className="text-err shrink-0 mt-0.5" />
      <div>
        <div className="font-medium text-err mb-0.5">Query rejected or invalid</div>
        <div className="text-muted-foreground font-mono break-all">{message}</div>
      </div>
    </div>
  );
}
