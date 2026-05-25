import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';
import { GitBranch, Radio, RefreshCw, Server, Activity } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  adminApi,
  metricValue,
  parsePrometheus,
  type ReplicationStatus,
} from '@/lib/admin';
import { formatCount } from '@/lib/utils';

export function ReplicationPage() {
  const repl = useQuery({
    queryKey: ['replication'],
    queryFn: adminApi.replication,
    refetchInterval: 2_000,
  });

  const metricsText = useQuery({
    queryKey: ['metrics'],
    queryFn: adminApi.metricsText,
    refetchInterval: 2_000,
  });

  const parsed = useMemo(
    () => (metricsText.data ? parsePrometheus(metricsText.data) : []),
    [metricsText.data],
  );

  const data: ReplicationStatus | undefined = repl.data;
  const role = data?.role ?? 'standalone';
  const topics = data?.topics ?? [];

  return (
    <div className="px-6 py-5 max-w-[1400px] mx-auto">
      <div className="flex items-baseline justify-between mb-4">
        <div>
          <h1 className="text-[18px] font-semibold tracking-tight leading-none flex items-center gap-2">
            <GitBranch size={16} className="text-primary" />
            Replication
          </h1>
          <p className="text-[11.5px] text-muted-foreground mt-1.5">
            Leader / follower topology and per-topic replication state.
          </p>
        </div>
        <Button
          variant="secondary"
          size="sm"
          onClick={() => {
            repl.refetch();
            metricsText.refetch();
          }}
          disabled={repl.isFetching}
        >
          <RefreshCw size={11} className={repl.isFetching ? 'animate-spin' : ''} />
          Refresh
        </Button>
      </div>

      {/* Role card */}
      <Card className="mb-4">
        <CardHeader className="flex flex-row items-center justify-between pb-2 border-b border-border">
          <CardTitle className="flex items-center gap-1.5">
            <Server size={11} /> Topology
          </CardTitle>
          <Badge
            variant={
              role === 'standalone' ? 'muted' : role === 'primary' ? 'primary' : 'ok'
            }
          >
            {role}
          </Badge>
        </CardHeader>
        <CardContent className="pt-3">
          {role === 'standalone' ? (
            <div className="text-[12px] text-muted-foreground">
              No replication peer configured. Set{' '}
              <code className="font-mono text-foreground bg-muted px-1 py-0.5 rounded-sm">
                [replication]
              </code>{' '}
              in <code className="font-mono text-foreground">cqserver.toml</code>{' '}
              with <code className="font-mono">role = "primary"</code> +{' '}
              <code className="font-mono">peer = "follower:9010"</code>, or
              <code className="font-mono"> role = "standby"</code> +{' '}
              <code className="font-mono">listen = "0.0.0.0:9010"</code>.
            </div>
          ) : (
            <dl className="grid grid-cols-3 gap-y-2 text-[12.5px]">
              <DetailCell label="Role">
                <span className="font-mono">{role}</span>
              </DetailCell>
              {data?.peer ? (
                <DetailCell label="Peer">
                  <code className="font-mono">{data.peer}</code>
                </DetailCell>
              ) : null}
              {data?.listen ? (
                <DetailCell label="Listen">
                  <code className="font-mono">{data.listen}</code>
                </DetailCell>
              ) : null}
              <DetailCell label="Connect attempts">
                <span className="font-mono tabular">
                  {formatCount(metricValue(parsed, 'cq_repl_connect_total') ?? 0)}
                </span>
              </DetailCell>
              <DetailCell label="Reconnects">
                <span className="font-mono tabular">
                  {formatCount(metricValue(parsed, 'cq_repl_reconnect_total') ?? 0)}
                </span>
              </DetailCell>
              <DetailCell label="Session errors">
                <span className="font-mono tabular">
                  {formatCount(metricValue(parsed, 'cq_repl_session_error_total') ?? 0)}
                </span>
              </DetailCell>
            </dl>
          )}
        </CardContent>
      </Card>

      {/* Per-topic table */}
      <Card>
        <CardHeader className="flex flex-row items-baseline justify-between pb-2 border-b border-border">
          <CardTitle className="flex items-center gap-1.5">
            <Radio size={11} /> Persistent topics
          </CardTitle>
          <span className="text-[11px] text-muted-foreground font-mono">
            polling 2s
          </span>
        </CardHeader>
        <CardContent className="p-0">
          {topics.length === 0 ? (
            <div className="py-8 text-center text-[12px] text-muted-foreground">
              No persistent topics. Replication only ships txlog-backed
              topics; mark a topic{' '}
              <code className="font-mono text-foreground">persist = true</code>{' '}
              to include it.
            </div>
          ) : (
            <table className="w-full text-[12px]">
              <thead>
                <tr className="text-muted-foreground border-b border-border">
                  <th className="text-left font-medium uppercase tracking-[0.08em] text-[10px] py-1.5 pl-3 pr-3">
                    Topic
                  </th>
                  <th className="text-right font-medium uppercase tracking-[0.08em] text-[10px] py-1.5 px-3">
                    Local seq
                  </th>
                  <th className="text-right font-medium uppercase tracking-[0.08em] text-[10px] py-1.5 px-3">
                    Shipped
                  </th>
                  <th className="text-right font-medium uppercase tracking-[0.08em] text-[10px] py-1.5 px-3">
                    Applied
                  </th>
                  <th className="text-right font-medium uppercase tracking-[0.08em] text-[10px] py-1.5 px-3">
                    Acked
                  </th>
                  <th className="text-right font-medium uppercase tracking-[0.08em] text-[10px] py-1.5 pr-3 pl-3">
                    Lag
                  </th>
                </tr>
              </thead>
              <tbody>
                {topics.map((t) => {
                  const shipped =
                    metricValue(parsed, 'cq_repl_shipped_max_sequence', {
                      topic: t.topic,
                    }) ?? 0;
                  const applied =
                    metricValue(parsed, 'cq_repl_applied_max_sequence', {
                      topic: t.topic,
                    }) ?? 0;
                  const acked =
                    metricValue(parsed, 'cq_repl_acked_max_sequence', {
                      topic: t.topic,
                    }) ?? 0;
                  const local = t.current_sequence ?? 0;
                  const lag = Math.max(0, local - applied);
                  const lagCls =
                    lag === 0
                      ? 'text-ok'
                      : lag < 1000
                      ? 'text-muted-foreground'
                      : 'text-warn';
                  return (
                    <tr
                      key={t.topic}
                      className="border-b border-border last:border-0 hover:bg-accent/30"
                    >
                      <td className="py-1.5 pl-3 pr-3 font-mono text-foreground">
                        {t.topic}
                      </td>
                      <td className="text-right py-1.5 px-3 font-mono tabular">
                        {formatCount(local)}
                      </td>
                      <td className="text-right py-1.5 px-3 font-mono tabular text-muted-foreground">
                        {shipped > 0 ? formatCount(shipped) : '—'}
                      </td>
                      <td className="text-right py-1.5 px-3 font-mono tabular text-muted-foreground">
                        {applied > 0 ? formatCount(applied) : '—'}
                      </td>
                      <td className="text-right py-1.5 px-3 font-mono tabular text-muted-foreground">
                        {acked > 0 ? formatCount(acked) : '—'}
                      </td>
                      <td
                        className={`text-right py-1.5 pr-3 pl-3 font-mono tabular ${lagCls}`}
                      >
                        {role === 'standalone' ? '—' : formatCount(lag)}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </CardContent>
      </Card>

      <div className="mt-4 flex items-center gap-2 text-[11px] text-muted-foreground font-mono">
        <Activity size={12} className="text-muted-foreground/60" />
        per-topic metrics from <code className="text-foreground">cq_repl_*</code>{' '}
        gauges scraped via <code className="text-foreground">/metrics</code>.
      </div>
    </div>
  );
}

function DetailCell({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-[0.1em] text-muted-foreground mb-0.5">
        {label}
      </dt>
      <dd>{children}</dd>
    </div>
  );
}
