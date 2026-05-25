import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';
import { Link } from 'react-router-dom';
import { ArrowUpRight, GitBranch, Layers, Radio, Server } from 'lucide-react';
import { MetricCard } from '@/components/monitor/MetricCard';
import { Sparkline } from '@/components/monitor/Sparkline';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Separator } from '@/components/ui/separator';
import {
  adminApi,
  metricSum,
  metricValue,
  parsePrometheus,
  type TopicInfo,
} from '@/lib/admin';
import { useSeries } from '@/lib/use-series';
import { formatBytes, formatCount, formatPercent, formatRate } from '@/lib/utils';

const POLL_FAST = 2_000;
const POLL_SLOW = 5_000;

export function OverviewPage() {
  const stats = useQuery({
    queryKey: ['stats'],
    queryFn: adminApi.stats,
    refetchInterval: POLL_FAST,
  });

  const topics = useQuery({
    queryKey: ['topics'],
    queryFn: adminApi.topics,
    refetchInterval: POLL_SLOW,
  });

  const replication = useQuery({
    queryKey: ['replication'],
    queryFn: adminApi.replication,
    refetchInterval: POLL_FAST,
    retry: false,
  });

  const metricsText = useQuery({
    queryKey: ['metrics'],
    queryFn: adminApi.metricsText,
    refetchInterval: POLL_FAST,
  });

  const parsed = useMemo(
    () => (metricsText.data ? parsePrometheus(metricsText.data) : []),
    [metricsText.data],
  );

  // Headline numbers
  const rss = stats.data?.processRssBytes ?? 0;
  const rssMb = rss / 1024 / 1024;
  const nTopics = stats.data?.topics ?? 0;
  const nSubs = stats.data?.totalSubscriptions ?? 0;
  const nRoutes = stats.data?.activeRoutes ?? 0;

  // Snapshot cache
  const cacheBytes = metricValue(parsed, 'cq_snapshot_cache_bytes') ?? 0;
  const cacheBytesZstd = metricValue(parsed, 'cq_snapshot_cache_bytes_zstd') ?? 0;
  const cacheCompressionPct = metricValue(parsed, 'cq_snapshot_compression_ratio_pct');

  // Publish rate (cumulative counter — derive rate from a rolling series)
  const publishTotal = metricSum(parsed, 'cq_publish_total');

  // Build sparkline series for each headline
  const rssSeries = useSeries(rssMb);
  const subSeries = useSeries(nSubs);
  const routeSeries = useSeries(nRoutes);
  const publishSeries = useSeries(publishTotal);
  const cacheSeries = useSeries(cacheBytes / 1024 / 1024);

  // Derive a per-second publish rate from the rolling cumulative series.
  const publishRate = useMemo(() => {
    if (publishSeries.length < 2) return 0;
    const last = publishSeries[publishSeries.length - 1];
    const first = publishSeries[0];
    const intervalSec = (POLL_FAST / 1000) * (publishSeries.length - 1);
    return intervalSec > 0 ? (last - first) / intervalSec : 0;
  }, [publishSeries]);

  // 5-minute delta on subscriptions: compare against ~150 samples ago
  // (at 2s polling, 150 samples = 5 min). Falls back to first sample.
  const subDelta = useMemo(() => {
    if (subSeries.length < 2) return 0;
    const lookback = Math.min(subSeries.length - 1, 150);
    return subSeries[subSeries.length - 1] - subSeries[subSeries.length - 1 - lookback];
  }, [subSeries]);

  const topRows: TopicInfo[] = useMemo(() => {
    const list = topics.data ?? [];
    return [...list].sort((a, b) => b.rowCount - a.rowCount).slice(0, 8);
  }, [topics.data]);

  const role = replication.data?.role ?? 'standalone';

  return (
    <div className="px-6 py-5 max-w-[1400px] mx-auto">
      {/* Page header */}
      <div className="flex items-baseline justify-between mb-4">
        <div>
          <h1 className="text-[18px] font-semibold tracking-tight leading-none">
            Overview
          </h1>
          <p className="text-[11.5px] text-muted-foreground mt-1.5">
            Live operator snapshot of cqserver health and throughput.
          </p>
        </div>
        <div className="flex items-center gap-1.5 font-mono text-[11px] text-muted-foreground">
          <span className="size-1.5 rounded-full bg-ok animate-pulse" />
          polling 2s
        </div>
      </div>

      {/* Headline metric strip */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-2.5 mb-5">
        <MetricCard
          label="Process RSS"
          value={
            <>
              {rssMb >= 1024 ? (rssMb / 1024).toFixed(2) : rssMb.toFixed(0)}
              <span className="text-[14px] text-muted-foreground font-mono ml-1.5 font-normal">
                {rssMb >= 1024 ? 'GB' : 'MB'}
              </span>
            </>
          }
          sub="resident memory"
          series={rssSeries}
          sparkColor="var(--info)"
          pulseKey={Math.round(rssMb)}
        />
        <MetricCard
          label="Subscriptions"
          value={formatCount(nSubs)}
          sub={`${nRoutes} active routes`}
          delta={
            subDelta !== 0
              ? `${subDelta > 0 ? '+' : ''}${subDelta} 5m`
              : '—'
          }
          trend={subDelta > 0 ? 'up' : subDelta < 0 ? 'down' : 'flat'}
          series={subSeries}
          sparkColor="var(--ok)"
          pulseKey={nSubs}
        />
        <MetricCard
          label="Topics"
          value={formatCount(nTopics)}
          sub={`${formatCount(stats.data?.totalRows ?? 0)} rows total`}
          series={routeSeries}
          sparkColor="var(--primary)"
          pulseKey={nTopics}
        />
        <MetricCard
          label="Publish rate"
          value={formatRate(publishRate)}
          sub={`${formatCount(publishTotal)} cumulative`}
          series={publishSeries}
          sparkColor="var(--warn)"
          pulseKey={Math.round(publishTotal)}
        />
      </div>

      {/* Middle row: Replication + Snapshot cache */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-2.5 mb-5">
        {/* Replication panel */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="flex items-center gap-1.5">
              <GitBranch size={11} /> Replication
            </CardTitle>
            <Badge
              variant={
                role === 'standalone' ? 'muted' : role === 'primary' ? 'primary' : 'ok'
              }
            >
              {role}
            </Badge>
          </CardHeader>
          <CardContent>
            {role === 'standalone' ? (
              <div className="text-[12px] text-muted-foreground">
                No leader or follower configured. Set{' '}
                <code className="font-mono text-foreground bg-muted px-1 py-0.5 rounded-sm">
                  [replication]
                </code>{' '}
                in <code className="font-mono text-foreground">cqserver.toml</code>{' '}
                to enable.
              </div>
            ) : (
              <div className="space-y-2">
                <div className="flex items-center gap-2 text-[12px]">
                  <span className="text-muted-foreground w-16">Peer</span>
                  <code className="font-mono text-foreground">
                    {replication.data?.peer || replication.data?.listen || '—'}
                  </code>
                </div>
                <Separator />
                <div className="text-[11px] text-muted-foreground">
                  Per-topic lag tracked on the{' '}
                  <Link to="/replication" className="text-primary hover:underline inline-flex items-center gap-0.5">
                    Replication screen <ArrowUpRight size={10} />
                  </Link>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Snapshot cache panel */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="flex items-center gap-1.5">
              <Layers size={11} /> Snapshot Cache
            </CardTitle>
            <span className="text-[11px] font-mono text-muted-foreground">256 MB cap</span>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-3 gap-3">
              <div>
                <div className="text-[10px] uppercase tracking-[0.1em] text-muted-foreground mb-1">
                  Bytes
                </div>
                <div className="font-mono tabular text-[18px] font-semibold leading-none">
                  {formatBytes(cacheBytes)}
                </div>
                <div className="mt-2">
                  <Sparkline
                    data={cacheSeries}
                    width={160}
                    height={28}
                    stroke="var(--primary)"
                  />
                </div>
              </div>
              <div>
                <div className="text-[10px] uppercase tracking-[0.1em] text-muted-foreground mb-1">
                  Zstd projection
                </div>
                <div className="font-mono tabular text-[18px] font-semibold leading-none">
                  {formatBytes(cacheBytesZstd)}
                </div>
                <div className="text-[11px] text-muted-foreground mt-2">
                  encoded but uncompressed on the wire (lib swap deferred)
                </div>
              </div>
              <div>
                <div className="text-[10px] uppercase tracking-[0.1em] text-muted-foreground mb-1">
                  Compression
                </div>
                <div className="font-mono tabular text-[18px] font-semibold leading-none text-ok">
                  {cacheCompressionPct != null
                    ? formatPercent(cacheCompressionPct, 1)
                    : '—'}
                </div>
                <div className="text-[11px] text-muted-foreground mt-2">
                  measured ratio (lower is better)
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Hottest topics */}
      <Card>
        <CardHeader className="flex flex-row items-baseline justify-between pb-2">
          <CardTitle className="flex items-center gap-1.5">
            <Radio size={11} /> Hottest Topics
          </CardTitle>
          <Link
            to="/topics"
            className="text-[11px] text-primary hover:underline inline-flex items-center gap-0.5"
          >
            Open Topics page <ArrowUpRight size={10} />
          </Link>
        </CardHeader>
        <CardContent className="pt-0">
          <table className="w-full text-[12px]">
            <thead>
              <tr className="text-muted-foreground border-b border-border">
                <th className="text-left font-medium uppercase tracking-[0.08em] text-[10px] py-1.5 pr-3">
                  Topic
                </th>
                <th className="text-right font-medium uppercase tracking-[0.08em] text-[10px] py-1.5 px-3">
                  Rows
                </th>
                <th className="text-right font-medium uppercase tracking-[0.08em] text-[10px] py-1.5 px-3">
                  Subs
                </th>
                <th className="text-right font-medium uppercase tracking-[0.08em] text-[10px] py-1.5 px-3">
                  Capacity
                </th>
                <th className="text-right font-medium uppercase tracking-[0.08em] text-[10px] py-1.5 px-3">
                  Seq
                </th>
                <th className="text-left font-medium uppercase tracking-[0.08em] text-[10px] py-1.5 pl-3">
                  Schema
                </th>
              </tr>
            </thead>
            <tbody>
              {topRows.length === 0 ? (
                <tr>
                  <td colSpan={6} className="py-6 text-center text-muted-foreground">
                    {topics.isLoading ? 'Loading topics…' : 'No topics yet.'}
                  </td>
                </tr>
              ) : (
                topRows.map((t) => (
                  <tr
                    key={t.name}
                    className="border-b border-border last:border-0 hover:bg-accent/30"
                  >
                    <td className="py-1.5 pr-3">
                      <Link
                        to={`/topics?focus=${encodeURIComponent(t.name)}`}
                        className="font-mono text-foreground hover:text-primary"
                      >
                        {t.name}
                      </Link>
                    </td>
                    <td className="text-right py-1.5 px-3 font-mono tabular">
                      {formatCount(t.rowCount)}
                    </td>
                    <td className="text-right py-1.5 px-3 font-mono tabular text-muted-foreground">
                      {t.subscriptions}
                    </td>
                    <td className="text-right py-1.5 px-3 font-mono tabular text-muted-foreground">
                      {formatCount(t.capacity)}
                    </td>
                    <td className="text-right py-1.5 px-3 font-mono tabular text-muted-foreground">
                      {formatCount(t.globalVersion)}
                    </td>
                    <td className="py-1.5 pl-3">
                      {t.schemaDiscovered ? (
                        <Badge variant="ok">discovered</Badge>
                      ) : (
                        <Badge variant="muted">pending</Badge>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </CardContent>
      </Card>

      {/* Footer note */}
      <div className="mt-6 flex items-center gap-2 text-[11px] text-muted-foreground font-mono">
        <Server size={12} className="text-muted-foreground/60" />
        every count above is live from{' '}
        <code className="text-foreground">/stats</code>,{' '}
        <code className="text-foreground">/topics</code>,{' '}
        <code className="text-foreground">/metrics</code>.
      </div>
    </div>
  );
}
