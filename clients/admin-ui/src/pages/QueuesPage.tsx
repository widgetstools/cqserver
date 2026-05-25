import { useQuery } from '@tanstack/react-query';
import { RefreshCw, Workflow, Users, Layers, Hash } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { adminApi, type QueueInfo } from '@/lib/admin';
import { formatCount } from '@/lib/utils';

export function QueuesPage() {
  const queues = useQuery({
    queryKey: ['queues'],
    queryFn: adminApi.queues,
    refetchInterval: 2_000,
  });

  const list: QueueInfo[] = queues.data ?? [];
  const total = list.length;
  const totalBuffered = list.reduce((acc, q) => acc + q.buffered, 0);
  const totalConsumers = list.reduce((acc, q) => acc + q.consumers, 0);

  return (
    <div className="px-6 py-5 max-w-[1400px] mx-auto">
      <div className="flex items-baseline justify-between mb-4">
        <div>
          <h1 className="text-[18px] font-semibold tracking-tight leading-none flex items-center gap-2">
            <Workflow size={16} className="text-primary" />
            Queues
          </h1>
          <p className="text-[11.5px] text-muted-foreground mt-1.5">
            Competing-consumer message queues. Each card is one queue topic.
          </p>
        </div>
        <Button
          variant="secondary"
          size="sm"
          onClick={() => queues.refetch()}
          disabled={queues.isFetching}
        >
          <RefreshCw size={11} className={queues.isFetching ? 'animate-spin' : ''} />
          Refresh
        </Button>
      </div>

      {/* Summary strip */}
      <div className="grid grid-cols-3 gap-2.5 mb-4">
        <SummaryCell label="Queues" value={formatCount(total)} icon={Workflow} />
        <SummaryCell
          label="Total buffered"
          value={formatCount(totalBuffered)}
          icon={Layers}
          tone={totalBuffered > 0 ? 'warn' : 'muted'}
        />
        <SummaryCell
          label="Total consumers"
          value={formatCount(totalConsumers)}
          icon={Users}
        />
      </div>

      {/* Queue cards */}
      {total === 0 ? (
        <Card>
          <CardContent className="py-10 text-center text-[12px] text-muted-foreground">
            {queues.isLoading
              ? 'Loading queues…'
              : 'No queue topics configured. Declare a [[queues]] block in cqserver.toml to add one.'}
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2.5">
          {list.map((q) => (
            <QueueCard key={q.name} queue={q} />
          ))}
        </div>
      )}
    </div>
  );
}

function QueueCard({ queue }: { queue: QueueInfo }) {
  const backedUp = queue.buffered > 0 && queue.consumers === 0;
  return (
    <Card className={backedUp ? 'border-warn/50' : ''}>
      <CardHeader className="flex flex-row items-baseline justify-between pb-2 border-b border-border">
        <CardTitle className="font-mono text-[12.5px] text-foreground normal-case tracking-normal">
          {queue.name}
        </CardTitle>
        {backedUp ? (
          <Badge variant="warn">no consumers</Badge>
        ) : queue.consumers > 0 ? (
          <Badge variant="ok">live</Badge>
        ) : (
          <Badge variant="muted">idle</Badge>
        )}
      </CardHeader>
      <CardContent className="pt-3">
        <dl className="grid grid-cols-3 gap-y-2.5 gap-x-3">
          <Cell label="Buffered" value={formatCount(queue.buffered)} icon={Layers} />
          <Cell label="Consumers" value={formatCount(queue.consumers)} icon={Users} />
          <Cell label="Sequence" value={formatCount(queue.sequence)} icon={Hash} />
        </dl>
      </CardContent>
    </Card>
  );
}

function Cell({
  label,
  value,
  icon: Icon,
}: {
  label: string;
  value: string;
  icon: React.ComponentType<{ size?: number; className?: string }>;
}) {
  return (
    <div>
      <dt className="flex items-center gap-1 text-[10px] uppercase tracking-[0.1em] text-muted-foreground">
        <Icon size={9} className="text-muted-foreground/70" />
        {label}
      </dt>
      <dd className="font-mono tabular text-[18px] font-semibold leading-none mt-1">
        {value}
      </dd>
    </div>
  );
}

function SummaryCell({
  label,
  value,
  icon: Icon,
  tone = 'muted',
}: {
  label: string;
  value: string;
  icon: React.ComponentType<{ size?: number; className?: string }>;
  tone?: 'muted' | 'warn' | 'err' | 'ok';
}) {
  const toneCls =
    tone === 'err'
      ? 'text-err'
      : tone === 'warn'
      ? 'text-warn'
      : tone === 'ok'
      ? 'text-ok'
      : 'text-foreground';
  return (
    <div className="flex items-baseline justify-between rounded-md border border-border bg-card px-3.5 py-3">
      <div className="flex flex-col gap-1.5">
        <span className="text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground font-medium">
          {label}
        </span>
        <span className={`font-mono tabular text-[22px] font-semibold leading-none ${toneCls}`}>
          {value}
        </span>
      </div>
      <Icon size={18} className="text-muted-foreground/40" />
    </div>
  );
}
