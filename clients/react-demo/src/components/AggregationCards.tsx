import { useEffect, useRef, useState } from 'react';
import { useCqClient } from '@/lib/CqClientContext';

interface PositionRow {
  positionKey: string;
  book: string;
  cusip: string;
  ticker: string;
  netQty: number;
  marketValue: number;
  unrealizedPnl: number;
  trades: number;
}

interface SecurityRow {
  cusip: string;
  sector: string;
}

interface TradeRow {
  tradeId: string;
  ticker: string;
  timestamp: string;
}

interface BarEntry {
  label: string;
  value: number;
}

const fmtInt = (n: number) => Number(n).toLocaleString();
const fmtNum = (n: number, dp = 0) =>
  Number(n).toLocaleString(undefined, { minimumFractionDigits: dp, maximumFractionDigits: dp });

export function AggregationCards() {
  const client = useCqClient();
  const positions = useRef<Map<string, PositionRow>>(new Map());
  const securities = useRef<Map<string, SecurityRow>>(new Map());
  const trades = useRef<TradeRow[]>([]);
  // Tick state drives re-render every second so the bars refresh even
  // when individual updates have been throttled.
  const [, setTick] = useState(0);

  useEffect(() => {
    const unsubPos = client.subscribe('/positions', {
      onSnapshot: (rows) => {
        positions.current = new Map(
          (rows as unknown as PositionRow[]).map((p) => [p.positionKey, p]),
        );
      },
      onUpdate: (row) => {
        const p = row as unknown as PositionRow;
        positions.current.set(p.positionKey, p);
      },
    });
    const unsubSec = client.subscribe('/securities', {
      onSnapshot: (rows) => {
        securities.current = new Map(
          (rows as unknown as SecurityRow[]).map((s) => [s.cusip, s]),
        );
      },
      onUpdate: (row) => {
        const s = row as unknown as SecurityRow;
        securities.current.set(s.cusip, s);
      },
    });
    // Deltas-only — the "trade count last 60s" card builds its window
    // from live publishes, so we don't pay the ~136k-row /trades SOW
    // cost (which adds 30+ seconds to first paint of the aggregations).
    const unsubTrd = client.subscribe(
      '/trades',
      {
        onUpdate: (row) => {
          const t = row as unknown as TradeRow;
          trades.current.unshift(t);
          if (trades.current.length > 5000) trades.current.length = 5000;
        },
      },
      { deltasOnly: true },
    );
    const timer = setInterval(() => setTick((n) => n + 1), 1000);
    return () => {
      unsubPos();
      unsubSec();
      unsubTrd();
      clearInterval(timer);
    };
  }, [client]);

  // Aggregations are recomputed every render (the timer above forces one
  // render per second). Don't memoize: the inputs live in refs whose
  // identity doesn't change between ticks, so any `useMemo` dep we could
  // express would either be stable (same cached output) or fire too
  // often. Recomputing 80 books / 8 sectors / 5 top / 5 tickers once a
  // second is free.

  const pnlByBook: BarEntry[] = (() => {
    const m = new Map<string, number>();
    for (const p of positions.current.values()) {
      m.set(p.book, (m.get(p.book) ?? 0) + (p.unrealizedPnl ?? 0));
    }
    return [...m.entries()]
      .map(([label, value]) => ({ label, value }))
      .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
      .slice(0, 8);
  })();

  const sectorExposure: BarEntry[] = (() => {
    const m = new Map<string, number>();
    for (const p of positions.current.values()) {
      const sector = securities.current.get(p.cusip)?.sector ?? '—';
      m.set(sector, (m.get(sector) ?? 0) + Math.abs(p.marketValue ?? 0));
    }
    return [...m.entries()]
      .map(([label, value]) => ({ label, value }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 8);
  })();

  const topPositions: BarEntry[] = [...positions.current.values()]
    .map((p) => ({
      label: `${p.book.slice(5, 14)} · ${p.ticker.slice(0, 10)}`,
      value: Math.abs(p.marketValue ?? 0),
    }))
    .sort((a, b) => b.value - a.value)
    .slice(0, 5);

  const tradeCountByTicker: BarEntry[] = (() => {
    const cutoff = Date.now() - 60_000;
    const m = new Map<string, number>();
    for (const t of trades.current) {
      const ts = new Date(t.timestamp).getTime();
      if (Number.isFinite(ts) && ts >= cutoff) {
        m.set(t.ticker, (m.get(t.ticker) ?? 0) + 1);
      }
    }
    return [...m.entries()]
      .map(([label, value]) => ({ label, value }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 5);
  })();

  return (
    <div className="grid grid-cols-2 gap-3 p-3 h-full overflow-auto">
      <AggCard title="P&L by book">
        <Bars entries={pnlByBook} signed valueFmt={(v) => fmtNum(v)} />
      </AggCard>
      <AggCard title="Net exposure by sector">
        <Bars entries={sectorExposure} valueFmt={(v) => fmtNum(v)} />
      </AggCard>
      <AggCard title="Top 5 positions by market value">
        <Bars entries={topPositions} valueFmt={(v) => fmtNum(v)} />
      </AggCard>
      <AggCard title="Trade count (last 60s) by ticker">
        <Bars entries={tradeCountByTicker} valueFmt={(v) => fmtInt(v)} />
      </AggCard>
    </div>
  );
}

function AggCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div
      className="rounded-md p-2.5"
      style={{
        background: 'var(--sf-bg-3)',
        border: '1px solid var(--sf-border)',
      }}
    >
      <div
        className="mb-1.5 text-[10px] uppercase tracking-wider font-medium"
        style={{ color: 'var(--sf-t-2)' }}
      >
        {title}
      </div>
      {children}
    </div>
  );
}

function Bars({
  entries,
  signed,
  valueFmt,
}: {
  entries: BarEntry[];
  signed?: boolean;
  valueFmt: (v: number) => string;
}) {
  if (entries.length === 0) {
    return (
      <div className="text-[11px] py-1" style={{ color: 'var(--sf-t-3)' }}>
        (no data yet)
      </div>
    );
  }
  const maxAbs = Math.max(...entries.map((e) => Math.abs(e.value))) || 1;
  return (
    <div className="flex flex-col gap-[2px]">
      {entries.map((e) => {
        const pct = (Math.abs(e.value) / maxAbs) * 100;
        const fillColor = signed
          ? e.value >= 0
            ? 'var(--sf-up)'
            : 'var(--sf-down)'
          : 'var(--sf-accent, var(--sf-up))';
        return (
          <div key={e.label} className="flex items-center gap-2 py-[2px] text-[11px]">
            <span
              className="truncate"
              style={{ width: 120, color: 'var(--sf-t-0)' }}
            >
              {e.label}
            </span>
            <span
              className="flex-1 relative overflow-hidden rounded-sm"
              style={{ height: 12, background: 'var(--sf-bg)' }}
            >
              <span
                className="absolute inset-y-0 left-0"
                style={{ width: `${pct}%`, background: fillColor, opacity: 0.7 }}
              />
            </span>
            <span
              className="text-right tabular-nums"
              style={{ width: 90, color: 'var(--sf-t-2)' }}
            >
              {valueFmt(e.value)}
            </span>
          </div>
        );
      })}
    </div>
  );
}
