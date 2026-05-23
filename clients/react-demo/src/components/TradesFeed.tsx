import { useEffect, useRef, useState } from 'react';
import { useCqClient } from '@/lib/CqClientContext';
import { cn } from '@/lib/utils';

interface Trade {
  tradeId: string;
  timestamp: string;
  ticker: string;
  side: 'BUY' | 'SELL';
  qty: number;
  price: number;
  book: string;
  trader: string;
}

const MAX_RECENT = 50;

const fmtInt = (n: number) => Number(n).toLocaleString();
const fmtPx = (n: number) =>
  Number(n).toLocaleString(undefined, { minimumFractionDigits: 4, maximumFractionDigits: 4 });
const shortTime = (iso: string) => (iso ? iso.slice(11, 19) : '—');

export function TradesFeed() {
  const client = useCqClient();
  const [trades, setTrades] = useState<Trade[]>([]);
  // Set of recently-flashed tradeIds so the UI animates only fresh rows.
  const flashSet = useRef<Set<string>>(new Set());

  useEffect(() => {
    const unsub = client.subscribe('/trades', {
      onSnapshot: (rows) => {
        // SOW can be ~100k trades; keep only the most-recent 50 by tradeId
        // (lex sort matches the publisher's zero-padded sequence format).
        const sorted = [...(rows as unknown as Trade[])]
          .sort((a, b) => (a.tradeId < b.tradeId ? 1 : -1))
          .slice(0, MAX_RECENT);
        setTrades(sorted);
      },
      onUpdate: (row) => {
        const t = row as unknown as Trade;
        flashSet.current.add(t.tradeId);
        // Clear the flash flag after the animation completes.
        setTimeout(() => flashSet.current.delete(t.tradeId), 700);
        setTrades((prev) => {
          const next = [t, ...prev];
          if (next.length > MAX_RECENT) next.length = MAX_RECENT;
          return next;
        });
      },
    });
    return unsub;
  }, [client]);

  return (
    <div className="h-full overflow-auto">
      <table className="w-full text-[11px]">
        <thead
          className="sticky top-0 z-10"
          style={{ background: 'var(--sf-bg-3)' }}
        >
          <tr style={{ color: 'var(--sf-t-2)' }}>
            <Th>Trade ID</Th>
            <Th>Time</Th>
            <Th>Ticker</Th>
            <Th>Side</Th>
            <Th align="right">Qty</Th>
            <Th align="right">Price</Th>
            <Th>Book</Th>
            <Th>Trader</Th>
          </tr>
        </thead>
        <tbody style={{ fontFamily: 'var(--sf-font-mono, ui-monospace)' }}>
          {trades.map((t) => (
            <tr
              key={t.tradeId}
              className={cn('trade-row', flashSet.current.has(t.tradeId) && 'trade-row-flash')}
              style={{ borderBottom: '1px solid var(--sf-border)' }}
            >
              <Td>{t.tradeId}</Td>
              <Td>{shortTime(t.timestamp)}</Td>
              <Td>{t.ticker}</Td>
              <Td>
                <span
                  className="font-semibold"
                  style={{
                    color: t.side === 'BUY' ? 'var(--sf-up)' : 'var(--sf-down)',
                  }}
                >
                  {t.side}
                </span>
              </Td>
              <Td align="right">{fmtInt(t.qty)}</Td>
              <Td align="right">{fmtPx(t.price)}</Td>
              <Td>{t.book}</Td>
              <Td>{t.trader}</Td>
            </tr>
          ))}
        </tbody>
      </table>
      <style>{`
        .trade-row td { padding: 4px 8px; transition: background-color 600ms ease-out; }
        .trade-row-flash td { background: var(--sf-flat, rgba(127, 182, 255, 0.18)); transition: none; }
      `}</style>
    </div>
  );
}

function Th({
  children,
  align = 'left',
}: {
  children: React.ReactNode;
  align?: 'left' | 'right';
}) {
  return (
    <th
      style={{
        textAlign: align,
        padding: '6px 8px',
        fontWeight: 500,
        fontSize: 10,
        textTransform: 'uppercase',
        letterSpacing: '0.5px',
        borderBottom: '1px solid var(--sf-border)',
        background: 'var(--sf-bg-3)',
      }}
    >
      {children}
    </th>
  );
}

function Td({
  children,
  align = 'left',
}: {
  children: React.ReactNode;
  align?: 'left' | 'right';
}) {
  return (
    <td
      style={{
        textAlign: align,
        whiteSpace: 'nowrap',
        fontVariantNumeric: 'tabular-nums',
      }}
    >
      {children}
    </td>
  );
}
