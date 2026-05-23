/// <reference types="node" />
/**
 * Fixed-income demo publisher.
 *
 * Scales to a large universe so the demo looks like a real desk:
 *   - ~500 securities published once at startup
 *   - 40,000 positions seeded (one per (book, cusip) cell), each backed by
 *     1–4 historical trades whose qty/price reproduce the position's
 *     netQty and weighted-avg cost basis
 *   - After seeding, market data ticks at ~20 Hz refresh marketValue and
 *     unrealizedPnl on every position holding the touched cusip
 *   - New synthetic fills land at ~10 Hz against existing positions
 *
 * Tuning (via env):
 *   TARGET_POSITIONS     default 40000
 *   TARGET_SECURITIES    default 500
 *   TARGET_BOOKS         default ceil(TARGET_POSITIONS / TARGET_SECURITIES) = 80
 *   MIN_TRADES_PER_POS   default 1
 *   MAX_TRADES_PER_POS   default 4
 *   TICK_RATE            default 20 (Hz)
 *   TRADE_RATE           default 10 (Hz)
 *   PUB_CONCURRENCY      default 500 (in-flight publishes per chunk)
 *   CQ_URL               default tcp://127.0.0.1:9007
 *
 * Run with: `npm run publish-fi-demo`
 */

import { Client } from '../src/index.js';
import {
  buildSecurities,
  buildBooks,
  TRADERS,
  COUNTERPARTIES,
  type Security,
} from './fi-data.js';

const URL = process.env.CQ_URL ?? 'tcp://127.0.0.1:9007';
const TARGET_POSITIONS = Number(process.env.TARGET_POSITIONS ?? 40_000);
const TARGET_SECURITIES = Number(process.env.TARGET_SECURITIES ?? 500);
const TARGET_BOOKS = Number(
  process.env.TARGET_BOOKS ?? Math.ceil(TARGET_POSITIONS / TARGET_SECURITIES),
);
const MIN_TRADES_PER_POS = Number(process.env.MIN_TRADES_PER_POS ?? 1);
const MAX_TRADES_PER_POS = Number(process.env.MAX_TRADES_PER_POS ?? 4);
const TICK_RATE_HZ = Number(process.env.TICK_RATE ?? 20);
const TRADE_RATE_HZ = Number(process.env.TRADE_RATE ?? 10);
const PUB_CONCURRENCY = Number(process.env.PUB_CONCURRENCY ?? 500);

const SECURITIES = buildSecurities(TARGET_SECURITIES);
const BOOKS = buildBooks(TARGET_BOOKS);

// ----- Types -----

interface PositionRow {
  book: string;
  cusip: string;
  ticker: string;
  netQty: number;        // face value (sum of buy − sell)
  avgCost: number;       // weighted avg cost basis per 100 face
  lastMid: number;       // last market mid
  marketValue: number;   // netQty * lastMid / 100
  unrealizedPnl: number; // (lastMid − avgCost) * netQty / 100
  trades: number;        // count of fills against this position
  [key: string]: unknown;
}

interface MarketRow {
  bid: number;
  ask: number;
  mid: number;
  yieldPct: number;
  [key: string]: unknown;
}

interface TradeRow {
  tradeId: string;
  timestamp: string;
  cusip: string;
  ticker: string;
  side: 'BUY' | 'SELL';
  qty: number;
  price: number;
  notional: number;
  trader: string;
  counterparty: string;
  book: string;
  assetClass: Security['assetClass'];
  sector: string;
  [key: string]: unknown;
}

// ----- Utilities -----

const pick = <T>(arr: readonly T[]): T => arr[Math.floor(Math.random() * arr.length)];
const rand = (min: number, max: number) => min + Math.random() * (max - min);
const randInt = (min: number, max: number) => Math.floor(rand(min, max + 1));
const round = (n: number, dp: number) => Math.round(n * 10 ** dp) / 10 ** dp;

function positionKey(book: string, cusip: string) {
  return `${book}|${cusip}`;
}

function yearsToMaturity(maturityISO: string): number {
  const now = new Date();
  const mat = new Date(maturityISO);
  return Math.max(0.01, (mat.getTime() - now.getTime()) / (365.25 * 24 * 3600 * 1000));
}

function yieldFromPrice(coupon: number, price: number, yrs: number): number {
  if (yrs <= 0) return coupon;
  return coupon + (100 - price) / yrs;
}

function applyTradeToPosition(
  pos: PositionRow,
  side: 'BUY' | 'SELL',
  qty: number,
  price: number,
  mid: number,
) {
  const signedQty = side === 'BUY' ? qty : -qty;
  const newQty = pos.netQty + signedQty;
  const addingTo =
    (pos.netQty >= 0 && signedQty > 0) || (pos.netQty <= 0 && signedQty < 0);
  const newAvgCost =
    addingTo && Math.abs(newQty) > 0.0001
      ? round((pos.avgCost * pos.netQty + price * signedQty) / newQty, 4)
      : pos.avgCost;
  pos.netQty = newQty;
  pos.avgCost = newAvgCost;
  pos.lastMid = mid;
  pos.marketValue = round((newQty * mid) / 100, 2);
  pos.unrealizedPnl = round(((mid - newAvgCost) * newQty) / 100, 2);
  pos.trades += 1;
}

function markToMarket(pos: PositionRow, mid: number) {
  pos.lastMid = mid;
  pos.marketValue = round((pos.netQty * mid) / 100, 2);
  pos.unrealizedPnl = round(((mid - pos.avgCost) * pos.netQty) / 100, 2);
}

// ----- In-memory mirrors -----

const market = new Map<string, MarketRow>();
const positions = new Map<string, PositionRow>(); // key = book|cusip
const positionsByCusip = new Map<string, Set<string>>(); // cusip -> set<positionKey>
const positionKeys: string[] = []; // flat index for random sampling in trade loop

function indexPosition(pos: PositionRow) {
  const pk = positionKey(pos.book, pos.cusip);
  positions.set(pk, pos);
  let set = positionsByCusip.get(pos.cusip);
  if (!set) {
    set = new Set();
    positionsByCusip.set(pos.cusip, set);
  }
  if (!set.has(pk)) {
    set.add(pk);
    positionKeys.push(pk);
  }
}

// ----- Chunked publisher (pipelined) -----

async function publishChunked<T>(
  label: string,
  items: T[],
  publish: (item: T) => Promise<unknown>,
) {
  const start = Date.now();
  for (let i = 0; i < items.length; i += PUB_CONCURRENCY) {
    const chunk = items.slice(i, i + PUB_CONCURRENCY);
    await Promise.all(chunk.map(publish));
    const done = Math.min(i + chunk.length, items.length);
    if (done % (PUB_CONCURRENCY * 10) === 0 || done === items.length) {
      const elapsed = (Date.now() - start) / 1000;
      const rate = done / Math.max(elapsed, 0.001);
      console.log(
        `  ${label}: ${done}/${items.length} (${rate.toFixed(0)} msg/s)`,
      );
    }
  }
}

// ----- Seed phase -----

async function publishSecurities(client: Client) {
  console.log(`Publishing ${SECURITIES.length} securities...`);
  await publishChunked('securities', SECURITIES, (s) =>
    client.publish('/securities', { ...s }),
  );
}

async function seedMarketData(client: Client) {
  console.log(`Seeding market data for ${SECURITIES.length} securities...`);
  const initialRows: Array<{ sec: Security; row: MarketRow }> = [];
  for (const sec of SECURITIES) {
    const spreadBp =
      sec.assetClass === 'UST' ? 0.5 : sec.assetClass === 'AGENCY' ? 1.0 : 2.5;
    const halfSpread = (sec.initialMid * spreadBp) / 10000;
    const row: MarketRow = {
      bid: round(sec.initialMid - halfSpread, 4),
      ask: round(sec.initialMid + halfSpread, 4),
      mid: sec.initialMid,
      yieldPct: round(
        yieldFromPrice(sec.coupon, sec.initialMid, yearsToMaturity(sec.maturity)),
        4,
      ),
    };
    market.set(sec.cusip, row);
    initialRows.push({ sec, row });
  }
  await publishChunked('market-data seed', initialRows, ({ sec, row }) =>
    client.publish('/fi-market-data', {
      cusip: sec.cusip,
      ticker: sec.ticker,
      assetClass: sec.assetClass,
      sector: sec.sector,
      ...row,
      timestamp: new Date().toISOString(),
    }),
  );
}

let nextTradeId = 1;
function newTradeId(): string {
  return `T${String(nextTradeId++).padStart(10, '0')}`;
}

async function seedPositionsAndTrades(client: Client) {
  // Pick (book, cusip) pairs deterministically: book-major. Truncate to
  // exactly TARGET_POSITIONS so the row count is predictable.
  const pairs: Array<{ book: string; sec: Security }> = [];
  outer: for (const book of BOOKS) {
    for (const sec of SECURITIES) {
      pairs.push({ book, sec });
      if (pairs.length >= TARGET_POSITIONS) break outer;
    }
  }
  console.log(
    `Generating ${pairs.length} positions (${BOOKS.length} books × ${SECURITIES.length} securities, capped at ${TARGET_POSITIONS}) ...`,
  );

  const trades: TradeRow[] = [];
  const positionRows: PositionRow[] = [];
  const baseTime = Date.now() - 60 * 60 * 1000; // backdate seed trades by ~1h

  for (const { book, sec } of pairs) {
    const mkt = market.get(sec.cusip)!;
    const pos: PositionRow = {
      book,
      cusip: sec.cusip,
      ticker: sec.ticker,
      netQty: 0,
      avgCost: 0,
      lastMid: mkt.mid,
      marketValue: 0,
      unrealizedPnl: 0,
      trades: 0,
    };

    const nTrades = randInt(MIN_TRADES_PER_POS, MAX_TRADES_PER_POS);
    // Bias direction: ~60% of positions are net long.
    const longBias = Math.random() < 0.6;

    for (let t = 0; t < nTrades; t++) {
      // 70% chance to trade in the bias direction.
      const directional = Math.random() < 0.7;
      const side: 'BUY' | 'SELL' =
        (directional && longBias) || (!directional && !longBias) ? 'BUY' : 'SELL';
      const qty = Math.floor(rand(100, 5000)) * 1000;
      const priceJitter = rand(-0.4, 0.4);
      const price = round(
        side === 'BUY' ? mkt.ask + priceJitter : mkt.bid + priceJitter,
        4,
      );
      const ts = new Date(baseTime + Math.floor(rand(0, 60 * 60 * 1000))).toISOString();
      trades.push({
        tradeId: newTradeId(),
        timestamp: ts,
        cusip: sec.cusip,
        ticker: sec.ticker,
        side,
        qty,
        price,
        notional: round((qty * price) / 100, 2),
        trader: pick(TRADERS),
        counterparty: pick(COUNTERPARTIES),
        book,
        assetClass: sec.assetClass,
        sector: sec.sector,
      });
      applyTradeToPosition(pos, side, qty, price, mkt.mid);
    }

    // Force a small non-zero netQty if randomness happened to flatten it,
    // so the demo doesn't render lots of empty rows.
    if (pos.netQty === 0) {
      const qty = Math.floor(rand(100, 2000)) * 1000;
      const price = round(mkt.mid + rand(-0.2, 0.2), 4);
      const side: 'BUY' | 'SELL' = longBias ? 'BUY' : 'SELL';
      trades.push({
        tradeId: newTradeId(),
        timestamp: new Date(baseTime + Math.floor(rand(0, 60 * 60 * 1000))).toISOString(),
        cusip: sec.cusip,
        ticker: sec.ticker,
        side,
        qty,
        price,
        notional: round((qty * price) / 100, 2),
        trader: pick(TRADERS),
        counterparty: pick(COUNTERPARTIES),
        book,
        assetClass: sec.assetClass,
        sector: sec.sector,
      });
      applyTradeToPosition(pos, side, qty, price, mkt.mid);
    }

    indexPosition(pos);
    positionRows.push(pos);
  }

  console.log(`Publishing ${trades.length} seed trades...`);
  await publishChunked('trades', trades, (t) => client.publish('/trades', t));

  console.log(`Publishing ${positionRows.length} positions...`);
  await publishChunked('positions', positionRows, (p) =>
    client.publish('/positions', { positionKey: positionKey(p.book, p.cusip), ...p }),
  );
}

// ----- Live phase -----

async function tickLoop(client: Client) {
  const intervalMs = 1000 / TICK_RATE_HZ;
  for (;;) {
    const sec = pick(SECURITIES);
    const prev = market.get(sec.cusip)!;
    // Random walk with reversion toward initialMid so prices stay sane.
    const drift = (sec.initialMid - prev.mid) * 0.02;
    const noise = rand(-0.05, 0.05);
    const newMid = Math.max(50, prev.mid + drift + noise);
    const spreadBp =
      sec.assetClass === 'UST' ? 0.5 : sec.assetClass === 'AGENCY' ? 1.0 : 2.5;
    const halfSpread = (newMid * spreadBp) / 10000;
    const row: MarketRow = {
      bid: round(newMid - halfSpread, 4),
      ask: round(newMid + halfSpread, 4),
      mid: round(newMid, 4),
      yieldPct: round(
        yieldFromPrice(sec.coupon, newMid, yearsToMaturity(sec.maturity)),
        4,
      ),
    };
    market.set(sec.cusip, row);

    // Fire the market-data publish without awaiting its ack — the next
    // tick doesn't need it.
    void client.publish('/fi-market-data', {
      cusip: sec.cusip,
      ticker: sec.ticker,
      assetClass: sec.assetClass,
      sector: sec.sector,
      ...row,
      timestamp: new Date().toISOString(),
    });

    // Refresh marketValue/PnL only on positions that hold this cusip,
    // using the secondary index for O(holders) work.
    const holders = positionsByCusip.get(sec.cusip);
    if (holders) {
      for (const pk of holders) {
        const pos = positions.get(pk);
        if (!pos) continue;
        markToMarket(pos, row.mid);
        void client.publish('/positions', {
          positionKey: pk,
          ...pos,
        });
      }
    }

    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

// ----- /risk: nested wide-schema rows derived from positions -----

/**
 * Build a /risk row as a nested JSON tree matching `config/schemas/risk.json`.
 * The server flattens this to dotted-path columns on publish — e.g.
 * `risk.duration` and `attribution.daily_carry` are queryable via
 * `WHERE risk.duration > 5`.
 *
 * Values are deterministic-looking but synthetic; no actual analytics
 * library is involved.
 */
function buildRiskRow(pos: PositionRow): Record<string, unknown> {
  const sec = SECURITIES.find((s) => s.cusip === pos.cusip);
  const mid = pos.lastMid;
  const yrs = sec ? yearsToMaturity(sec.maturity) : 5;
  // crude duration proxy
  const duration = round(Math.min(yrs * 0.9, 30), 3);
  const ir01 = round(Math.abs(pos.netQty) * duration / 10_000, 2);
  return {
    positionKey: positionKey(pos.book, pos.cusip),
    book: pos.book,
    trader: pick(TRADERS),
    instrument: {
      cusip: pos.cusip,
      ticker: pos.ticker,
      issuer: sec?.issuer ?? '',
      assetClass: sec?.assetClass ?? '',
      sector: sec?.sector ?? '',
      subSector: sec?.sector ?? '',
      rating: pick(['AAA', 'AA', 'A', 'BBB', 'BB']),
      currency: 'USD',
      country: 'US',
      exchange: sec?.assetClass === 'UST' ? 'OTC' : 'NYSE',
      couponPct: sec?.coupon ?? 0,
      maturity: sec?.maturity ?? '',
      callable: 'N',
      putable: 'N',
      seniority: 'SR',
    },
    position: {
      netQty: pos.netQty,
      longQty: Math.max(0, pos.netQty),
      shortQty: Math.max(0, -pos.netQty),
      avgCost: pos.avgCost,
      lastMid: mid,
      lastBid: round(mid - 0.05, 4),
      lastAsk: round(mid + 0.05, 4),
      marketValue: pos.marketValue,
      unrealizedPnl: pos.unrealizedPnl,
      realizedPnl: 0,
      dayPnl: round(pos.unrealizedPnl * 0.1, 2),
      mtdPnl: round(pos.unrealizedPnl * 0.4, 2),
      ytdPnl: round(pos.unrealizedPnl * 1.0, 2),
      carry: round(pos.netQty * (sec?.coupon ?? 0) / 365.25 / 100, 2),
      rollDown: 0,
      tradeCount: pos.trades,
      lastTradeTs: new Date().toISOString(),
    },
    risk: {
      duration,
      modifiedDuration: round(duration / (1 + (sec?.coupon ?? 0) / 200), 3),
      effectiveDuration: round(duration * 0.98, 3),
      convexity: round(duration * duration * 0.1, 3),
      modConvexity: round(duration * duration * 0.09, 3),
      ytm: round((sec?.coupon ?? 0) + (100 - mid) / yrs, 4),
      ytw: round((sec?.coupon ?? 0) + (100 - mid) / yrs, 4),
      ytc: round((sec?.coupon ?? 0) + (100 - mid) / yrs - 0.05, 4),
      ytp: 0,
      spreadDv01: round(ir01 * 0.6, 2),
      creditDv01: round(ir01 * 0.4, 2),
      rateDv01: ir01,
      asw: round(rand(-30, 80), 2),
      zspread: round(rand(20, 200), 2),
      oasSpread: round(rand(15, 180), 2),
      iSpread: round(rand(10, 150), 2),
      swapSpread: round(rand(-5, 30), 2),
      treasurySpread: round(rand(0, 100), 2),
      benchmarkSpread: round(rand(0, 80), 2),
      currentYield: round((sec?.coupon ?? 0) / mid * 100, 4),
      ir01_USD: ir01,
      ir01_EUR: round(ir01 * 0.4, 2),
      ir01_GBP: round(ir01 * 0.2, 2),
      ir01_JPY: round(ir01 * 0.1, 2),
      ir01_CHF: 0,
      ir01_CAD: 0,
      kr01_3m: round(ir01 * 0.05, 2),
      kr01_6m: round(ir01 * 0.08, 2),
      kr01_1y: round(ir01 * 0.12, 2),
      kr01_2y: round(ir01 * 0.18, 2),
      kr01_5y: round(ir01 * 0.3, 2),
      kr01_10y: round(ir01 * 0.25, 2),
      kr01_30y: round(ir01 * 0.02, 2),
    },
    var: {
      var_1d_95: round(Math.abs(pos.marketValue) * 0.01, 2),
      var_1d_99: round(Math.abs(pos.marketValue) * 0.018, 2),
      var_5d_95: round(Math.abs(pos.marketValue) * 0.022, 2),
      var_5d_99: round(Math.abs(pos.marketValue) * 0.04, 2),
      var_10d_99: round(Math.abs(pos.marketValue) * 0.057, 2),
      es_1d_95: round(Math.abs(pos.marketValue) * 0.014, 2),
      es_1d_99: round(Math.abs(pos.marketValue) * 0.025, 2),
      es_10d_99: round(Math.abs(pos.marketValue) * 0.08, 2),
    },
    stress: {
      parallel_up_50bp: round(-pos.netQty * duration * 0.005, 2),
      parallel_down_50bp: round(pos.netQty * duration * 0.005, 2),
      parallel_up_100bp: round(-pos.netQty * duration * 0.01, 2),
      parallel_down_100bp: round(pos.netQty * duration * 0.01, 2),
      steepener_2y10y: round(pos.netQty * 0.002, 2),
      flattener_2y10y: round(-pos.netQty * 0.002, 2),
      credit_up_50bp: round(-pos.netQty * 0.003, 2),
      credit_down_50bp: round(pos.netQty * 0.003, 2),
    },
    limits: {
      grossLimit: 10_000_000_000,
      grossUsed: round(Math.abs(pos.marketValue), 2),
      grossPct: round(Math.abs(pos.marketValue) / 10_000_000_000 * 100, 4),
      netLimit: 5_000_000_000,
      netUsed: round(pos.marketValue, 2),
      netPct: round(pos.marketValue / 5_000_000_000 * 100, 4),
      dv01Limit: 1_000_000,
      dv01Used: ir01,
      dv01Pct: round(ir01 / 1_000_000 * 100, 4),
      varLimit: 100_000_000,
      varUsed: round(Math.abs(pos.marketValue) * 0.018, 2),
      varPct: round(Math.abs(pos.marketValue) * 0.018 / 100_000_000 * 100, 4),
    },
    attribution: {
      daily_carry: round(pos.netQty * (sec?.coupon ?? 0) / 365.25 / 100, 2),
      daily_rates: round(pos.unrealizedPnl * 0.4, 2),
      daily_spread: round(pos.unrealizedPnl * 0.3, 2),
      daily_fx: 0,
      daily_convexity: round(pos.unrealizedPnl * 0.05, 2),
      mtd_carry: round(pos.netQty * (sec?.coupon ?? 0) / 12 / 100, 2),
      mtd_rates: round(pos.unrealizedPnl * 1.5, 2),
      mtd_spread: round(pos.unrealizedPnl * 0.8, 2),
      ytd_carry: round(pos.netQty * (sec?.coupon ?? 0) / 100, 2),
      ytd_rates: round(pos.unrealizedPnl * 4, 2),
      ytd_spread: round(pos.unrealizedPnl * 2, 2),
    },
    liquidity: {
      adv30d: round(rand(1e5, 5e6), 0),
      bidAskBps: round(rand(0.5, 10), 2),
      isOnTheRun: Math.random() < 0.1 ? 'Y' : 'N',
      amountOutstanding: round(rand(5e8, 5e9), 0),
      freeFloat: round(rand(0.6, 0.95), 4),
    },
  };
}

async function seedRisk(client: Client, positionRows: PositionRow[]) {
  console.log(`Publishing ${positionRows.length} risk rows...`);
  await publishChunked('risk', positionRows, (p) =>
    client.publish('/risk', buildRiskRow(p)),
  );
}

const RISK_REFRESH_HZ = Number(process.env.RISK_REFRESH_HZ ?? 1);
const RISK_REFRESH_BATCH = Number(process.env.RISK_REFRESH_BATCH ?? 200);

async function riskLoop(client: Client) {
  // Refreshes a random batch of risk rows every ~1s — keeps the
  // `/risk` topic ticking without overwhelming the wire (40k rows
  // refreshed at full rate would be ~5MB of nested JSON per second).
  const intervalMs = 1000 / RISK_REFRESH_HZ;
  for (;;) {
    if (positionKeys.length === 0) {
      await new Promise((r) => setTimeout(r, intervalMs));
      continue;
    }
    for (let i = 0; i < RISK_REFRESH_BATCH; i++) {
      const pk = positionKeys[Math.floor(Math.random() * positionKeys.length)];
      const pos = positions.get(pk);
      if (!pos) continue;
      void client.publish('/risk', buildRiskRow(pos));
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

async function tradeLoop(client: Client) {
  const intervalMs = 1000 / TRADE_RATE_HZ;
  for (;;) {
    if (positionKeys.length === 0) {
      await new Promise((r) => setTimeout(r, intervalMs));
      continue;
    }
    // Sample an existing position uniformly at random.
    const pk = positionKeys[Math.floor(Math.random() * positionKeys.length)];
    const pos = positions.get(pk);
    if (!pos) {
      await new Promise((r) => setTimeout(r, intervalMs));
      continue;
    }
    const mkt = market.get(pos.cusip)!;
    const sec = SECURITIES.find((s) => s.cusip === pos.cusip)!;
    const side: 'BUY' | 'SELL' = Math.random() < 0.5 ? 'BUY' : 'SELL';
    const qty = Math.floor(rand(100, 3000)) * 1000;
    const price = side === 'BUY' ? mkt.ask : mkt.bid;

    void client.publish('/trades', {
      tradeId: newTradeId(),
      timestamp: new Date().toISOString(),
      cusip: pos.cusip,
      ticker: pos.ticker,
      side,
      qty,
      price: round(price, 4),
      notional: round((qty * price) / 100, 2),
      trader: pick(TRADERS),
      counterparty: pick(COUNTERPARTIES),
      book: pos.book,
      assetClass: sec.assetClass,
      sector: sec.sector,
    });

    applyTradeToPosition(pos, side, qty, price, mkt.mid);
    void client.publish('/positions', { positionKey: pk, ...pos });

    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

// ----- Main -----

async function main() {
  console.log(`Connecting to cqserver at ${URL}...`);
  const client = await Client.connect(URL);
  console.log(
    `Universe: ${SECURITIES.length} securities, ${BOOKS.length} books, target ${TARGET_POSITIONS} positions`,
  );

  const t0 = Date.now();
  await publishSecurities(client);
  await seedMarketData(client);
  await seedPositionsAndTrades(client);
  // Seed /risk from the just-built positions. Each position becomes a
  // nested risk row; the server flattens it to ~112 dotted columns.
  const posRows = [...positions.values()];
  await seedRisk(client, posRows);
  const seedSecs = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`Seed phase complete in ${seedSecs}s.`);

  console.log(
    `Streaming: ${TICK_RATE_HZ} ticks/sec, ${TRADE_RATE_HZ} trades/sec, ` +
      `risk refresh ${RISK_REFRESH_HZ}/sec × ${RISK_REFRESH_BATCH} rows`,
  );
  console.log('Ctrl-C to stop.');
  await Promise.all([tickLoop(client), tradeLoop(client), riskLoop(client)]);
}

main().catch((err) => {
  console.error('Publisher failed:', err);
  process.exit(1);
});
