/// <reference types="node" />
/**
 * Generates JSON files for each FI demo table.
 *
 * Writes (under examples/data/):
 *   - securities.json       — ~500 instruments
 *   - fi-market-data.json   — one mid/bid/ask per cusip
 *   - positions.json        — 40,000 (book, cusip) positions with P&L
 *   - trades.json           — synthetic fills that explain each position
 *                             (1–4 per position, ~100k total)
 *
 * The data is internally consistent:
 *   - Each row in positions.json corresponds to one (book, cusip) cell
 *   - The trades.json rows that share that (book, cusip) sum to the
 *     position's netQty, with the position's avgCost being the weighted
 *     average of their fill prices on adds
 *   - fi-market-data.json provides the lastMid each position is marked
 *     against, so marketValue = netQty * mid / 100 holds row-by-row
 *
 * Tuning (env vars):
 *   TARGET_POSITIONS     default 40000
 *   TARGET_SECURITIES    default 500
 *   TARGET_BOOKS         default ceil(TARGET_POSITIONS / TARGET_SECURITIES)
 *   MIN_TRADES_PER_POS   default 1
 *   MAX_TRADES_PER_POS   default 4
 *   OUTPUT_DIR           default ./examples/data (relative to cwd)
 *
 * Run with: `npm run generate-fi-data`
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import {
  buildSecurities,
  buildBooks,
  TRADERS,
  COUNTERPARTIES,
  type Security,
} from './fi-data.js';

const TARGET_POSITIONS = Number(process.env.TARGET_POSITIONS ?? 40_000);
const TARGET_SECURITIES = Number(process.env.TARGET_SECURITIES ?? 500);
const TARGET_BOOKS = Number(
  process.env.TARGET_BOOKS ?? Math.ceil(TARGET_POSITIONS / TARGET_SECURITIES),
);
const MIN_TRADES_PER_POS = Number(process.env.MIN_TRADES_PER_POS ?? 1);
const MAX_TRADES_PER_POS = Number(process.env.MAX_TRADES_PER_POS ?? 4);
const OUTPUT_DIR =
  process.env.OUTPUT_DIR ?? path.join(process.cwd(), 'examples', 'data');

interface PositionRow {
  positionKey: string;
  book: string;
  cusip: string;
  ticker: string;
  netQty: number;
  avgCost: number;
  lastMid: number;
  marketValue: number;
  unrealizedPnl: number;
  trades: number;
}

interface MarketRow {
  cusip: string;
  ticker: string;
  assetClass: Security['assetClass'];
  sector: string;
  bid: number;
  ask: number;
  mid: number;
  yieldPct: number;
  timestamp: string;
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
}

const pick = <T>(arr: readonly T[]): T => arr[Math.floor(Math.random() * arr.length)];
const rand = (min: number, max: number) => min + Math.random() * (max - min);
const randInt = (min: number, max: number) => Math.floor(rand(min, max + 1));
const round = (n: number, dp: number) => Math.round(n * 10 ** dp) / 10 ** dp;

function yearsToMaturity(maturityISO: string): number {
  const now = new Date();
  const mat = new Date(maturityISO);
  return Math.max(0.01, (mat.getTime() - now.getTime()) / (365.25 * 24 * 3600 * 1000));
}

function yieldFromPrice(coupon: number, price: number, yrs: number): number {
  if (yrs <= 0) return coupon;
  return coupon + (100 - price) / yrs;
}

function buildMarketRows(securities: Security[]): Map<string, MarketRow> {
  const ts = new Date().toISOString();
  const out = new Map<string, MarketRow>();
  for (const sec of securities) {
    const spreadBp =
      sec.assetClass === 'UST' ? 0.5 : sec.assetClass === 'AGENCY' ? 1.0 : 2.5;
    const halfSpread = (sec.initialMid * spreadBp) / 10000;
    out.set(sec.cusip, {
      cusip: sec.cusip,
      ticker: sec.ticker,
      assetClass: sec.assetClass,
      sector: sec.sector,
      bid: round(sec.initialMid - halfSpread, 4),
      ask: round(sec.initialMid + halfSpread, 4),
      mid: sec.initialMid,
      yieldPct: round(
        yieldFromPrice(sec.coupon, sec.initialMid, yearsToMaturity(sec.maturity)),
        4,
      ),
      timestamp: ts,
    });
  }
  return out;
}

let nextTradeId = 1;
function newTradeId(): string {
  return `T${String(nextTradeId++).padStart(10, '0')}`;
}

function buildPositionsAndTrades(
  securities: Security[],
  books: string[],
  marketByCusip: Map<string, MarketRow>,
): { positions: PositionRow[]; trades: TradeRow[] } {
  const positions: PositionRow[] = [];
  const trades: TradeRow[] = [];
  const baseTime = Date.now() - 60 * 60 * 1000; // backdate seed trades by ~1h

  // book-major pairing so the first ~N securities populate the first book
  // before moving on. Truncated at TARGET_POSITIONS.
  outer: for (const book of books) {
    for (const sec of securities) {
      if (positions.length >= TARGET_POSITIONS) break outer;
      const mkt = marketByCusip.get(sec.cusip)!;

      let netQty = 0;
      let avgCost = 0;
      let tradeCount = 0;

      const nTrades = randInt(MIN_TRADES_PER_POS, MAX_TRADES_PER_POS);
      const longBias = Math.random() < 0.6;

      const applyFill = (side: 'BUY' | 'SELL', qty: number, price: number) => {
        const signedQty = side === 'BUY' ? qty : -qty;
        const newQty = netQty + signedQty;
        const addingTo =
          (netQty >= 0 && signedQty > 0) || (netQty <= 0 && signedQty < 0);
        if (addingTo && Math.abs(newQty) > 0.0001) {
          avgCost = round((avgCost * netQty + price * signedQty) / newQty, 4);
        }
        netQty = newQty;
        tradeCount += 1;
      };

      for (let t = 0; t < nTrades; t++) {
        const directional = Math.random() < 0.7;
        const side: 'BUY' | 'SELL' =
          (directional && longBias) || (!directional && !longBias) ? 'BUY' : 'SELL';
        const qty = Math.floor(rand(100, 5000)) * 1000;
        const priceJitter = rand(-0.4, 0.4);
        const price = round(
          side === 'BUY' ? mkt.ask + priceJitter : mkt.bid + priceJitter,
          4,
        );
        const ts = new Date(
          baseTime + Math.floor(rand(0, 60 * 60 * 1000)),
        ).toISOString();
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
        applyFill(side, qty, price);
      }

      // Force non-zero netQty so the position is non-empty in the demo.
      if (netQty === 0) {
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
        applyFill(side, qty, price);
      }

      const mid = mkt.mid;
      positions.push({
        positionKey: `${book}|${sec.cusip}`,
        book,
        cusip: sec.cusip,
        ticker: sec.ticker,
        netQty,
        avgCost,
        lastMid: mid,
        marketValue: round((netQty * mid) / 100, 2),
        unrealizedPnl: round(((mid - avgCost) * netQty) / 100, 2),
        trades: tradeCount,
      });
    }
  }
  return { positions, trades };
}

function writeJson(filename: string, data: unknown): { bytes: number; rows: number } {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const outPath = path.join(OUTPUT_DIR, filename);
  const json = JSON.stringify(data, null, 0);
  fs.writeFileSync(outPath, json);
  const rows = Array.isArray(data) ? data.length : 1;
  return { bytes: json.length, rows };
}

function main() {
  const t0 = Date.now();

  console.log(
    `Generating universe: ${TARGET_SECURITIES} securities × ${TARGET_BOOKS} books (target ${TARGET_POSITIONS} positions)`,
  );
  const securities = buildSecurities(TARGET_SECURITIES);
  const books = buildBooks(TARGET_BOOKS);
  const marketByCusip = buildMarketRows(securities);
  const { positions, trades } = buildPositionsAndTrades(securities, books, marketByCusip);
  const market = Array.from(marketByCusip.values());

  const files: Array<[string, unknown]> = [
    ['securities.json', securities],
    ['fi-market-data.json', market],
    ['positions.json', positions],
    ['trades.json', trades],
  ];

  console.log(`Writing to ${OUTPUT_DIR}`);
  for (const [name, data] of files) {
    const { bytes, rows } = writeJson(name, data);
    const mb = (bytes / (1024 * 1024)).toFixed(2);
    console.log(`  ${name.padEnd(24)}  ${String(rows).padStart(7)} rows  ${mb.padStart(7)} MB`);
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(2);
  console.log(`Done in ${elapsed}s.`);
}

main();
