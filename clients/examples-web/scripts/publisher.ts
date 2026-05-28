/**
 * Atlas publisher — emits the rich 200-column positions/trades universe
 * into cqserver, then ticks it server-side at ~2 Hz so every browser
 * subscriber sees live updates without any client-side simulation.
 *
 * Why this exists:
 *   The Atlas examples-web app subscribes to /positions, /trades,
 *   /securities and /fi-market-data over the cqserver WebSocket. The
 *   "live" feel of the dashboards comes entirely from this publisher —
 *   nothing in the browser mutates positions or invents trades.
 *
 * Pipeline:
 *   1. Generate the deterministic universe via data-gen (~480 positions
 *      and ~1900 historical trades drawn from the rich schema).
 *   2. Publish initial snapshots in chunks (the server stores the SOW).
 *   3. Tick loop: bump ~10% of positions every 500ms with Box-Muller
 *      price moves, publish updated rows; ~70% of ticks append a fresh
 *      synthesized trade.
 *
 * Bundled with esbuild → .demo-run/atlas-publisher.mjs and executed
 * with Node 24. See scripts/run-publisher.sh.
 */
import { Client } from '@cqserver/client';
import {
  generatePositions,
  generateTrades,
  type Position,
  type Trade,
} from '../src/lib/data-gen';
import {
  ALGOS, BROKERS, COUNTERPARTIES, VENUES, SIDES, ORDER_TYPES, TIF,
} from '../src/lib/refdata';

// ── Tunables ────────────────────────────────────────────────────
//
// The defaults below are tuned for "demonstrate cqserver throughput"
// rather than realism. At 50ms / 30% / 5 trades-per-tick:
//   ~480 positions × 0.30 ÷ 0.05s = ~2,880 position updates/sec
//   ~5 trades-per-tick ÷ 0.05s    = ~100 trades/sec
// cqserver conflates /positions at 50ms so the wire rate per row is
// capped, keeping the browser comfortable while still showcasing the
// fan-out the server is doing under the hood.
const CQ_URL = process.env.CQ_URL ?? 'tcp://127.0.0.1:9007';
const ADMIN_URL = process.env.CQ_ADMIN_URL ?? 'http://127.0.0.1:8085';
const TICK_INTERVAL_MS = Number(process.env.TICK_MS ?? 50);
const TICK_PCT = Number(process.env.TICK_PCT ?? 0.30);
const TRADES_PER_TICK = Number(process.env.TRADES_PER_TICK ?? 5);
// Size of the generated positions universe. Default 480 (snappy demo with
// a high tick rate); override e.g. `POSITIONS=40000` for a large SOW —
// pair a big universe with a low TICK_PCT so the tick volume stays sane.
const POSITION_COUNT = Number(process.env.POSITIONS ?? 480);
// Rows per `publishBatch` wire frame, and how many such frames to keep
// in flight. Each batch is ONE round-trip for `SEED_CHUNK` rows, so a
// large universe seeds in (rows / SEED_CHUNK / concurrency) round-trips
// instead of one per row.
const SEED_CHUNK = 100;
// Shallow pipeline: many large frames in flight over one TCP connection
// can deadlock (publisher blocks writing while the server blocks writing
// acks) → ack timeout. A small frame + low concurrency trickles but
// never stalls. High-throughput bulk seeding needs a dedicated server
// load path (see notes), not deeper client pipelining.
const SEED_CONCURRENCY = 4;

function pick<T>(arr: readonly T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]!;
}

// ── Position bump (price + dependent fields) ────────────────────
function bumpPosition(p: Position): Position {
  const u1 = Math.max(Math.random(), 1e-9);
  const u2 = Math.random();
  const z = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
  const drift = z * 0.0035;

  const prev = (p.last_price as number) || 0;
  const newPrice = Math.max(0.01, prev * (1 + drift));
  const fx = (p.fx_rate_local_usd as number) || 1;
  const qty = (p.quantity as number) || 0;
  const avgCost = (p.average_cost as number) || prev;
  const prevClose = (p.previous_close as number) || prev;
  const prevUPnLUsd = (p.unrealized_pnl_usd as number) || 0;

  const mvLocal = qty * newPrice;
  const mvUsd = mvLocal * fx;
  const upnlLocal = (newPrice - avgCost) * qty;
  const upnlUsd = upnlLocal * fx;
  const dayPnlDelta = upnlUsd - prevUPnLUsd;

  const priceChangePct = ((newPrice - prevClose) / Math.max(prevClose, 1e-6)) * 100;
  return {
    ...p,
    last_price: newPrice,
    last_price_local: newPrice,
    last_price_usd: newPrice * fx,
    price_change: newPrice - prevClose,
    price_change_pct: priceChangePct,
    market_value: mvLocal,
    market_value_local: mvLocal,
    market_value_usd: mvUsd,
    // Pre-shaped weighted-aggregate inputs. cqserver's aggregate
    // lattice doesn't accept arithmetic expressions inside SUM, so
    // any weighted-average view (e.g. /v_heatmap_sector_region) needs
    // the numerator and denominator as standalone columns. Done here
    // on the publisher so the React layer never reshapes.
    mv_x_pct: mvUsd * priceChangePct,
    mv_abs: Math.abs(mvUsd),
    unrealized_pnl: upnlLocal,
    unrealized_pnl_local: upnlLocal,
    unrealized_pnl_usd: upnlUsd,
    total_pnl: upnlUsd + ((p.realized_pnl_usd as number) || 0),
    day_pnl: ((p.day_pnl as number) || 0) + dayPnlDelta,
    last_updated_ts: new Date().toISOString(),
  };
}

let synthTradeCounter = 0;
function synthesizeTrade(p: Position): Trade {
  synthTradeCounter++;
  const id = `TRD-LIVE-${synthTradeCounter.toString().padStart(7, '0')}`;
  const nowMs = Date.now();
  const nowIso = new Date(nowMs).toISOString();
  const lastLocal = (p.last_price_local as number) || 1;
  const price = lastLocal * (0.997 + Math.random() * 0.006);
  const arrival = price * (0.998 + Math.random() * 0.004);
  const vwap = price * (0.997 + Math.random() * 0.006);
  const twap = price * (0.996 + Math.random() * 0.008);
  const close = price * (0.99 + Math.random() * 0.02);
  const side = pick(SIDES);
  const sign = side === 'BUY' || side === 'COVER' ? 1 : -1;
  const absQty = Math.max(1, Math.round(Math.random() * 2500));
  const tradeQty = absQty * sign;
  const fx = (p.fx_rate_local_usd as number) || 1;
  const notionalLocal = absQty * price;
  const notionalUsd = notionalLocal * fx;
  const commBps = 0.5 + Math.random() * 5;
  const commission = (notionalLocal * commBps) / 10000;
  const exFee = notionalLocal * 1e-5 * (0.6 + Math.random());
  const totalFeesLocal = commission + exFee;
  const totalFeesUsd = totalFeesLocal * fx;
  const broker = pick(BROKERS);

  return {
    trade_id: id,
    parent_order_id: `ORD-LIVE-${synthTradeCounter.toString().padStart(8, '0')}`,
    root_order_id: `ROOT-LIVE-${Math.floor(synthTradeCounter / 3).toString().padStart(7, '0')}`,
    position_id: p.position_id,
    account_id: p.account_id,
    portfolio_id: p.portfolio_id,
    book_id: p.book_id,
    book_name: p.book_name,
    trader_id: p.trader_id,
    trader_name: p.trader_name,
    strategy_id: p.strategy_id,
    external_trade_id: `EXT-${100000 + Math.floor(Math.random() * 899999)}`,
    block_trade_id: Math.random() < 0.25 ? `BLK-${1000 + Math.floor(Math.random() * 8999)}` : '',
    cusip: p.cusip,
    isin: p.isin,
    sedol: p.sedol,
    bbg_ticker: p.bbg_ticker,
    ric: p.ric,
    figi: p.figi,
    symbol: p.symbol,
    security_name: p.security_name,
    asset_class: p.asset_class,
    instrument_type: p.instrument_type,
    currency: p.currency,
    settlement_currency: p.currency,
    issuer: p.issuer,
    issuer_country: p.issuer_country,
    issuer_region: p.issuer_region,
    issuer_sector: p.issuer_sector,
    side,
    quantity: tradeQty,
    quantity_filled: absQty,
    quantity_open: 0,
    quantity_canceled: 0,
    price,
    price_arrival: arrival,
    price_close: close,
    price_vwap: vwap,
    price_twap: twap,
    price_market_on_open: arrival * 0.999,
    price_market_on_close: close,
    price_benchmark: vwap,
    slippage_arrival_bps: ((price - arrival) / arrival) * 10000 * sign,
    slippage_vwap_bps: ((price - vwap) / vwap) * 10000 * sign,
    slippage_twap_bps: ((price - twap) / twap) * 10000 * sign,
    slippage_close_bps: ((price - close) / close) * 10000 * sign,
    fill_count: 1 + Math.floor(Math.random() * 20),
    fill_first_ts: nowIso,
    fill_last_ts: nowIso,
    fill_duration_ms: 50 + Math.floor(Math.random() * 4000),
    execution_venue: pick(VENUES),
    execution_algo: pick(ALGOS),
    algo_aggressiveness: Math.random(),
    algo_horizon_min: 1 + Math.floor(Math.random() * 120),
    order_type: pick(ORDER_TYPES),
    time_in_force: pick(TIF),
    price_limit: price * (0.99 + Math.random() * 0.02),
    participation_pct: 0.5 + Math.random() * 20,
    dark_pool_pct: Math.random() * 40,
    notional: notionalLocal,
    notional_local: notionalLocal,
    notional_usd: notionalUsd,
    gross_value: notionalLocal,
    net_value: notionalLocal - totalFeesLocal,
    commission,
    commission_bps: commBps,
    commission_per_share: commission / absQty,
    exchange_fee: exFee,
    clearing_fee: exFee * 0.5,
    settlement_fee: exFee * 0.3,
    regulatory_fee: notionalLocal * 5e-6,
    sec_fee: side === 'SELL' ? notionalLocal * 2.29e-5 : 0,
    taf_fee: absQty * 0.000119,
    broker: broker.name,
    counterparty: pick(COUNTERPARTIES),
    counterparty_country: pick(['US', 'GB', 'DE', 'FR', 'JP', 'CH']),
    total_fees: totalFeesLocal,
    total_fees_local: totalFeesLocal,
    total_fees_usd: totalFeesUsd,
    fx_rate_at_trade: fx,
    fx_rate_at_settle: fx,
    order_create_ts: nowIso,
    order_route_ts: nowIso,
    order_ack_ts: nowIso,
    order_open_ts: nowIso,
    first_fill_ts: nowIso,
    last_fill_ts: nowIso,
    order_done_ts: nowIso,
    trade_ts: nowIso,
    last_event_ts: nowIso,
    status: 'FILLED',
    lifecycle_stage: 'EXECUTION',
    matching_status: 'MATCHED',
    settlement_status: 'PENDING',
    portfolio_impact_usd: notionalUsd * 0.001 * sign,
    portfolio_impact_pct: 0.1 * sign,
    expected_market_impact_bps: 2 + Math.random() * 10,
    realized_market_impact_bps: 1 + Math.random() * 12,
    impact_decay_minutes: 10 + Math.floor(Math.random() * 120),
    crowdedness_score: Math.random(),
    liquidity_score: Math.random(),
    participation_30min_pct: Math.random() * 20,
    participation_eod_pct: Math.random() * 8,
    is_position_increase: side === 'BUY',
    attribution_alpha: (Math.random() - 0.5) * 1000,
    attribution_beta: (Math.random() - 0.5) * 1500,
    attribution_currency: (Math.random() - 0.5) * 300,
    attribution_sector: (Math.random() - 0.5) * 500,
    attribution_residual: (Math.random() - 0.5) * 200,
    signal_source: 'INTERNAL_RESEARCH',
    order_origin: 'API',
    system_of_record: 'PROPRIETARY',
    trade_capture_system: 'INHOUSE_TCS',
    front_office_user: p.trader_id,
    ops_notes: 'AUTO_GENERATED',
    compliance_review_status: 'APPROVED',
    last_modified_by: p.trader_id,
    last_modified_ts: nowIso,
    version: 1,
  } as Trade;
}

// ── Bounded-concurrency chunk publish ───────────────────────────
async function publishChunked<T extends Record<string, unknown>>(
  client: Client,
  topic: string,
  rows: T[],
  concurrency = SEED_CONCURRENCY,
  chunkSize = SEED_CHUNK,
): Promise<void> {
  // Slice into batch frames; `publishBatch` commits each frame in one
  // wire round-trip (vs one per row), so throughput is bounded by
  // round-trips, not row count — the difference between a multi-minute
  // and a sub-second seed for a large universe.
  const batches: T[][] = [];
  for (let i = 0; i < rows.length; i += chunkSize) {
    batches.push(rows.slice(i, i + chunkSize));
  }
  let next = 0;
  let published = 0;
  const worker = async (): Promise<void> => {
    while (next < batches.length) {
      const batch = batches[next++]!;
      await client.publishBatch(topic, batch);
      published += batch.length;
      process.stdout.write(`  ${topic} ${published}/${rows.length}\r`);
    }
  };
  const workers = Array.from(
    { length: Math.min(concurrency, batches.length) },
    () => worker(),
  );
  await Promise.all(workers);
  process.stdout.write(`  ${topic} ${published}/${rows.length}\n`);
}

/** Probe a topic's row count via the admin HTTP endpoint. Returns 0
 *  on any failure (admin unreachable, topic absent) — caller treats
 *  that as "must seed." */
async function topicRowCount(name: string): Promise<number> {
  try {
    const r = await fetch(`${ADMIN_URL}/topics`);
    if (!r.ok) return 0;
    const arr = (await r.json()) as Array<{ name: string; rowCount: number }>;
    return arr.find((t) => t.name === name)?.rowCount ?? 0;
  } catch {
    return 0;
  }
}

// ── Main ────────────────────────────────────────────────────────
async function main(): Promise<void> {
  console.log(`[atlas-publisher] connecting → ${CQ_URL}`);
  // Generous ack timeout: a large seed keeps many batches in flight, so
  // a frame can wait behind others on the connection before its ack.
  const client = await Client.connect(CQ_URL, { ackTimeoutMs: 60_000 });

  // Skip the seed entirely when the topic already has rows (persisted
  // data restored from the txlog on cqserver restart). RESEED=1 forces
  // a re-seed regardless — paired with the start script's txlog wipe.
  const reseed = process.env.RESEED === '1';
  const existingPositions = await topicRowCount('/positions');
  const needSeed = reseed || existingPositions === 0;

  let positions: Position[] = [];
  if (needSeed) {
    console.log('[atlas-publisher] generating universe ...');
    positions = generatePositions(POSITION_COUNT).map((p) => ({ ...p }));
    // Trades scale with positions (~4 each by default → 160k+ at a
    // large universe). TRADES_PER_POSITION lets a big run pick a
    // realistic ratio without overrunning the publisher.
    const trades = generateTrades(positions, Number(process.env.TRADES_PER_POSITION ?? 4));

    console.log(
      `[atlas-publisher] seeding /positions (${positions.length}) and /trades (${trades.length}) ...`,
    );
    await publishChunked(client, '/positions', positions);
    await publishChunked(client, '/trades', trades);
  } else {
    console.log(
      `[atlas-publisher] /positions already has ${existingPositions} rows (persisted) — skipping seed.`,
    );
    // Regenerate the same deterministic universe so the tick loop has
    // the row identifiers it needs to bump existing rows (generators
    // use fixed seeds → same positions/trades the txlog holds).
    positions = generatePositions(POSITION_COUNT).map((p) => ({ ...p }));
    void generateTrades(positions, Number(process.env.TRADES_PER_POSITION ?? 4));
  }

  console.log(
    `[atlas-publisher] tick loop: ${TICK_INTERVAL_MS}ms, ${(TICK_PCT * 100).toFixed(0)}% positions per tick, ${TRADES_PER_TICK} trades per tick`,
  );
  // High-rate publishers can't afford per-publish ack tracking — at
  // ~3000 publishes/sec, the pending-RPC map fills faster than the
  // server can drain it and every ack times out 30s later, cascading
  // into a stuck loop. `publishUnacked` sends the frame without
  // registering it for ack resolution; the server's ack still
  // arrives but dispatch silently drops the unknown cid.
  let publishErrors = 0;
  let nextErrorLog = Date.now() + 5_000;
  // Returns a promise that resolves once the transport has accepted the
  // frame for delivery (the TCP write callback fired). Awaiting it in the
  // tick loop is what gives us write-level backpressure — if the socket
  // is backed up, the next tick is naturally delayed instead of piling
  // unflushed writes (and their closures) onto the heap until V8 OOMs.
  // Note: this still does NOT wait for the server ack, so it doesn't
  // reintroduce the pending-RPC pileup that publishUnacked exists to avoid.
  const safePublish = (topic: string, data: Record<string, unknown>): Promise<void> =>
    client.publishUnacked(topic, data).catch((err: unknown) => {
      publishErrors++;
      if (Date.now() >= nextErrorLog) {
        console.warn(
          `[atlas-publisher] ${publishErrors} publish failure(s) in last 5s; latest:`,
          err instanceof Error ? err.message : err,
        );
        publishErrors = 0;
        nextErrorLog = Date.now() + 5_000;
      }
    });

  // Stop the loop the moment the connection drops, and exit non-zero so a
  // supervisor can relaunch — previously the loop kept firing into a dead
  // socket, going silent with only a 5s-throttled warning and never
  // recovering.
  let stopped = false;
  client.onClose(() => {
    stopped = true;
    console.error('[atlas-publisher] connection closed — exiting (exit 1) for relaunch');
    process.exit(1);
  });

  const tick = async (): Promise<void> => {
    const startedAt = Date.now();
    const pending: Array<Promise<void>> = [];
    const n = Math.max(8, Math.floor(positions.length * TICK_PCT));
    const touched = new Set<number>();
    for (let i = 0; i < n; i++) {
      const ix = Math.floor(Math.random() * positions.length);
      if (touched.has(ix)) continue;
      touched.add(ix);
      const bumped = bumpPosition(positions[ix]!);
      positions[ix] = bumped;
      pending.push(safePublish('/positions', bumped as Record<string, unknown>));
    }
    for (let i = 0; i < TRADES_PER_TICK; i++) {
      const parent = positions[Math.floor(Math.random() * positions.length)];
      if (!parent) break;
      const t = synthesizeTrade(parent);
      // Intentionally not retained: the seeded `trades` array is never read
      // after the initial snapshot, so appending here is a pure unbounded
      // leak (O(n) unshift, ~100 rows/sec). The row is already published.
      pending.push(safePublish('/trades', t as Record<string, unknown>));
    }
    // Backpressure: wait for this tick's writes to flush before scheduling
    // the next one, then keep cadence by subtracting the time spent.
    await Promise.all(pending);
    if (stopped) return;
    const elapsed = Date.now() - startedAt;
    setTimeout(() => void tick(), Math.max(0, TICK_INTERVAL_MS - elapsed));
  };
  void tick();

  // Belt-and-suspenders: don't let an orphan rejection slip through
  // (e.g. SDK internals throwing before the publish Promise is
  // returned). Treat them the same way as publish failures.
  process.on('unhandledRejection', (err) => {
    publishErrors++;
    if (Date.now() >= nextErrorLog) {
      console.warn(
        `[atlas-publisher] unhandledRejection swallowed:`,
        err instanceof Error ? err.message : err,
      );
      nextErrorLog = Date.now() + 5_000;
    }
  });

  const shutdown = async () => {
    console.log('\n[atlas-publisher] shutting down');
    await client.close();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('[atlas-publisher] fatal:', err);
  process.exit(1);
});
