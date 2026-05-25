// Tick engine — a tiny live-market simulator that mutates positions
// in place every ~700ms and occasionally appends synthetic trades.
// React components subscribe via `useLivePositions()` /
// `useLiveTrades()` (built on `useSyncExternalStore`) and re-render
// when the engine notifies.
//
// The engine is a module-level singleton: starting it has zero cost
// until the first subscriber arrives, and it self-pauses when the
// last subscriber unmounts. This keeps inactive tabs cheap.

import { useSyncExternalStore } from 'react';
import { generatePositions, generateTrades, type Position, type Trade } from './data-gen';
import { ALGOS, BROKERS, COUNTERPARTIES, VENUES, SIDES, ORDER_TYPES, TIF } from './refdata';

// ── Mutation helpers ───────────────────────────────────────────
function pick<T>(arr: readonly T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]!;
}

/**
 * Bump a single position's last price by a small normal-ish move and
 * recompute every dependent field so KPIs / PnL stay self-consistent.
 * Returns a *new* Position object (shallow clone with updates) so
 * AG-Grid's getRowId-driven row diff treats it as an update.
 */
function bumpPosition(p: Position): Position {
  // Box-Muller normal — most ticks are tiny; tails reach ~±2.5%.
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

  return {
    ...p,
    last_price: newPrice,
    last_price_local: newPrice,
    last_price_usd: newPrice * fx,
    price_change: newPrice - prevClose,
    price_change_pct: ((newPrice - prevClose) / Math.max(prevClose, 1e-6)) * 100,
    market_value: mvLocal,
    market_value_local: mvLocal,
    market_value_usd: mvUsd,
    unrealized_pnl: upnlLocal,
    unrealized_pnl_local: upnlLocal,
    unrealized_pnl_usd: upnlUsd,
    total_pnl: upnlUsd + ((p.realized_pnl_usd as number) || 0),
    day_pnl: ((p.day_pnl as number) || 0) + dayPnlDelta,
    last_updated_ts: new Date().toISOString().replace('Z', ''),
  };
}

let synthTradeCounter = 0;

/**
 * Synthesize a single new trade for a randomly chosen position. The
 * trade carries through every column expected by the trade schema —
 * we copy reference + instrument fields from the parent position and
 * fill execution / fee / lifecycle fields with believable randoms.
 */
function synthesizeTrade(p: Position): Trade {
  synthTradeCounter++;
  const id = `TRD-LIVE-${synthTradeCounter.toString().padStart(7, '0')}`;
  const nowMs = Date.now();
  const nowIso = new Date(nowMs).toISOString().replace('Z', '');
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
  const commission = notionalLocal * commBps / 10000;
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
    external_trade_id: `EXT-${(100000 + Math.floor(Math.random() * 899999))}`,
    block_trade_id: Math.random() < 0.25 ? `BLK-${(1000 + Math.floor(Math.random() * 8999))}` : '',
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
    mark_to_market: 0,
    mark_to_market_usd: 0,
    unrealized_pnl: 0,
    realized_pnl: 0,
    trade_pnl: -totalFeesUsd,
    fx_rate_at_trade: fx,
    fx_rate_at_settle: fx,
    commission,
    commission_bps: commBps,
    commission_per_share: commission / absQty,
    exchange_fee: exFee,
    clearing_fee: exFee * 0.5,
    settlement_fee: exFee * 0.3,
    regulatory_fee: notionalLocal * 5e-6,
    sec_fee: side === 'SELL' ? notionalLocal * 2.29e-5 : 0,
    taf_fee: absQty * 0.000119,
    ftt_tax: 0,
    stamp_duty: 0,
    financial_transaction_tax: 0,
    broker_markup: Math.random() * 100,
    broker_markdown: Math.random() * 50,
    soft_dollar_eligible: Math.random() < 0.4,
    total_fees: totalFeesLocal,
    total_fees_local: totalFeesLocal,
    total_fees_usd: totalFeesUsd,
    counterparty: pick(COUNTERPARTIES),
    counterparty_lei: '',
    counterparty_country: pick(['US', 'GB', 'DE', 'FR', 'JP', 'CH']),
    broker: broker.name,
    broker_lei: '',
    broker_country: pick(['US', 'GB', 'DE']),
    executing_dealer: broker.name,
    clearing_member: pick(['GS_CLR', 'MS_CLR', 'JPM_CLR']),
    give_up_broker: '',
    give_in_broker: '',
    settlement_agent: 'STATE_STREET',
    prime_broker: p.prime_broker,
    order_create_ts: nowIso,
    order_route_ts: nowIso,
    order_ack_ts: nowIso,
    order_open_ts: nowIso,
    first_fill_ts: nowIso,
    last_fill_ts: nowIso,
    order_done_ts: nowIso,
    trade_ts: nowIso,
    allocation_ts: nowIso,
    confirmation_ts: nowIso,
    settlement_ts: nowIso,
    last_event_ts: nowIso,
    status: 'FILLED',
    lifecycle_stage: 'EXECUTION',
    allocation_status: 'PENDING',
    confirmation_status: 'PENDING',
    matching_status: 'MATCHED',
    settlement_status: 'PENDING',
    clearing_status: 'PENDING',
    break_flag: false,
    break_reason: '',
    amendments_count: 0,
    cancellations_count: 0,
    partial_fill_count: 0,
    reject_count: 0,
    last_amendment_ts: '',
    last_cancellation_ts: '',
    block_quantity: absQty,
    allocated_quantity: absQty,
    unallocated_quantity: 0,
    allocation_method: 'AVERAGE_PRICE',
    allocation_count: 1,
    average_allocation_size: absQty,
    allocation_min: 0,
    allocation_max: absQty,
    allocation_currency: p.currency,
    allocation_fx_rate: fx,
    settle_date_actual: '',
    settle_date_expected: '',
    days_to_settle: 2,
    settlement_amount: notionalLocal + totalFeesLocal,
    settlement_amount_usd: notionalUsd + totalFeesUsd,
    settlement_currency_x: p.currency,
    settlement_account: '',
    dvp_flag: true,
    settlement_instructions: 'STANDARD',
    settlement_reference: '',
    mifid_flag: p.issuer_region === 'EMEA',
    mifid_decision_maker: p.trader_id,
    mifid_execution_decision_maker: p.trader_id,
    mifid_short_selling_indicator: side === 'SELL' ? 'SHORT' : 'NA',
    dodd_frank_swap_flag: false,
    emir_reporting_flag: p.issuer_region === 'EMEA',
    cat_reporting_flag: p.issuer_country === 'US',
    consolidated_audit_trail_id: '',
    lei_buy_side: '',
    lei_sell_side: '',
    uti: '',
    usi: '',
    portfolio_impact_usd: notionalUsd * 0.001 * sign,
    portfolio_impact_pct: 0.1 * sign,
    risk_increase_dv01: 0,
    risk_increase_var: 0,
    concentration_increase_pct: 0,
    correlation_to_book: 0.5,
    expected_market_impact_bps: 2 + Math.random() * 10,
    realized_market_impact_bps: 1 + Math.random() * 12,
    impact_decay_minutes: 10 + Math.floor(Math.random() * 120),
    crowdedness_score: Math.random(),
    liquidity_score: Math.random(),
    participation_30min_pct: Math.random() * 20,
    participation_eod_pct: Math.random() * 8,
    position_change_pct: (tradeQty / Math.max(1, (p.quantity as number) || 1)) * 100,
    is_position_increase: side === 'BUY',
    attribution_alpha: (Math.random() - 0.5) * 1000,
    attribution_beta: (Math.random() - 0.5) * 1500,
    attribution_currency: (Math.random() - 0.5) * 300,
    attribution_country: 0,
    attribution_sector: (Math.random() - 0.5) * 500,
    attribution_factor_value: 0,
    attribution_factor_momentum: 0,
    attribution_factor_quality: 0,
    attribution_factor_size: 0,
    attribution_residual: (Math.random() - 0.5) * 200,
    signal_id: '',
    signal_source: 'INTERNAL_RESEARCH',
    signal_strength: 0,
    research_note_id: '',
    recommendation_target: 0,
    recommendation_stop: 0,
    research_horizon_days: 0,
    signal_confidence: 0,
    order_origin: 'API',
    system_of_record: 'PROPRIETARY',
    trade_capture_system: 'INHOUSE_TCS',
    front_office_user: p.trader_id,
    middle_office_user: '',
    back_office_user: '',
    ops_notes: 'AUTO_GENERATED',
    compliance_review_status: 'APPROVED',
    compliance_reviewer: '',
    last_modified_by: p.trader_id,
    last_modified_ts: nowIso,
    version: 1,
  } as Trade;
}

// ── Engine singleton ────────────────────────────────────────────
class TickEngine {
  private positions: Position[];
  private trades: Trade[];
  private positionsSnap: Position[];
  private tradesSnap: Trade[];
  private posSubs = new Set<() => void>();
  private trdSubs = new Set<() => void>();
  private interval: ReturnType<typeof setInterval> | null = null;
  private tickCount = 0;

  constructor() {
    this.positions = generatePositions().map((p) => ({ ...p }));
    this.trades = generateTrades(this.positions);
    this.positionsSnap = [...this.positions];
    this.tradesSnap = this.trades;
  }

  subscribePositions = (cb: () => void): (() => void) => {
    this.posSubs.add(cb);
    this.maybeStart();
    return () => {
      this.posSubs.delete(cb);
      this.maybeStop();
    };
  };

  subscribeTrades = (cb: () => void): (() => void) => {
    this.trdSubs.add(cb);
    this.maybeStart();
    return () => {
      this.trdSubs.delete(cb);
      this.maybeStop();
    };
  };

  getPositionsSnapshot = (): Position[] => this.positionsSnap;
  getTradesSnapshot = (): Trade[] => this.tradesSnap;
  getTickCount = (): number => this.tickCount;

  private maybeStart() {
    if (this.interval == null && (this.posSubs.size + this.trdSubs.size) > 0) {
      this.interval = setInterval(() => this.step(), 750);
    }
  }

  private maybeStop() {
    if (this.interval != null && this.posSubs.size === 0 && this.trdSubs.size === 0) {
      clearInterval(this.interval);
      this.interval = null;
    }
  }

  private step() {
    this.tickCount += 1;
    // Bump ~10% of positions
    const n = Math.max(8, Math.floor(this.positions.length * 0.10));
    const touched = new Set<number>();
    for (let i = 0; i < n; i++) {
      const ix = Math.floor(Math.random() * this.positions.length);
      if (touched.has(ix)) continue;
      touched.add(ix);
      this.positions[ix] = bumpPosition(this.positions[ix]!);
    }
    this.positionsSnap = [...this.positions];

    // Append a fresh trade ~70% of ticks → roughly 1 every ~1s
    if (Math.random() < 0.7) {
      const parent = this.positions[Math.floor(Math.random() * this.positions.length)];
      if (parent) {
        const t = synthesizeTrade(parent);
        this.trades = [t, ...this.trades];
        this.tradesSnap = this.trades;
      }
    }

    for (const cb of this.posSubs) cb();
    for (const cb of this.trdSubs) cb();
  }
}

const engine = new TickEngine();

export function useLivePositions(): Position[] {
  return useSyncExternalStore(engine.subscribePositions, engine.getPositionsSnapshot);
}

export function useLiveTrades(): Trade[] {
  return useSyncExternalStore(engine.subscribeTrades, engine.getTradesSnapshot);
}
