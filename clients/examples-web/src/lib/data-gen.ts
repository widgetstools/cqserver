// Data generator — produces deterministic positions + related trades
// using the seeded Rng. Called once at app load, cached in memory.
//
// Realism principles:
//   - Bond positions get coupon/duration/rating/cs01; equities get
//     greeks-as-zero, vol, beta. Option-style fields are populated
//     only when asset_class == OPTION (we synthesize a slice of
//     options out of the equity universe so the option-specific
//     columns aren't always null).
//   - Each position gets 1..6 trades, all sharing position_id, with
//     side-aware quantity so the position quantity ≈ sum(trade qty).
//   - PnL columns reconcile: total = unrealized + realized; usd =
//     local * fx_rate; mtd/qtd/ytd are decay fractions of total.

import { Rng } from './rng';
import {
  ISSUERS, BOOKS, TRADERS, VENUES, ALGOS, BROKERS,
  COUNTERPARTIES, CUSTODIANS, RATINGS_SP, RATINGS_MOODY,
  ESG_GRADES, DURATION_BUCKETS, LIQUIDITY_TIERS,
  SIDES, ORDER_TYPES, TIF, TRADE_STATUSES, LIFECYCLE_STAGES,
  FX, type IssuerRef,
} from './refdata';

export interface Position { [key: string]: unknown; position_id: string }
export interface Trade { [key: string]: unknown; trade_id: string; position_id: string }

// ── helpers ────────────────────────────────────────────────────
const dayMs = 86_400_000;
const now = Date.UTC(2026, 4, 22, 14, 0, 0); // fixed clock → deterministic ts

function pad(n: number, w: number) { return n.toString().padStart(w, '0'); }
function dateStr(ts: number) { return new Date(ts).toISOString().slice(0, 10); }
// Canonical RFC 3339 with trailing `Z` — required by cqserver's native
// timestamp type so the JSON-string parse keeps the UTC anchor explicit.
// The trailing-Z-strip used in earlier versions was a workaround for a
// legacy schema that treated *_ts columns as plain strings.
function dateTimeStr(ts: number) { return new Date(ts).toISOString(); }

function priceForAsset(rng: Rng, ac: string): number {
  switch (ac) {
    case 'EQUITY': return rng.uniform(8, 1200);
    case 'GOVT_BOND': return rng.uniform(85, 108);
    case 'CORP_BOND': return rng.uniform(70, 112);
    case 'OPTION': return rng.uniform(0.1, 45);
    case 'FUTURE': return rng.uniform(60, 5000);
    case 'SWAP': return 100;
    case 'FX': return 1;
    case 'REPO': return 100;
    default: return 100;
  }
}

function quantityForAsset(rng: Rng, ac: string): number {
  switch (ac) {
    case 'EQUITY': return Math.round(rng.uniform(50, 80_000));
    case 'GOVT_BOND': return Math.round(rng.uniform(100_000, 25_000_000) / 1000) * 1000;
    case 'CORP_BOND': return Math.round(rng.uniform(100_000, 8_000_000) / 1000) * 1000;
    case 'OPTION': return Math.round(rng.uniform(1, 500)) * 100;
    case 'FUTURE': return Math.round(rng.uniform(1, 200));
    case 'SWAP': return Math.round(rng.uniform(1_000_000, 100_000_000) / 1_000_000) * 1_000_000;
    default: return 1000;
  }
}

function ratingComposite(sp: string, moody: string): string {
  // Greatly simplified — return the better of the two by index.
  const spIx = RATINGS_SP.indexOf(sp as typeof RATINGS_SP[number]);
  const mdyIx = RATINGS_MOODY.indexOf(moody as typeof RATINGS_MOODY[number]);
  return Math.min(spIx, mdyIx) <= 7 ? sp : moody;
}

function ratingGrade(r: string): string {
  const i = RATINGS_SP.indexOf(r as typeof RATINGS_SP[number]);
  if (i >= 0) return i <= 7 ? 'INVESTMENT_GRADE' : 'HIGH_YIELD';
  return 'NOT_RATED';
}

// ── positions ──────────────────────────────────────────────────
export function generatePositions(count = 480, seed = 0x42abcd): Position[] {
  const rng = new Rng(seed);
  const positions: Position[] = [];

  for (let i = 0; i < count; i++) {
    // Pick an issuer; small fraction are synthesized as options on equities.
    let issuer: IssuerRef = rng.pick(ISSUERS);
    let isOption = false;
    if (issuer.asset_class === 'EQUITY' && rng.chance(0.18)) {
      isOption = true;
    }

    const ac = isOption ? 'OPTION' : issuer.asset_class;
    const isBond = ac === 'GOVT_BOND' || ac === 'CORP_BOND';
    const isOpt = ac === 'OPTION';

    const book = rng.pick(BOOKS);
    const trader = rng.pick(TRADERS);

    const qty = quantityForAsset(rng, ac) * (rng.chance(0.18) ? -1 : 1);
    const lastLocal = priceForAsset(rng, ac);
    const avgCostLocal = lastLocal * rng.uniform(0.85, 1.18);
    const prevClose = lastLocal * rng.uniform(0.97, 1.03);
    const openPrice = prevClose * rng.uniform(0.995, 1.005);
    const fx = FX[issuer.ccy] ?? 1.0;
    const lastUsd = lastLocal * fx;
    const avgCostUsd = avgCostLocal * fx;

    const notionalLocal = Math.abs(qty) * lastLocal;
    const notionalUsd = notionalLocal * fx;
    const mvLocal = qty * lastLocal;
    const mvUsd = mvLocal * fx;
    const costBasisLocal = qty * avgCostLocal;
    const costBasisUsd = costBasisLocal * fx;

    const uPnLLocal = (lastLocal - avgCostLocal) * qty;
    const rPnLLocal = uPnLLocal * rng.uniform(-0.15, 0.4);
    const uPnLUsd = uPnLLocal * fx;
    const rPnLUsd = rPnLLocal * fx;
    const totalPnL = uPnLUsd + rPnLUsd;
    const dayPnL = totalPnL * rng.uniform(0.01, 0.12) * (rng.chance(0.5) ? 1 : -1);

    const sp = rng.pick(RATINGS_SP);
    const moody = rng.pick(RATINGS_MOODY);
    const fitch = rng.pick(RATINGS_SP);
    const compR = ratingComposite(sp, moody);
    const grade = ratingGrade(compR);

    const ratingIx = RATINGS_SP.indexOf(sp);
    const csBps = isBond ? rng.uniform(20, 600) + ratingIx * 35 : rng.uniform(50, 280);
    const cds5y = csBps * rng.uniform(0.7, 1.3);

    const tradeTs = now - rng.int(0, 540) * dayMs;
    const settleTs = tradeTs + (isBond ? 2 * dayMs : 2 * dayMs);
    const issueTs = isBond ? now - rng.int(180, 8 * 365) * dayMs : tradeTs;
    const matTs = isBond
      ? issueTs + rng.int(2, 30) * 365 * dayMs
      : isOpt ? now + rng.int(7, 540) * dayMs
      : issueTs + 30 * 365 * dayMs;

    const expiryTs = isOpt ? now + rng.int(7, 540) * dayMs : matTs;
    const dte = isOpt ? Math.round((expiryTs - now) / dayMs) : 0;

    const strike = isOpt ? lastLocal * rng.uniform(0.6, 1.4) : 0;
    const moneyness = isOpt ? lastLocal / strike : 0;
    const iv = isOpt ? rng.uniform(15, 80) : 0;
    const atmVol = isOpt ? rng.uniform(15, 60) : 0;

    const delta = isOpt ? rng.uniform(-1, 1) : (ac === 'EQUITY' ? 1 : 0);
    const gamma = isOpt ? rng.uniform(0, 0.3) : 0;
    const vega = isOpt ? rng.uniform(0, 5) : 0;
    const theta = isOpt ? rng.uniform(-2, 0) : 0;
    const rho = isOpt ? rng.uniform(-1, 1) * 0.5 : (isBond ? rng.uniform(-15, 15) : 0);

    const modDur = isBond ? rng.uniform(0.3, 14.5) : 0;
    const macDur = modDur * 1.02;
    const convex = isBond ? modDur * modDur * 0.012 : 0;
    const dv01 = isBond ? -mvUsd * modDur * 0.0001 : 0;
    const cs01Usd = isBond ? -mvUsd * 0.0001 * rng.uniform(0.6, 4.5) : 0;

    const var1d95 = Math.abs(mvUsd) * rng.uniform(0.01, 0.06);
    const var1d99 = var1d95 * rng.uniform(1.35, 1.6);
    const var10d95 = var1d95 * Math.sqrt(10);
    const var10d99 = var1d99 * Math.sqrt(10);

    const exposureGross = Math.abs(mvUsd);
    const exposureNet = mvUsd;
    const exposureLong = mvUsd > 0 ? mvUsd : 0;
    const exposureShort = mvUsd < 0 ? -mvUsd : 0;

    const sectorPct = rng.uniform(0.5, 8);
    const ctyPct = rng.uniform(0.3, 12);
    const ccyPct = rng.uniform(0.2, 22);

    const durBucket = isBond ? rng.pick(DURATION_BUCKETS) : '0-1Y';
    const matBucket = isBond ? durBucket : isOpt ? (dte < 30 ? '0-1Y' : '1-3Y') : '30Y+';
    const liqTier = rng.pick(LIQUIDITY_TIERS);

    const breachCount = rng.int(0, 4);
    const limitUtilPct = rng.uniform(15, 105);
    const compliance = breachCount > 2 ? 'BREACH' : breachCount > 0 ? 'WARNING' : 'CLEAR';

    const positionId = `POS-${pad(2026, 4)}${pad(i + 1, 5)}`;

    const p: Position = {
      // identifiers
      position_id: positionId,
      parent_position_id: rng.chance(0.05) ? `POS-${pad(2026, 4)}${pad(rng.int(1, count), 5)}` : '',
      account_id: `ACC-${pad(rng.int(100, 999), 3)}`,
      account_name: `Fund ${rng.pick(['Alpha', 'Beta', 'Gamma', 'Delta', 'Theta', 'Sigma', 'Omega'])}-${rng.int(1, 12)}`,
      portfolio_id: `PF-${pad(rng.int(1, 80), 3)}`,
      portfolio_name: `${book.name} Portfolio`,
      book_id: book.id,
      book_name: book.name,
      desk: book.desk,
      trader_id: trader.id,
      trader_name: trader.name,
      strategy_id: book.strategy,
      strategy_name: `${book.strategy}-${pad(rng.int(1, 12), 2)}`,

      // security
      cusip: issuer.cusip,
      isin: issuer.isin,
      sedol: issuer.sedol,
      bbg_ticker: issuer.bbg,
      ric: issuer.ric,
      figi: issuer.figi,
      lei: issuer.lei,
      symbol: isOpt ? `${issuer.symbol} ${dateStr(expiryTs).slice(2).replace(/-/g, '')} ${rng.chance(0.5) ? 'C' : 'P'} ${strike.toFixed(0)}` : issuer.symbol,
      security_name: isOpt ? `${issuer.name} OPTION` : issuer.name,
      issuer: issuer.name,
      issuer_country: issuer.country,
      issuer_region: issuer.region,
      issuer_sector: issuer.sector,
      issuer_industry: issuer.industry,
      asset_class: ac,
      instrument_type: isOpt ? 'LISTED_OPTION' : isBond ? 'BOND' : ac === 'EQUITY' ? 'COMMON_STOCK' : ac,
      underlying_id: isOpt ? issuer.cusip : '',
      underlying_symbol: isOpt ? issuer.symbol : '',
      currency: issuer.ccy,
      settlement_currency: issuer.ccy,
      trading_venue: issuer.exchange,
      listing_venue: issuer.exchange,
      listing_country: issuer.country,
      issue_date: dateStr(issueTs),
      maturity_date: dateStr(matTs),
      coupon_type: isBond ? rng.pick(['FIXED', 'FLOATING', 'STEP_UP', 'ZERO']) : 'NONE',
      coupon_rate: isBond ? rng.uniform(0.5, 8.5) : 0,
      coupon_freq: isBond ? rng.pick(['SEMI_ANNUAL', 'ANNUAL', 'QUARTERLY']) : 'NONE',
      day_count_convention: isBond ? rng.pick(['ACT_360', 'ACT_365', '30_360', 'ACT_ACT']) : 'NONE',
      callable_flag: isBond && rng.chance(0.3),
      putable_flag: isBond && rng.chance(0.1),
      convertible_flag: isBond && rng.chance(0.05),
      inflation_linked: isBond && rng.chance(0.08),

      // rating
      rating_sp: isBond ? sp : '',
      rating_moody: isBond ? moody : '',
      rating_fitch: isBond ? fitch : '',
      rating_composite: isBond ? compR : '',
      rating_grade: isBond ? grade : '',
      credit_curve_id: isBond ? `CRV-${issuer.symbol}-${issuer.ccy}` : '',
      credit_spread_bps: isBond ? csBps : 0,
      cds_5y_bps: isBond ? cds5y : 0,
      default_probability_1y: isBond ? csBps / 10000 : 0,
      recovery_rate: isBond ? rng.uniform(0.30, 0.55) : 0,

      // qty / value
      quantity: qty,
      quantity_long: qty > 0 ? qty : 0,
      quantity_short: qty < 0 ? -qty : 0,
      quantity_t_minus_1: qty + rng.int(-1000, 1000),
      quantity_change: rng.int(-2000, 2000),
      opening_price: openPrice,
      last_price: lastLocal,
      last_price_local: lastLocal,
      last_price_usd: lastUsd,
      previous_close: prevClose,
      price_change: lastLocal - prevClose,
      price_change_pct: ((lastLocal - prevClose) / prevClose) * 100,
      average_cost: avgCostLocal,
      average_cost_local: avgCostLocal,
      average_cost_usd: avgCostUsd,
      notional: notionalLocal,
      notional_local: notionalLocal,
      notional_usd: notionalUsd,
      market_value: mvLocal,
      market_value_local: mvLocal,
      market_value_usd: mvUsd,
      mv_x_pct: mvUsd * (((lastLocal - prevClose) / prevClose) * 100),
      mv_abs: Math.abs(mvUsd),
      cost_basis: costBasisLocal,
      cost_basis_local: costBasisLocal,
      cost_basis_usd: costBasisUsd,
      nav_pct: Math.abs(mvUsd) / 100_000_000 * 100,

      // pnl
      unrealized_pnl: uPnLLocal,
      unrealized_pnl_local: uPnLLocal,
      unrealized_pnl_usd: uPnLUsd,
      realized_pnl: rPnLLocal,
      realized_pnl_local: rPnLLocal,
      realized_pnl_usd: rPnLUsd,
      total_pnl: totalPnL,
      day_pnl: dayPnL,
      mtd_pnl: totalPnL * rng.uniform(0.05, 0.30),
      qtd_pnl: totalPnL * rng.uniform(0.15, 0.55),
      ytd_pnl: totalPnL * rng.uniform(0.40, 0.95),
      itd_pnl: totalPnL,
      fx_pnl: uPnLUsd * rng.uniform(-0.2, 0.2),
      price_pnl: uPnLUsd * rng.uniform(0.6, 1.0),
      carry_pnl: isBond ? notionalUsd * 0.0001 * rng.uniform(5, 80) : 0,
      coupon_pnl: isBond ? notionalUsd * 0.0001 * rng.uniform(100, 400) : 0,
      dividend_pnl: ac === 'EQUITY' ? Math.abs(mvUsd) * 0.0001 * rng.uniform(0, 80) : 0,
      accrued_interest: isBond ? notionalUsd * rng.uniform(0.001, 0.04) : 0,
      accrued_dividend: ac === 'EQUITY' ? Math.abs(mvUsd) * rng.uniform(0, 0.01) : 0,
      amortization: isBond ? rng.uniform(-500, 500) : 0,
      cost_carry: rng.uniform(-300, 300),
      financing_pnl: rng.uniform(-1500, 1500),
      pnl_attribution_alpha: totalPnL * rng.uniform(0.1, 0.45),
      pnl_attribution_beta: totalPnL * rng.uniform(0.2, 0.55),
      pnl_attribution_residual: totalPnL * rng.uniform(-0.15, 0.15),

      // risk
      delta, gamma, vega, theta, rho,
      delta_dollar: delta * mvUsd,
      gamma_dollar: gamma * mvUsd,
      vega_dollar: vega * mvUsd * 0.01,
      theta_dollar: theta * 100,
      dv01,
      dv01_usd: dv01,
      cs01_bps: isBond ? rng.uniform(0.5, 4.5) : 0,
      cs01_usd: cs01Usd,
      duration_modified: modDur,
      duration_macaulay: macDur,
      effective_duration: modDur * rng.uniform(0.95, 1.05),
      convexity: convex,
      spread_duration: isBond ? modDur * rng.uniform(0.85, 1.0) : 0,
      key_rate_1y: isBond ? dv01 * rng.uniform(-0.05, 0.15) : 0,
      key_rate_2y: isBond ? dv01 * rng.uniform(-0.05, 0.20) : 0,
      key_rate_5y: isBond ? dv01 * rng.uniform(0.10, 0.30) : 0,
      key_rate_10y: isBond ? dv01 * rng.uniform(0.20, 0.45) : 0,
      key_rate_30y: isBond ? dv01 * rng.uniform(0.10, 0.30) : 0,
      beta: ac === 'EQUITY' ? rng.uniform(0.4, 1.8) : isBond ? 0 : 0,
      beta_alt: rng.uniform(0.3, 1.6),
      tracking_error_bps: rng.uniform(20, 350),
      var_1d_95: var1d95,
      var_1d_99: var1d99,
      var_10d_95: var10d95,
      var_10d_99: var10d99,

      // exposure
      exposure_gross: exposureGross,
      exposure_net: exposureNet,
      exposure_long_usd: exposureLong,
      exposure_short_usd: exposureShort,
      sector_exposure_pct: sectorPct,
      country_exposure_pct: ctyPct,
      currency_exposure_pct: ccyPct,
      duration_bucket: durBucket,
      maturity_bucket: matBucket,
      liquidity_tier: liqTier,
      days_to_liquidate_50pct: rng.int(1, 12),
      days_to_liquidate_100pct: rng.int(3, 40),
      adv_pct: rng.uniform(0.5, 18),
      concentration_pct_portfolio: rng.uniform(0.1, 8),
      concentration_pct_book: rng.uniform(0.5, 22),

      // option
      strike: strike,
      option_type: isOpt ? (rng.chance(0.5) ? 'CALL' : 'PUT') : '',
      option_style: isOpt ? rng.pick(['AMERICAN', 'EUROPEAN']) : '',
      expiry_date: isOpt ? dateStr(expiryTs) : '',
      days_to_expiry: dte,
      moneyness: moneyness,
      implied_vol: iv,
      realized_vol_30d: isOpt ? rng.uniform(12, 60) : 0,
      realized_vol_90d: isOpt ? rng.uniform(15, 55) : 0,
      atm_vol: atmVol,
      skew_25d: isOpt ? rng.uniform(-8, 12) : 0,
      term_struct_slope: isOpt ? rng.uniform(-0.4, 0.6) : 0,

      // fx
      fx_rate_local_usd: fx,
      fx_rate_t_minus_1: fx * rng.uniform(0.99, 1.01),
      fx_change_pct: rng.uniform(-1.5, 1.5),
      hedge_ratio: rng.uniform(0, 1),
      hedge_pnl: rng.uniform(-12000, 12000),
      unhedged_exposure: Math.abs(mvUsd) * rng.uniform(0, 0.4),
      fx_carry_bps: rng.uniform(-40, 80),
      fx_forward_implied: fx * rng.uniform(0.97, 1.03),

      // lifecycle
      trade_date: dateStr(tradeTs),
      settlement_date: dateStr(settleTs),
      settlement_status: rng.pick(['SETTLED', 'PENDING', 'FAILED', 'PARTIAL']),
      cleared_flag: rng.chance(0.85),
      clearing_house: rng.pick(['DTCC', 'LCH', 'CME_CLR', 'EUREX_CLR', 'OCC']),
      custodian: rng.pick(CUSTODIANS),
      prime_broker: rng.pick(['GS_PB', 'MS_PB', 'JPM_PB', 'UBS_PB']),
      external_account_id: `EXT-${pad(rng.int(1000, 9999), 4)}`,
      reg_reporting_status: rng.pick(['SUBMITTED', 'PENDING', 'REJECTED', 'NA']),
      compliance_status: compliance,
      restricted_flag: rng.chance(0.04),
      restriction_reason: rng.chance(0.04) ? rng.pick(['INSIDER', 'WATCH_LIST', 'RESTRICTED_LIST']) : '',
      last_updated_ts: dateTimeStr(now - rng.int(0, 600) * 1000),
      last_recon_ts: dateTimeStr(now - rng.int(60, 24 * 3600) * 1000),
      recon_break_flag: rng.chance(0.06),

      // limits
      risk_limit_var: var1d95 * rng.uniform(2, 5),
      risk_limit_dv01: Math.abs(dv01) * rng.uniform(2, 4),
      risk_limit_notional: Math.abs(notionalUsd) * rng.uniform(1.5, 4),
      risk_limit_utilization_pct: limitUtilPct,
      position_limit: Math.abs(qty) * rng.uniform(1.5, 6),
      position_limit_pct_used: rng.uniform(10, 100),
      stop_loss_threshold: Math.abs(mvUsd) * 0.05,
      take_profit_threshold: Math.abs(mvUsd) * 0.08,
      limit_breach_count: breachCount,
      last_limit_breach_ts: breachCount > 0 ? dateTimeStr(now - rng.int(60, 7 * 86400) * 1000) : '',

      // regulatory / esg
      esg_score: rng.uniform(25, 95),
      esg_grade: rng.pick(ESG_GRADES),
      carbon_intensity: rng.uniform(5, 280),
      sfdr_classification: rng.pick(['ARTICLE_6', 'ARTICLE_8', 'ARTICLE_9']),
      sustainable_label: rng.chance(0.35),
      regulatory_capital_bucket: rng.pick(['CET1', 'AT1', 'T2', 'NA']),
      lcr_eligible: rng.chance(0.5),
      hqla_level: rng.pick(['L1', 'L2A', 'L2B', 'NA']),
    };

    positions.push(p);
  }

  return positions;
}

// ── trades ─────────────────────────────────────────────────────
export function generateTrades(positions: Position[], avgPerPosition = 4, seed = 0x8a31cd): Trade[] {
  const rng = new Rng(seed);
  const trades: Trade[] = [];
  let tradeCounter = 0;

  for (const p of positions) {
    const n = rng.int(1, avgPerPosition * 2);
    let runningQty = 0;
    const targetQty = p.quantity as number;

    for (let j = 0; j < n; j++) {
      tradeCounter++;
      const tradeId = `TRD-${pad(2026, 4)}${pad(tradeCounter, 7)}`;
      const isLast = j === n - 1;

      // Allocate quantity towards the target. Final trade fills the gap.
      const tradeQty = isLast
        ? targetQty - runningQty
        : Math.round((targetQty / n) * rng.uniform(0.6, 1.4));
      runningQty += tradeQty;

      const side: typeof SIDES[number] = tradeQty >= 0 ? 'BUY' : 'SELL';
      const absQty = Math.abs(tradeQty);

      const lastLocal = p.last_price_local as number;
      const price = lastLocal * rng.uniform(0.985, 1.015);
      const arrival = price * rng.uniform(0.998, 1.002);
      const vwap = price * rng.uniform(0.996, 1.004);
      const twap = price * rng.uniform(0.995, 1.005);
      const close = price * rng.uniform(0.99, 1.01);

      const slipArr = (price - arrival) / arrival * 10000 * (side === 'BUY' ? 1 : -1);
      const slipVWAP = (price - vwap) / vwap * 10000 * (side === 'BUY' ? 1 : -1);
      const slipTWAP = (price - twap) / twap * 10000 * (side === 'BUY' ? 1 : -1);

      const fx = p.fx_rate_local_usd as number;
      const notionalLocal = absQty * price;
      const notionalUsd = notionalLocal * fx;

      const commBps = rng.uniform(0.5, 12);
      const commission = notionalLocal * commBps / 10000;
      const exFee = notionalLocal * rng.uniform(0.00001, 0.00008);
      const clearFee = exFee * rng.uniform(0.3, 0.8);
      const setFee = exFee * rng.uniform(0.2, 0.5);
      const regFee = notionalLocal * rng.uniform(0.000002, 0.00001);
      const secFee = notionalLocal * 0.0000229;
      const taf = absQty * 0.000119;
      const ftt = p.issuer_region === 'EMEA' ? notionalLocal * rng.uniform(0.001, 0.005) : 0;
      const stamp = p.issuer_country === 'GB' ? notionalLocal * 0.005 : 0;
      const totalFeesLocal = commission + exFee + clearFee + setFee + regFee + secFee + taf + ftt + stamp;
      const totalFeesUsd = totalFeesLocal * fx;

      const orderCreateTs = now - rng.int(60, 5 * 86400) * 1000;
      const orderRouteTs = orderCreateTs + rng.int(100, 8000);
      const orderAckTs = orderRouteTs + rng.int(50, 2000);
      const orderOpenTs = orderAckTs + rng.int(20, 500);
      const firstFillTs = orderOpenTs + rng.int(40, 600_000);
      const lastFillTs = firstFillTs + rng.int(0, 5_000_000);
      const orderDoneTs = lastFillTs + rng.int(0, 5000);
      const tradeTs = lastFillTs;
      const allocTs = tradeTs + rng.int(60_000, 600_000);
      const confTs = allocTs + rng.int(30_000, 300_000);
      const setTs = tradeTs + 2 * dayMs;

      const status = rng.pick(TRADE_STATUSES);
      const stage = rng.pick(LIFECYCLE_STAGES);

      const broker = rng.pick(BROKERS);
      const cpty = rng.pick(COUNTERPARTIES);

      const fillCount = rng.int(1, 35);
      const fillDur = lastFillTs - firstFillTs;

      const t: Trade = {
        // identifiers
        trade_id: tradeId,
        parent_order_id: `ORD-${pad(tradeCounter, 8)}`,
        root_order_id: `ROOT-${pad(Math.floor(tradeCounter / 3), 7)}`,
        position_id: p.position_id,
        account_id: p.account_id,
        portfolio_id: p.portfolio_id,
        book_id: p.book_id,
        book_name: p.book_name,
        trader_id: p.trader_id,
        trader_name: p.trader_name,
        strategy_id: p.strategy_id,
        external_trade_id: `EXT-${pad(rng.int(100000, 999999), 6)}`,
        block_trade_id: rng.chance(0.3) ? `BLK-${pad(rng.int(1000, 9999), 4)}` : '',

        // instrument (denormalized)
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

        // execution
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
        slippage_arrival_bps: slipArr,
        slippage_vwap_bps: slipVWAP,
        slippage_twap_bps: slipTWAP,
        slippage_close_bps: (price - close) / close * 10000 * (side === 'BUY' ? 1 : -1),
        fill_count: fillCount,
        fill_first_ts: dateTimeStr(firstFillTs),
        fill_last_ts: dateTimeStr(lastFillTs),
        fill_duration_ms: fillDur,
        execution_venue: rng.pick(VENUES),
        execution_algo: rng.pick(ALGOS),
        algo_aggressiveness: rng.uniform(0, 1),
        algo_horizon_min: rng.int(1, 240),
        order_type: rng.pick(ORDER_TYPES),
        time_in_force: rng.pick(TIF),
        price_limit: price * rng.uniform(0.99, 1.01),
        participation_pct: rng.uniform(0.5, 25),
        dark_pool_pct: rng.uniform(0, 45),

        // value
        notional: notionalLocal,
        notional_local: notionalLocal,
        notional_usd: notionalUsd,
        gross_value: notionalLocal,
        net_value: notionalLocal - totalFeesLocal,
        mark_to_market: (lastLocal - price) * absQty * (side === 'BUY' ? 1 : -1),
        mark_to_market_usd: (lastLocal - price) * absQty * (side === 'BUY' ? 1 : -1) * fx,
        unrealized_pnl: (lastLocal - price) * absQty * (side === 'BUY' ? 1 : -1) * fx,
        realized_pnl: 0,
        trade_pnl: -totalFeesUsd + (lastLocal - price) * absQty * fx * (side === 'BUY' ? 1 : -1),
        fx_rate_at_trade: fx,
        fx_rate_at_settle: fx * rng.uniform(0.998, 1.002),

        // fees
        commission,
        commission_bps: commBps,
        commission_per_share: commission / absQty,
        exchange_fee: exFee,
        clearing_fee: clearFee,
        settlement_fee: setFee,
        regulatory_fee: regFee,
        sec_fee: side === 'SELL' ? secFee : 0,
        taf_fee: taf,
        ftt_tax: ftt,
        stamp_duty: stamp,
        financial_transaction_tax: ftt,
        broker_markup: rng.uniform(0, 200),
        broker_markdown: rng.uniform(0, 100),
        soft_dollar_eligible: rng.chance(0.4),
        total_fees: totalFeesLocal,
        total_fees_local: totalFeesLocal,
        total_fees_usd: totalFeesUsd,

        // counterparty
        counterparty: cpty,
        counterparty_lei: `CPTY${pad(rng.int(1000, 9999), 4)}LEI${pad(rng.int(10, 99), 2)}`,
        counterparty_country: rng.pick(['US', 'GB', 'DE', 'FR', 'JP', 'CH']),
        broker: broker.name,
        broker_lei: `BRK${broker.id.slice(-3)}LEI${pad(rng.int(10, 99), 2)}`,
        broker_country: rng.pick(['US', 'GB', 'DE', 'CH']),
        executing_dealer: broker.name,
        clearing_member: rng.pick(['GS_CLR', 'MS_CLR', 'JPM_CLR', 'CITI_CLR']),
        give_up_broker: rng.chance(0.1) ? rng.pick(BROKERS).name : '',
        give_in_broker: rng.chance(0.1) ? rng.pick(BROKERS).name : '',
        settlement_agent: rng.pick(CUSTODIANS),
        prime_broker: p.prime_broker,

        // timestamps
        order_create_ts: dateTimeStr(orderCreateTs),
        order_route_ts: dateTimeStr(orderRouteTs),
        order_ack_ts: dateTimeStr(orderAckTs),
        order_open_ts: dateTimeStr(orderOpenTs),
        first_fill_ts: dateTimeStr(firstFillTs),
        last_fill_ts: dateTimeStr(lastFillTs),
        order_done_ts: dateTimeStr(orderDoneTs),
        trade_ts: dateTimeStr(tradeTs),
        allocation_ts: dateTimeStr(allocTs),
        confirmation_ts: dateTimeStr(confTs),
        settlement_ts: dateTimeStr(setTs),
        last_event_ts: dateTimeStr(confTs),

        // lifecycle
        status,
        lifecycle_stage: stage,
        allocation_status: rng.pick(['ALLOCATED', 'PENDING', 'PARTIAL']),
        confirmation_status: rng.pick(['CONFIRMED', 'PENDING', 'DISPUTED']),
        matching_status: rng.pick(['MATCHED', 'UNMATCHED', 'PARTIAL']),
        settlement_status: rng.pick(['SETTLED', 'PENDING', 'FAILED']),
        clearing_status: rng.pick(['CLEARED', 'PENDING', 'FAILED']),
        break_flag: rng.chance(0.04),
        break_reason: rng.chance(0.04) ? rng.pick(['PRICE_BREAK', 'QTY_BREAK', 'CCY_BREAK', 'SETTLE_BREAK']) : '',
        amendments_count: rng.int(0, 3),
        cancellations_count: rng.int(0, 2),
        partial_fill_count: rng.int(0, 8),
        reject_count: rng.int(0, 2),
        last_amendment_ts: rng.chance(0.3) ? dateTimeStr(orderRouteTs + rng.int(1000, 10000)) : '',
        last_cancellation_ts: rng.chance(0.2) ? dateTimeStr(orderRouteTs + rng.int(1000, 10000)) : '',

        // allocation
        block_quantity: absQty * rng.uniform(1, 3),
        allocated_quantity: absQty,
        unallocated_quantity: 0,
        allocation_method: rng.pick(['PRO_RATA', 'AVERAGE_PRICE', 'PRIORITY', 'MANUAL']),
        allocation_count: rng.int(1, 8),
        average_allocation_size: absQty / Math.max(1, rng.int(1, 8)),
        allocation_min: absQty * 0.05,
        allocation_max: absQty * 0.6,
        allocation_currency: p.currency,
        allocation_fx_rate: fx,

        // settlement
        settle_date_actual: dateStr(setTs),
        settle_date_expected: dateStr(setTs),
        days_to_settle: 2,
        settlement_amount: notionalLocal + totalFeesLocal,
        settlement_amount_usd: notionalUsd + totalFeesUsd,
        settlement_currency_x: p.currency,
        settlement_account: `SET-${pad(rng.int(1000, 9999), 4)}`,
        dvp_flag: rng.chance(0.95),
        settlement_instructions: rng.pick(['STANDARD', 'CUSTOM', 'PRIME_BROKER', 'TRIPARTY']),
        settlement_reference: `REF-${pad(tradeCounter, 8)}`,

        // regulatory
        mifid_flag: p.issuer_region === 'EMEA',
        mifid_decision_maker: p.trader_id,
        mifid_execution_decision_maker: p.trader_id,
        mifid_short_selling_indicator: side === 'SELL' ? rng.pick(['SHORT', 'LONG', 'NA']) : 'NA',
        dodd_frank_swap_flag: p.asset_class === 'SWAP',
        emir_reporting_flag: p.issuer_region === 'EMEA',
        cat_reporting_flag: p.issuer_country === 'US',
        consolidated_audit_trail_id: `CAT-${pad(tradeCounter, 9)}`,
        lei_buy_side: `BUY${pad(rng.int(1000, 9999), 4)}LEI${pad(rng.int(10, 99), 2)}`,
        lei_sell_side: `SLL${pad(rng.int(1000, 9999), 4)}LEI${pad(rng.int(10, 99), 2)}`,
        uti: `UTI-${pad(tradeCounter, 10)}`,
        usi: p.asset_class === 'SWAP' ? `USI-${pad(tradeCounter, 10)}` : '',

        // risk / impact
        portfolio_impact_usd: notionalUsd * rng.uniform(0, 0.05) * (side === 'BUY' ? 1 : -1),
        portfolio_impact_pct: rng.uniform(0, 1.5) * (side === 'BUY' ? 1 : -1),
        risk_increase_dv01: rng.uniform(-5000, 5000),
        risk_increase_var: rng.uniform(-50000, 50000),
        concentration_increase_pct: rng.uniform(-0.5, 0.5),
        correlation_to_book: rng.uniform(-0.3, 0.9),
        expected_market_impact_bps: rng.uniform(0.5, 15),
        realized_market_impact_bps: rng.uniform(0.3, 18),
        impact_decay_minutes: rng.int(2, 240),
        crowdedness_score: rng.uniform(0, 1),
        liquidity_score: rng.uniform(0.1, 1),
        participation_30min_pct: rng.uniform(0.5, 25),
        participation_eod_pct: rng.uniform(0.2, 12),
        position_change_pct: tradeQty / Math.max(1, p.quantity as number) * 100,
        is_position_increase: side === 'BUY',

        // attribution
        attribution_alpha: rng.uniform(-1500, 2500),
        attribution_beta: rng.uniform(-2000, 2500),
        attribution_currency: rng.uniform(-500, 500),
        attribution_country: rng.uniform(-300, 300),
        attribution_sector: rng.uniform(-800, 1200),
        attribution_factor_value: rng.uniform(-0.5, 0.6),
        attribution_factor_momentum: rng.uniform(-0.4, 0.7),
        attribution_factor_quality: rng.uniform(-0.3, 0.5),
        attribution_factor_size: rng.uniform(-0.4, 0.3),
        attribution_residual: rng.uniform(-500, 500),

        // research
        signal_id: rng.chance(0.4) ? `SIG-${pad(rng.int(1, 999), 3)}` : '',
        signal_source: rng.pick(['INTERNAL_RESEARCH', 'BROKER', 'QUANT_MODEL', 'NEWS_NLP', 'NONE']),
        signal_strength: rng.uniform(0, 1),
        research_note_id: rng.chance(0.3) ? `NOTE-${pad(rng.int(100, 999), 3)}` : '',
        recommendation_target: price * rng.uniform(1.05, 1.3),
        recommendation_stop: price * rng.uniform(0.8, 0.95),
        research_horizon_days: rng.int(7, 180),
        signal_confidence: rng.uniform(0.4, 0.95),

        // audit
        order_origin: rng.pick(['EMS', 'OMS', 'PHONE', 'CHAT', 'API']),
        system_of_record: rng.pick(['CHARLES_RIVER', 'ALADDIN', 'BLOOMBERG_AIM', 'PROPRIETARY']),
        trade_capture_system: rng.pick(['INHOUSE_TCS', 'BROADRIDGE', 'CALYPSO']),
        front_office_user: p.trader_id,
        middle_office_user: `MO-${pad(rng.int(100, 999), 3)}`,
        back_office_user: `BO-${pad(rng.int(100, 999), 3)}`,
        ops_notes: rng.chance(0.2) ? rng.pick(['VERIFY_ALLOC', 'CHECK_FEE', 'PB_CONFIRMED', 'SETTLE_AMENDED']) : '',
        compliance_review_status: rng.pick(['APPROVED', 'PENDING', 'ESCALATED']),
        compliance_reviewer: `CMPL-${pad(rng.int(100, 999), 3)}`,
        last_modified_by: p.trader_id,
        last_modified_ts: dateTimeStr(confTs),
        version: rng.int(1, 5),
      };

      trades.push(t);
    }
  }

  return trades;
}

// ── memoized loader ───────────────────────────────────────────
let _positions: Position[] | null = null;
let _trades: Trade[] | null = null;

export function getPositions(): Position[] {
  if (!_positions) _positions = generatePositions();
  return _positions;
}

export function getTrades(): Trade[] {
  if (!_trades) _trades = generateTrades(getPositions());
  return _trades;
}
