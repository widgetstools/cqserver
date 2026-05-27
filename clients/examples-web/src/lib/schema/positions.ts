// Position schema — 200+ columns grouped into logical sections.
// Every column declares its data type, display group, default
// formatter, and whether it's a JOIN key into trades / securities.
//
// This schema is read by:
//   - AG-Grid column-defs builder (positions-cols.ts)
//   - The query library (to know which columns are aggregatable etc.)
//   - The data generator (to know what values to produce per column)

export type PositionColType =
  | 'string'
  | 'enum'
  | 'date'
  | 'datetime'
  | 'int'
  | 'qty'         // signed integer
  | 'price'       // 4-decimal currency
  | 'ccy'         // currency amount (1000s scaled)
  | 'pct'         // -100..100 with 2 dp
  | 'bps'         // basis points
  | 'rate'        // float in 0..1
  | 'bool'
  | 'tag';        // short categorical

export type PositionColGroup =
  | 'identifiers'
  | 'security'
  | 'rating'
  | 'qty_value'
  | 'pnl'
  | 'risk'
  | 'exposure'
  | 'option'
  | 'fx'
  | 'lifecycle'
  | 'limits'
  | 'regulatory';

export interface PositionColumn {
  field: string;
  label: string;
  type: PositionColType;
  group: PositionColGroup;
  joinKey?: 'trade_position' | 'security';
  description?: string;
  /** When true, value is signed and should be rendered with num-pos/num-neg classes. */
  signed?: boolean;
}

export const POSITION_COLUMNS: PositionColumn[] = [
  // ── IDENTIFIERS ──────────────────────────────────────────
  { field: 'position_id', label: 'Position ID', type: 'string', group: 'identifiers', joinKey: 'trade_position' },
  { field: 'parent_position_id', label: 'Parent ID', type: 'string', group: 'identifiers' },
  { field: 'account_id', label: 'Account ID', type: 'string', group: 'identifiers' },
  { field: 'account_name', label: 'Account', type: 'string', group: 'identifiers' },
  { field: 'portfolio_id', label: 'Portfolio ID', type: 'string', group: 'identifiers' },
  { field: 'portfolio_name', label: 'Portfolio', type: 'string', group: 'identifiers' },
  { field: 'book_id', label: 'Book ID', type: 'string', group: 'identifiers' },
  { field: 'book_name', label: 'Book', type: 'string', group: 'identifiers' },
  { field: 'desk', label: 'Desk', type: 'enum', group: 'identifiers' },
  { field: 'trader_id', label: 'Trader ID', type: 'string', group: 'identifiers' },
  { field: 'trader_name', label: 'Trader', type: 'string', group: 'identifiers' },
  { field: 'strategy_id', label: 'Strategy ID', type: 'string', group: 'identifiers' },
  { field: 'strategy_name', label: 'Strategy', type: 'string', group: 'identifiers' },

  // ── SECURITY ────────────────────────────────────────────
  { field: 'cusip', label: 'CUSIP', type: 'string', group: 'security', joinKey: 'security' },
  { field: 'isin', label: 'ISIN', type: 'string', group: 'security', joinKey: 'security' },
  { field: 'sedol', label: 'SEDOL', type: 'string', group: 'security' },
  { field: 'bbg_ticker', label: 'BBG', type: 'string', group: 'security' },
  { field: 'ric', label: 'RIC', type: 'string', group: 'security' },
  { field: 'figi', label: 'FIGI', type: 'string', group: 'security' },
  { field: 'lei', label: 'LEI', type: 'string', group: 'security' },
  { field: 'symbol', label: 'Symbol', type: 'string', group: 'security' },
  { field: 'security_name', label: 'Security', type: 'string', group: 'security' },
  { field: 'issuer', label: 'Issuer', type: 'string', group: 'security' },
  { field: 'issuer_country', label: 'Issuer Cty', type: 'enum', group: 'security' },
  { field: 'issuer_region', label: 'Region', type: 'enum', group: 'security' },
  { field: 'issuer_sector', label: 'Sector', type: 'enum', group: 'security' },
  { field: 'issuer_industry', label: 'Industry', type: 'enum', group: 'security' },
  { field: 'asset_class', label: 'Asset Class', type: 'enum', group: 'security' },
  { field: 'instrument_type', label: 'Inst Type', type: 'enum', group: 'security' },
  { field: 'underlying_id', label: 'Underlying ID', type: 'string', group: 'security' },
  { field: 'underlying_symbol', label: 'Underlying', type: 'string', group: 'security' },
  { field: 'currency', label: 'CCY', type: 'enum', group: 'security' },
  { field: 'settlement_currency', label: 'Settle CCY', type: 'enum', group: 'security' },
  { field: 'trading_venue', label: 'Venue', type: 'enum', group: 'security' },
  { field: 'listing_venue', label: 'Listing', type: 'enum', group: 'security' },
  { field: 'listing_country', label: 'Listing Cty', type: 'enum', group: 'security' },
  { field: 'issue_date', label: 'Issue Date', type: 'date', group: 'security' },
  { field: 'maturity_date', label: 'Maturity', type: 'date', group: 'security' },
  { field: 'coupon_type', label: 'Coupon Type', type: 'enum', group: 'security' },
  { field: 'coupon_rate', label: 'Coupon %', type: 'pct', group: 'security' },
  { field: 'coupon_freq', label: 'Freq', type: 'enum', group: 'security' },
  { field: 'day_count_convention', label: 'DCC', type: 'enum', group: 'security' },
  { field: 'callable_flag', label: 'Callable', type: 'bool', group: 'security' },
  { field: 'putable_flag', label: 'Putable', type: 'bool', group: 'security' },
  { field: 'convertible_flag', label: 'Convertible', type: 'bool', group: 'security' },
  { field: 'inflation_linked', label: 'Inflation', type: 'bool', group: 'security' },

  // ── RATING / CREDIT ─────────────────────────────────────
  { field: 'rating_sp', label: 'S&P', type: 'enum', group: 'rating' },
  { field: 'rating_moody', label: 'Moody', type: 'enum', group: 'rating' },
  { field: 'rating_fitch', label: 'Fitch', type: 'enum', group: 'rating' },
  { field: 'rating_composite', label: 'Composite', type: 'enum', group: 'rating' },
  { field: 'rating_grade', label: 'Grade', type: 'enum', group: 'rating' },
  { field: 'credit_curve_id', label: 'Curve', type: 'string', group: 'rating' },
  { field: 'credit_spread_bps', label: 'Spread bps', type: 'bps', group: 'rating', signed: false },
  { field: 'cds_5y_bps', label: 'CDS 5Y bps', type: 'bps', group: 'rating' },
  { field: 'default_probability_1y', label: 'PD 1Y', type: 'rate', group: 'rating' },
  { field: 'recovery_rate', label: 'RR', type: 'rate', group: 'rating' },

  // ── QUANTITY / VALUE ────────────────────────────────────
  { field: 'quantity', label: 'Qty', type: 'qty', group: 'qty_value', signed: true },
  { field: 'quantity_long', label: 'Qty Long', type: 'qty', group: 'qty_value' },
  { field: 'quantity_short', label: 'Qty Short', type: 'qty', group: 'qty_value' },
  { field: 'quantity_t_minus_1', label: 'Qty T-1', type: 'qty', group: 'qty_value', signed: true },
  { field: 'quantity_change', label: 'Δ Qty', type: 'qty', group: 'qty_value', signed: true },
  { field: 'opening_price', label: 'Open', type: 'price', group: 'qty_value' },
  { field: 'last_price', label: 'Last', type: 'price', group: 'qty_value' },
  { field: 'last_price_local', label: 'Last Loc', type: 'price', group: 'qty_value' },
  { field: 'last_price_usd', label: 'Last USD', type: 'price', group: 'qty_value' },
  { field: 'previous_close', label: 'Prev Close', type: 'price', group: 'qty_value' },
  { field: 'price_change', label: 'Δ Price', type: 'price', group: 'qty_value', signed: true },
  { field: 'price_change_pct', label: 'Δ Price %', type: 'pct', group: 'qty_value', signed: true },
  { field: 'average_cost', label: 'Avg Cost', type: 'price', group: 'qty_value' },
  { field: 'average_cost_local', label: 'Avg Cost Loc', type: 'price', group: 'qty_value' },
  { field: 'average_cost_usd', label: 'Avg Cost USD', type: 'price', group: 'qty_value' },
  { field: 'notional', label: 'Notional', type: 'ccy', group: 'qty_value' },
  { field: 'notional_local', label: 'Notional Loc', type: 'ccy', group: 'qty_value' },
  { field: 'notional_usd', label: 'Notional USD', type: 'ccy', group: 'qty_value' },
  { field: 'market_value', label: 'MV', type: 'ccy', group: 'qty_value', signed: true },
  { field: 'market_value_local', label: 'MV Loc', type: 'ccy', group: 'qty_value', signed: true },
  { field: 'market_value_usd', label: 'MV USD', type: 'ccy', group: 'qty_value', signed: true },
  // Pre-shaped numerator + denominator for MV-weighted aggregates
  // (consumed by /v_heatmap_sector_region). Not displayed by default.
  { field: 'mv_x_pct', label: 'MV·Δ%', type: 'ccy', group: 'qty_value', signed: true },
  { field: 'mv_abs', label: 'MV abs', type: 'ccy', group: 'qty_value' },
  { field: 'cost_basis', label: 'Cost Basis', type: 'ccy', group: 'qty_value' },
  { field: 'cost_basis_local', label: 'CB Loc', type: 'ccy', group: 'qty_value' },
  { field: 'cost_basis_usd', label: 'CB USD', type: 'ccy', group: 'qty_value' },
  { field: 'nav_pct', label: 'NAV %', type: 'pct', group: 'qty_value' },

  // ── PNL ─────────────────────────────────────────────────
  { field: 'unrealized_pnl', label: 'UPnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'unrealized_pnl_local', label: 'UPnL Loc', type: 'ccy', group: 'pnl', signed: true },
  { field: 'unrealized_pnl_usd', label: 'UPnL USD', type: 'ccy', group: 'pnl', signed: true },
  { field: 'realized_pnl', label: 'RPnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'realized_pnl_local', label: 'RPnL Loc', type: 'ccy', group: 'pnl', signed: true },
  { field: 'realized_pnl_usd', label: 'RPnL USD', type: 'ccy', group: 'pnl', signed: true },
  { field: 'total_pnl', label: 'Total PnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'day_pnl', label: 'Day PnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'mtd_pnl', label: 'MTD PnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'qtd_pnl', label: 'QTD PnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'ytd_pnl', label: 'YTD PnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'itd_pnl', label: 'ITD PnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'fx_pnl', label: 'FX PnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'price_pnl', label: 'Price PnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'carry_pnl', label: 'Carry PnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'coupon_pnl', label: 'Coupon PnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'dividend_pnl', label: 'Div PnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'accrued_interest', label: 'Acc Int', type: 'ccy', group: 'pnl' },
  { field: 'accrued_dividend', label: 'Acc Div', type: 'ccy', group: 'pnl' },
  { field: 'amortization', label: 'Amort', type: 'ccy', group: 'pnl', signed: true },
  { field: 'cost_carry', label: 'Cost Carry', type: 'ccy', group: 'pnl', signed: true },
  { field: 'financing_pnl', label: 'Financing PnL', type: 'ccy', group: 'pnl', signed: true },
  { field: 'pnl_attribution_alpha', label: 'Attr α', type: 'ccy', group: 'pnl', signed: true },
  { field: 'pnl_attribution_beta', label: 'Attr β', type: 'ccy', group: 'pnl', signed: true },
  { field: 'pnl_attribution_residual', label: 'Attr ε', type: 'ccy', group: 'pnl', signed: true },

  // ── RISK / GREEKS ───────────────────────────────────────
  { field: 'delta', label: 'Delta', type: 'rate', group: 'risk', signed: true },
  { field: 'gamma', label: 'Gamma', type: 'rate', group: 'risk' },
  { field: 'vega', label: 'Vega', type: 'rate', group: 'risk' },
  { field: 'theta', label: 'Theta', type: 'rate', group: 'risk', signed: true },
  { field: 'rho', label: 'Rho', type: 'rate', group: 'risk', signed: true },
  { field: 'delta_dollar', label: 'Δ$', type: 'ccy', group: 'risk', signed: true },
  { field: 'gamma_dollar', label: 'Γ$', type: 'ccy', group: 'risk' },
  { field: 'vega_dollar', label: 'V$', type: 'ccy', group: 'risk' },
  { field: 'theta_dollar', label: 'Θ$', type: 'ccy', group: 'risk', signed: true },
  { field: 'dv01', label: 'DV01', type: 'ccy', group: 'risk', signed: true },
  { field: 'dv01_usd', label: 'DV01 USD', type: 'ccy', group: 'risk', signed: true },
  { field: 'cs01_bps', label: 'CS01 bps', type: 'bps', group: 'risk' },
  { field: 'cs01_usd', label: 'CS01 USD', type: 'ccy', group: 'risk', signed: true },
  { field: 'duration_modified', label: 'Mod Dur', type: 'rate', group: 'risk' },
  { field: 'duration_macaulay', label: 'Mac Dur', type: 'rate', group: 'risk' },
  { field: 'effective_duration', label: 'Eff Dur', type: 'rate', group: 'risk' },
  { field: 'convexity', label: 'Convexity', type: 'rate', group: 'risk' },
  { field: 'spread_duration', label: 'Sprd Dur', type: 'rate', group: 'risk' },
  { field: 'key_rate_1y', label: 'KR 1Y', type: 'rate', group: 'risk', signed: true },
  { field: 'key_rate_2y', label: 'KR 2Y', type: 'rate', group: 'risk', signed: true },
  { field: 'key_rate_5y', label: 'KR 5Y', type: 'rate', group: 'risk', signed: true },
  { field: 'key_rate_10y', label: 'KR 10Y', type: 'rate', group: 'risk', signed: true },
  { field: 'key_rate_30y', label: 'KR 30Y', type: 'rate', group: 'risk', signed: true },
  { field: 'beta', label: 'Beta', type: 'rate', group: 'risk' },
  { field: 'beta_alt', label: 'Beta Alt', type: 'rate', group: 'risk' },
  { field: 'tracking_error_bps', label: 'TE bps', type: 'bps', group: 'risk' },
  { field: 'var_1d_95', label: 'VaR 1d 95', type: 'ccy', group: 'risk' },
  { field: 'var_1d_99', label: 'VaR 1d 99', type: 'ccy', group: 'risk' },
  { field: 'var_10d_95', label: 'VaR 10d 95', type: 'ccy', group: 'risk' },
  { field: 'var_10d_99', label: 'VaR 10d 99', type: 'ccy', group: 'risk' },

  // ── EXPOSURE ────────────────────────────────────────────
  { field: 'exposure_gross', label: 'Exp Gross', type: 'ccy', group: 'exposure' },
  { field: 'exposure_net', label: 'Exp Net', type: 'ccy', group: 'exposure', signed: true },
  { field: 'exposure_long_usd', label: 'Long USD', type: 'ccy', group: 'exposure' },
  { field: 'exposure_short_usd', label: 'Short USD', type: 'ccy', group: 'exposure' },
  { field: 'sector_exposure_pct', label: 'Sector %', type: 'pct', group: 'exposure' },
  { field: 'country_exposure_pct', label: 'Country %', type: 'pct', group: 'exposure' },
  { field: 'currency_exposure_pct', label: 'CCY %', type: 'pct', group: 'exposure' },
  { field: 'duration_bucket', label: 'Dur Bucket', type: 'enum', group: 'exposure' },
  { field: 'maturity_bucket', label: 'Mat Bucket', type: 'enum', group: 'exposure' },
  { field: 'liquidity_tier', label: 'Liq Tier', type: 'enum', group: 'exposure' },
  { field: 'days_to_liquidate_50pct', label: 'Liq Days 50', type: 'int', group: 'exposure' },
  { field: 'days_to_liquidate_100pct', label: 'Liq Days 100', type: 'int', group: 'exposure' },
  { field: 'adv_pct', label: 'ADV %', type: 'pct', group: 'exposure' },
  { field: 'concentration_pct_portfolio', label: 'Conc Pfl %', type: 'pct', group: 'exposure' },
  { field: 'concentration_pct_book', label: 'Conc Bk %', type: 'pct', group: 'exposure' },

  // ── OPTION ──────────────────────────────────────────────
  { field: 'strike', label: 'Strike', type: 'price', group: 'option' },
  { field: 'option_type', label: 'Type', type: 'enum', group: 'option' },
  { field: 'option_style', label: 'Style', type: 'enum', group: 'option' },
  { field: 'expiry_date', label: 'Expiry', type: 'date', group: 'option' },
  { field: 'days_to_expiry', label: 'DTE', type: 'int', group: 'option' },
  { field: 'moneyness', label: 'Moneyness', type: 'rate', group: 'option' },
  { field: 'implied_vol', label: 'IV', type: 'pct', group: 'option' },
  { field: 'realized_vol_30d', label: 'RV 30d', type: 'pct', group: 'option' },
  { field: 'realized_vol_90d', label: 'RV 90d', type: 'pct', group: 'option' },
  { field: 'atm_vol', label: 'ATM Vol', type: 'pct', group: 'option' },
  { field: 'skew_25d', label: 'Skew 25Δ', type: 'pct', group: 'option', signed: true },
  { field: 'term_struct_slope', label: 'Term Slope', type: 'rate', group: 'option', signed: true },

  // ── FX ──────────────────────────────────────────────────
  { field: 'fx_rate_local_usd', label: 'FX → USD', type: 'rate', group: 'fx' },
  { field: 'fx_rate_t_minus_1', label: 'FX T-1', type: 'rate', group: 'fx' },
  { field: 'fx_change_pct', label: 'Δ FX %', type: 'pct', group: 'fx', signed: true },
  { field: 'hedge_ratio', label: 'Hedge Ratio', type: 'rate', group: 'fx' },
  { field: 'hedge_pnl', label: 'Hedge PnL', type: 'ccy', group: 'fx', signed: true },
  { field: 'unhedged_exposure', label: 'Unhedged Exp', type: 'ccy', group: 'fx' },
  { field: 'fx_carry_bps', label: 'FX Carry bps', type: 'bps', group: 'fx', signed: true },
  { field: 'fx_forward_implied', label: 'Fwd Implied', type: 'rate', group: 'fx' },

  // ── LIFECYCLE / OPS ─────────────────────────────────────
  { field: 'trade_date', label: 'Trade Date', type: 'date', group: 'lifecycle' },
  { field: 'settlement_date', label: 'Settle Date', type: 'date', group: 'lifecycle' },
  { field: 'settlement_status', label: 'Settle Status', type: 'enum', group: 'lifecycle' },
  { field: 'cleared_flag', label: 'Cleared', type: 'bool', group: 'lifecycle' },
  { field: 'clearing_house', label: 'CH', type: 'enum', group: 'lifecycle' },
  { field: 'custodian', label: 'Custodian', type: 'enum', group: 'lifecycle' },
  { field: 'prime_broker', label: 'PB', type: 'enum', group: 'lifecycle' },
  { field: 'external_account_id', label: 'Ext Acct', type: 'string', group: 'lifecycle' },
  { field: 'reg_reporting_status', label: 'Reg Rpt', type: 'enum', group: 'lifecycle' },
  { field: 'compliance_status', label: 'Compliance', type: 'enum', group: 'lifecycle' },
  { field: 'restricted_flag', label: 'Restricted', type: 'bool', group: 'lifecycle' },
  { field: 'restriction_reason', label: 'Restrict Reason', type: 'enum', group: 'lifecycle' },
  { field: 'last_updated_ts', label: 'Updated', type: 'datetime', group: 'lifecycle' },
  { field: 'last_recon_ts', label: 'Last Recon', type: 'datetime', group: 'lifecycle' },
  { field: 'recon_break_flag', label: 'Break', type: 'bool', group: 'lifecycle' },

  // ── LIMITS ──────────────────────────────────────────────
  { field: 'risk_limit_var', label: 'Lim VaR', type: 'ccy', group: 'limits' },
  { field: 'risk_limit_dv01', label: 'Lim DV01', type: 'ccy', group: 'limits' },
  { field: 'risk_limit_notional', label: 'Lim Notnl', type: 'ccy', group: 'limits' },
  { field: 'risk_limit_utilization_pct', label: 'Lim Util %', type: 'pct', group: 'limits' },
  { field: 'position_limit', label: 'Pos Limit', type: 'ccy', group: 'limits' },
  { field: 'position_limit_pct_used', label: 'Pos Limit %', type: 'pct', group: 'limits' },
  { field: 'stop_loss_threshold', label: 'Stop Loss', type: 'ccy', group: 'limits' },
  { field: 'take_profit_threshold', label: 'TP', type: 'ccy', group: 'limits' },
  { field: 'limit_breach_count', label: 'Breaches', type: 'int', group: 'limits' },
  { field: 'last_limit_breach_ts', label: 'Last Breach', type: 'datetime', group: 'limits' },

  // ── REGULATORY / ESG ────────────────────────────────────
  { field: 'esg_score', label: 'ESG Score', type: 'pct', group: 'regulatory' },
  { field: 'esg_grade', label: 'ESG Grade', type: 'enum', group: 'regulatory' },
  { field: 'carbon_intensity', label: 'Carbon Int', type: 'pct', group: 'regulatory' },
  { field: 'sfdr_classification', label: 'SFDR', type: 'enum', group: 'regulatory' },
  { field: 'sustainable_label', label: 'Sustainable', type: 'bool', group: 'regulatory' },
  { field: 'regulatory_capital_bucket', label: 'Cap Bucket', type: 'enum', group: 'regulatory' },
  { field: 'lcr_eligible', label: 'LCR', type: 'bool', group: 'regulatory' },
  { field: 'hqla_level', label: 'HQLA', type: 'enum', group: 'regulatory' },
];

// Quick lookup for the data generator.
export const POSITION_FIELDS = POSITION_COLUMNS.map((c) => c.field);
