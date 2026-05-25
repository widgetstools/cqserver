// Trade schema — 200+ columns covering full trade lifecycle.
// Trades are linked to positions via `position_id`. Most identifier
// columns (cusip, isin, symbol, asset_class, currency, issuer) are
// denormalized so trade-level grids work without a JOIN, while the
// JOIN to positions still demonstrates relational cqserver features.

export type TradeColType =
  | 'string'
  | 'enum'
  | 'date'
  | 'datetime'
  | 'int'
  | 'qty'
  | 'price'
  | 'ccy'
  | 'pct'
  | 'bps'
  | 'rate'
  | 'bool'
  | 'tag';

export type TradeColGroup =
  | 'identifiers'
  | 'instrument'
  | 'execution'
  | 'value'
  | 'fees'
  | 'counterparty'
  | 'timestamps'
  | 'lifecycle'
  | 'allocation'
  | 'settlement'
  | 'regulatory'
  | 'risk_impact'
  | 'attribution'
  | 'research'
  | 'audit';

export interface TradeColumn {
  field: string;
  label: string;
  type: TradeColType;
  group: TradeColGroup;
  joinKey?: 'positions' | 'securities';
  description?: string;
  signed?: boolean;
}

export const TRADE_COLUMNS: TradeColumn[] = [
  // ── IDENTIFIERS ──────────────────────────────────────────
  { field: 'trade_id', label: 'Trade ID', type: 'string', group: 'identifiers' },
  { field: 'parent_order_id', label: 'Parent Order', type: 'string', group: 'identifiers' },
  { field: 'root_order_id', label: 'Root Order', type: 'string', group: 'identifiers' },
  { field: 'position_id', label: 'Position ID', type: 'string', group: 'identifiers', joinKey: 'positions' },
  { field: 'account_id', label: 'Account ID', type: 'string', group: 'identifiers' },
  { field: 'portfolio_id', label: 'Portfolio ID', type: 'string', group: 'identifiers' },
  { field: 'book_id', label: 'Book ID', type: 'string', group: 'identifiers' },
  { field: 'book_name', label: 'Book', type: 'string', group: 'identifiers' },
  { field: 'trader_id', label: 'Trader ID', type: 'string', group: 'identifiers' },
  { field: 'trader_name', label: 'Trader', type: 'string', group: 'identifiers' },
  { field: 'strategy_id', label: 'Strategy ID', type: 'string', group: 'identifiers' },
  { field: 'external_trade_id', label: 'Ext Trade ID', type: 'string', group: 'identifiers' },
  { field: 'block_trade_id', label: 'Block ID', type: 'string', group: 'identifiers' },

  // ── INSTRUMENT (denormalized for fast filtering) ─────────
  { field: 'cusip', label: 'CUSIP', type: 'string', group: 'instrument' },
  { field: 'isin', label: 'ISIN', type: 'string', group: 'instrument' },
  { field: 'sedol', label: 'SEDOL', type: 'string', group: 'instrument' },
  { field: 'bbg_ticker', label: 'BBG', type: 'string', group: 'instrument' },
  { field: 'ric', label: 'RIC', type: 'string', group: 'instrument' },
  { field: 'figi', label: 'FIGI', type: 'string', group: 'instrument' },
  { field: 'symbol', label: 'Symbol', type: 'string', group: 'instrument' },
  { field: 'security_name', label: 'Security', type: 'string', group: 'instrument' },
  { field: 'asset_class', label: 'Asset Class', type: 'enum', group: 'instrument' },
  { field: 'instrument_type', label: 'Inst Type', type: 'enum', group: 'instrument' },
  { field: 'currency', label: 'CCY', type: 'enum', group: 'instrument' },
  { field: 'settlement_currency', label: 'Settle CCY', type: 'enum', group: 'instrument' },
  { field: 'issuer', label: 'Issuer', type: 'string', group: 'instrument' },
  { field: 'issuer_country', label: 'Issuer Cty', type: 'enum', group: 'instrument' },
  { field: 'issuer_region', label: 'Region', type: 'enum', group: 'instrument' },
  { field: 'issuer_sector', label: 'Sector', type: 'enum', group: 'instrument' },

  // ── EXECUTION ───────────────────────────────────────────
  { field: 'side', label: 'Side', type: 'enum', group: 'execution' },
  { field: 'quantity', label: 'Qty', type: 'qty', group: 'execution', signed: true },
  { field: 'quantity_filled', label: 'Qty Filled', type: 'qty', group: 'execution' },
  { field: 'quantity_open', label: 'Qty Open', type: 'qty', group: 'execution' },
  { field: 'quantity_canceled', label: 'Qty Cnxld', type: 'qty', group: 'execution' },
  { field: 'price', label: 'Price', type: 'price', group: 'execution' },
  { field: 'price_arrival', label: 'Arrival', type: 'price', group: 'execution' },
  { field: 'price_close', label: 'Close', type: 'price', group: 'execution' },
  { field: 'price_vwap', label: 'VWAP', type: 'price', group: 'execution' },
  { field: 'price_twap', label: 'TWAP', type: 'price', group: 'execution' },
  { field: 'price_market_on_open', label: 'MOO', type: 'price', group: 'execution' },
  { field: 'price_market_on_close', label: 'MOC', type: 'price', group: 'execution' },
  { field: 'price_benchmark', label: 'Benchmark', type: 'price', group: 'execution' },
  { field: 'slippage_arrival_bps', label: 'Slip Arr bps', type: 'bps', group: 'execution', signed: true },
  { field: 'slippage_vwap_bps', label: 'Slip VWAP bps', type: 'bps', group: 'execution', signed: true },
  { field: 'slippage_twap_bps', label: 'Slip TWAP bps', type: 'bps', group: 'execution', signed: true },
  { field: 'slippage_close_bps', label: 'Slip Close bps', type: 'bps', group: 'execution', signed: true },
  { field: 'fill_count', label: 'Fills', type: 'int', group: 'execution' },
  { field: 'fill_first_ts', label: 'First Fill', type: 'datetime', group: 'execution' },
  { field: 'fill_last_ts', label: 'Last Fill', type: 'datetime', group: 'execution' },
  { field: 'fill_duration_ms', label: 'Fill Dur ms', type: 'int', group: 'execution' },
  { field: 'execution_venue', label: 'Venue', type: 'enum', group: 'execution' },
  { field: 'execution_algo', label: 'Algo', type: 'enum', group: 'execution' },
  { field: 'algo_aggressiveness', label: 'Aggro', type: 'rate', group: 'execution' },
  { field: 'algo_horizon_min', label: 'Horizon min', type: 'int', group: 'execution' },
  { field: 'order_type', label: 'Ord Type', type: 'enum', group: 'execution' },
  { field: 'time_in_force', label: 'TIF', type: 'enum', group: 'execution' },
  { field: 'price_limit', label: 'Limit Price', type: 'price', group: 'execution' },
  { field: 'participation_pct', label: 'Part %', type: 'pct', group: 'execution' },
  { field: 'dark_pool_pct', label: 'Dark %', type: 'pct', group: 'execution' },

  // ── NOTIONAL / VALUE ────────────────────────────────────
  { field: 'notional', label: 'Notional', type: 'ccy', group: 'value' },
  { field: 'notional_local', label: 'Notional Loc', type: 'ccy', group: 'value' },
  { field: 'notional_usd', label: 'Notional USD', type: 'ccy', group: 'value' },
  { field: 'gross_value', label: 'Gross', type: 'ccy', group: 'value' },
  { field: 'net_value', label: 'Net', type: 'ccy', group: 'value' },
  { field: 'mark_to_market', label: 'MTM', type: 'ccy', group: 'value', signed: true },
  { field: 'mark_to_market_usd', label: 'MTM USD', type: 'ccy', group: 'value', signed: true },
  { field: 'unrealized_pnl', label: 'UPnL', type: 'ccy', group: 'value', signed: true },
  { field: 'realized_pnl', label: 'RPnL', type: 'ccy', group: 'value', signed: true },
  { field: 'trade_pnl', label: 'Trade PnL', type: 'ccy', group: 'value', signed: true },
  { field: 'fx_rate_at_trade', label: 'FX @ Trade', type: 'rate', group: 'value' },
  { field: 'fx_rate_at_settle', label: 'FX @ Settle', type: 'rate', group: 'value' },

  // ── FEES ────────────────────────────────────────────────
  { field: 'commission', label: 'Commission', type: 'ccy', group: 'fees' },
  { field: 'commission_bps', label: 'Comm bps', type: 'bps', group: 'fees' },
  { field: 'commission_per_share', label: 'Comm/Sh', type: 'ccy', group: 'fees' },
  { field: 'exchange_fee', label: 'Exch Fee', type: 'ccy', group: 'fees' },
  { field: 'clearing_fee', label: 'Clear Fee', type: 'ccy', group: 'fees' },
  { field: 'settlement_fee', label: 'Settle Fee', type: 'ccy', group: 'fees' },
  { field: 'regulatory_fee', label: 'Reg Fee', type: 'ccy', group: 'fees' },
  { field: 'sec_fee', label: 'SEC Fee', type: 'ccy', group: 'fees' },
  { field: 'taf_fee', label: 'TAF', type: 'ccy', group: 'fees' },
  { field: 'ftt_tax', label: 'FTT', type: 'ccy', group: 'fees' },
  { field: 'stamp_duty', label: 'Stamp', type: 'ccy', group: 'fees' },
  { field: 'financial_transaction_tax', label: 'FTT EU', type: 'ccy', group: 'fees' },
  { field: 'broker_markup', label: 'Broker Mkup', type: 'ccy', group: 'fees' },
  { field: 'broker_markdown', label: 'Broker Mkdn', type: 'ccy', group: 'fees' },
  { field: 'soft_dollar_eligible', label: 'Soft$', type: 'bool', group: 'fees' },
  { field: 'total_fees', label: 'Total Fees', type: 'ccy', group: 'fees' },
  { field: 'total_fees_local', label: 'Fees Loc', type: 'ccy', group: 'fees' },
  { field: 'total_fees_usd', label: 'Fees USD', type: 'ccy', group: 'fees' },

  // ── COUNTERPARTY ────────────────────────────────────────
  { field: 'counterparty', label: 'CPty', type: 'enum', group: 'counterparty' },
  { field: 'counterparty_lei', label: 'CPty LEI', type: 'string', group: 'counterparty' },
  { field: 'counterparty_country', label: 'CPty Cty', type: 'enum', group: 'counterparty' },
  { field: 'broker', label: 'Broker', type: 'enum', group: 'counterparty' },
  { field: 'broker_lei', label: 'Broker LEI', type: 'string', group: 'counterparty' },
  { field: 'broker_country', label: 'Broker Cty', type: 'enum', group: 'counterparty' },
  { field: 'executing_dealer', label: 'Exec Dealer', type: 'enum', group: 'counterparty' },
  { field: 'clearing_member', label: 'Clear Mem', type: 'enum', group: 'counterparty' },
  { field: 'give_up_broker', label: 'Give Up', type: 'enum', group: 'counterparty' },
  { field: 'give_in_broker', label: 'Give In', type: 'enum', group: 'counterparty' },
  { field: 'settlement_agent', label: 'Settle Agt', type: 'enum', group: 'counterparty' },
  { field: 'prime_broker', label: 'PB', type: 'enum', group: 'counterparty' },

  // ── TIMESTAMPS ──────────────────────────────────────────
  { field: 'order_create_ts', label: 'Order Create', type: 'datetime', group: 'timestamps' },
  { field: 'order_route_ts', label: 'Order Route', type: 'datetime', group: 'timestamps' },
  { field: 'order_ack_ts', label: 'Order Ack', type: 'datetime', group: 'timestamps' },
  { field: 'order_open_ts', label: 'Order Open', type: 'datetime', group: 'timestamps' },
  { field: 'first_fill_ts', label: 'First Fill', type: 'datetime', group: 'timestamps' },
  { field: 'last_fill_ts', label: 'Last Fill', type: 'datetime', group: 'timestamps' },
  { field: 'order_done_ts', label: 'Order Done', type: 'datetime', group: 'timestamps' },
  { field: 'trade_ts', label: 'Trade TS', type: 'datetime', group: 'timestamps' },
  { field: 'allocation_ts', label: 'Alloc TS', type: 'datetime', group: 'timestamps' },
  { field: 'confirmation_ts', label: 'Conf TS', type: 'datetime', group: 'timestamps' },
  { field: 'settlement_ts', label: 'Settle TS', type: 'datetime', group: 'timestamps' },
  { field: 'last_event_ts', label: 'Last Event', type: 'datetime', group: 'timestamps' },

  // ── LIFECYCLE ───────────────────────────────────────────
  { field: 'status', label: 'Status', type: 'enum', group: 'lifecycle' },
  { field: 'lifecycle_stage', label: 'Stage', type: 'enum', group: 'lifecycle' },
  { field: 'allocation_status', label: 'Alloc Status', type: 'enum', group: 'lifecycle' },
  { field: 'confirmation_status', label: 'Conf Status', type: 'enum', group: 'lifecycle' },
  { field: 'matching_status', label: 'Match', type: 'enum', group: 'lifecycle' },
  { field: 'settlement_status', label: 'Settle Status', type: 'enum', group: 'lifecycle' },
  { field: 'clearing_status', label: 'Clear Status', type: 'enum', group: 'lifecycle' },
  { field: 'break_flag', label: 'Break', type: 'bool', group: 'lifecycle' },
  { field: 'break_reason', label: 'Break Reason', type: 'enum', group: 'lifecycle' },
  { field: 'amendments_count', label: 'Amends', type: 'int', group: 'lifecycle' },
  { field: 'cancellations_count', label: 'Cnxls', type: 'int', group: 'lifecycle' },
  { field: 'partial_fill_count', label: 'Partials', type: 'int', group: 'lifecycle' },
  { field: 'reject_count', label: 'Rejects', type: 'int', group: 'lifecycle' },
  { field: 'last_amendment_ts', label: 'Last Amend', type: 'datetime', group: 'lifecycle' },
  { field: 'last_cancellation_ts', label: 'Last Cnxl', type: 'datetime', group: 'lifecycle' },

  // ── ALLOCATION ──────────────────────────────────────────
  { field: 'block_quantity', label: 'Block Qty', type: 'qty', group: 'allocation' },
  { field: 'allocated_quantity', label: 'Alloc Qty', type: 'qty', group: 'allocation' },
  { field: 'unallocated_quantity', label: 'Unalloc Qty', type: 'qty', group: 'allocation' },
  { field: 'allocation_method', label: 'Alloc Mth', type: 'enum', group: 'allocation' },
  { field: 'allocation_count', label: 'Alloc Count', type: 'int', group: 'allocation' },
  { field: 'average_allocation_size', label: 'Avg Alloc', type: 'qty', group: 'allocation' },
  { field: 'allocation_min', label: 'Min Alloc', type: 'qty', group: 'allocation' },
  { field: 'allocation_max', label: 'Max Alloc', type: 'qty', group: 'allocation' },
  { field: 'allocation_currency', label: 'Alloc CCY', type: 'enum', group: 'allocation' },
  { field: 'allocation_fx_rate', label: 'Alloc FX', type: 'rate', group: 'allocation' },

  // ── SETTLEMENT ──────────────────────────────────────────
  { field: 'settle_date_actual', label: 'Settle Actual', type: 'date', group: 'settlement' },
  { field: 'settle_date_expected', label: 'Settle Exp', type: 'date', group: 'settlement' },
  { field: 'days_to_settle', label: 'Days/Settle', type: 'int', group: 'settlement' },
  { field: 'settlement_amount', label: 'Settle Amt', type: 'ccy', group: 'settlement' },
  { field: 'settlement_amount_usd', label: 'Settle USD', type: 'ccy', group: 'settlement' },
  { field: 'settlement_currency_x', label: 'Settle CCY', type: 'enum', group: 'settlement' },
  { field: 'settlement_account', label: 'Settle Acct', type: 'string', group: 'settlement' },
  { field: 'dvp_flag', label: 'DVP', type: 'bool', group: 'settlement' },
  { field: 'settlement_instructions', label: 'Instr', type: 'string', group: 'settlement' },
  { field: 'settlement_reference', label: 'Settle Ref', type: 'string', group: 'settlement' },

  // ── REGULATORY ──────────────────────────────────────────
  { field: 'mifid_flag', label: 'MiFID', type: 'bool', group: 'regulatory' },
  { field: 'mifid_decision_maker', label: 'MiFID DM', type: 'string', group: 'regulatory' },
  { field: 'mifid_execution_decision_maker', label: 'MiFID EDM', type: 'string', group: 'regulatory' },
  { field: 'mifid_short_selling_indicator', label: 'Short Ind', type: 'enum', group: 'regulatory' },
  { field: 'dodd_frank_swap_flag', label: 'DF Swap', type: 'bool', group: 'regulatory' },
  { field: 'emir_reporting_flag', label: 'EMIR', type: 'bool', group: 'regulatory' },
  { field: 'cat_reporting_flag', label: 'CAT', type: 'bool', group: 'regulatory' },
  { field: 'consolidated_audit_trail_id', label: 'CAT ID', type: 'string', group: 'regulatory' },
  { field: 'lei_buy_side', label: 'LEI Buy', type: 'string', group: 'regulatory' },
  { field: 'lei_sell_side', label: 'LEI Sell', type: 'string', group: 'regulatory' },
  { field: 'uti', label: 'UTI', type: 'string', group: 'regulatory' },
  { field: 'usi', label: 'USI', type: 'string', group: 'regulatory' },

  // ── RISK / IMPACT ───────────────────────────────────────
  { field: 'portfolio_impact_usd', label: 'Pfl Impact USD', type: 'ccy', group: 'risk_impact', signed: true },
  { field: 'portfolio_impact_pct', label: 'Pfl Impact %', type: 'pct', group: 'risk_impact', signed: true },
  { field: 'risk_increase_dv01', label: 'Δ DV01', type: 'ccy', group: 'risk_impact', signed: true },
  { field: 'risk_increase_var', label: 'Δ VaR', type: 'ccy', group: 'risk_impact', signed: true },
  { field: 'concentration_increase_pct', label: 'Δ Conc %', type: 'pct', group: 'risk_impact', signed: true },
  { field: 'correlation_to_book', label: 'Corr Book', type: 'rate', group: 'risk_impact', signed: true },
  { field: 'expected_market_impact_bps', label: 'Exp Impact bps', type: 'bps', group: 'risk_impact' },
  { field: 'realized_market_impact_bps', label: 'Real Impact bps', type: 'bps', group: 'risk_impact' },
  { field: 'impact_decay_minutes', label: 'Impact Decay', type: 'int', group: 'risk_impact' },
  { field: 'crowdedness_score', label: 'Crowd Score', type: 'rate', group: 'risk_impact' },
  { field: 'liquidity_score', label: 'Liq Score', type: 'rate', group: 'risk_impact' },
  { field: 'participation_30min_pct', label: 'Part 30m %', type: 'pct', group: 'risk_impact' },
  { field: 'participation_eod_pct', label: 'Part EOD %', type: 'pct', group: 'risk_impact' },
  { field: 'position_change_pct', label: 'Δ Pos %', type: 'pct', group: 'risk_impact', signed: true },
  { field: 'is_position_increase', label: 'Pos Increase', type: 'bool', group: 'risk_impact' },

  // ── ATTRIBUTION ─────────────────────────────────────────
  { field: 'attribution_alpha', label: 'Attr α', type: 'ccy', group: 'attribution', signed: true },
  { field: 'attribution_beta', label: 'Attr β', type: 'ccy', group: 'attribution', signed: true },
  { field: 'attribution_currency', label: 'Attr CCY', type: 'ccy', group: 'attribution', signed: true },
  { field: 'attribution_country', label: 'Attr Cty', type: 'ccy', group: 'attribution', signed: true },
  { field: 'attribution_sector', label: 'Attr Sect', type: 'ccy', group: 'attribution', signed: true },
  { field: 'attribution_factor_value', label: 'F Value', type: 'rate', group: 'attribution', signed: true },
  { field: 'attribution_factor_momentum', label: 'F Mom', type: 'rate', group: 'attribution', signed: true },
  { field: 'attribution_factor_quality', label: 'F Qual', type: 'rate', group: 'attribution', signed: true },
  { field: 'attribution_factor_size', label: 'F Size', type: 'rate', group: 'attribution', signed: true },
  { field: 'attribution_residual', label: 'Attr ε', type: 'ccy', group: 'attribution', signed: true },

  // ── RESEARCH ────────────────────────────────────────────
  { field: 'signal_id', label: 'Signal ID', type: 'string', group: 'research' },
  { field: 'signal_source', label: 'Signal Src', type: 'enum', group: 'research' },
  { field: 'signal_strength', label: 'Sig Strength', type: 'rate', group: 'research' },
  { field: 'research_note_id', label: 'Note ID', type: 'string', group: 'research' },
  { field: 'recommendation_target', label: 'Tgt Price', type: 'price', group: 'research' },
  { field: 'recommendation_stop', label: 'Stop Price', type: 'price', group: 'research' },
  { field: 'research_horizon_days', label: 'Horizon d', type: 'int', group: 'research' },
  { field: 'signal_confidence', label: 'Sig Conf', type: 'rate', group: 'research' },

  // ── AUDIT ───────────────────────────────────────────────
  { field: 'order_origin', label: 'Origin', type: 'enum', group: 'audit' },
  { field: 'system_of_record', label: 'SoR', type: 'enum', group: 'audit' },
  { field: 'trade_capture_system', label: 'TCS', type: 'enum', group: 'audit' },
  { field: 'front_office_user', label: 'FO User', type: 'string', group: 'audit' },
  { field: 'middle_office_user', label: 'MO User', type: 'string', group: 'audit' },
  { field: 'back_office_user', label: 'BO User', type: 'string', group: 'audit' },
  { field: 'ops_notes', label: 'Notes', type: 'string', group: 'audit' },
  { field: 'compliance_review_status', label: 'Compl Status', type: 'enum', group: 'audit' },
  { field: 'compliance_reviewer', label: 'Compl Rvw', type: 'string', group: 'audit' },
  { field: 'last_modified_by', label: 'Mod By', type: 'string', group: 'audit' },
  { field: 'last_modified_ts', label: 'Mod TS', type: 'datetime', group: 'audit' },
  { field: 'version', label: 'Version', type: 'int', group: 'audit' },
];

export const TRADE_FIELDS = TRADE_COLUMNS.map((c) => c.field);
