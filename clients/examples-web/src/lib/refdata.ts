// Reference data used to synthesize realistic positions + trades.
// Hand-curated for plausibility — every issuer maps to a real country,
// sector, and asset class so cross-tabs (sector × region etc.) tell a
// believable story when displayed.

export interface IssuerRef {
  symbol: string;
  name: string;
  cusip: string;
  isin: string;
  sedol: string;
  bbg: string;
  ric: string;
  figi: string;
  lei: string;
  country: string;
  region: 'NA' | 'EMEA' | 'APAC' | 'LATAM';
  sector: string;
  industry: string;
  ccy: string;
  asset_class: 'EQUITY' | 'GOVT_BOND' | 'CORP_BOND' | 'SWAP' | 'OPTION' | 'FUTURE' | 'FX' | 'REPO';
  exchange: string;
}

// Helpers to spin up identifiers that look real without being real.
const fakeCusip = (sym: string) =>
  `${sym.charCodeAt(0).toString(36).padStart(3, '0')}${sym.length.toString().padStart(2, '0')}${(
    sym.charCodeAt(sym.length - 1) * 7
  )
    .toString(36)
    .padStart(4, '0')}`
    .slice(0, 9)
    .toUpperCase();

const fakeISIN = (country: string, cusip: string) => `${country}${cusip}${(cusip.length + country.length) % 10}`;
const fakeSedol = (sym: string) => `${sym.charCodeAt(0)}${sym.length}${sym.charCodeAt(sym.length - 1)}`.padEnd(7, '0').slice(0, 7);
const fakeFigi = (sym: string) => `BBG00${sym.padEnd(7, 'A').slice(0, 7).toUpperCase()}1`;
const fakeLEI = (sym: string) =>
  `${(sym.charCodeAt(0) * 31).toString(36).padStart(4, '0')}${(sym.length * 71).toString(36).padStart(8, '0')}${sym
    .slice(-2)
    .toUpperCase()
    .padEnd(8, '0')
    .slice(0, 8)}`.toUpperCase().padEnd(20, '0').slice(0, 20);

function eq(
  symbol: string,
  name: string,
  country: string,
  region: IssuerRef['region'],
  sector: string,
  industry: string,
  ccy: string,
  exchange: string,
): IssuerRef {
  const cusip = fakeCusip(symbol);
  return {
    symbol,
    name,
    cusip,
    isin: fakeISIN(country, cusip),
    sedol: fakeSedol(symbol),
    bbg: `${symbol} ${exchange === 'NYSE' || exchange === 'NASDAQ' ? 'US' : exchange === 'LSE' ? 'LN' : exchange === 'TSE' ? 'JT' : exchange === 'XETRA' ? 'GR' : 'EQ'} Equity`,
    ric: `${symbol}.${exchange === 'NYSE' ? 'N' : exchange === 'NASDAQ' ? 'O' : exchange === 'LSE' ? 'L' : exchange === 'TSE' ? 'T' : exchange === 'XETRA' ? 'DE' : 'X'}`,
    figi: fakeFigi(symbol),
    lei: fakeLEI(symbol),
    country,
    region,
    sector,
    industry,
    ccy,
    asset_class: 'EQUITY',
    exchange,
  };
}

function gb(symbol: string, name: string, country: string, region: IssuerRef['region'], ccy: string): IssuerRef {
  const cusip = fakeCusip(symbol);
  return {
    symbol,
    name,
    cusip,
    isin: fakeISIN(country, cusip),
    sedol: fakeSedol(symbol),
    bbg: `${symbol} Govt`,
    ric: `${country}${symbol.slice(-2)}=RR`,
    figi: fakeFigi(symbol),
    lei: fakeLEI(symbol),
    country,
    region,
    sector: 'Sovereign',
    industry: 'Government',
    ccy,
    asset_class: 'GOVT_BOND',
    exchange: 'OTC',
  };
}

function cb(symbol: string, name: string, country: string, region: IssuerRef['region'], sector: string, industry: string, ccy: string): IssuerRef {
  const cusip = fakeCusip(symbol);
  return {
    symbol,
    name,
    cusip,
    isin: fakeISIN(country, cusip),
    sedol: fakeSedol(symbol),
    bbg: `${symbol} Corp`,
    ric: `${symbol}=RR`,
    figi: fakeFigi(symbol),
    lei: fakeLEI(symbol),
    country,
    region,
    sector,
    industry,
    ccy,
    asset_class: 'CORP_BOND',
    exchange: 'OTC',
  };
}

export const ISSUERS: IssuerRef[] = [
  // ── US equities ─────────────────────────────────────────────
  eq('AAPL', 'Apple Inc',                'US', 'NA', 'Technology',     'Consumer Electronics', 'USD', 'NASDAQ'),
  eq('MSFT', 'Microsoft Corp',           'US', 'NA', 'Technology',     'Software',             'USD', 'NASDAQ'),
  eq('GOOGL','Alphabet Inc — Class A',   'US', 'NA', 'Communications', 'Interactive Media',    'USD', 'NASDAQ'),
  eq('AMZN', 'Amazon.com Inc',           'US', 'NA', 'Consumer Disc',  'E-Commerce',           'USD', 'NASDAQ'),
  eq('NVDA', 'NVIDIA Corp',              'US', 'NA', 'Technology',     'Semiconductors',       'USD', 'NASDAQ'),
  eq('META', 'Meta Platforms Inc',       'US', 'NA', 'Communications', 'Interactive Media',    'USD', 'NASDAQ'),
  eq('TSLA', 'Tesla Inc',                'US', 'NA', 'Consumer Disc',  'Automobiles',          'USD', 'NASDAQ'),
  eq('JPM',  'JPMorgan Chase & Co',      'US', 'NA', 'Financials',     'Diversified Banks',    'USD', 'NYSE'),
  eq('BAC',  'Bank of America Corp',     'US', 'NA', 'Financials',     'Diversified Banks',    'USD', 'NYSE'),
  eq('WFC',  'Wells Fargo & Co',         'US', 'NA', 'Financials',     'Diversified Banks',    'USD', 'NYSE'),
  eq('GS',   'Goldman Sachs Group',      'US', 'NA', 'Financials',     'Investment Banking',   'USD', 'NYSE'),
  eq('MS',   'Morgan Stanley',           'US', 'NA', 'Financials',     'Investment Banking',   'USD', 'NYSE'),
  eq('XOM',  'Exxon Mobil Corp',         'US', 'NA', 'Energy',         'Integrated Oil',       'USD', 'NYSE'),
  eq('CVX',  'Chevron Corp',             'US', 'NA', 'Energy',         'Integrated Oil',       'USD', 'NYSE'),
  eq('PFE',  'Pfizer Inc',               'US', 'NA', 'Health Care',    'Pharmaceuticals',      'USD', 'NYSE'),
  eq('JNJ',  'Johnson & Johnson',        'US', 'NA', 'Health Care',    'Pharmaceuticals',      'USD', 'NYSE'),
  eq('UNH',  'UnitedHealth Group',       'US', 'NA', 'Health Care',    'Managed Care',         'USD', 'NYSE'),
  eq('KO',   'Coca-Cola Co',             'US', 'NA', 'Consumer Stap',  'Soft Drinks',          'USD', 'NYSE'),
  eq('PEP',  'PepsiCo Inc',              'US', 'NA', 'Consumer Stap',  'Beverages',            'USD', 'NASDAQ'),
  eq('WMT',  'Walmart Inc',              'US', 'NA', 'Consumer Stap',  'Hypermarkets',         'USD', 'NYSE'),
  // ── Europe equities ────────────────────────────────────────
  eq('NOVN', 'Novartis AG',              'CH', 'EMEA', 'Health Care',    'Pharmaceuticals',    'CHF', 'XETRA'),
  eq('NESN', 'Nestle SA',                'CH', 'EMEA', 'Consumer Stap',  'Packaged Foods',     'CHF', 'XETRA'),
  eq('ASML', 'ASML Holding NV',          'NL', 'EMEA', 'Technology',     'Semi Equipment',     'EUR', 'XETRA'),
  eq('SAP',  'SAP SE',                   'DE', 'EMEA', 'Technology',     'Application SW',     'EUR', 'XETRA'),
  eq('LVMH', 'LVMH Moet Hennessy',       'FR', 'EMEA', 'Consumer Disc',  'Luxury Goods',       'EUR', 'XETRA'),
  eq('HSBA', 'HSBC Holdings plc',        'GB', 'EMEA', 'Financials',     'Diversified Banks',  'GBP', 'LSE'),
  eq('BP',   'BP plc',                   'GB', 'EMEA', 'Energy',         'Integrated Oil',     'GBP', 'LSE'),
  eq('AZN',  'AstraZeneca plc',          'GB', 'EMEA', 'Health Care',    'Pharmaceuticals',    'GBP', 'LSE'),
  eq('VOD',  'Vodafone Group plc',       'GB', 'EMEA', 'Communications', 'Wireless Telco',     'GBP', 'LSE'),
  eq('SHEL', 'Shell plc',                'GB', 'EMEA', 'Energy',         'Integrated Oil',     'GBP', 'LSE'),
  // ── Asia equities ──────────────────────────────────────────
  eq('7203', 'Toyota Motor Corp',        'JP', 'APAC', 'Consumer Disc',  'Automobiles',        'JPY', 'TSE'),
  eq('6758', 'Sony Group Corp',          'JP', 'APAC', 'Consumer Disc',  'Consumer Electronics','JPY', 'TSE'),
  eq('9984', 'SoftBank Group Corp',      'JP', 'APAC', 'Communications', 'Wireless Telco',     'JPY', 'TSE'),
  eq('9988', 'Alibaba Group Holding',    'CN', 'APAC', 'Consumer Disc',  'E-Commerce',         'HKD', 'HKEX'),
  eq('700',  'Tencent Holdings Ltd',     'CN', 'APAC', 'Communications', 'Interactive Media',  'HKD', 'HKEX'),
  eq('005930','Samsung Electronics',     'KR', 'APAC', 'Technology',     'Consumer Electronics','KRW', 'KRX'),
  eq('TSMC', 'Taiwan Semiconductor',     'TW', 'APAC', 'Technology',     'Semiconductors',     'TWD', 'TWSE'),
  eq('CBA',  'Commonwealth Bank',        'AU', 'APAC', 'Financials',     'Diversified Banks',  'AUD', 'ASX'),
  eq('BHP',  'BHP Group',                'AU', 'APAC', 'Materials',      'Diversified Mining', 'AUD', 'ASX'),
  // ── LATAM ──────────────────────────────────────────────────
  eq('VALE', 'Vale SA',                  'BR', 'LATAM','Materials',      'Iron Ore Mining',    'BRL', 'B3'),
  eq('PBR',  'Petroleo Brasileiro',      'BR', 'LATAM','Energy',         'Integrated Oil',     'BRL', 'B3'),
  // ── Govt bonds ─────────────────────────────────────────────
  gb('UST10Y', 'US Treasury 10Y',        'US', 'NA',  'USD'),
  gb('UST30Y', 'US Treasury 30Y',        'US', 'NA',  'USD'),
  gb('UST2Y',  'US Treasury 2Y',         'US', 'NA',  'USD'),
  gb('UKT10Y', 'UK Gilt 10Y',            'GB', 'EMEA','GBP'),
  gb('BUND10', 'German Bund 10Y',        'DE', 'EMEA','EUR'),
  gb('BTP10',  'Italy BTP 10Y',          'IT', 'EMEA','EUR'),
  gb('JGB10',  'Japan Govt Bond 10Y',    'JP', 'APAC','JPY'),
  gb('OAT10',  'France OAT 10Y',         'FR', 'EMEA','EUR'),
  // ── Corp bonds ─────────────────────────────────────────────
  cb('AAPL26', 'Apple 4.5 2026',         'US', 'NA',  'Technology', 'Consumer Electronics', 'USD'),
  cb('JPM28',  'JPMorgan 5.1 2028',      'US', 'NA',  'Financials', 'Diversified Banks',    'USD'),
  cb('GS27',   'Goldman Sachs 4.7 2027', 'US', 'NA',  'Financials', 'Investment Banking',   'USD'),
  cb('XOM30',  'Exxon Mobil 3.9 2030',   'US', 'NA',  'Energy',     'Integrated Oil',       'USD'),
];

// Sectors / regions / books / strategies — used by aggregations + pivot.
export const SECTORS = Array.from(new Set(ISSUERS.map((i) => i.sector)));
export const REGIONS: IssuerRef['region'][] = ['NA', 'EMEA', 'APAC', 'LATAM'];
export const CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'HKD', 'AUD', 'CAD', 'KRW', 'TWD', 'BRL'] as const;

export const FX: Record<string, number> = {
  USD: 1.0,
  EUR: 1.09,
  GBP: 1.27,
  JPY: 0.0067,
  CHF: 1.14,
  HKD: 0.128,
  AUD: 0.66,
  CAD: 0.74,
  KRW: 0.00075,
  TWD: 0.031,
  BRL: 0.20,
};

export const BOOKS = [
  { id: 'B1001', name: 'Global Macro',         strategy: 'MACRO',     desk: 'Macro' },
  { id: 'B1002', name: 'Equity Long-Short',    strategy: 'EQLS',      desk: 'Equity' },
  { id: 'B1003', name: 'Credit Relative Val',  strategy: 'CREDIT_RV', desk: 'Credit' },
  { id: 'B1004', name: 'Vol Arbitrage',        strategy: 'VOL_ARB',   desk: 'Vol' },
  { id: 'B1005', name: 'Index Replication',    strategy: 'INDEX',     desk: 'Equity' },
  { id: 'B1006', name: 'Rates Curve',          strategy: 'RATES_RV',  desk: 'Rates' },
  { id: 'B1007', name: 'EM Sovereign',         strategy: 'EM_SOV',    desk: 'EM' },
  { id: 'B1008', name: 'High-Yield Carry',     strategy: 'HY_CARRY',  desk: 'Credit' },
] as const;

export const TRADERS = [
  { id: 'T501', name: 'Diaz, Maria' },
  { id: 'T502', name: 'Chen, Wei' },
  { id: 'T503', name: 'Kowalski, Jan' },
  { id: 'T504', name: 'Adebayo, Ola' },
  { id: 'T505', name: 'Petrov, Anya' },
  { id: 'T506', name: 'Singh, Priya' },
  { id: 'T507', name: 'Mueller, Hans' },
  { id: 'T508', name: 'Tanaka, Hiroshi' },
] as const;

export const VENUES = [
  'NYSE', 'NASDAQ', 'BATS', 'IEX', 'LSE', 'XETRA', 'EURONEXT', 'CME', 'EUREX',
  'TSE', 'HKEX', 'ASX', 'KRX', 'TWSE', 'B3', 'OTC', 'DARK_POOL', 'LIQUIDNET',
] as const;

export const ALGOS = ['VWAP', 'TWAP', 'POV', 'IMPACT', 'CLOSE', 'IS', 'DARK_AGG', 'SNIPER', 'PEG', 'SOR'] as const;

export const BROKERS = [
  { id: 'BRK-GS',  name: 'Goldman Sachs' },
  { id: 'BRK-MS',  name: 'Morgan Stanley' },
  { id: 'BRK-JPM', name: 'JPMorgan' },
  { id: 'BRK-CITI','name': 'Citi' },
  { id: 'BRK-UBS', name: 'UBS' },
  { id: 'BRK-CS',  name: 'Credit Suisse' },
  { id: 'BRK-BCS', name: 'Barclays' },
  { id: 'BRK-DB',  name: 'Deutsche Bank' },
] as const;

export const COUNTERPARTIES = [
  'GS_NY', 'MS_NY', 'JPM_NY', 'CITI_NY', 'UBS_ZH', 'CS_ZH', 'BCS_LN', 'DB_FR',
  'HSBC_LN', 'BNP_PA', 'SG_PA', 'NOMURA_TY', 'MIZUHO_TY',
] as const;

export const CUSTODIANS = ['STATE_STREET', 'BNY_MELLON', 'NORTHERN_TRUST', 'JPM_CUST', 'CITI_CUST'] as const;

export const RATINGS_SP = ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-', 'B', 'CCC'] as const;
export const RATINGS_MOODY = ['Aaa', 'Aa1', 'Aa2', 'Aa3', 'A1', 'A2', 'A3', 'Baa1', 'Baa2', 'Baa3', 'Ba1', 'Ba2', 'Ba3', 'B1', 'Caa'] as const;

export const ESG_GRADES = ['AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC'] as const;

export const DURATION_BUCKETS = ['0-1Y', '1-3Y', '3-5Y', '5-7Y', '7-10Y', '10-15Y', '15-20Y', '20-30Y', '30Y+'] as const;
export const LIQUIDITY_TIERS = ['TIER_1_LIQUID', 'TIER_2_LIQUID', 'TIER_3_LESS_LIQUID', 'TIER_4_ILLIQUID'] as const;

export const SIDES = ['BUY', 'SELL', 'SHORT', 'COVER'] as const;
export const ORDER_TYPES = ['MARKET', 'LIMIT', 'STOP', 'STOP_LIMIT', 'MOC', 'MOO', 'LOC', 'LOO'] as const;
export const TIF = ['DAY', 'IOC', 'FOK', 'GTC', 'GTD', 'OPG', 'CLS'] as const;
export const TRADE_STATUSES = ['NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELED', 'REJECTED', 'EXPIRED', 'PENDING_REVIEW'] as const;
export const LIFECYCLE_STAGES = ['PRE_TRADE', 'EXECUTION', 'CONFIRMATION', 'CLEARING', 'SETTLEMENT', 'SETTLED'] as const;
