/**
 * Reference data for the FI demo.
 *
 * Exports a curated set of ~30 hand-crafted instruments (the BASE_*) and
 * procedural builders that scale the universe out to whatever the demo
 * needs (default 500 securities × 80 books = 40,000 (book, cusip) cells).
 *
 * None of the CUSIPs here are real — they look the part for demo purposes.
 */

export interface Security {
  cusip: string;
  ticker: string;
  issuer: string;
  coupon: number;        // % annual coupon
  maturity: string;      // YYYY-MM-DD
  assetClass: 'UST' | 'CORP' | 'MUNI' | 'AGENCY';
  sector: string;        // Treasury / Tech / Banks / etc.
  currency: 'USD';
  initialMid: number;    // starting price for the tick generator (per 100 face)
}

// ----- Curated base set (30 instruments) -----

export const BASE_SECURITIES: Security[] = [
  // US Treasuries
  { cusip: '912828YK0', ticker: 'T 2.5 11/30', issuer: 'US TREASURY', coupon: 2.5,  maturity: '2030-11-30', assetClass: 'UST',  sector: 'Treasury', currency: 'USD', initialMid: 98.42 },
  { cusip: '912828Z94', ticker: 'T 1.875 02/32', issuer: 'US TREASURY', coupon: 1.875, maturity: '2032-02-15', assetClass: 'UST', sector: 'Treasury', currency: 'USD', initialMid: 91.18 },
  { cusip: '912810TM0', ticker: 'T 3.0 08/52', issuer: 'US TREASURY', coupon: 3.0,  maturity: '2052-08-15', assetClass: 'UST', sector: 'Treasury', currency: 'USD', initialMid: 85.31 },
  { cusip: '91282CDH0', ticker: 'T 4.125 11/27', issuer: 'US TREASURY', coupon: 4.125, maturity: '2027-11-30', assetClass: 'UST', sector: 'Treasury', currency: 'USD', initialMid: 99.87 },
  { cusip: '912797HK1', ticker: 'T-BILL 6M', issuer: 'US TREASURY', coupon: 0.0, maturity: '2026-11-15', assetClass: 'UST', sector: 'Treasury', currency: 'USD', initialMid: 97.65 },
  { cusip: '91282CGE3', ticker: 'T 4.875 10/30', issuer: 'US TREASURY', coupon: 4.875, maturity: '2030-10-31', assetClass: 'UST', sector: 'Treasury', currency: 'USD', initialMid: 102.18 },

  // Tech corporates
  { cusip: '037833DT4', ticker: 'AAPL 3.85 05/43', issuer: 'APPLE INC',     coupon: 3.85, maturity: '2043-05-04', assetClass: 'CORP', sector: 'Tech',     currency: 'USD', initialMid: 84.55 },
  { cusip: '594918BR4', ticker: 'MSFT 3.7 08/46',  issuer: 'MICROSOFT CORP', coupon: 3.7,  maturity: '2046-08-08', assetClass: 'CORP', sector: 'Tech',     currency: 'USD', initialMid: 83.10 },
  { cusip: '023135BX3', ticker: 'AMZN 4.05 08/47', issuer: 'AMAZON.COM INC',  coupon: 4.05, maturity: '2047-08-22', assetClass: 'CORP', sector: 'Tech',     currency: 'USD', initialMid: 81.42 },
  { cusip: '02079KAB0', ticker: 'GOOGL 1.998 08/26', issuer: 'ALPHABET INC', coupon: 1.998, maturity: '2026-08-15', assetClass: 'CORP', sector: 'Tech',     currency: 'USD', initialMid: 96.20 },
  { cusip: '64110LAW4', ticker: 'NFLX 4.875 06/30',  issuer: 'NETFLIX INC',  coupon: 4.875, maturity: '2030-06-15', assetClass: 'CORP', sector: 'Tech',     currency: 'USD', initialMid: 99.30 },
  { cusip: '88160RAG6', ticker: 'TSLA 5.3 08/25',    issuer: 'TESLA INC',     coupon: 5.3,  maturity: '2025-08-15', assetClass: 'CORP', sector: 'Tech',     currency: 'USD', initialMid: 100.45 },

  // Banks
  { cusip: '06051GHB0', ticker: 'BAC 4.083 03/38',   issuer: 'BANK OF AMERICA', coupon: 4.083, maturity: '2038-03-20', assetClass: 'CORP', sector: 'Banks', currency: 'USD', initialMid: 89.10 },
  { cusip: '46625HJL5', ticker: 'JPM 4.452 12/29',   issuer: 'JPMORGAN CHASE',  coupon: 4.452, maturity: '2029-12-05', assetClass: 'CORP', sector: 'Banks', currency: 'USD', initialMid: 99.65 },
  { cusip: '38141GVR1', ticker: 'GS 3.8 03/30',      issuer: 'GOLDMAN SACHS',   coupon: 3.8,   maturity: '2030-03-15', assetClass: 'CORP', sector: 'Banks', currency: 'USD', initialMid: 93.27 },
  { cusip: '617446HZ4', ticker: 'MS 3.625 01/27',    issuer: 'MORGAN STANLEY',  coupon: 3.625, maturity: '2027-01-20', assetClass: 'CORP', sector: 'Banks', currency: 'USD', initialMid: 97.40 },
  { cusip: '949746SH5', ticker: 'WFC 4.15 01/29',    issuer: 'WELLS FARGO',     coupon: 4.15,  maturity: '2029-01-24', assetClass: 'CORP', sector: 'Banks', currency: 'USD', initialMid: 96.85 },

  // Energy
  { cusip: '30231GBC5', ticker: 'XOM 3.482 03/30',   issuer: 'EXXON MOBIL',  coupon: 3.482, maturity: '2030-03-19', assetClass: 'CORP', sector: 'Energy', currency: 'USD', initialMid: 93.71 },
  { cusip: '166764BG4', ticker: 'CVX 3.078 05/50',   issuer: 'CHEVRON CORP', coupon: 3.078, maturity: '2050-05-11', assetClass: 'CORP', sector: 'Energy', currency: 'USD', initialMid: 75.42 },
  { cusip: '20030NDL4', ticker: 'COP 4.3 08/28',     issuer: 'CONOCOPHILLIPS', coupon: 4.3,  maturity: '2028-08-15', assetClass: 'CORP', sector: 'Energy', currency: 'USD', initialMid: 98.55 },

  // Pharma
  { cusip: '478160CD4', ticker: 'JNJ 2.45 03/26',    issuer: 'JOHNSON & JOHNSON', coupon: 2.45, maturity: '2026-03-01', assetClass: 'CORP', sector: 'Pharma', currency: 'USD', initialMid: 96.18 },
  { cusip: '717081EZ9', ticker: 'PFE 4.2 05/30',     issuer: 'PFIZER INC',  coupon: 4.2,  maturity: '2030-05-15', assetClass: 'CORP', sector: 'Pharma', currency: 'USD', initialMid: 96.40 },
  { cusip: '00287YBQ7', ticker: 'ABBV 4.05 11/39',   issuer: 'ABBVIE INC',  coupon: 4.05, maturity: '2039-11-21', assetClass: 'CORP', sector: 'Pharma', currency: 'USD', initialMid: 87.20 },

  // Telecom / Media
  { cusip: '00206RGB7', ticker: 'T 4.35 06/29',      issuer: 'AT&T INC',  coupon: 4.35,  maturity: '2029-06-15', assetClass: 'CORP', sector: 'Telecom', currency: 'USD', initialMid: 95.66 },
  { cusip: '92343VEM5', ticker: 'VZ 4.812 03/39',    issuer: 'VERIZON',   coupon: 4.812, maturity: '2039-03-15', assetClass: 'CORP', sector: 'Telecom', currency: 'USD', initialMid: 91.78 },
  { cusip: '254687FK7', ticker: 'DIS 3.7 09/49',     issuer: 'WALT DISNEY', coupon: 3.7,  maturity: '2049-09-15', assetClass: 'CORP', sector: 'Media',   currency: 'USD', initialMid: 80.04 },

  // Agency
  { cusip: '3137EAEU9', ticker: 'FHLMC 1.5 02/25',   issuer: 'FREDDIE MAC', coupon: 1.5, maturity: '2025-02-12', assetClass: 'AGENCY', sector: 'Agency', currency: 'USD', initialMid: 99.92 },
  { cusip: '3135G06H1', ticker: 'FNMA 2.875 09/30',  issuer: 'FANNIE MAE',  coupon: 2.875, maturity: '2030-09-12', assetClass: 'AGENCY', sector: 'Agency', currency: 'USD', initialMid: 92.40 },

  // Muni
  { cusip: '64966LAT5', ticker: 'NYC GO 4.0 08/35',  issuer: 'NEW YORK CITY',  coupon: 4.0, maturity: '2035-08-01', assetClass: 'MUNI', sector: 'Muni', currency: 'USD', initialMid: 96.85 },
  { cusip: '13063DAC9', ticker: 'CA ST 5.0 04/40',   issuer: 'STATE OF CALIFORNIA', coupon: 5.0, maturity: '2040-04-01', assetClass: 'MUNI', sector: 'Muni', currency: 'USD', initialMid: 105.20 },
];

// ----- Issuer pool used to procedurally extend the universe -----

interface IssuerTemplate {
  ticker: string;     // short symbol used in synthesized tickers
  issuer: string;
  assetClass: Security['assetClass'];
  sector: string;
  baseCoupon: number; // rough median coupon for this issuer family
  basePrice: number;  // rough median price (per 100 face)
}

const ISSUER_POOL: IssuerTemplate[] = [
  // Treasury family (we'll synth UST issuances at different maturities)
  { ticker: 'T',       issuer: 'US TREASURY',          assetClass: 'UST',    sector: 'Treasury', baseCoupon: 3.5,  basePrice: 95 },
  { ticker: 'TIPS',    issuer: 'US TREASURY TIPS',     assetClass: 'UST',    sector: 'Treasury', baseCoupon: 1.0,  basePrice: 92 },
  { ticker: 'STRIPS',  issuer: 'US TREASURY STRIPS',   assetClass: 'UST',    sector: 'Treasury', baseCoupon: 0.0,  basePrice: 78 },
  // IG corporates — banks
  { ticker: 'CITI',    issuer: 'CITIGROUP INC',        assetClass: 'CORP',   sector: 'Banks', baseCoupon: 4.2,  basePrice: 95 },
  { ticker: 'PNC',     issuer: 'PNC FINANCIAL',        assetClass: 'CORP',   sector: 'Banks', baseCoupon: 3.9,  basePrice: 94 },
  { ticker: 'USB',     issuer: 'US BANCORP',           assetClass: 'CORP',   sector: 'Banks', baseCoupon: 3.7,  basePrice: 94 },
  { ticker: 'TFC',     issuer: 'TRUIST FINANCIAL',     assetClass: 'CORP',   sector: 'Banks', baseCoupon: 4.0,  basePrice: 93 },
  { ticker: 'COF',     issuer: 'CAPITAL ONE',          assetClass: 'CORP',   sector: 'Banks', baseCoupon: 4.5,  basePrice: 92 },
  // Tech
  { ticker: 'ORCL',    issuer: 'ORACLE CORP',          assetClass: 'CORP',   sector: 'Tech',  baseCoupon: 4.0,  basePrice: 92 },
  { ticker: 'IBM',     issuer: 'IBM CORP',             assetClass: 'CORP',   sector: 'Tech',  baseCoupon: 3.5,  basePrice: 90 },
  { ticker: 'CSCO',    issuer: 'CISCO SYSTEMS',        assetClass: 'CORP',   sector: 'Tech',  baseCoupon: 3.6,  basePrice: 93 },
  { ticker: 'INTC',    issuer: 'INTEL CORP',           assetClass: 'CORP',   sector: 'Tech',  baseCoupon: 4.0,  basePrice: 89 },
  { ticker: 'CRM',     issuer: 'SALESFORCE INC',       assetClass: 'CORP',   sector: 'Tech',  baseCoupon: 3.7,  basePrice: 91 },
  { ticker: 'ADBE',    issuer: 'ADOBE INC',            assetClass: 'CORP',   sector: 'Tech',  baseCoupon: 3.4,  basePrice: 92 },
  // Energy
  { ticker: 'OXY',     issuer: 'OCCIDENTAL PETROLEUM', assetClass: 'CORP',   sector: 'Energy', baseCoupon: 5.5, basePrice: 90 },
  { ticker: 'SLB',     issuer: 'SCHLUMBERGER',         assetClass: 'CORP',   sector: 'Energy', baseCoupon: 3.9, basePrice: 92 },
  { ticker: 'EOG',     issuer: 'EOG RESOURCES',        assetClass: 'CORP',   sector: 'Energy', baseCoupon: 4.2, basePrice: 93 },
  { ticker: 'KMI',     issuer: 'KINDER MORGAN',        assetClass: 'CORP',   sector: 'Energy', baseCoupon: 4.6, basePrice: 91 },
  // Pharma / health
  { ticker: 'MRK',     issuer: 'MERCK & CO',           assetClass: 'CORP',   sector: 'Pharma', baseCoupon: 3.6, basePrice: 92 },
  { ticker: 'LLY',     issuer: 'ELI LILLY',            assetClass: 'CORP',   sector: 'Pharma', baseCoupon: 3.4, basePrice: 93 },
  { ticker: 'BMY',     issuer: 'BRISTOL MYERS',        assetClass: 'CORP',   sector: 'Pharma', baseCoupon: 4.0, basePrice: 91 },
  { ticker: 'GILD',    issuer: 'GILEAD SCIENCES',      assetClass: 'CORP',   sector: 'Pharma', baseCoupon: 4.1, basePrice: 90 },
  { ticker: 'CVS',     issuer: 'CVS HEALTH',           assetClass: 'CORP',   sector: 'Pharma', baseCoupon: 4.3, basePrice: 91 },
  // Consumer
  { ticker: 'KO',      issuer: 'COCA-COLA CO',         assetClass: 'CORP',   sector: 'Consumer', baseCoupon: 3.5, basePrice: 94 },
  { ticker: 'PEP',     issuer: 'PEPSICO INC',          assetClass: 'CORP',   sector: 'Consumer', baseCoupon: 3.6, basePrice: 94 },
  { ticker: 'PG',      issuer: 'PROCTER & GAMBLE',     assetClass: 'CORP',   sector: 'Consumer', baseCoupon: 3.3, basePrice: 95 },
  { ticker: 'WMT',     issuer: 'WALMART INC',          assetClass: 'CORP',   sector: 'Consumer', baseCoupon: 3.7, basePrice: 95 },
  { ticker: 'HD',      issuer: 'HOME DEPOT',           assetClass: 'CORP',   sector: 'Consumer', baseCoupon: 3.9, basePrice: 95 },
  { ticker: 'COST',    issuer: 'COSTCO WHOLESALE',     assetClass: 'CORP',   sector: 'Consumer', baseCoupon: 3.5, basePrice: 95 },
  { ticker: 'MCD',     issuer: 'MCDONALDS CORP',       assetClass: 'CORP',   sector: 'Consumer', baseCoupon: 3.8, basePrice: 94 },
  // Industrials
  { ticker: 'GE',      issuer: 'GENERAL ELECTRIC',     assetClass: 'CORP',   sector: 'Industrial', baseCoupon: 4.2, basePrice: 92 },
  { ticker: 'CAT',     issuer: 'CATERPILLAR',          assetClass: 'CORP',   sector: 'Industrial', baseCoupon: 3.7, basePrice: 93 },
  { ticker: 'DE',      issuer: 'DEERE & CO',           assetClass: 'CORP',   sector: 'Industrial', baseCoupon: 3.6, basePrice: 93 },
  { ticker: 'BA',      issuer: 'BOEING CO',            assetClass: 'CORP',   sector: 'Industrial', baseCoupon: 5.0, basePrice: 89 },
  { ticker: 'LMT',     issuer: 'LOCKHEED MARTIN',      assetClass: 'CORP',   sector: 'Industrial', baseCoupon: 3.9, basePrice: 93 },
  { ticker: 'UPS',     issuer: 'UNITED PARCEL',        assetClass: 'CORP',   sector: 'Industrial', baseCoupon: 3.8, basePrice: 93 },
  // Auto
  { ticker: 'F',       issuer: 'FORD MOTOR CO',        assetClass: 'CORP',   sector: 'Auto', baseCoupon: 5.5, basePrice: 88 },
  { ticker: 'GM',      issuer: 'GENERAL MOTORS',       assetClass: 'CORP',   sector: 'Auto', baseCoupon: 5.2, basePrice: 89 },
  // Utilities
  { ticker: 'DUK',     issuer: 'DUKE ENERGY',          assetClass: 'CORP',   sector: 'Utilities', baseCoupon: 3.8, basePrice: 93 },
  { ticker: 'SO',      issuer: 'SOUTHERN CO',          assetClass: 'CORP',   sector: 'Utilities', baseCoupon: 3.9, basePrice: 92 },
  { ticker: 'NEE',     issuer: 'NEXTERA ENERGY',       assetClass: 'CORP',   sector: 'Utilities', baseCoupon: 3.7, basePrice: 93 },
  // REITs
  { ticker: 'AMT',     issuer: 'AMERICAN TOWER',       assetClass: 'CORP',   sector: 'REIT', baseCoupon: 3.8, basePrice: 91 },
  { ticker: 'PLD',     issuer: 'PROLOGIS INC',         assetClass: 'CORP',   sector: 'REIT', baseCoupon: 4.0, basePrice: 92 },
  { ticker: 'O',       issuer: 'REALTY INCOME',        assetClass: 'CORP',   sector: 'REIT', baseCoupon: 4.1, basePrice: 91 },
  // Agencies
  { ticker: 'FHLB',    issuer: 'FEDERAL HOME LOAN BK', assetClass: 'AGENCY', sector: 'Agency', baseCoupon: 3.0, basePrice: 95 },
  { ticker: 'GNMA',    issuer: 'GINNIE MAE',           assetClass: 'AGENCY', sector: 'Agency', baseCoupon: 3.5, basePrice: 94 },
  // Munis
  { ticker: 'IL ST',   issuer: 'STATE OF ILLINOIS',    assetClass: 'MUNI',   sector: 'Muni', baseCoupon: 4.5, basePrice: 96 },
  { ticker: 'TX ST',   issuer: 'STATE OF TEXAS',       assetClass: 'MUNI',   sector: 'Muni', baseCoupon: 3.8, basePrice: 99 },
  { ticker: 'FL ST',   issuer: 'STATE OF FLORIDA',     assetClass: 'MUNI',   sector: 'Muni', baseCoupon: 3.6, basePrice: 99 },
  { ticker: 'PA ST',   issuer: 'STATE OF PENNSYLVANIA',assetClass: 'MUNI',   sector: 'Muni', baseCoupon: 4.0, basePrice: 98 },
  { ticker: 'MA ST',   issuer: 'STATE OF MASSACHUSETTS',assetClass:'MUNI',   sector: 'Muni', baseCoupon: 3.9, basePrice: 99 },
  { ticker: 'LA CITY', issuer: 'CITY OF LOS ANGELES',  assetClass: 'MUNI',   sector: 'Muni', baseCoupon: 4.2, basePrice: 97 },
  { ticker: 'CHI CITY',issuer: 'CITY OF CHICAGO',      assetClass: 'MUNI',   sector: 'Muni', baseCoupon: 4.8, basePrice: 94 },
];

// ----- Procedural builders -----

/**
 * Build a security universe of the requested size. The first BASE_SECURITIES
 * entries are the hand-crafted ones; everything beyond is generated by
 * cycling the issuer pool over a maturity grid.
 *
 * CUSIPs are synthetic and deterministic: `S` + 8 hex digits of the index,
 * so subsequent runs publish the same universe.
 */
export function buildSecurities(target: number): Security[] {
  if (target <= BASE_SECURITIES.length) return BASE_SECURITIES.slice(0, target);
  const out: Security[] = [...BASE_SECURITIES];

  // Maturity grid: ~24 evenly-spaced points from 1y to 40y out, repeated
  // across the issuer pool. The combination gives ~24 * issuers ≈ 1.2k cells.
  const now = new Date();
  const maturityYears: number[] = [];
  for (let y = 1; y <= 40; y += 1.5) maturityYears.push(y);

  let i = 0;
  while (out.length < target) {
    const tpl = ISSUER_POOL[i % ISSUER_POOL.length];
    const ys = maturityYears[Math.floor(i / ISSUER_POOL.length) % maturityYears.length];
    const maturity = new Date(now.getTime() + ys * 365.25 * 24 * 3600 * 1000);
    const maturityISO = maturity.toISOString().slice(0, 10);
    // Coupon: jitter around base by ±0.6, snap to 0.125 grid.
    const coupon = roundTo(tpl.baseCoupon + ((i * 7919) % 13) / 10 - 0.6, 0.125);
    // Price: jitter ±5 around base, never below 70.
    const price = roundTo(Math.max(70, tpl.basePrice + (((i * 6151) % 11) - 5) * 0.9), 0.01);
    const idx = out.length;
    const cusip = 'S' + idx.toString(16).toUpperCase().padStart(8, '0');
    const mmYY = `${String(maturity.getUTCMonth() + 1).padStart(2, '0')}/${String(maturity.getUTCFullYear()).slice(2)}`;
    out.push({
      cusip,
      ticker: `${tpl.ticker} ${coupon.toFixed(3)} ${mmYY}`,
      issuer: tpl.issuer,
      coupon,
      maturity: maturityISO,
      assetClass: tpl.assetClass,
      sector: tpl.sector,
      currency: 'USD',
      initialMid: price,
    });
    i++;
  }
  return out;
}

function roundTo(value: number, step: number): number {
  return Math.round(value / step) * step;
}

// ----- Books -----

export const BASE_BOOKS = [
  'BOOK-RATES',
  'BOOK-CREDIT-IG',
  'BOOK-CREDIT-HY',
  'BOOK-MUNI',
  'BOOK-PROP',
] as const;
export type BaseBook = typeof BASE_BOOKS[number];

const STRATEGIES = [
  'RATES', 'CREDIT-IG', 'CREDIT-HY', 'MUNI', 'PROP',
  'FLOW', 'MM', 'RV', 'BASIS', 'CARRY', 'CURVE', 'RELVAL',
];
const REGIONS = ['US', 'EU', 'UK', 'APAC', 'LATAM', 'EMEA'];

/**
 * Build a book universe of the requested size. Starts with the curated
 * BASE_BOOKS and then synthesizes `BOOK-${strategy}-${region}-${nn}`.
 */
export function buildBooks(target: number): string[] {
  if (target <= BASE_BOOKS.length) return [...BASE_BOOKS].slice(0, target);
  const out: string[] = [...BASE_BOOKS];
  let i = 0;
  while (out.length < target) {
    const s = STRATEGIES[i % STRATEGIES.length];
    const r = REGIONS[Math.floor(i / STRATEGIES.length) % REGIONS.length];
    const n = Math.floor(i / (STRATEGIES.length * REGIONS.length)) + 1;
    out.push(`BOOK-${s}-${r}-${String(n).padStart(2, '0')}`);
    i++;
  }
  return out;
}

// ----- Backwards-compatible default exports -----
//
// The previous version exposed `SECURITIES` and `BOOKS` as ready-made
// constants. Keep those for any importer that still references them, but
// at the small base size — anything wanting a larger universe should call
// the builders directly.

export const SECURITIES: Security[] = BASE_SECURITIES;
export const BOOKS = BASE_BOOKS;
export type Book = BaseBook;

export const TRADERS = ['alice', 'bob', 'carol', 'dave', 'eve', 'frank', 'grace', 'heidi'] as const;
export const COUNTERPARTIES = ['CITI', 'GS', 'JPM', 'MS', 'BAC', 'BARC', 'CS', 'DB', 'HSBC', 'UBS', 'RBC', 'NOMURA'] as const;
