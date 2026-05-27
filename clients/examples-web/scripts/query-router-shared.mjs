// Mirror of the topic-detection + alias-stripping helpers in
// src/examples/ex08-query-builder/index.tsx. Kept in plain JS so
// the standalone test script can `node` it without TS tooling.

const KNOWN_TOPICS = new Set([
  'positions',
  'trades',
  'securities',
  'fi-market-data',
  'risk',
  // Pre-configured AMPS materialised views (defined in cqserver.toml).
  'v_pnl_by_book',
  'v_net_exposure',
  'v_trades_by_compliance',
  'v_pnl_by_sector',
  'v_heatmap_sector_region',
  'v_cross_asset_pivot',
  'v_compliance_counts',
  'v_slippage_venue_algo',
]);

export function detectTopic(sql) {
  const m = /from\s+([\w-]+)/i.exec(sql);
  if (!m) return null;
  const raw = m[1].toLowerCase();
  // Try the underscored form first (matches view topics like
  // `/v_pnl_by_book`), then the hyphenated form (`/fi-market-data`).
  if (KNOWN_TOPICS.has(raw)) return `/${raw}`;
  const dashed = raw.replace(/_/g, '-');
  if (KNOWN_TOPICS.has(dashed)) return `/${dashed}`;
  return null;
}

export function hasJoin(sql) {
  return /\bjoin\b/i.test(sql);
}

export function stripAliases(sql) {
  const aliasRe = /(?:from|join)\s+(\w+)\s+(\w+)\b/gi;
  const aliases = [];
  let m;
  while ((m = aliasRe.exec(sql)) !== null) {
    const alias = m[2];
    const lower = alias.toLowerCase();
    if ([
      'using', 'on', 'where', 'inner', 'left', 'right', 'full',
      'outer', 'cross', 'group', 'order', 'limit', 'having',
    ].includes(lower)) {
      continue;
    }
    aliases.push(alias);
  }
  if (aliases.length === 0) return sql;
  let out = sql;
  for (const a of aliases) {
    const escA = a.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re = new RegExp(`\\b${escA}\\.`, 'g');
    out = out.replace(re, '');
  }
  out = out.replace(/(\bfrom\s+\w+)\s+\w+\b/gi, (full, fromPart) => {
    const next = full.slice(fromPart.length).trim().toLowerCase();
    if ([
      'where', 'join', 'inner', 'left', 'right', 'full', 'outer',
      'cross', 'group', 'order', 'limit', 'having', 'using', 'on',
    ].some((k) => next.startsWith(k))) {
      return full;
    }
    return fromPart;
  });
  out = out.replace(/(\bjoin\s+\w+)\s+(\w+)\b/gi, (full, joinPart, next) => {
    const lower = next.toLowerCase();
    if ([
      'using', 'on', 'where', 'inner', 'left', 'right', 'full',
      'outer', 'cross', 'group', 'order', 'limit', 'having',
    ].includes(lower)) {
      return full;
    }
    return joinPart;
  });
  return out;
}
