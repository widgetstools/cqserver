import { useEffect, useMemo, useState } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { GridPanel } from '@/components/panels/GridPanel';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { QUERIES, type QueryEntry, type QueryFeature } from '@/lib/queries/library';
import { DOCS_BY_ID } from '@/docs';
import { Search, ChevronDown, ChevronRight } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { ColDef } from 'ag-grid-community';
import { fmtCcy, fmtSigned, fmtBps } from '@/lib/format';
import { useLiveQuery, type LiveQuerySpec } from '@/lib/use-live-query';
import { getCqClient, type Row } from '@/lib/cq-store';

const FEATURE_LABEL: Record<QueryFeature, string> = {
  join: 'Joins',
  filter: 'Filters',
  agg: 'Aggregations',
  pivot: 'Pivots',
  view: 'Views',
  window: 'Window Functions',
};

const FEATURE_ORDER: QueryFeature[] = ['join', 'filter', 'agg', 'pivot', 'view', 'window'];

/**
 * Detect which topic the SQL targets. cqserver's SOW path resolves
 * the topic from the message envelope (not the FROM clause), so we
 * extract the first identifier after FROM and route to the
 * corresponding topic. Aliases are stripped (`FROM positions p` →
 * positions). cqserver rewrites the FROM clause server-side, so the
 * SQL is sent verbatim.
 */
const KNOWN_TOPICS = new Set([
  'positions',
  'trades',
  'securities',
  'fi-market-data',
  'risk',
]);

function detectTopic(sql: string): string | null {
  const m = /from\s+([\w-]+)/i.exec(sql);
  if (!m) return null;
  const ident = m[1]!.toLowerCase().replace(/_/g, '-');
  if (KNOWN_TOPICS.has(ident)) return `/${ident}`;
  // Also accept the rewritten `t` placeholder and pass through to /positions.
  return null;
}

/**
 * Crude but effective JOIN detector. cqserver's `sowAndSubscribe`
 * path runs `parse_query` against ONLY the left topic's schema —
 * any column from the right side ("trade_id" when subscribing to
 * /positions) trips an "Unknown column" error. The JOIN executor
 * lives in the one-shot SOW path (`deliver_join_snapshot`), so
 * multi-topic queries fall back to `client.sow()` and render a
 * static snapshot instead of a live grid. Single-topic queries
 * (SELECT / WHERE / GROUP BY / aggregates) get the live treatment.
 *
 * The regex tolerates whitespace + various JOIN flavors (LEFT,
 * RIGHT, FULL, INNER, AS OF) and the BROADCAST hint.
 */
function hasJoin(sql: string): boolean {
  return /\bjoin\b/i.test(sql);
}

/**
 * Strip `<alias>.` prefixes from column references so cqserver's
 * parser — which doesn't carry a table-alias resolution table —
 * sees the bare column name from the combined JOIN schema.
 *
 * Aliases come from `FROM <table> <alias>` and `JOIN <table> <alias>`.
 * We collect them, then rewrite every `<alias>.<word>` to `<word>`
 * everywhere else in the SQL. Column-name collisions across the two
 * sides (e.g. both topics carry `symbol`) are resolved by the
 * combined schema's column ordering — the left side wins, which is
 * good enough for the demo queries.
 */
function stripAliases(sql: string): string {
  const aliasRe = /(?:from|join)\s+(\w+)\s+(\w+)\b/gi;
  const aliases: string[] = [];
  let m: RegExpExecArray | null;
  while ((m = aliasRe.exec(sql)) !== null) {
    const alias = m[2]!;
    // Filter out SQL reserved words that match the same shape
    // (FROM positions WHERE → m[2]=='WHERE'). USING / ON / WHERE /
    // INNER never legally appear as aliases.
    const lower = alias.toLowerCase();
    if (['using', 'on', 'where', 'inner', 'left', 'right', 'full', 'outer', 'cross', 'group', 'order', 'limit', 'having'].includes(lower)) {
      continue;
    }
    aliases.push(alias);
  }
  if (aliases.length === 0) return sql;
  let out = sql;
  // Strip `alias.` prefixes.
  for (const a of aliases) {
    const escA = a.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re = new RegExp(`\\b${escA}\\.`, 'g');
    out = out.replace(re, '');
  }
  // Drop the alias tokens from FROM/JOIN clauses too — leaving the
  // bare table name is what cqserver's parser expects.
  out = out.replace(/(\bfrom\s+\w+)\s+\w+\b/gi, (full, fromPart) => {
    // Only strip when the next word ISN'T a SQL keyword.
    const next = full.slice(fromPart.length).trim().toLowerCase();
    if (['where', 'join', 'inner', 'left', 'right', 'full', 'outer', 'cross', 'group', 'order', 'limit', 'having', 'using', 'on'].some((k) => next.startsWith(k))) {
      return full;
    }
    return fromPart;
  });
  out = out.replace(/(\bjoin\s+\w+)\s+(\w+)\b/gi, (full, joinPart, next: string) => {
    const lower = next.toLowerCase();
    if (['using', 'on', 'where', 'inner', 'left', 'right', 'full', 'outer', 'cross', 'group', 'order', 'limit', 'having'].includes(lower)) {
      return full;
    }
    return joinPart;
  });
  return out;
}

function inferColDefs(rows: Record<string, unknown>[]): ColDef[] {
  if (rows.length === 0) return [];
  return Object.keys(rows[0]!).map((k) => {
    const sample = rows.find((r) => r[k] != null)?.[k];
    const isNumber = typeof sample === 'number';
    const lk = k.toLowerCase();
    let valueFormatter: ColDef['valueFormatter'];
    if (isNumber) {
      if (/bps$/.test(lk)) valueFormatter = (p) => fmtBps(p.value as number, 2);
      else if (/usd$|notional|exposure|mv$|pnl|fees|var/.test(lk)) {
        const signed = /pnl|net|delta/.test(lk);
        valueFormatter = (p) => (signed ? fmtSigned(p.value as number) : fmtCcy(p.value as number));
      } else {
        valueFormatter = (p) => (p.value as number).toLocaleString('en-US', { maximumFractionDigits: 2 });
      }
    }
    return {
      field: k,
      headerName: k.toUpperCase().replace(/_/g, ' '),
      width: 140,
      valueFormatter,
      cellClass: isNumber ? 'tabular-cell text-right' : '',
    } as ColDef;
  });
}

/**
 * Best-effort row-id extractor for an arbitrary query result. Uses
 * common stable-id columns when present; falls back to a hash of
 * every cell so even synth aggregate rows are uniquely identified
 * across delta batches.
 *
 * AG-Grid needs a stable id to map `applyTransactionAsync({update})`
 * back to a row — the wrong choice produces "row recreated on every
 * delta" symptoms (no flash, full re-render). The aggregate views
 * (GROUP BY / pivots) have stable group-key tuples, which the
 * stringified-row fallback hashes.
 */
function adhocRowId(row: Row): string {
  return String(
    row.position_id ??
      row.trade_id ??
      row.cusip ??
      row.book_name ??
      row.symbol ??
      // Group-key tuples — pick the most likely composite shapes.
      (row.book_id != null && row.cusip != null ? `${row.book_id}|${row.cusip}` : undefined) ??
      (row.issuer_sector != null && row.issuer_region != null
        ? `${row.issuer_sector}|${row.issuer_region}`
        : undefined) ??
      // Last resort — canonicalize every cell. Aggregate rows whose
      // values shift over time would change id (bad), but for live
      // SOW the grouping columns are stable; only the AGG values
      // change. We hash only string keys (the GROUP BY shape) when
      // available.
      JSON.stringify(row),
  );
}

/**
 * Per-query metadata kept once a query has been Run. cqserver's
 * sub-time JOIN evaluator only lives in the one-shot SOW path
 * (`deliver_join_snapshot` in router.rs) — the streaming
 * `sowAndSubscribe` path runs against the LEFT topic's schema only
 * and trips "Unknown column" on any right-side ref. So the runner
 * forks by mode:
 *
 *   - `mode = 'live'`  — single-topic SELECT / WHERE / GROUP BY /
 *     aggregates. Live `sowAndSubscribe`; the grid ticks on every
 *     matched-row update.
 *   - `mode = 'static'` — multi-topic JOIN queries. One-shot `sow()`
 *     against the left topic; grid renders the result frozen. Still
 *     uses ag-grid `rowData`, just no live deltas (cqserver doesn't
 *     stream join results today).
 */
interface LiveRunContext {
  mode: 'live';
  spec: LiveQuerySpec;
  cols: ColDef[] | null;
  startedAt: number;
  qid: string;
}

interface StaticRunContext {
  mode: 'static';
  rows: Row[];
  cols: ColDef[];
  elapsedMs: number;
  qid: string;
}

type RunContext = LiveRunContext | StaticRunContext;

export function QueryBuilderCanvas() {
  const [filter, setFilter] = useState('');
  const [selectedId, setSelectedId] = useState<string>(QUERIES[0]!.id);
  const [openGroups, setOpenGroups] = useState<Set<QueryFeature>>(new Set(['join', 'agg']));
  const [editorValue, setEditorValue] = useState<string>(QUERIES[0]!.sql);
  const [run, setRun] = useState<RunContext | null>(null);
  const [runError, setRunError] = useState<string | null>(null);
  const [runningStatic, setRunningStatic] = useState(false);

  // Drive the live subscription off the current Run (only when in
  // live mode). Each Run press tears down the old sub and opens a
  // new one — the hook re-renders automatically when the spec
  // identity changes.
  const liveSpec = run?.mode === 'live' ? run.spec : null;
  const live = useLiveQuery(liveSpec);

  // Surface either the start-time error (couldn't even open the sub)
  // or any subsequent runtime error the server returned.
  const error = runError ?? live?.error ?? null;

  // For live mode: once the snapshot lands, derive column defs from
  // the first row and cache them on the RunContext so subsequent
  // ticks don't reshape the grid. Build-once-per-Run.
  if (run?.mode === 'live' && !run.cols && live && live.size > 0) {
    const inferred = inferColDefs(live.getSnapshot() as Record<string, unknown>[]);
    if (inferred.length > 0) {
      // Mutate the run context in place — React has already
      // re-rendered via `live.size`; cols are presentation-only.
      run.cols = inferred;
    }
  }

  const filtered = useMemo(() => {
    if (!filter.trim()) return QUERIES;
    const q = filter.toLowerCase();
    return QUERIES.filter(
      (qq) =>
        qq.title.toLowerCase().includes(q) ||
        qq.synopsis.toLowerCase().includes(q) ||
        qq.sql.toLowerCase().includes(q) ||
        qq.feature.includes(q),
    );
  }, [filter]);

  const groups = useMemo(() => {
    const m = new Map<QueryFeature, QueryEntry[]>();
    for (const q of filtered) {
      const arr = m.get(q.feature) ?? [];
      arr.push(q);
      m.set(q.feature, arr);
    }
    return m;
  }, [filtered]);

  const selected = QUERIES.find((q) => q.id === selectedId) ?? QUERIES[0]!;

  const select = (q: QueryEntry) => {
    setSelectedId(q.id);
    setEditorValue(q.sql);
    setRun(null);
    setRunError(null);
    setRunningStatic(false);
  };

  const handleRun = (sql: string) => {
    setRunError(null);
    const topic = detectTopic(sql);
    if (!topic) {
      setRunError(
        'No known topic in FROM clause. Recognised: positions, trades, securities, fi-market-data, risk',
      );
      setRun(null);
      return;
    }
    // cqserver's SQL parser doesn't carry a table-alias resolution
    // table, so we strip `<alias>.` prefixes before sending. The
    // editor still shows the readable alias form to the user.
    const wireSql = stripAliases(sql);

    // JOIN queries take the static snapshot path — server's
    // subscribe-time evaluator can't handle multi-topic schemas.
    // Non-JOIN queries take the live path.
    if (hasJoin(wireSql)) {
      void runStatic(topic, wireSql, selected.id);
    } else {
      setRun({
        mode: 'live',
        spec: { topic, sql: wireSql, getRowId: adhocRowId },
        cols: null,
        startedAt: performance.now(),
        qid: selected.id,
      });
    }
  };

  const runStatic = async (topic: string, sql: string, qid: string) => {
    setRun(null); // tear down any previous live sub
    setRunningStatic(true);
    const start = performance.now();
    try {
      const client = getCqClient();
      if (!client) throw new Error('cqserver client not connected');
      const rows = (await client.sow(topic, { sql })) as Row[];
      setRun({
        mode: 'static',
        rows,
        cols: inferColDefs(rows as Record<string, unknown>[]),
        elapsedMs: performance.now() - start,
        qid,
      });
    } catch (err) {
      setRunError(err instanceof Error ? err.message : String(err));
    } finally {
      setRunningStatic(false);
    }
  };

  // Status helpers for the panel chrome.
  const liveStatus = live?.status ?? 'idle';
  const runningLive = run?.mode === 'live' && (liveStatus === 'connecting' || liveStatus === 'snapshotting');
  const running = runningStatic || runningLive;
  const rowCount = run?.mode === 'live'
    ? (live?.size ?? 0)
    : run?.mode === 'static'
      ? run.rows.length
      : 0;
  const isLive = run?.mode === 'live' && liveStatus === 'live';
  // Surface the catalog id of the actively-rendered run for the badge.
  // (Currently unused but kept for future "stale result" detection.)
  void run?.qid;

  // Effect-style elapsed-ms ticker so the chrome counter advances
  // while waiting for the snapshot. Forces a render every 250 ms
  // when a live query is mid-snapshot.
  const [now, setNow] = useState(0);
  useEffect(() => {
    if (!runningLive) return;
    const t = setInterval(() => setNow(performance.now()), 250);
    return () => clearInterval(t);
  }, [runningLive]);
  const elapsedMs =
    run?.mode === 'live'
      ? (runningLive ? now - run.startedAt : 0)
      : run?.mode === 'static'
        ? run.elapsedMs
        : 0;

  const panels: DockPanelSpec[] = [
    {
      id: 'library',
      title: 'Query Library · 40+ patterns',
      render: () => (
        <PanelChrome title={`Query Library · ${QUERIES.length} patterns`}>
          <div className="p-2 border-b border-border">
            <div className="relative">
              <Search size={11} className="absolute left-2 top-1/2 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                placeholder="Filter library…"
                className="pl-6 h-7 text-[11.5px]"
              />
            </div>
          </div>
          <div className="overflow-y-auto py-1" style={{ maxHeight: 'calc(100% - 50px)' }}>
            {FEATURE_ORDER.map((f) => {
              const list = groups.get(f) ?? [];
              if (list.length === 0) return null;
              const open = openGroups.has(f);
              return (
                <div key={f} className="mb-1">
                  <button
                    onClick={() => setOpenGroups((s) => {
                      const n = new Set(s);
                      if (n.has(f)) n.delete(f); else n.add(f);
                      return n;
                    })}
                    className="w-full flex items-center gap-1.5 px-3 py-1 text-[10px] font-mono uppercase tracking-[0.1em] text-muted-foreground hover:text-foreground"
                  >
                    {open ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
                    <span>{FEATURE_LABEL[f]}</span>
                    <span className="ml-auto font-mono">{list.length}</span>
                  </button>
                  {open ? (
                    <div>
                      {list.map((q) => {
                        const isSelected = q.id === selectedId;
                        return (
                          <div
                            key={q.id}
                            onClick={() => select(q)}
                            // Active and hover states both wash the row with the
                            // accent background. The design-system pairs
                            // `--accent` with `--accent-foreground` for
                            // guaranteed contrast (dark-on-mint in the
                            // dark theme, white-on-teal in the light
                            // theme), so we switch the whole row to
                            // `text-accent-foreground` to inherit that
                            // pairing instead of fighting it with
                            // `text-foreground`. The id chip also
                            // brightens via `text-accent-foreground/80`
                            // so it reads clearly without competing
                            // with the title.
                            className={cn(
                              'group px-3 py-1.5 cursor-pointer border-l-2 transition-colors',
                              isSelected
                                ? 'border-signal bg-accent text-accent-foreground'
                                : 'border-transparent text-muted-foreground hover:bg-accent hover:text-accent-foreground',
                            )}
                          >
                            <div
                              className={cn(
                                'text-[11.5px] font-medium leading-tight',
                                isSelected
                                  ? 'text-accent-foreground'
                                  : 'text-foreground group-hover:text-accent-foreground',
                              )}
                            >
                              {q.title}
                            </div>
                            {/*
                             * Synopsis: truncated by default so the
                             * library stays scannable; expands to full
                             * wrapped text on hover or when selected.
                             */}
                            <div
                              className={cn(
                                'text-[10px] mt-0.5 leading-snug',
                                isSelected
                                  ? 'text-accent-foreground whitespace-normal'
                                  : 'text-muted-foreground truncate group-hover:whitespace-normal group-hover:overflow-visible group-hover:text-accent-foreground',
                              )}
                            >
                              {q.synopsis}
                            </div>
                            <div
                              className={cn(
                                'text-[9.5px] font-mono uppercase tracking-[0.06em] mt-0.5',
                                isSelected
                                  ? 'text-accent-foreground/80'
                                  : 'text-muted-foreground/80 group-hover:text-accent-foreground/80',
                              )}
                            >
                              {q.id}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        </PanelChrome>
      ),
    },
    {
      id: 'editor',
      title: `${selected.id} · ${selected.title}`,
      render: () => (
        <SqlPanel
          title={`${selected.id} · ${selected.title}`}
          value={editorValue}
          onChange={setEditorValue}
          onRun={(sql) => handleRun(sql)}
          planSummary={selected.explain ?? `${selected.feature.toUpperCase()} · ${selected.id}`}
        />
      ),
    },
    {
      id: 'results',
      title: 'Results',
      render: () => (
        <PanelChrome
          title="Results"
          right={
            running ? (
              <span className="font-mono text-[10px] text-muted-foreground">running on cqserver…</span>
            ) : error ? (
              <Badge variant="err" className="!text-[9px]">ERROR</Badge>
            ) : run?.mode === 'live' ? (
              <div className="flex items-center gap-1.5">
                <Badge variant={isLive ? 'ok' : 'muted'} className="!text-[9px]">
                  {isLive ? 'cqserver · LIVE' : `cqserver · ${liveStatus}`}
                </Badge>
                <span className="font-mono text-[10px] text-muted-foreground">
                  {rowCount} rows{isLive ? ' · ticking' : elapsedMs > 0 ? ` · ${elapsedMs.toFixed(0)} ms` : ''}
                </span>
              </div>
            ) : run?.mode === 'static' ? (
              <div className="flex items-center gap-1.5">
                <Badge variant="ok" className="!text-[9px]">cqserver · snapshot</Badge>
                <span className="font-mono text-[10px] text-muted-foreground">
                  {rowCount} rows · {elapsedMs.toFixed(0)} ms · JOIN (static)
                </span>
              </div>
            ) : (
              <span className="font-mono text-[10px] text-muted-foreground">no results — press Run ▸</span>
            )
          }
        >
          {error ? (
            <div className="p-4 text-[11.5px]">
              <div className="atlas-eyebrow mb-2 text-err">Server error</div>
              <pre className="font-mono text-[11px] bg-muted border border-border rounded-sm p-2 whitespace-pre-wrap text-foreground">
                {error}
              </pre>
              <p className="text-muted-foreground mt-3 leading-relaxed">
                Single-topic <code className="font-mono">SELECT</code> /{' '}
                <code className="font-mono">WHERE</code> /{' '}
                <code className="font-mono">GROUP BY</code> / aggregates stream live via{' '}
                <code className="font-mono text-foreground">sowAndSubscribe(topic, {'{'} sql {'}'})</code>.
                Multi-topic JOIN queries fall back to a one-shot{' '}
                <code className="font-mono text-foreground">sow(topic, {'{'} sql {'}'})</code>{' '}
                (cqserver's subscribe-time evaluator parses against a single topic schema).
                For LIVE join output, declare a materialized view in{' '}
                <code className="font-mono">cqserver.toml</code>.
              </p>
            </div>
          ) : run?.mode === 'live' && live && run.cols ? (
            <GridPanel
              title=""
              colDefs={run.cols}
              getRowId={adhocRowId}
              liveSubscription={live}
            />
          ) : run?.mode === 'static' ? (
            <GridPanel
              title=""
              rows={run.rows as Record<string, unknown>[]}
              colDefs={run.cols}
              getRowId={adhocRowId}
            />
          ) : run ? (
            <div className="p-6 text-center">
              <div className="atlas-eyebrow mb-2">snapshotting…</div>
              <p className="text-[11.5px] text-muted-foreground max-w-xs mx-auto">
                Waiting for cqserver to ship the first delta. Once the snapshot lands,
                this grid will tick live on every source-row change.
              </p>
            </div>
          ) : (
            <div className="p-6 text-center">
              <div className="atlas-eyebrow mb-2">empty</div>
              <p className="text-[11.5px] text-muted-foreground max-w-xs mx-auto">
                Select a pattern on the left, edit if you like, then press{' '}
                <code className="font-mono text-signal">Run ▸</code>. Non-JOIN queries
                stream live via <code className="font-mono">applyTransactionAsync</code>;
                JOIN queries render a one-shot snapshot.
              </p>
            </div>
          )}
        </PanelChrome>
      ),
    },
    {
      id: 'synopsis',
      title: 'Pattern Notes',
      // Pin to the right edge so the docs strip stays next to the
      // SQL editor + result grid instead of getting buried below.
      pin: 'right',
      render: () => (
        <PanelChrome title="Pattern Notes">
          <div className="p-4 space-y-3 text-[12px]">
            <div className="atlas-eyebrow !text-[9px]">/{selected.id}</div>
            <h3 className="text-[14px] font-semibold leading-snug">{selected.title}</h3>
            <p className="text-muted-foreground leading-relaxed">{selected.synopsis}</p>
            <div className="flex flex-wrap gap-1">
              <span className="feature-tag" data-kind={selected.feature}>{selected.feature}</span>
              <Badge variant="muted">id · {selected.id}</Badge>
            </div>
            {selected.explain ? (
              <>
                <div className="atlas-eyebrow !text-[9px] mt-3">Estimated plan</div>
                <code className="block bg-muted border border-border rounded-sm px-2 py-1.5 text-[10.5px] font-mono leading-relaxed">
                  {selected.explain}
                </code>
              </>
            ) : null}
          </div>
        </PanelChrome>
      ),
    },
    {
      id: 'notes',
      title: 'Help · ex08.md',
      pin: 'right',
      render: () => <MarkdownPanel title="Help · ex08.md" filename="ex08.md" source={DOCS_BY_ID['query-builder']} />,
    },
  ];

  const layout: DockLayoutStep[] = [
    { id: 'library' },
    // Library is the pattern catalogue — shrink it by 30 % vs the
    // default 50/50 split so the SQL editor + result grid get the
    // working area they deserve. Editor takes 65 % of the parent
    // split → library lands at 35 % (≈ 0.7 × 50 %).
    { id: 'editor', relativeTo: 'library', direction: 'right', size: 0.65 },
    { id: 'synopsis', relativeTo: 'editor', direction: 'right' },
    { id: 'results', relativeTo: 'editor', direction: 'below' },
    { id: 'notes', relativeTo: 'synopsis', direction: 'below' },
  ];

  return <DockSurface panels={panels} layout={layout} />;
}
