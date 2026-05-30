/**
 * Query — Chapter 08, Ad-Hoc SQL. Catalog rail on the left; SQL editor
 * and result grid on the right. Runner forks live vs static by JOIN
 * detection (see scopes/query.ts).
 */
import { useMemo, useState } from 'react';
import { QueryLibrary } from '../components/QueryLibrary';
import { SqlEditor } from '../components/SqlEditor';
import { QueryResult } from '../components/QueryResult';
import { QueryPivotResult } from '../components/QueryPivotResult';
import {
  QUERIES,
  FEATURE_LABEL,
  detectRunMode,
  detectFromTopic,
  stripAliases,
  fmtCount,
  fmtMs,
  type QueryEntry,
} from '../scopes/query';
import { parsePivotSql, inferPivotDisplay } from '../scopes/pivotGrid';
import { useLiveQuery, type LiveQuerySpec } from '@/lib/use-live-query';
import { runOneShotSql, type Row } from '@/lib/use-subscription';

const adhocRowId = (r: Row): string =>
  String(
    // JOIN fan-out (positions ⨝ trades) repeats one position_id across
    // many trade_ids, and a bare /trades row likewise shares its
    // position_id with sibling trades. Keying on position_id alone makes
    // AG Grid's getRowId collapse those to one row per position (the grid
    // drops duplicate ids), so the result grid looked empty/short. When
    // both ids are present, key on the composite so every row is unique.
    (r.position_id != null && r.trade_id != null
      ? `${r.position_id}|${r.trade_id}`
      : undefined) ??
      r.position_id ??
      r.trade_id ??
      r.cusip ??
      r.book_name ??
      r.symbol ??
      (r.book_id != null && r.cusip != null
        ? `${r.book_id}|${r.cusip}`
        : undefined) ??
      (r.issuer_sector != null && r.issuer_region != null
        ? `${r.issuer_sector}|${r.issuer_region}`
        : undefined) ??
      JSON.stringify(r),
  );

interface StaticRun {
  mode: 'static';
  rows: Row[];
  elapsedMs: number;
  qid: number;
}
interface LiveRun {
  mode: 'live';
  spec: LiveQuerySpec;
  qid: number;
}
type Run = StaticRun | LiveRun;

function QueryStat({ label, value, emphasis }: { label: string; value: string; emphasis?: boolean }) {
  return (
    <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
      <span style={{ fontSize: 9, letterSpacing: '.18em', color: 'var(--atlas-fg-faint)' }}>{label}</span>
      <span
        style={{
          fontSize: 12,
          fontWeight: 600,
          fontFeatureSettings: '"tnum"',
          color: emphasis ? 'var(--atlas-amber)' : 'var(--atlas-fg)',
        }}
      >
        {value}
      </span>
    </div>
  );
}

export function QueryChapter() {
  const [selected, setSelected] = useState<QueryEntry>(QUERIES[0]!);
  const [editorValue, setEditorValue] = useState<string>(QUERIES[0]!.sql);
  const [run, setRun] = useState<Run | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [sqlOpen, setSqlOpen] = useState(true);

  const pivotSpec = useMemo(() => parsePivotSql(editorValue), [editorValue]);
  const pivotDisplay = useMemo(
    () => selected.pivotDisplay ?? (pivotSpec ? inferPivotDisplay(pivotSpec) : null),
    [selected.pivotDisplay, pivotSpec],
  );
  const showPivotGrid = pivotSpec != null && pivotDisplay != null;

  const liveSpec = run?.mode === 'live' ? run.spec : null;
  const live = useLiveQuery(liveSpec);

  const runQuery = async () => {
    setError(null);
    const wireSql = stripAliases(editorValue);
    const mode = detectRunMode(wireSql);
    const topic = detectFromTopic(wireSql);
    const qid = Date.now();
    if (mode === 'live') {
      setRun({ mode: 'live', spec: { topic, sql: wireSql, getRowId: adhocRowId }, qid });
      return;
    }
    setBusy(true);
    const started = performance.now();
    try {
      const rows = await runOneShotSql(topic, wireSql);
      const elapsedMs = performance.now() - started;
      setRun({ mode: 'static', rows: rows as Row[], elapsedMs, qid });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setRun(null);
    } finally {
      setBusy(false);
    }
  };

  const onSelectQuery = (q: QueryEntry) => {
    setSelected(q);
    setEditorValue(q.sql);
    setError(null);
    setRun(null);
    setSqlOpen(true);
  };

  const liveError = live?.error ?? null;
  const surfacedError = error ?? liveError;

  const rowCount =
    run?.mode === 'static'
      ? run.rows.length
      : run?.mode === 'live'
        ? (live?.size ?? 0)
        : 0;
  const elapsed = run?.mode === 'static' ? fmtMs(run.elapsedMs) : '—';
  const status = surfacedError
    ? `error · ${surfacedError}`
    : run?.mode === 'live'
      ? `${rowCount.toLocaleString()} rows · live`
      : run?.mode === 'static'
        ? `${rowCount.toLocaleString()} rows · ${elapsed} · static (JOIN)`
        : 'press RUN';

  const stateLabel = surfacedError ? 'ERROR' : busy ? 'BUSY' : run ? 'OK' : 'IDLE';

  const resultTitle = useMemo(
    () => (run?.mode === 'live' ? 'RESULT · live' : run?.mode === 'static' ? 'RESULT · static' : 'RESULT'),
    [run?.mode],
  );

  return (
    <>
      <header
        style={{
          position: 'relative',
          zIndex: 1,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 16,
          padding: '8px 20px',
          borderBottom: '1px solid var(--atlas-rule)',
          flexShrink: 0,
        }}
      >
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 10, minWidth: 0 }}>
          <span style={{ fontSize: 9, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)' }}>08 · QUERY</span>
          <span
            style={{
              fontSize: 22,
              fontWeight: 700,
              letterSpacing: '-.02em',
              lineHeight: 1,
              color: 'var(--atlas-amber)',
            }}
          >
            query.
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 18, flexShrink: 0 }}>
          <QueryStat label="ROWS" value={fmtCount(rowCount)} emphasis />
          <QueryStat label="MODE" value={run?.mode?.toUpperCase() ?? '—'} />
          <QueryStat label="STATE" value={stateLabel} emphasis={!!surfacedError || busy} />
        </div>
      </header>
      <div
        style={{
          position: 'relative',
          zIndex: 1,
          flex: 1,
          display: 'flex',
          flexDirection: 'row',
          minHeight: 0,
        }}
      >
        <QueryLibrary selectedId={selected.id} onSelect={onSelectQuery} compact />
        <div
          style={{
            flex: 1,
            display: 'flex',
            flexDirection: 'column',
            minHeight: 0,
            minWidth: 0,
          }}
        >
          <div
            style={{
              flexShrink: 0,
              padding: '8px 14px',
              borderBottom: '1px solid var(--atlas-rule)',
              display: 'flex',
              alignItems: 'flex-start',
              gap: 10,
              minWidth: 0,
            }}
          >
            <span
              style={{
                fontSize: 9,
                letterSpacing: '.14em',
                padding: '3px 6px',
                border: '1px solid var(--atlas-rule)',
                color: 'var(--atlas-fg-dim)',
                flexShrink: 0,
                marginTop: 2,
              }}
            >
              {FEATURE_LABEL[selected.feature]}
            </span>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div
                style={{
                  fontSize: 12,
                  fontWeight: 600,
                  color: 'var(--atlas-fg)',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {selected.title}
              </div>
              <div
                style={{
                  fontSize: 10,
                  color: 'var(--atlas-fg-faint)',
                  marginTop: 2,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {selected.synopsis}
              </div>
            </div>
            <button
              type="button"
              onClick={() => setSqlOpen((o) => !o)}
              style={{
                flexShrink: 0,
                background: 'transparent',
                border: '1px solid var(--atlas-rule)',
                color: 'var(--atlas-fg-dim)',
                fontFamily: 'var(--atlas-font)',
                fontSize: 9,
                letterSpacing: '.16em',
                padding: '4px 8px',
                cursor: 'pointer',
              }}
            >
              {sqlOpen ? 'HIDE SQL' : 'SHOW SQL'}
            </button>
          </div>

          {sqlOpen ? (
            <SqlEditor
              value={editorValue}
              onChange={setEditorValue}
              onRun={runQuery}
              status={status}
              error={surfacedError}
              busy={busy}
              compact
            />
          ) : (
            <div
              style={{
                flexShrink: 0,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: 12,
                padding: '6px 14px',
                borderBottom: '1px solid var(--atlas-rule)',
                background: 'var(--atlas-surface)',
              }}
            >
              <div
                style={{
                  flex: 1,
                  fontSize: 10,
                  color: error ? 'var(--atlas-neg)' : 'var(--atlas-fg-faint)',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {surfacedError ?? status}
              </div>
              <button
                onClick={runQuery}
                disabled={busy}
                style={{
                  background: 'var(--atlas-amber)',
                  color: 'var(--atlas-ink)',
                  border: 'none',
                  padding: '5px 14px',
                  fontFamily: 'var(--atlas-font)',
                  fontSize: 11,
                  fontWeight: 700,
                  letterSpacing: '.18em',
                  cursor: busy ? 'wait' : 'pointer',
                  opacity: busy ? 0.6 : 1,
                  flexShrink: 0,
                }}
              >
                {busy ? 'RUNNING…' : 'RUN'}
              </button>
            </div>
          )}

          {showPivotGrid && pivotDisplay ? (
            <QueryPivotResult
              key={run?.qid ?? 'idle'}
              pivotSpec={pivotSpec}
              pivotDisplay={pivotDisplay}
              liveSubscription={run?.mode === 'live' ? (live ?? undefined) : undefined}
              staticRows={run?.mode === 'static' ? run.rows : undefined}
              compact
            />
          ) : (
            <QueryResult
              title={resultTitle}
              status={status}
              liveSubscription={run?.mode === 'live' ? (live ?? undefined) : undefined}
              staticRows={run?.mode === 'static' ? run.rows : undefined}
              getRowId={adhocRowId}
              compact
            />
          )}
        </div>
      </div>
    </>
  );
}
