/**
 * Query — Chapter 08, Ad-Hoc SQL. The catalog rail on the left, an
 * editable SQL editor top right, a result grid bottom right. The
 * runner forks by mode (see scopes/query.ts comment): live for
 * single-topic queries, static for multi-topic JOIN queries.
 */
import { useMemo, useState } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { QueryLibrary } from '../components/QueryLibrary';
import { SqlEditor } from '../components/SqlEditor';
import { QueryResult } from '../components/QueryResult';
import {
  QUERIES,
  detectRunMode,
  detectFromTopic,
  stripAliases,
  fmtCount,
  fmtMs,
  type QueryEntry,
} from '../scopes/query';
import { useLiveQuery, type LiveQuerySpec } from '@/lib/use-live-query';
import { runOneShotSql, type Row } from '@/lib/use-subscription';

const adhocRowId = (r: Row): string =>
  String(
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

export function QueryChapter() {
  const [selected, setSelected] = useState<QueryEntry>(QUERIES[0]!);
  const [editorValue, setEditorValue] = useState<string>(QUERIES[0]!.sql);
  const [run, setRun] = useState<Run | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const liveSpec = run?.mode === 'live' ? run.spec : null;
  const live = useLiveQuery(liveSpec);

  const runQuery = async () => {
    setError(null);
    // Strip `alias.` prefixes so cqserver's parser doesn't trip on
    // `p.symbol`-style references (it has no alias-resolution table).
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

  const kpis = useMemo<Kpi[]>(
    () => [
      { label: 'CATALOG', value: fmtCount(QUERIES.length), caption: 'pre-built queries', emphasis: true },
      { label: 'MODE', value: run?.mode?.toUpperCase() ?? '—', caption: 'live = stream · static = SOW' },
      { label: 'ROWS', value: fmtCount(rowCount), caption: 'result' },
      { label: 'ELAPSED', value: elapsed, caption: 'one-shot run' },
      {
        label: 'STATE',
        value: surfacedError ? 'ERROR' : busy ? 'BUSY' : run ? 'OK' : 'IDLE',
        caption: 'runner',
        emphasis: !!surfacedError || busy,
      },
    ],
    [run, rowCount, elapsed, busy, surfacedError],
  );

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 08 — QUERY"
        title="query."
        sub="Pick from the catalog or write your own. Single-topic queries open a live sowAndSubscribe and tick on every match; multi-topic JOIN queries fall back to a one-shot SOW because cqserver's join evaluator is on the static path only."
        hero={<HeroMetric label="RESULT" value={fmtCount(rowCount)} detail={status} />}
      />
      <KpiStrip kpis={kpis} />
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
        <QueryLibrary selectedId={selected.id} onSelect={onSelectQuery} />
        <div
          style={{
            flex: 1,
            display: 'flex',
            flexDirection: 'column',
            minHeight: 0,
          }}
        >
          <SqlEditor
            value={editorValue}
            onChange={setEditorValue}
            onRun={runQuery}
            status={status}
            error={surfacedError}
            busy={busy}
          />
          <QueryResult
            title={run?.mode === 'live' ? 'RESULT · live · ticking' : 'RESULT · static'}
            status={status}
            liveSubscription={run?.mode === 'live' ? (live ?? undefined) : undefined}
            staticRows={run?.mode === 'static' ? run.rows : undefined}
            getRowId={adhocRowId}
          />
        </div>
      </div>
    </>
  );
}
