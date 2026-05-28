/**
 * Join — Chapter 06. Demonstrates cqserver's server-side JOIN by
 * showing both raw sides AND the joined aggregate side-by-side.
 *
 * Layout:
 *   ┌─────────────────────────────────────────────────────────────┐
 *   │ ChapterHead                                                 │
 *   ├─────────────────────────────────────────────────────────────┤
 *   │ FilterRail (BOOK chip)                                      │
 *   ├─────────────────────────────────────────────────────────────┤
 *   │ KpiStrip                                                    │
 *   ├──────────────────┬───────────────────┬──────────────────────┤
 *   │ /positions       │ /trades           │ /v_trades_by_         │
 *   │  WHERE book=X    │  WHERE book=X     │  compliance           │
 *   │                  │   AND FILLED      │  (server-joined)      │
 *   └──────────────────┴───────────────────┴──────────────────────┘
 *
 * The BOOK chip filters the two raw sides locally. The joined view is
 * computed on the server across the entire book and is always shown in
 * full — the chip does not constrain it, because /v_trades_by_compliance
 * groups by compliance_status only. The intentional reading: "your
 * local view is filtered to one book; cqserver's join still runs across
 * the whole firm and gives you the rollup."
 */
import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import {
  JOIN_CHIPS,
  JOIN_BOOK_OPTIONS,
  JOIN_POSITIONS_COL_DEFS,
  JOIN_TRADES_COL_DEFS,
  JOIN_RESULT_COL_DEFS,
  fmtMillions,
  fmtCount,
} from '../scopes/join';

const positionRowId = (r: Row): string => String(r.position_id ?? '');
const tradeRowId = (r: Row): string => String(r.trade_id ?? '');
const joinRowId = (r: Row): string => String(r.compliance_status ?? '');

export function JoinChapter() {
  const scope = useChapterScope(JOIN_CHIPS);

  // Compose the trades-side filter — chip WHERE plus status='FILLED'
  // so the right pane stays tickable (~1/7 of the trade universe).
  const tradesFilter = useMemo(() => {
    return scope.filterExpression
      ? `${scope.filterExpression} AND status = 'FILLED'`
      : `status = 'FILLED'`;
  }, [scope.filterExpression]);

  const positionsSub = useSubscription('/positions', scope.filterExpression, positionRowId);
  const tradesSub = useSubscription('/trades', tradesFilter, tradeRowId);
  // Joined aggregate — runs unfiltered, deliberately ignoring the chip.
  const joinSub = useSubscription('/v_trades_by_compliance', null, joinRowId);

  const chipOptions = useMemo(() => ({ BOOK: JOIN_BOOK_OPTIONS }), []);

  const totals = useMemo(() => {
    let trades = 0, fees = 0, slipSum = 0, slipN = 0;
    for (const r of joinSub.rows) {
      trades += Number(r.n_trades ?? 0);
      fees += Number(r.total_fees ?? 0);
      const slip = Number(r.avg_slip_arr ?? 0);
      if (Number.isFinite(slip)) { slipSum += slip; slipN += 1; }
    }
    return { trades, fees, avgSlip: slipN > 0 ? slipSum / slipN : 0 };
  }, [joinSub.rows]);

  const breachRow = useMemo(
    () => joinSub.rows.find((r) => r.compliance_status === 'BREACH'),
    [joinSub.rows],
  );

  const kpis = useMemo<Kpi[]>(
    () => [
      { label: 'POSITIONS', value: fmtCount(positionsSub.size), caption: `book = ${scope.state.BOOK ?? 'all'}`, emphasis: true },
      { label: 'TRADES', value: fmtCount(tradesSub.size), caption: 'filled · same book' },
      { label: 'BUCKETS', value: fmtCount(joinSub.rows.length), caption: 'compliance states' },
      { label: 'JOIN TRADES', value: fmtCount(totals.trades), caption: 'across firm', emphasis: true },
      { label: 'FEES', value: fmtMillions(totals.fees), caption: 'sum · usd' },
      { label: 'BREACH', value: fmtCount(Number(breachRow?.n_trades ?? 0)), caption: 'flagged trades', emphasis: true },
    ],
    [positionsSub.size, tradesSub.size, joinSub.rows.length, totals, breachRow, scope.state.BOOK],
  );

  const positionsStatus = positionsSub.status === 'live'
    ? `${positionsSub.size.toLocaleString()} rows · live`
    : `${positionsSub.status}…`;
  const tradesStatus = tradesSub.status === 'live'
    ? `${tradesSub.size.toLocaleString()} rows · live`
    : `${tradesSub.status}…`;
  const joinStatus = joinSub.status === 'live'
    ? `${joinSub.size.toLocaleString()} buckets · live`
    : `${joinSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 06 — JOIN"
        title="join."
        sub="/trades joined to /positions on position_id. The left two grids are the raw sides of the join filtered locally by book; the right grid is cqserver's materialised join — recomputed on every trade or position mutation, served unfiltered."
        hero={<HeroMetric label="BREACH" value={fmtCount(Number(breachRow?.n_trades ?? 0))} detail="trades flagged" />}
      />
      <FilterRail
        chips={[...JOIN_CHIPS]}
        state={scope.state}
        options={chipOptions}
        onChange={scope.setState}
        subscriptionSummary={scope.summary}
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
        <div
          style={{
            flex: 1,
            display: 'flex',
            flexDirection: 'column',
            minHeight: 0,
            borderRight: '1px solid var(--atlas-rule)',
          }}
        >
          <DataTable<Row>
            title="LHS · /positions"
            status={positionsStatus}
            colDefs={JOIN_POSITIONS_COL_DEFS}
            getRowId={positionRowId}
            liveSubscription={positionsSub}
          />
        </div>
        <div
          style={{
            flex: 1,
            display: 'flex',
            flexDirection: 'column',
            minHeight: 0,
            borderRight: '1px solid var(--atlas-rule)',
          }}
        >
          <DataTable<Row>
            title="MID · /trades · FILLED"
            status={tradesStatus}
            colDefs={JOIN_TRADES_COL_DEFS}
            getRowId={tradeRowId}
            liveSubscription={tradesSub}
          />
        </div>
        <div
          style={{
            flex: 1,
            display: 'flex',
            flexDirection: 'column',
            minHeight: 0,
          }}
        >
          <DataTable<Row>
            title="RHS · /v_trades_by_compliance · server join"
            status={joinStatus}
            colDefs={JOIN_RESULT_COL_DEFS}
            getRowId={joinRowId}
            liveSubscription={joinSub}
          />
        </div>
      </div>
    </>
  );
}
