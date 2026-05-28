/**
 * Pulse — Chapter 01, Live Book. The first Atlas chapter on real
 * cqserver data. Pattern:
 *   - 4 view subscriptions seed KPIs + chip option lists + the two
 *     left-column visualisations (sector ladder, book bars)
 *   - 1 filtered subscription on /positions drives the data table
 *   - `useChapterScope` owns the chip state and composes the WHERE
 *     expression every chip toggle re-emits
 *
 * Layout:
 *   ┌────────────────────────────────────────────────┐
 *   │ ChapterHead                                    │
 *   ├────────────────────────────────────────────────┤
 *   │ FilterRail                                     │
 *   ├────────────────────────────────────────────────┤
 *   │ KpiStrip (6 cards)                             │
 *   ├──────────────────────┬─────────────────────────┤
 *   │ Sector PnL ladder    │                         │
 *   │ ───────              │  Positions grid         │
 *   │ Book contribution    │                         │
 *   └──────────────────────┴─────────────────────────┘
 */
import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { SectorLadder } from '../components/SectorLadder';
import { BookBars } from '../components/BookBars';
import { useChapterScope, distinctValues } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import {
  PULSE_CHIPS,
  PULSE_KPIS,
  PULSE_COL_DEFS,
  fmtSignedMillions,
  fmtMillions,
  fmtCount,
} from '../scopes/pulse';

const positionRowId = (r: Row): string => String(r.position_id ?? '');
// Aggregate-view rowId extractors. Without these, Sub's fallback would
// JSON.stringify each row — including the live PnL value — so every
// tick mints a fresh key, the old row stays in the Map, and React
// renders the same book/sector twice.
const bookRowId = (r: Row): string => String(r.book_name ?? '');
const sectorRowId = (r: Row): string => String(r.issuer_sector ?? '');
const complianceRowId = (r: Row): string => String(r.compliance_status ?? '');
// Single-row degenerate aggregate — pin to a constant key.
const totalsRowId = (_r: Row): string => 'totals';

export function PulseChapter() {
  const scope = useChapterScope(PULSE_CHIPS);

  // View subscriptions — small row counts, used to derive chip options,
  // the aggregate KPI row, and the two visualisation columns.
  const bookSub = useSubscription('/v_pnl_by_book', null, bookRowId);
  const sectorSub = useSubscription('/v_pnl_by_sector', null, sectorRowId);
  const complianceSub = useSubscription('/v_compliance_counts', null, complianceRowId);
  const totalsSub = useSubscription('/v_book_totals', null, totalsRowId);

  // Primary subscription — /positions filtered server-side by the chip selection.
  const positionsSub = useSubscription('/positions', scope.filterExpression, positionRowId);

  // Derive chip option lists from the view snapshots.
  const chipOptions = useMemo(
    () => ({
      BOOK: ['All', ...distinctValues(bookSub.rows, 'book_name')],
      SECTOR: ['All', ...distinctValues(sectorSub.rows, 'issuer_sector')],
      COMPLIANCE: ['All', ...distinctValues(complianceSub.rows, 'compliance_status')],
    }),
    [bookSub.rows, sectorSub.rows, complianceSub.rows],
  );

  // Derive KPI values from the aggregate row + breach count.
  const kpis = useMemo<Kpi[]>(() => {
    const t = (totalsSub.rows[0] ?? {}) as Record<string, unknown>;
    const breachRow = complianceSub.rows.find((r) => r.compliance_status === 'BREACH');
    const breaches = breachRow ? Number(breachRow.n_positions) : 0;
    return PULSE_KPIS.map((def): Kpi => {
      const raw =
        def.source === '__breaches__' ? breaches : Number(t[def.source] ?? 0);
      const value =
        def.format === 'currency-m'
          ? fmtMillions(raw)
          : def.format === 'currency-m-signed'
            ? fmtSignedMillions(raw)
            : fmtCount(raw);
      return {
        label: def.label,
        value,
        caption: def.caption,
        emphasis: def.emphasis,
      };
    });
  }, [totalsSub.rows, complianceSub.rows]);

  const heroValue = useMemo(() => {
    const t = (totalsSub.rows[0] ?? {}) as Record<string, unknown>;
    return fmtSignedMillions(Number(t.unrealized_pnl ?? 0));
  }, [totalsSub.rows]);

  const status =
    positionsSub.status === 'live'
      ? `${positionsSub.size.toLocaleString()} rows · live`
      : `${positionsSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 01 — LIVE BOOK"
        title="pulse."
        sub="A continuous read of the firm's book — KPIs, sector ladder, book contribution, breaches. Every figure server-computed by a materialized view; nothing aggregated in the browser."
        hero={<HeroMetric label="UNREALISED PnL" value={heroValue} detail="from /v_book_totals" />}
      />
      <FilterRail
        chips={[...PULSE_CHIPS]}
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
            width: '38%',
            minWidth: 340,
            display: 'flex',
            flexDirection: 'column',
            minHeight: 0,
            borderRight: '1px solid var(--atlas-rule)',
          }}
        >
          <SectorLadder
            title="SECTOR PnL · day_pnl"
            rows={sectorSub.rows}
            labelKey="issuer_sector"
            valueKey="day_pnl"
            limit={14}
            format={fmtSignedMillions}
          />
          <BookBars
            title="BOOK CONTRIBUTION · unrealized_pnl"
            rows={bookSub.rows}
            labelKey="book_name"
            valueKey="unrealized_pnl"
            format={fmtSignedMillions}
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
            title="POSITIONS · 8 of 206 cols"
            status={status}
            colDefs={PULSE_COL_DEFS}
            getRowId={positionRowId}
            liveSubscription={positionsSub}
          />
        </div>
      </div>
    </>
  );
}
