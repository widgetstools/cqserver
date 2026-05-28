// clients/examples-web/src/atlas/preview/PulsePreview.tsx
import { useMemo, useState } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import type { ChipSpec } from '../types';
import {
  makePulseRows,
  PULSE_COL_DEFS,
  PULSE_KPIS,
  BOOK_OPTIONS,
  SECTOR_OPTIONS,
  COMPLIANCE_OPTIONS,
  type PulseRow,
} from './placeholderData';

const PULSE_CHIPS: readonly ChipSpec[] = [
  { key: 'BOOK', column: 'book_name', default: 'RATES-US' },
  { key: 'SECTOR', column: 'issuer_sector' },
  { key: 'COMPLIANCE', column: 'compliance_status' },
];

export function PulsePreview() {
  const [scope, setScope] = useState<Record<string, string | undefined>>({ BOOK: 'RATES-US' });
  const rows = useMemo(() => makePulseRows(80), []);
  const chipOptions = useMemo(
    () => ({ BOOK: BOOK_OPTIONS, SECTOR: SECTOR_OPTIONS, COMPLIANCE: COMPLIANCE_OPTIONS }),
    [],
  );
  const summary = scope.BOOK ? `book_name = '${scope.BOOK}'` : '(unfiltered — would stream ~40k rows)';
  const positionRowId = (r: PulseRow): string => r.position_id;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 01 — LIVE BOOK"
        title="pulse."
        sub="A continuous read of the firm’s book — KPIs, sector ladder, book contribution, breaches. Every figure server-computed by a materialized view; nothing aggregated in the browser."
        hero={<HeroMetric label="UNREALISED PnL" value="+$3.21M" detail="vs prev close · 4,820 ticks" />}
      />
      <FilterRail
        chips={[...PULSE_CHIPS]}
        state={scope}
        options={chipOptions}
        onChange={setScope}
        subscriptionSummary={summary}
      />
      <KpiStrip kpis={PULSE_KPIS} />
      <DataTable<PulseRow>
        title="POSITIONS · 23 of 207 cols"
        status={`${rows.length.toLocaleString()} rows · placeholder data (Phase 1)`}
        rows={rows}
        colDefs={PULSE_COL_DEFS}
        getRowId={positionRowId}
      />
    </>
  );
}
