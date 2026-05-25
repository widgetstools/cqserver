import type { ExampleId } from './shared';
import { LivePnlCanvas } from './ex01-live-pnl';
import { TradeBlotterCanvas } from './ex02-trade-blotter';
import { CrossAssetPivotCanvas } from './ex03-cross-asset-pivot';
import { TickingHeatmapCanvas } from './ex04-ticking-heatmap';
import { MaterializedViewCanvas } from './ex05-materialized-view';
import { JoinsCanvas } from './ex06-joins';
import { SlippageCanvas } from './ex07-slippage-agg';
import { QueryBuilderCanvas } from './ex08-query-builder';

export function ExampleCanvas({ id }: { id: ExampleId }) {
  switch (id) {
    case 'live-pnl':          return <LivePnlCanvas />;
    case 'trade-blotter':     return <TradeBlotterCanvas />;
    case 'cross-asset-pivot': return <CrossAssetPivotCanvas />;
    case 'ticking-heatmap':   return <TickingHeatmapCanvas />;
    case 'materialized-view': return <MaterializedViewCanvas />;
    case 'joins':             return <JoinsCanvas />;
    case 'slippage-agg':      return <SlippageCanvas />;
    case 'query-builder':     return <QueryBuilderCanvas />;
  }
}
