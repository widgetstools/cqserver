/**
 * Thin widget wrappers used by the dock-manager registry. Each reads
 * theme from context so the registry map can stay stable (which avoids
 * re-mounting the panel — and re-subscribing — every time the user
 * toggles the palette).
 */
import { MarketDataBlotter } from './MarketDataBlotter';
import { TradesFeed } from './TradesFeed';
import { PositionsBlotter } from './PositionsBlotter';
import { AggregationsGrids } from './AggregationsGrids';
import { PivotPanel } from './PivotPanel';
import { useThemePrefs } from '@/lib/ThemeContext';

export function MarketDataWidget() {
  const { palette, mode } = useThemePrefs();
  return <MarketDataBlotter palette={palette} mode={mode} />;
}

export function TradesWidget() {
  return <TradesFeed />;
}

export function PositionsWidget() {
  const { palette, mode } = useThemePrefs();
  return <PositionsBlotter palette={palette} mode={mode} />;
}

export function AggregationsWidget() {
  const { palette, mode } = useThemePrefs();
  return <AggregationsGrids palette={palette} mode={mode} />;
}

export function PivotWidget() {
  const { palette, mode } = useThemePrefs();
  return <PivotPanel palette={palette} mode={mode} />;
}

// Stable registry — passed once to <DockManagerCore widgets={WIDGETS} />.
export const WIDGETS = {
  'market-data': MarketDataWidget,
  trades: TradesWidget,
  positions: PositionsWidget,
  aggregations: AggregationsWidget,
  pivot: PivotWidget,
};
