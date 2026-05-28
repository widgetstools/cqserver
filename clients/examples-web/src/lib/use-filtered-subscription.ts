/**
 * Legacy compatibility shim. The canonical hook is
 * `useSubscription` in ./use-subscription.ts — this file exists only
 * so the eight existing chapters keep compiling between Phase 2's
 * worker landing and Phase 3+'s chapter migration into the Atlas
 * surface. New code should import `useSubscription` directly.
 */
import { useSubscription, type SubscriptionHandle } from './use-subscription';

export type { ConnectionStatus, Row, DeltaBatch } from './use-subscription';

/** Same topic union the previous hook accepted. Kept for the demo
 *  examples that pass string literals; new callers should pass any
 *  string topic to `useSubscription`. */
export type TopicName =
  | '/positions'
  | '/trades'
  | '/securities'
  | '/fi-market-data'
  | '/v_net_exposure'
  | '/v_slippage_venue_algo'
  | '/v_pnl_by_sector'
  | '/v_pnl_by_book'
  | '/v_compliance_counts'
  | '/v_cross_asset_pivot'
  | '/v_heatmap_sector_region'
  | '/v_trades_by_compliance'
  | '/v_book_totals';

export type FilteredSubscription = SubscriptionHandle;

export function useFilteredSubscription(
  topic: TopicName,
  filter: string | null,
): FilteredSubscription {
  return useSubscription(topic, filter);
}
