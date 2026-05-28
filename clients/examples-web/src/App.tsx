/**
 * App entrypoint. The Atlas chapter app is now the only UI; the
 * legacy 8-tab dock was retired in Phase 5. See git history for the
 * old shell (AtlasPreviewApp + LegacyApp).
 */
import { AtlasApp } from '@/atlas/app/AtlasApp';

export function App() {
  return <AtlasApp />;
}
