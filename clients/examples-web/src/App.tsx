import { useState } from 'react';
import * as TabsPrimitive from '@radix-ui/react-tabs';
import { ThemeProvider } from '@/components/theme/ThemeProvider';
import { AtlasHeader } from '@/components/atlas/AtlasHeader';
import { ContextBar } from '@/components/atlas/ContextBar';
import { EXAMPLES, exampleById } from '@/examples/registry';
import type { ExampleId } from '@/examples/shared';
import { ExampleCanvas } from '@/examples/ExampleCanvas';
import { FEATURE_META, orderFeatures } from '@/lib/features';
// Side-effect import: opens the cqserver WebSocket and starts streaming
// positions / trades / securities / fi-market-data into the live store
// before any example needs them.
import '@/lib/cq-store';
import { AtlasPreviewApp } from '@/atlas/preview/AtlasPreviewApp';

/**
 * App shell — header, slim tab strip, single-row context band,
 * dock-managed example canvas.
 *
 * Tabs: 32px row, serial (mono) + short label (Inter), 2px teal
 * underline on active. ContextBar: 32px row, names the example with
 * a teal accent bar, feature dots, optional LIVE pulse.
 */
export function App() {
  // Phase 1 redesign preview — opt-in via #atlas hash. The legacy app
  // continues to render at every other URL. Routing is decided outside
  // any hook so the two branches never share a hook frame.
  if (typeof window !== 'undefined' && window.location.hash === '#atlas') {
    return <AtlasPreviewApp />;
  }
  return <LegacyApp />;
}

function LegacyApp() {
  const [active, setActive] = useState<ExampleId>('live-pnl');
  const ex = exampleById(active);

  return (
    <ThemeProvider>
      <div className="h-screen w-screen flex flex-col bg-background text-foreground overflow-hidden">
        <AtlasHeader />
        <main className="flex-1 min-w-0 flex flex-col min-h-0">
          <TabsPrimitive.Root value={active} onValueChange={(v) => setActive(v as ExampleId)} className="flex-1 flex flex-col min-h-0">
            <div className="atlas-tabstrip-wrap">
              <TabsPrimitive.List className="atlas-tabstrip">
                {EXAMPLES.map((e) => {
                  const features = orderFeatures(e.features);
                  const isLive = e.features.includes('stream');
                  return (
                    <TabsPrimitive.Trigger key={e.id} value={e.id} className="atlas-tab">
                      <span className="atlas-tab-top">
                        <span className="atlas-tab-serial">{e.serial}</span>
                        <span className="atlas-tab-label">{e.shortLabel}</span>
                      </span>
                      <span className="atlas-tab-features">
                        {features.map((f) => {
                          const meta = FEATURE_META[f];
                          return (
                            <span
                              key={f}
                              className="atlas-tab-feature"
                              style={{ ['--feature-color' as never]: meta.colorVar }}
                              title={`${meta.name} — ${meta.blurb}`}
                            >
                              {meta.glyph}
                            </span>
                          );
                        })}
                      </span>
                      {isLive && <span className="atlas-tab-live" aria-hidden />}
                    </TabsPrimitive.Trigger>
                  );
                })}
              </TabsPrimitive.List>
            </div>

            {EXAMPLES.map((e) => (
              <TabsPrimitive.Content
                key={e.id}
                value={e.id}
                className="flex-1 min-h-0 flex flex-col focus:outline-none data-[state=inactive]:hidden"
              >
                {active === e.id ? (
                  <>
                    <ContextBar example={ex} />
                    <div className="flex-1 min-h-0 fade-up">
                      <ExampleCanvas id={e.id} />
                    </div>
                  </>
                ) : null}
              </TabsPrimitive.Content>
            ))}
          </TabsPrimitive.Root>
        </main>
      </div>
    </ThemeProvider>
  );
}
