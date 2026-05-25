import { useState } from 'react';
import * as TabsPrimitive from '@radix-ui/react-tabs';
import { ThemeProvider } from '@/components/theme/ThemeProvider';
import { AtlasHeader } from '@/components/atlas/AtlasHeader';
import { ContextBar } from '@/components/atlas/ContextBar';
import { EXAMPLES, exampleById } from '@/examples/registry';
import type { ExampleId } from '@/examples/shared';
import { ExampleCanvas } from '@/examples/ExampleCanvas';

/**
 * App shell — header, slim tab strip, single-row context band,
 * dock-managed example canvas.
 *
 * Tabs: 32px row, serial (mono) + short label (Inter), 2px teal
 * underline on active. ContextBar: 32px row, names the example with
 * a teal accent bar, feature dots, optional LIVE pulse.
 */
export function App() {
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
                {EXAMPLES.map((e) => (
                  <TabsPrimitive.Trigger key={e.id} value={e.id} className="atlas-tab">
                    <span className="atlas-tab-serial">{e.serial}</span>
                    <span className="atlas-tab-label">{e.shortLabel}</span>
                  </TabsPrimitive.Trigger>
                ))}
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
