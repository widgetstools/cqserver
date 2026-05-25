import { useState } from 'react';
import { ThemeProvider } from '@/components/theme/ThemeProvider';
import { AtlasHeader } from '@/components/atlas/AtlasHeader';
import { ExampleRubric } from '@/components/atlas/ExampleRubric';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';
import { EXAMPLES, exampleById } from '@/examples/registry';
import type { ExampleId } from '@/examples/shared';
import { ExampleCanvas } from '@/examples/ExampleCanvas';

/**
 * App shell — header, top tab strip, dock-managed example canvas.
 *
 * The left-rail Atlas index was removed: the top tab strip (with
 * icon + serial + title) is enough to navigate between examples and
 * frees up the full canvas width for grids, heatmaps and dock panels.
 */
export function App() {
  const [active, setActive] = useState<ExampleId>('live-pnl');
  const ex = exampleById(active);

  return (
    <ThemeProvider>
      <div className="h-screen w-screen flex flex-col bg-background text-foreground overflow-hidden">
        <AtlasHeader />
        <main className="flex-1 min-w-0 flex flex-col min-h-0">
          <Tabs value={active} onValueChange={(v) => setActive(v as ExampleId)} className="flex-1 flex flex-col min-h-0">
            <div className="border-b border-border bg-card overflow-x-auto shrink-0">
              <TabsList className="px-3 pt-1.5 pb-0 gap-0 border-0">
                {EXAMPLES.map((e) => (
                  <TabsTrigger key={e.id} value={e.id} className="!px-3 !gap-2">
                    <e.icon size={11} className="shrink-0 opacity-70" />
                    <span className="font-mono">{e.serial}</span>
                    <span className="hidden md:inline text-muted-foreground/80 normal-case tracking-normal font-sans text-[10.5px]">
                      {e.title.replace(/^.+— /, '')}
                    </span>
                  </TabsTrigger>
                ))}
              </TabsList>
            </div>

            {EXAMPLES.map((e) => (
              <TabsContent
                key={e.id}
                value={e.id}
                className="flex-1 min-h-0 flex flex-col data-[state=inactive]:hidden"
              >
                {active === e.id ? (
                  <>
                    <ExampleRubric example={ex} eyebrow={`/${e.id}`} />
                    <div className="flex-1 min-h-0 fade-up">
                      <ExampleCanvas id={e.id} />
                    </div>
                  </>
                ) : null}
              </TabsContent>
            ))}
          </Tabs>
        </main>
      </div>
    </ThemeProvider>
  );
}
