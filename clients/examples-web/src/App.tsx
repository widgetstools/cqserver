import { useState } from 'react';
import { ThemeProvider } from '@/components/theme/ThemeProvider';
import { AtlasHeader } from '@/components/atlas/AtlasHeader';
import { AtlasIndex } from '@/components/atlas/AtlasIndex';
import { ExampleRubric } from '@/components/atlas/ExampleRubric';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';
import { EXAMPLES, exampleById } from '@/examples/registry';
import type { ExampleId } from '@/examples/shared';
import { ExampleCanvas } from '@/examples/ExampleCanvas';

export function App() {
  const [active, setActive] = useState<ExampleId>('live-pnl');
  const ex = exampleById(active);

  return (
    <ThemeProvider>
      <div className="h-screen w-screen flex flex-col bg-background text-foreground overflow-hidden">
        <AtlasHeader />
        <div className="flex-1 flex min-h-0">
          <AtlasIndex active={active} onSelect={setActive} />
          <main className="flex-1 min-w-0 flex flex-col">
            <Tabs value={active} onValueChange={(v) => setActive(v as ExampleId)} className="flex-1 flex flex-col min-h-0">
              <div className="border-b border-border bg-card overflow-x-auto">
                <TabsList className="px-3 pt-1.5 pb-0 gap-0 border-0">
                  {EXAMPLES.map((e) => (
                    <TabsTrigger key={e.id} value={e.id} className="!px-3">
                      <e.icon size={11} className="shrink-0 opacity-70" />
                      <span>{e.serial}</span>
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
      </div>
    </ThemeProvider>
  );
}
