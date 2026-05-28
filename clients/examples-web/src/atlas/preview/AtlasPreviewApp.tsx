import { useState } from 'react';
import { AppShell } from '../components/AppShell';
import { StationsNav } from '../components/StationsNav';
import { Footer } from '../components/Footer';
import { PulseChapter } from '../chapters/PulseChapter';
import { TapeChapter } from '../chapters/TapeChapter';
import { LensChapter } from '../chapters/LensChapter';
import { HeatChapter } from '../chapters/HeatChapter';
import { ViewChapter } from '../chapters/ViewChapter';
import { JoinChapter } from '../chapters/JoinChapter';
import type { ChapterId } from '../types';

/** Stub for any chapter that hasn't been migrated yet (Phase 3 = Pulse only). */
function ComingSoon({ id }: { id: ChapterId }) {
  return (
    <div
      style={{
        flex: 1,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: 'var(--atlas-fg-faint)',
        fontSize: 11,
        letterSpacing: '.3em',
      }}
    >
      {id.toUpperCase()} · arriving in a later phase
    </div>
  );
}

export function AtlasPreviewApp() {
  const [active, setActive] = useState<ChapterId>('pulse');

  return (
    <AppShell hint="phase 3 · pulse live">
      <StationsNav active={active} onChange={setActive} />
      <main style={{ position: 'relative', zIndex: 1, flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        {active === 'pulse' ? <PulseChapter />
          : active === 'tape' ? <TapeChapter />
          : active === 'lens' ? <LensChapter />
          : active === 'heat' ? <HeatChapter />
          : active === 'view' ? <ViewChapter />
          : active === 'join' ? <JoinChapter />
          : <ComingSoon id={active} />}
      </main>
      <Footer />
    </AppShell>
  );
}
