import { useState } from 'react';
import { AppShell } from '../components/AppShell';
import { StationsNav } from '../components/StationsNav';
import { Footer } from '../components/Footer';
import { PulsePreview } from './PulsePreview';
import type { ChapterId } from '../types';

/** Stub for any chapter that hasn't been migrated yet (Phase 1 = Pulse only). */
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
    <AppShell hint="phase 1 preview · placeholder data">
      <StationsNav active={active} onChange={setActive} />
      <main style={{ position: 'relative', zIndex: 1, flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        {active === 'pulse' ? <PulsePreview /> : <ComingSoon id={active} />}
      </main>
      <Footer cadence="250ms cadence" tickStats="placeholder" />
    </AppShell>
  );
}
