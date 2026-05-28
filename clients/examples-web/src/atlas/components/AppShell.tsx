/**
 * AppShell — the topmost band: cq · atlas wordmark on the left,
 * connection summary on the right. Sits above the stations rail.
 */
import type { ReactNode } from 'react';

interface AppShellProps {
  /** e.g. 'ws://127.0.0.1:9008'. Falls back to a sensible default. */
  connection?: string;
  /** Optional right-side hint, e.g. '40,000 / 340,130'. */
  hint?: string;
  /** The rest of the page (stations + chapter content). */
  children: ReactNode;
}

export function AppShell({ connection = 'ws://127.0.0.1:9008', hint, children }: AppShellProps) {
  return (
    <div
      className="atlas-root atlas-app"
      style={{ height: '100vh', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}
    >
      <header
        style={{
          position: 'relative',
          zIndex: 1,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '14px 24px',
          borderBottom: '1px solid var(--atlas-rule)',
          fontSize: 11,
          letterSpacing: '.22em',
        }}
      >
        <div>
          <span style={{ color: 'var(--atlas-amber)', fontWeight: 700 }}>cq</span> · atlas
        </div>
        <div style={{ color: 'var(--atlas-fg-dim)', fontSize: 10 }}>
          cqserver · {connection}
          {hint ? ` · ${hint}` : null}
        </div>
      </header>
      {children}
    </div>
  );
}
