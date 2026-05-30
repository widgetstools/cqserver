/**
 * AppShell — wordmark, connection summary, theme toggle (top right).
 */
import type { ReactNode } from 'react';
import { ThemeToggle } from '../theme/ThemeContext';

interface AppShellProps {
  connection?: string;
  hint?: string;
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
        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
          <div style={{ color: 'var(--atlas-fg-dim)', fontSize: 10 }}>
            cqserver · {connection}
            {hint ? ` · ${hint}` : null}
          </div>
          <ThemeToggle />
        </div>
      </header>
      {children}
    </div>
  );
}
