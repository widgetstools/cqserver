import type { ConnectionStatus } from '@/lib/cqClient';

interface StatusPillProps {
  status: ConnectionStatus;
  extra?: string;
}

const LABEL: Record<ConnectionStatus, string> = {
  idle: 'idle',
  connecting: 'connecting…',
  connected: 'connected',
  snapshotting: 'loading snapshot…',
  live: 'live',
  disconnected: 'disconnected',
};

export function StatusPill({ status, extra }: StatusPillProps) {
  const live = status === 'live';
  const bad = status === 'disconnected';
  return (
    <span
      className="inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[11px]"
      style={{
        background: live ? 'var(--sf-up-soft, rgba(45,212,191,0.12))' : 'var(--sf-bg-3)',
        color: bad ? 'var(--sf-down)' : live ? 'var(--sf-up)' : 'var(--sf-t-1)',
        border: '1px solid var(--sf-border)',
      }}
    >
      <span
        className="inline-block h-1.5 w-1.5 rounded-full"
        style={{ background: bad ? 'var(--sf-down)' : live ? 'var(--sf-up)' : 'var(--sf-t-3)' }}
      />
      {LABEL[status]}
      {extra ? <span style={{ color: 'var(--sf-t-2)' }}>· {extra}</span> : null}
    </span>
  );
}
