import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { CqClient, type ConnectionStatus } from './cqClient';
import { CqWorkerClient } from './cqWorkerClient';

// Public surface — both implementations satisfy this shape, so
// components don't care which one they got. Keep parity with the
// concrete classes' subscribe/onStatus/connect/close signatures.
type CqClientLike = CqClient | CqWorkerClient;

interface CqContextValue {
  client: CqClientLike;
  status: ConnectionStatus;
}

const CqContext = createContext<CqContextValue | null>(null);

interface ProviderProps {
  url: string;
  children: ReactNode;
}

// Opt-in to the SharedWorker via env. Default OFF so the existing
// behavior is preserved until we've validated this on every browser
// the demo ships to. Toggle with VITE_CQ_USE_WORKER=1 in .env.local.
const USE_WORKER = import.meta.env.VITE_CQ_USE_WORKER === '1';

export function CqClientProvider({ url, children }: ProviderProps) {
  const client = useMemo<CqClientLike>(() => {
    if (USE_WORKER && typeof SharedWorker !== 'undefined') {
      return new CqWorkerClient(url);
    }
    return new CqClient(url);
  }, [url]);
  const [status, setStatus] = useState<ConnectionStatus>('idle');

  useEffect(() => {
    const unsub = client.onStatus(setStatus);
    client.connect();
    return () => {
      unsub();
      client.close();
    };
  }, [client]);

  const value = useMemo(() => ({ client, status }), [client, status]);
  return <CqContext.Provider value={value}>{children}</CqContext.Provider>;
}

export function useCqClient(): CqClientLike {
  const ctx = useContext(CqContext);
  if (!ctx) throw new Error('useCqClient must be used inside CqClientProvider');
  return ctx.client;
}

export function useCqStatus(): ConnectionStatus {
  const ctx = useContext(CqContext);
  if (!ctx) throw new Error('useCqStatus must be used inside CqClientProvider');
  return ctx.status;
}
