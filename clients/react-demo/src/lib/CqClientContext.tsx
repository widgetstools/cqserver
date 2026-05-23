import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { CqClient, type ConnectionStatus } from './cqClient';

interface CqContextValue {
  client: CqClient;
  status: ConnectionStatus;
}

const CqContext = createContext<CqContextValue | null>(null);

interface ProviderProps {
  url: string;
  children: ReactNode;
}

export function CqClientProvider({ url, children }: ProviderProps) {
  const client = useMemo(() => new CqClient(url), [url]);
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

export function useCqClient(): CqClient {
  const ctx = useContext(CqContext);
  if (!ctx) throw new Error('useCqClient must be used inside CqClientProvider');
  return ctx.client;
}

export function useCqStatus(): ConnectionStatus {
  const ctx = useContext(CqContext);
  if (!ctx) throw new Error('useCqStatus must be used inside CqClientProvider');
  return ctx.status;
}
