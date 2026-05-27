import { useCallback, useEffect, useState } from 'react';
import { fetchCatalog, type CatalogEntry } from './catalog';

export interface CatalogState {
  entries: CatalogEntry[];
  loading: boolean;
  error: string | null;
  /** Re-fetch the catalog (e.g. after a view is created elsewhere). */
  refresh: () => void;
}

/**
 * Fetch the schema catalog on mount and expose a manual `refresh`.
 * Aborts the in-flight request on unmount / re-fetch so a late
 * response never sets state on an unmounted component.
 */
export function useCatalog(): CatalogState {
  const [entries, setEntries] = useState<CatalogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [nonce, setNonce] = useState(0);

  const refresh = useCallback(() => setNonce((n) => n + 1), []);

  useEffect(() => {
    const ac = new AbortController();
    setLoading(true);
    setError(null);
    fetchCatalog(ac.signal)
      .then((e) => {
        if (ac.signal.aborted) return;
        setEntries(e);
        setLoading(false);
      })
      .catch((err: unknown) => {
        if (ac.signal.aborted) return;
        setError(err instanceof Error ? err.message : String(err));
        setLoading(false);
      });
    return () => ac.abort();
  }, [nonce]);

  return { entries, loading, error, refresh };
}
