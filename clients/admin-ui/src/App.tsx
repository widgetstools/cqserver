import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom';
import { TooltipProvider } from '@/components/ui/tooltip';
import { ThemeProvider } from '@/components/theme/ThemeProvider';
import { AppShell } from '@/components/layout/AppShell';
import { OverviewPage } from '@/pages/OverviewPage';
import { TopicsPage } from '@/pages/TopicsPage';
import { SubscriptionsPage } from '@/pages/SubscriptionsPage';
import { QueuesPage } from '@/pages/QueuesPage';
import { PlaceholderPage } from '@/pages/PlaceholderPage';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // We poll explicitly per-screen; disable the global refetch-on-focus
      // to prevent burst-refresh when the operator alt-tabs back.
      refetchOnWindowFocus: false,
      staleTime: 0,
      gcTime: 60_000,
      retry: false,
    },
  },
});

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <TooltipProvider delayDuration={200}>
          <BrowserRouter>
            <Routes>
              <Route element={<AppShell />}>
                <Route index element={<OverviewPage />} />
                <Route path="/topics" element={<TopicsPage />} />
                <Route path="/subscriptions" element={<SubscriptionsPage />} />
                <Route
                  path="/replication"
                  element={
                    <PlaceholderPage
                      title="Replication"
                      description="Leader / follower lag, replication topology, filters + transforms."
                      worklogRef="U5"
                    />
                  }
                />
                <Route
                  path="/views"
                  element={
                    <PlaceholderPage
                      title="Views"
                      description="Materialized continuous queries with source linkage."
                      worklogRef="U5"
                    />
                  }
                />
                <Route path="/queues" element={<QueuesPage />} />
                <Route
                  path="/metrics"
                  element={
                    <PlaceholderPage
                      title="Metrics"
                      description="Live Prometheus series browser + pinned sparklines."
                      worklogRef="U6"
                    />
                  }
                />
                <Route
                  path="/explain"
                  element={
                    <PlaceholderPage
                      title="Query Explain"
                      description="Estimate query cost before subscribing. Depends on QUERY_GUARDRAILS G2."
                      worklogRef="U6"
                    />
                  }
                />
                <Route
                  path="/config"
                  element={
                    <PlaceholderPage
                      title="Config"
                      description="Read-only view of the running cqserver.toml."
                      worklogRef="U5"
                    />
                  }
                />
                <Route path="*" element={<Navigate to="/" replace />} />
              </Route>
            </Routes>
          </BrowserRouter>
        </TooltipProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
}
