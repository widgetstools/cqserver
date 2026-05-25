import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom';
import { TooltipProvider } from '@/components/ui/tooltip';
import { ThemeProvider } from '@/components/theme/ThemeProvider';
import { AppShell } from '@/components/layout/AppShell';
import { OverviewPage } from '@/pages/OverviewPage';
import { TopicsPage } from '@/pages/TopicsPage';
import { SubscriptionsPage } from '@/pages/SubscriptionsPage';
import { QueuesPage } from '@/pages/QueuesPage';
import { ViewsPage } from '@/pages/ViewsPage';
import { ReplicationPage } from '@/pages/ReplicationPage';
import { ConfigPage } from '@/pages/ConfigPage';
import { MetricsPage } from '@/pages/MetricsPage';
import { ExplainPage } from '@/pages/ExplainPage';

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
          {/* In production the bundle is served from /ui/* by cqserver;
              dev mode serves from /. import.meta.env.BASE_URL is set
              by Vite (matches the `base` option) and ends with a `/`. */}
          <BrowserRouter basename={import.meta.env.BASE_URL.replace(/\/$/, '')}>
            <Routes>
              <Route element={<AppShell />}>
                <Route index element={<OverviewPage />} />
                <Route path="/topics" element={<TopicsPage />} />
                <Route path="/subscriptions" element={<SubscriptionsPage />} />
                <Route path="/replication" element={<ReplicationPage />} />
                <Route path="/views" element={<ViewsPage />} />
                <Route path="/queues" element={<QueuesPage />} />
                <Route path="/metrics" element={<MetricsPage />} />
                <Route path="/explain" element={<ExplainPage />} />
                <Route path="/config" element={<ConfigPage />} />
                <Route path="*" element={<Navigate to="/" replace />} />
              </Route>
            </Routes>
          </BrowserRouter>
        </TooltipProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
}
