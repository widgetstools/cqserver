import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { lazy, Suspense } from 'react';
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom';
import { TooltipProvider } from '@/components/ui/tooltip';
import { ThemeProvider } from '@/components/theme/ThemeProvider';
import { AppShell } from '@/components/layout/AppShell';
import {
  CommandPaletteMount,
  GlobalRefreshMount,
  PageFilterFocusMount,
} from '@/components/CommandPalette';
import { KeyboardProvider } from '@/lib/keyboard';

// Code splitting: each page becomes its own chunk via React.lazy. The
// first-load bundle drops from one big ~1 MB blob to a small shell
// (~150 KB gzipped) + per-page chunks pulled in on navigation.
const OverviewPage = lazy(() =>
  import('@/pages/OverviewPage').then((m) => ({ default: m.OverviewPage })),
);
const TopicsPage = lazy(() =>
  import('@/pages/TopicsPage').then((m) => ({ default: m.TopicsPage })),
);
const SubscriptionsPage = lazy(() =>
  import('@/pages/SubscriptionsPage').then((m) => ({ default: m.SubscriptionsPage })),
);
const QueuesPage = lazy(() =>
  import('@/pages/QueuesPage').then((m) => ({ default: m.QueuesPage })),
);
const ViewsPage = lazy(() =>
  import('@/pages/ViewsPage').then((m) => ({ default: m.ViewsPage })),
);
const ReplicationPage = lazy(() =>
  import('@/pages/ReplicationPage').then((m) => ({ default: m.ReplicationPage })),
);
const ConfigPage = lazy(() =>
  import('@/pages/ConfigPage').then((m) => ({ default: m.ConfigPage })),
);
const MetricsPage = lazy(() =>
  import('@/pages/MetricsPage').then((m) => ({ default: m.MetricsPage })),
);
const ExplainPage = lazy(() =>
  import('@/pages/ExplainPage').then((m) => ({ default: m.ExplainPage })),
);
const UserGuidePage = lazy(() =>
  import('@/pages/UserGuidePage').then((m) => ({ default: m.UserGuidePage })),
);

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      staleTime: 0,
      gcTime: 60_000,
      retry: false,
    },
  },
});

function PageFallback() {
  return (
    <div className="px-6 py-10 max-w-[1400px] mx-auto">
      <div className="h-3 w-32 bg-muted rounded-sm animate-pulse mb-3" />
      <div className="h-2.5 w-64 bg-muted/70 rounded-sm animate-pulse" />
    </div>
  );
}

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <TooltipProvider delayDuration={200}>
          {/* In production the bundle is served from /ui/* by cqserver;
              dev mode serves from /. import.meta.env.BASE_URL is set
              by Vite (matches the `base` option) and ends with a `/`. */}
          <BrowserRouter basename={import.meta.env.BASE_URL.replace(/\/$/, '')}>
            {/* KeyboardProvider lives INSIDE BrowserRouter so it can
                consult `useLocation()` for route-change auto-close. */}
            <KeyboardProvider>
              {/* Command palette + cheat sheet keyboard layer. Lives
                  inside the router so commands can `useNavigate()`. */}
              <CommandPaletteMount />
              <GlobalRefreshMount />
              <PageFilterFocusMount />
              <Routes>
                <Route element={<AppShell />}>
                  <Route
                    index
                    element={
                      <Suspense fallback={<PageFallback />}>
                        <OverviewPage />
                      </Suspense>
                    }
                  />
                  <Route
                    path="/topics"
                    element={
                      <Suspense fallback={<PageFallback />}>
                        <TopicsPage />
                      </Suspense>
                    }
                  />
                  <Route
                    path="/subscriptions"
                    element={
                      <Suspense fallback={<PageFallback />}>
                        <SubscriptionsPage />
                      </Suspense>
                    }
                  />
                  <Route
                    path="/replication"
                    element={
                      <Suspense fallback={<PageFallback />}>
                        <ReplicationPage />
                      </Suspense>
                    }
                  />
                  <Route
                    path="/views"
                    element={
                      <Suspense fallback={<PageFallback />}>
                        <ViewsPage />
                      </Suspense>
                    }
                  />
                  <Route
                    path="/queues"
                    element={
                      <Suspense fallback={<PageFallback />}>
                        <QueuesPage />
                      </Suspense>
                    }
                  />
                  <Route
                    path="/metrics"
                    element={
                      <Suspense fallback={<PageFallback />}>
                        <MetricsPage />
                      </Suspense>
                    }
                  />
                  <Route
                    path="/explain"
                    element={
                      <Suspense fallback={<PageFallback />}>
                        <ExplainPage />
                      </Suspense>
                    }
                  />
                  <Route
                    path="/config"
                    element={
                      <Suspense fallback={<PageFallback />}>
                        <ConfigPage />
                      </Suspense>
                    }
                  />
                  <Route
                    path="/guide"
                    element={
                      <Suspense fallback={<PageFallback />}>
                        <UserGuidePage />
                      </Suspense>
                    }
                  />
                  <Route path="*" element={<Navigate to="/" replace />} />
                </Route>
              </Routes>
            </KeyboardProvider>
          </BrowserRouter>
        </TooltipProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
}
