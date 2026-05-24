import { useEffect, useRef, useState } from 'react';
import {
  DockManagerCore,
  type DockManagerCoreHandle,
  type WidgetProps,
} from '@widgetstools/react-dock-manager';
import {
  createDefaultState,
  type DockviewApi,
} from '@widgetstools/dock-manager-core';
import '@widgetstools/react-dock-manager/styles.css';

import { Header } from '@/components/Header';
import { WIDGETS } from '@/components/Widgets';
import { StatusPill } from '@/components/StatusPill';
import { CqClientProvider, useCqStatus } from '@/lib/CqClientContext';
import { ThemeProvider } from '@/lib/ThemeContext';
import type { Palette, ThemeMode } from '@/lib/agGridTheme';

const WS_URL = import.meta.env.VITE_CQ_WS_URL ?? 'ws://127.0.0.1:9008/cq/json';
const STORAGE_KEY = 'cqserver-demo:prefs';

interface Prefs {
  palette: Palette;
  mode: ThemeMode;
}

function loadPrefs(): Prefs {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const p = JSON.parse(raw) as Partial<Prefs>;
      return { palette: p.palette ?? 'teal', mode: p.mode ?? 'dark' };
    }
  } catch {
    // ignore
  }
  return { palette: 'teal', mode: 'dark' };
}

export default function App() {
  const initial = loadPrefs();
  const [palette, setPalette] = useState<Palette>(initial.palette);
  const [mode, setMode] = useState<ThemeMode>(initial.mode);

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', mode);
    document.documentElement.setAttribute('data-palette', palette);
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify({ palette, mode }));
    } catch {
      // ignore
    }
  }, [palette, mode]);

  return (
    <CqClientProvider url={WS_URL}>
      <ThemeProvider palette={palette} mode={mode}>
        <div className="flex h-full flex-col">
          <Header
            palette={palette}
            mode={mode}
            onPaletteChange={setPalette}
            onModeChange={setMode}
            wsUrl={WS_URL}
          />
          <Workspace mode={mode} />
        </div>
      </ThemeProvider>
    </CqClientProvider>
  );
}

function Workspace({ mode }: { mode: ThemeMode }) {
  const status = useCqStatus();
  const dockRef = useRef<DockManagerCoreHandle>(null);
  const initialState = useRef(createDefaultState()).current;

  const onReady = (api: DockviewApi) => {
    // Lay out 4 panels in a 2×2 grid. `targetGroupId` is a group id (not
    // a panel id), so we resolve via getGroupForPanel after each add:
    //   ┌─────────────────┬───────────────┐
    //   │ Market data     │ Recent trades │
    //   ├─────────────────┼───────────────┤
    //   │ Positions       │ Aggregations  │
    //   └─────────────────┴───────────────┘
    api.addPanel({ id: 'market-data', title: 'Market data', widgetType: 'market-data' });
    const mdGroup = api.getGroupForPanel('market-data');

    api.addPanel({
      id: 'trades',
      title: 'Recent trades',
      widgetType: 'trades',
      position: 'right',
      targetGroupId: mdGroup ?? undefined,
    });
    const trGroup = api.getGroupForPanel('trades');

    api.addPanel({
      id: 'positions',
      title: 'Positions',
      widgetType: 'positions',
      position: 'bottom',
      targetGroupId: mdGroup ?? undefined,
    });

    api.addPanel({
      id: 'aggregations',
      title: 'Aggregations',
      widgetType: 'aggregations',
      position: 'bottom',
      targetGroupId: trGroup ?? undefined,
    });

    // Pivot tab joins the Aggregations group — same bottom-right
    // quadrant. Omitting `position` adds it as a sibling tab in
    // the target group rather than splitting it.
    const aggGroup = api.getGroupForPanel('aggregations');
    api.addPanel({
      id: 'pivot',
      title: 'Pivot',
      widgetType: 'pivot',
      targetGroupId: aggGroup ?? undefined,
    });
  };

  return (
    <main className="flex-1 min-h-0 flex flex-col">
      <div
        className="px-4 py-1.5 flex items-center gap-3 text-xs"
        style={{
          borderBottom: '1px solid var(--sf-border)',
          background: 'var(--sf-bg-2)',
          color: 'var(--sf-t-2)',
        }}
      >
        <StatusPill status={status} />
        <span style={{ color: 'var(--sf-t-3)' }}>
          tip: drag panel tabs to rearrange, double-click a tab to float
        </span>
      </div>
      <div className="flex-1 min-h-0">
        <DockManagerCore
          ref={dockRef}
          initialState={initialState}
          widgets={WIDGETS as Record<string, React.ComponentType<WidgetProps>>}
          onReady={onReady}
          theme={mode}
          className="cq-dock"
        />
      </div>
      <style>{`
        .cq-dock { width: 100%; height: 100%; }
      `}</style>
    </main>
  );
}
