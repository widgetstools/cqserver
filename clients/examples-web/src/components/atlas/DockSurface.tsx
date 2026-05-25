import { useCallback, useMemo, type ReactNode } from 'react';
import {
  DockviewReact,
  type DockviewReadyEvent,
  type IDockviewPanelProps,
  type DockviewApi,
} from 'dockview-react';

/**
 * DockSurface — declarative dock-manager wrapper.
 *
 * An example builds a `panels` object mapping panel-id → render-fn,
 * plus a `layout` array describing initial geometry (which panels
 * appear where). The surface boots Dockview, registers each panel as
 * a stable React component, then constructs the layout in `onReady`.
 *
 * Panels are draggable, dockable, splitable, tabbable. State is not
 * persisted across reloads — examples are meant to be reset-friendly.
 */

export type DockDirection = 'right' | 'below' | 'left' | 'above';

export interface DockPanelSpec {
  id: string;
  /** Display title in the tab strip. */
  title: string;
  /** Render-fn producing the panel body. */
  render: () => ReactNode;
}

export interface DockLayoutStep {
  /** Panel ID to add. */
  id: string;
  /** Relative direction from `relativeTo`. Omit for the first panel. */
  direction?: DockDirection;
  /** Reference panel ID. Omit for the first panel. */
  relativeTo?: string;
  /** Optional sizing hint as a fraction (0..1). */
  size?: number;
}

interface DockSurfaceProps {
  panels: DockPanelSpec[];
  layout: DockLayoutStep[];
  /** Optional onReady listener (e.g. to grab the api for external commands). */
  onReady?: (api: DockviewApi) => void;
}

export function DockSurface({ panels, layout, onReady }: DockSurfaceProps) {
  const components = useMemo(() => {
    const map: Record<string, (props: IDockviewPanelProps) => ReactNode> = {};
    for (const p of panels) {
      map[p.id] = () => <>{p.render()}</>;
    }
    return map;
  }, [panels]);

  const handleReady = useCallback((ev: DockviewReadyEvent) => {
    const api = ev.api;
    for (const step of layout) {
      const spec = panels.find((p) => p.id === step.id);
      if (!spec) continue;
      api.addPanel({
        id: step.id,
        component: step.id,
        title: spec.title,
        position: step.relativeTo
          ? { referencePanel: step.relativeTo, direction: step.direction }
          : undefined,
        initialWidth: step.size ? Math.max(220, step.size * 1200) : undefined,
      });
    }
    onReady?.(api);
  }, [layout, panels, onReady]);

  return (
    <div className="w-full h-full">
      <DockviewReact
        components={components}
        onReady={handleReady}
        className="dv-react-context"
      />
    </div>
  );
}
