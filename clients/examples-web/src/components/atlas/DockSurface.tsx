import { useMemo, useCallback, type ReactNode } from 'react';
import {
  DockManagerCore,
  type WidgetProps,
} from '@widgetstools/react-dock-manager';
import {
  createDefaultState,
  type DockviewApi,
  type DockPosition,
  type DockEdge,
  type LayoutNode,
  type Placement,
  type SplitNode,
} from '@widgetstools/dock-manager-core';
import { useTheme } from '@/components/theme/ThemeProvider';

/**
 * DockSurface — declarative dock-manager wrapper around the
 * `@widgetstools/react-dock-manager` `DockManagerCore` component.
 *
 * An example builds a `panels` object mapping panel-id → render-fn
 * plus a `layout` array describing initial geometry (which panels
 * appear where). The surface boots DockManagerCore with the empty
 * default state, then in `onReady` calls `api.addPanel(...)` in
 * sequence to assemble the layout. Panel content is rendered via
 * `renderPanel` so each panel-id resolves to its registered React
 * subtree.
 *
 * The package's `theme` prop is forwarded from the Atlas theme so
 * dark/light toggle propagates into the dock chrome too.
 */

// Our authoring type — examples write these as if describing
// directional steps (`below` is more readable than `bottom`). We map
// to the package's `DockPosition` underneath.
export type DockDirection = 'right' | 'below' | 'left' | 'above' | 'center';

const DIRECTION_TO_POSITION: Record<DockDirection, DockPosition> = {
  right: 'right',
  below: 'bottom',
  left: 'left',
  above: 'top',
  center: 'center',
};

export interface DockPanelSpec {
  id: string;
  title: string;
  render: () => ReactNode;
  /**
   * When set, the panel is unpinned (auto-hidden) on the given edge
   * after the layout is built. The package renders a 28px-wide tab
   * strip on that edge; hovering / clicking the tab expands a flyout
   * with the panel content. Used for help / notes panels so the main
   * canvas stays uncluttered.
   */
  pin?: DockEdge;
  /** Optional width/height (px) for the flyout when unpinned. */
  pinSize?: number;
}

export interface DockLayoutStep {
  id: string;
  direction?: DockDirection;
  relativeTo?: string;
  /**
   * Fraction of the parent split this panel should occupy (0–1). Sets
   * the percent on the immediate enclosing split right after the panel
   * is inserted, so a step like
   *
   *     { id: 'editor', relativeTo: 'library', direction: 'right', size: 0.7 }
   *
   * makes editor 70 % wide and library 30 %. Only honored on
   * two-child splits — for arbitrary arity, set sizes manually via
   * the `onReady(api)` escape hatch.
   */
  size?: number;
}

interface DockSurfaceProps {
  panels: DockPanelSpec[];
  layout: DockLayoutStep[];
  onReady?: (api: DockviewApi) => void;
}

export function DockSurface({ panels, layout, onReady }: DockSurfaceProps) {
  const { theme } = useTheme();

  const initialState = useMemo(() => createDefaultState(), []);

  const byId = useMemo(() => {
    const m = new Map<string, DockPanelSpec>();
    for (const p of panels) m.set(p.id, p);
    return m;
  }, [panels]);

  // renderPanel is called by the core whenever a panel's content
  // needs to mount. We dispatch by panelId into our spec registry so
  // every example panel produces its real React subtree.
  const renderPanel = useCallback(
    (panelId: string, _panel: unknown, _api: unknown): ReactNode => {
      const spec = byId.get(panelId);
      if (!spec) return null;
      return spec.render();
    },
    [byId],
  );

  // Tab labels: the package defaults to `panel.title`. We can leave
  // this undefined so the default mono uppercase-tracked styling
  // from the styles.css applies; pass our own only if we want extra
  // chrome later.
  const widgets = useMemo<Record<string, React.ComponentType<WidgetProps>>>(() => ({}), []);

  const handleReady = useCallback(
    (api: DockviewApi) => {
      // Build the layout step-by-step. The first added panel goes
      // into whatever group `createDefaultState` provided; every
      // subsequent panel is docked relative to a previously added
      // panel's group via `targetGroupId` + `position`.
      for (let i = 0; i < layout.length; i++) {
        const step = layout[i]!;
        const spec = byId.get(step.id);
        if (!spec) continue;

        if (i === 0 || !step.relativeTo) {
          api.addPanel({
            id: step.id,
            panelId: step.id,
            title: spec.title,
            widgetType: step.id,
          });
          continue;
        }

        const groupId = api.getGroupForPanel(step.relativeTo);
        const dir = step.direction ?? 'right';
        api.addPanel({
          id: step.id,
          panelId: step.id,
          title: spec.title,
          widgetType: step.id,
          targetGroupId: groupId ?? undefined,
          position: DIRECTION_TO_POSITION[dir],
        });

        // Apply per-step sizing if requested. We walk the layout tree
        // looking for the split that now contains this panel as a
        // direct child; the dock-manager's `resizeSplit` takes percent
        // sizes that sum to 100.
        if (typeof step.size === 'number' && step.size > 0 && step.size < 1) {
          const found = findEnclosingSplit(api.layout, step.id);
          if (found && found.split.children.length === 2) {
            const sizes = [0, 0];
            sizes[found.childIndex] = Math.round(step.size * 100);
            sizes[1 - found.childIndex] = 100 - sizes[found.childIndex]!;
            api.resizeSplit(found.split.id, sizes);
          }
        }
      }

      // Pin/unpin pass: any panel with `pin: <edge>` is moved from its
      // docked position into the edge auto-hide strip. The package's
      // `unpinPanel` auto-detects the edge from the panel's current
      // layout position; if that detection disagrees with what we
      // requested, we patch the placement via `loadState`.
      const pinSpecs = panels.filter((p) => p.pin);
      for (const spec of pinSpecs) {
        api.unpinPanel(spec.id);
      }
      if (pinSpecs.length) {
        const state = api.state;
        let changed = false;
        const placements = new Map<string, Placement>(state.placements);
        for (const spec of pinSpecs) {
          const pl = placements.get(spec.id);
          if (!pl || pl.type !== 'unpinned') continue;
          const wantEdge = spec.pin!;
          const wantSize = spec.pinSize ?? 360;
          if (pl.edge !== wantEdge || pl.size !== wantSize) {
            placements.set(spec.id, { ...pl, edge: wantEdge, size: wantSize });
            changed = true;
          }
        }
        if (changed) {
          api.loadState({ ...state, placements });
        }
      }

      onReady?.(api);
    },
    [layout, byId, panels, onReady],
  );

  return (
    <div className="w-full h-full">
      <DockManagerCore
        initialState={initialState}
        widgets={widgets}
        renderPanel={renderPanel}
        onReady={handleReady}
        theme={theme}
        className="cq-dock"
      />
    </div>
  );
}

/**
 * Walk a LayoutNode tree looking for the split that directly contains
 * a tab-group with `panelId` as one of its panels. Used by the
 * `DockLayoutStep.size` post-processing to find the split whose sizes
 * should be adjusted after a panel is inserted.
 */
function findEnclosingSplit(
  root: LayoutNode,
  panelId: string,
): { split: SplitNode; childIndex: number } | null {
  if (root.type !== 'split') return null;
  for (let i = 0; i < root.children.length; i++) {
    const child = root.children[i]!;
    if (child.type === 'tabgroup' && child.panels.includes(panelId)) {
      return { split: root, childIndex: i };
    }
    if (child.type === 'split') {
      const inner = findEnclosingSplit(child, panelId);
      if (inner) return inner;
    }
  }
  return null;
}
