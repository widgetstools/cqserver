import { NavLink, useNavigate } from 'react-router-dom';
import {
  Activity,
  BookOpen,
  Database,
  Eye,
  GitBranch,
  ListChecks,
  Settings,
  Sigma,
  Workflow,
  ScrollText,
  Beaker,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { useShortcut } from '@/lib/keyboard';

interface NavItem {
  to: string;
  label: string;
  icon: React.ComponentType<{ size?: number; className?: string }>;
  group: 'live' | 'data' | 'system' | 'reference';
}

const NAV: NavItem[] = [
  { to: '/', label: 'Overview', icon: Activity, group: 'live' },
  { to: '/subscriptions', label: 'Subscriptions', icon: ListChecks, group: 'live' },
  { to: '/replication', label: 'Replication', icon: GitBranch, group: 'live' },

  { to: '/topics', label: 'Topics', icon: Database, group: 'data' },
  { to: '/views', label: 'Views', icon: Eye, group: 'data' },
  { to: '/queues', label: 'Queues', icon: Workflow, group: 'data' },

  { to: '/metrics', label: 'Metrics', icon: Sigma, group: 'system' },
  { to: '/explain', label: 'Explain', icon: Beaker, group: 'system' },
  { to: '/config', label: 'Config', icon: ScrollText, group: 'system' },

  { to: '/guide', label: 'User Guide', icon: BookOpen, group: 'reference' },
];

const GROUP_LABEL: Record<NavItem['group'], string> = {
  live: 'Live',
  data: 'Data',
  system: 'System',
  reference: 'Reference',
};

/** Returns the flat index (1-based) of an item in NAV. Used for the
 *  Alt+N keyboard hint shown next to each sidebar row. */
function navOrdinal(to: string): number {
  return NAV.findIndex((n) => n.to === to) + 1;
}

export function Sidebar() {
  const navigate = useNavigate();
  // Alt+1..9 — jump to NAV[i-1].to. We expand this manually rather
  // than looping so each useShortcut call has a stable identity
  // and React's rules-of-hooks are happy.
  const nav = (i: number) => () => {
    if (NAV[i]) navigate(NAV[i].to);
  };
  useShortcut({ label: `Jump to ${NAV[0]?.label ?? '#1'}`, combo: 'alt+1', run: nav(0), group: 'navigation' });
  useShortcut({ label: `Jump to ${NAV[1]?.label ?? '#2'}`, combo: 'alt+2', run: nav(1), group: 'navigation' });
  useShortcut({ label: `Jump to ${NAV[2]?.label ?? '#3'}`, combo: 'alt+3', run: nav(2), group: 'navigation' });
  useShortcut({ label: `Jump to ${NAV[3]?.label ?? '#4'}`, combo: 'alt+4', run: nav(3), group: 'navigation' });
  useShortcut({ label: `Jump to ${NAV[4]?.label ?? '#5'}`, combo: 'alt+5', run: nav(4), group: 'navigation' });
  useShortcut({ label: `Jump to ${NAV[5]?.label ?? '#6'}`, combo: 'alt+6', run: nav(5), group: 'navigation' });
  useShortcut({ label: `Jump to ${NAV[6]?.label ?? '#7'}`, combo: 'alt+7', run: nav(6), group: 'navigation' });
  useShortcut({ label: `Jump to ${NAV[7]?.label ?? '#8'}`, combo: 'alt+8', run: nav(7), group: 'navigation' });
  useShortcut({ label: `Jump to ${NAV[8]?.label ?? '#9'}`, combo: 'alt+9', run: nav(8), group: 'navigation' });

  return (
    <aside className="w-[228px] shrink-0 flex flex-col border-r border-border bg-card">
      <div className="px-4 py-3.5 border-b border-border">
        <div className="flex items-center gap-2.5">
          <div className="size-6 rounded-sm bg-primary/15 border border-primary/40 flex items-center justify-center">
            <span className="text-primary text-[10px] font-mono font-bold tracking-tight">cq</span>
          </div>
          <div className="flex flex-col leading-none">
            <span className="text-[13.5px] font-semibold tracking-tight">cqserver</span>
            <span className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground mt-0.5">
              admin
            </span>
          </div>
        </div>
      </div>

      <nav className="flex-1 overflow-y-auto py-2">
        {(['live', 'data', 'system', 'reference'] as const).map((group) => (
          <div key={group} className="mb-3">
            <div className="px-4 pb-1 text-[10px] uppercase tracking-[0.12em] text-muted-foreground/70 font-medium">
              {GROUP_LABEL[group]}
            </div>
            <ul>
              {NAV.filter((n) => n.group === group).map(({ to, label, icon: Icon }) => {
                const ord = navOrdinal(to);
                return (
                  <li key={to}>
                    <NavLink
                      to={to}
                      end={to === '/'}
                      className={({ isActive }) =>
                        cn(
                          'group relative flex items-center gap-2.5 px-4 py-1.5 text-[12.5px] transition-colors',
                          isActive
                            ? 'text-foreground'
                            : 'text-muted-foreground hover:text-foreground hover:bg-accent/50',
                        )
                      }
                    >
                      {({ isActive }) => (
                        <>
                          {isActive ? (
                            <span className="absolute left-0 top-1/2 -translate-y-1/2 h-4 w-[2px] bg-primary rounded-r-sm" />
                          ) : null}
                          <Icon
                            size={14}
                            className={cn(
                              'shrink-0',
                              isActive ? 'text-primary' : 'text-muted-foreground/80',
                            )}
                          />
                          <span className="flex-1">{label}</span>
                          {ord >= 1 && ord <= 9 ? (
                            <span
                              className={cn(
                                'shrink-0 font-mono text-[9.5px] px-1 py-px rounded border opacity-0 group-hover:opacity-100 transition-opacity',
                                isActive
                                  ? 'border-primary/40 text-primary'
                                  : 'border-border text-muted-foreground/70',
                              )}
                              aria-label={`Press Alt+${ord} to jump here`}
                            >
                              Alt+{ord}
                            </span>
                          ) : null}
                        </>
                      )}
                    </NavLink>
                  </li>
                );
              })}
            </ul>
          </div>
        ))}
      </nav>

      <div className="border-t border-border px-4 py-2.5 flex items-center gap-2">
        <Settings size={13} className="text-muted-foreground/60" />
        <span className="text-[11px] text-muted-foreground font-mono">v0.1.0</span>
      </div>
    </aside>
  );
}
