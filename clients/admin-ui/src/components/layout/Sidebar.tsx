import { NavLink } from 'react-router-dom';
import {
  Activity,
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

interface NavItem {
  to: string;
  label: string;
  icon: React.ComponentType<{ size?: number; className?: string }>;
  group: 'live' | 'data' | 'system';
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
];

const GROUP_LABEL: Record<NavItem['group'], string> = {
  live: 'Live',
  data: 'Data',
  system: 'System',
};

export function Sidebar() {
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
        {(['live', 'data', 'system'] as const).map((group) => (
          <div key={group} className="mb-3">
            <div className="px-4 pb-1 text-[10px] uppercase tracking-[0.12em] text-muted-foreground/70 font-medium">
              {GROUP_LABEL[group]}
            </div>
            <ul>
              {NAV.filter((n) => n.group === group).map(({ to, label, icon: Icon }) => (
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
                      </>
                    )}
                  </NavLink>
                </li>
              ))}
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
