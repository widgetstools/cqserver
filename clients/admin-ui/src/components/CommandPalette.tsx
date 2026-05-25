/**
 * Command Palette (Ctrl+K) — Linear/VSCode-style overlay.
 *
 * Three sources of commands:
 *   - Routes: the sidebar items so an operator can jump anywhere with
 *     `o` (Overview), `t` (Topics), `r` (Replication), etc.
 *   - Topics: live list from /topics; selecting one jumps to the
 *     Topics page with `?focus=<name>`.
 *   - Actions: ops verbs that hit admin endpoints (shrink-all, etc.).
 *
 * Fuzzy match is intentionally lightweight: a case-insensitive
 * substring filter that ranks shorter matches (more relevant) first.
 * No external dependency.
 */

import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import {
  Activity,
  Beaker,
  ChevronRight,
  Database,
  Eye,
  FileText,
  GitBranch,
  ListChecks,
  Search,
  Sigma,
  Trash2,
  Workflow,
  Zap,
} from 'lucide-react';
import { adminApi } from '@/lib/admin';
import { cn } from '@/lib/utils';
import { useShortcut } from '@/lib/keyboard';

interface Cmd {
  id: string;
  label: string;
  hint?: string;
  group: 'Navigate' | 'Topics' | 'Actions';
  icon: React.ComponentType<{ size?: number; className?: string }>;
  run: () => void | Promise<void>;
}

const ROUTE_CMDS: Array<Omit<Cmd, 'run'> & { to: string }> = [
  { id: 'nav-overview', label: 'Overview', hint: 'Health snapshot', group: 'Navigate', icon: Activity, to: '/' },
  { id: 'nav-subs', label: 'Subscriptions', hint: 'Live wire view', group: 'Navigate', icon: ListChecks, to: '/subscriptions' },
  { id: 'nav-repl', label: 'Replication', hint: 'Topology + lag', group: 'Navigate', icon: GitBranch, to: '/replication' },
  { id: 'nav-topics', label: 'Topics', hint: 'AG-Grid of every topic', group: 'Navigate', icon: Database, to: '/topics' },
  { id: 'nav-views', label: 'Views', hint: 'Materialized aggregates', group: 'Navigate', icon: Eye, to: '/views' },
  { id: 'nav-queues', label: 'Queues', hint: 'Competing-consumer queues', group: 'Navigate', icon: Workflow, to: '/queues' },
  { id: 'nav-metrics', label: 'Metrics', hint: 'Prometheus browser', group: 'Navigate', icon: Sigma, to: '/metrics' },
  { id: 'nav-explain', label: 'Explain', hint: 'Estimate query cost', group: 'Navigate', icon: Beaker, to: '/explain' },
  { id: 'nav-config', label: 'Config', hint: 'Live cqserver.toml', group: 'Navigate', icon: FileText, to: '/config' },
];

interface CommandPaletteProps {
  open: boolean;
  onClose: () => void;
}

export function CommandPalette({ open, onClose }: CommandPaletteProps) {
  const [q, setQ] = useState('');
  const [active, setActive] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const navigate = useNavigate();
  const location = useLocation();

  // Auto-close on route change so the palette never lingers after
  // an operator selects a command that navigates somewhere.
  useEffect(() => {
    if (open) onClose();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [location.pathname]);

  const topics = useQuery({
    queryKey: ['topics'],
    queryFn: adminApi.topics,
    refetchInterval: 10_000,
    enabled: open, // only load when the palette is visible
  });

  const cmds = useMemo<Cmd[]>(() => {
    const out: Cmd[] = ROUTE_CMDS.map(({ to, ...c }) => ({
      ...c,
      run: () => {
        navigate(to);
        onClose();
      },
    }));

    for (const t of topics.data ?? []) {
      out.push({
        id: `topic-${t.name}`,
        label: `Open ${t.name}`,
        hint: `${t.rowCount.toLocaleString()} rows · ${t.columnCount} cols`,
        group: 'Topics',
        icon: Database,
        run: () => {
          navigate(`/topics?focus=${encodeURIComponent(t.name)}`);
          onClose();
        },
      });
    }

    // Actions
    out.push({
      id: 'action-shrink-all',
      label: 'Shrink stores (all topics)',
      hint: 'POST /admin/shrink-store-all',
      group: 'Actions',
      icon: Trash2,
      run: async () => {
        if (!window.confirm('Shrink stores on every topic? Frees unused row slots.')) return;
        try {
          await adminApi.shrinkStoreAll();
        } finally {
          onClose();
        }
      },
    });
    out.push({
      id: 'action-refresh',
      label: 'Hard refresh page',
      hint: 'Reload SPA shell',
      group: 'Actions',
      icon: Zap,
      run: () => {
        window.location.reload();
      },
    });

    return out;
  }, [topics.data, navigate, onClose]);

  // Filter + rank.
  const filtered = useMemo<Cmd[]>(() => {
    if (!q.trim()) {
      // Default order: navigate group first, then 6 most-recent topics, then actions.
      return [
        ...cmds.filter((c) => c.group === 'Navigate'),
        ...cmds.filter((c) => c.group === 'Topics').slice(0, 6),
        ...cmds.filter((c) => c.group === 'Actions'),
      ];
    }
    const needle = q.trim().toLowerCase();
    const scored: Array<{ c: Cmd; score: number }> = [];
    for (const c of cmds) {
      const hay = `${c.label} ${c.hint ?? ''}`.toLowerCase();
      const idx = hay.indexOf(needle);
      if (idx < 0) continue;
      // Lower score = better. Earlier match wins; prefix beats midword.
      const score = idx === 0 ? 0 : idx + (hay.startsWith(needle) ? 0 : 100);
      scored.push({ c, score });
    }
    scored.sort((a, b) => a.score - b.score || a.c.label.localeCompare(b.c.label));
    return scored.map((s) => s.c).slice(0, 24);
  }, [q, cmds]);

  // Reset state when opening; auto-focus the input.
  useEffect(() => {
    if (open) {
      setQ('');
      setActive(0);
      // small delay so the modal is in the DOM before focus
      setTimeout(() => inputRef.current?.focus(), 0);
    }
  }, [open]);

  // Clamp active when filter changes.
  useEffect(() => {
    if (active >= filtered.length) setActive(0);
  }, [active, filtered.length]);

  if (!open) return null;

  const groups: Cmd['group'][] = ['Navigate', 'Topics', 'Actions'];
  const byGroup = new Map<Cmd['group'], Cmd[]>();
  for (const g of groups) byGroup.set(g, []);
  for (const c of filtered) byGroup.get(c.group)!.push(c);

  // Build a flat index so arrow keys move through visible items
  // including the group separators (which we skip).
  let flatIndex = 0;
  const renderedItems: Array<{ c: Cmd; idx: number }> = [];
  for (const g of groups) {
    for (const c of byGroup.get(g) ?? []) {
      renderedItems.push({ c, idx: flatIndex++ });
    }
  }

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActive((a) => Math.min(a + 1, renderedItems.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActive((a) => Math.max(a - 1, 0));
    } else if (e.key === 'Enter') {
      e.preventDefault();
      const item = renderedItems[active];
      if (item) item.c.run();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      onClose();
    }
  };

  return (
    <div
      className="fixed inset-0 z-40 bg-background/70 backdrop-blur-sm flex items-start justify-center pt-[12vh]"
      onClick={onClose}
    >
      <div
        className="w-[600px] max-w-[94vw] rounded-md border border-border bg-card shadow-xl overflow-hidden"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center gap-2 px-3.5 py-2.5 border-b border-border">
          <Search size={14} className="text-muted-foreground shrink-0" />
          <input
            ref={inputRef}
            value={q}
            onChange={(e) => setQ(e.target.value)}
            onKeyDown={onKeyDown}
            placeholder="Type to search routes, topics, actions…"
            className="flex-1 bg-transparent border-0 outline-none text-[13px] text-foreground placeholder:text-muted-foreground"
          />
          <kbd className="font-mono text-[10px] text-muted-foreground border border-border rounded px-1.5 py-0.5">
            Esc
          </kbd>
        </div>
        <div className="max-h-[55vh] overflow-y-auto p-1">
          {renderedItems.length === 0 ? (
            <div className="py-8 text-center text-[12px] text-muted-foreground">
              No matches for "{q}".
            </div>
          ) : (
            groups.map((g) => {
              const items = byGroup.get(g) ?? [];
              if (items.length === 0) return null;
              return (
                <div key={g} className="mb-1.5 last:mb-0">
                  <div className="text-[10px] uppercase tracking-[0.1em] text-muted-foreground/70 px-3 py-1">
                    {g}
                  </div>
                  {items.map((c) => {
                    const flat = renderedItems.findIndex((r) => r.c === c);
                    const isActive = flat === active;
                    const Icon = c.icon;
                    return (
                      <button
                        key={c.id}
                        onMouseEnter={() => setActive(flat)}
                        onClick={() => c.run()}
                        className={cn(
                          'w-full flex items-center gap-3 px-3 py-2 rounded-sm text-left transition-colors',
                          isActive ? 'bg-info-muted' : 'hover:bg-accent/50',
                        )}
                      >
                        <Icon
                          size={14}
                          className={cn(
                            'shrink-0',
                            isActive ? 'text-primary' : 'text-muted-foreground/80',
                          )}
                        />
                        <span className="flex-1 text-[12.5px] text-foreground truncate">
                          {c.label}
                        </span>
                        {c.hint ? (
                          <span className="text-[11px] text-muted-foreground truncate font-mono">
                            {c.hint}
                          </span>
                        ) : null}
                        {isActive ? (
                          <ChevronRight size={12} className="text-primary shrink-0" />
                        ) : null}
                      </button>
                    );
                  })}
                </div>
              );
            })
          )}
        </div>
        <div className="px-3.5 py-1.5 border-t border-border text-[10.5px] text-muted-foreground flex items-center gap-3">
          <span>
            <Kbd>↑</Kbd> <Kbd>↓</Kbd> navigate
          </span>
          <span>
            <Kbd>Enter</Kbd> run
          </span>
          <span>
            <Kbd>Ctrl</Kbd>+<Kbd>/</Kbd> all shortcuts
          </span>
        </div>
      </div>
    </div>
  );
}

function Kbd({ children }: { children: React.ReactNode }) {
  return (
    <kbd className="font-mono text-[10px] text-muted-foreground border border-border rounded px-1 py-0.5 bg-muted">
      {children}
    </kbd>
  );
}

/** Convenience wrapper: registers Ctrl+K → open palette. Drop into App. */
export function CommandPaletteMount() {
  const [open, setOpen] = useState(false);
  useShortcut({
    label: 'Open command palette',
    combo: 'ctrl+k',
    run: () => setOpen(true),
    group: 'global',
  });
  return <CommandPalette open={open} onClose={() => setOpen(false)} />;
}

/** F5 → invalidate every active TanStack Query so the current page
 *  refetches. Lives alongside the palette so we have one keyboard
 *  mount point in App.tsx. */
export function GlobalRefreshMount() {
  const qc = useQueryClient();
  useShortcut({
    label: 'Refresh data on this page',
    combo: 'f5',
    run: () => {
      qc.invalidateQueries();
    },
    group: 'data',
  });
  return null;
}

/** Ctrl+F → focus the first input on the page tagged
 *  `data-page-filter`. Selects existing text so the operator can
 *  type-replace immediately. */
export function PageFilterFocusMount() {
  useShortcut({
    label: 'Focus page filter',
    combo: 'ctrl+f',
    run: () => {
      const el = document.querySelector<HTMLInputElement>(
        'input[data-page-filter]',
      );
      if (el) {
        el.focus();
        el.select();
      }
    },
    group: 'data',
  });
  return null;
}
