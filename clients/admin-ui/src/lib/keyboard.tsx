/**
 * Windows-style keyboard shortcut registry.
 *
 * Bind Ctrl-based combos (not ⌘) to match Windows / Linux / cross-
 * platform IDE conventions. Mac operators get the same shortcuts —
 * just hold Ctrl, not ⌘.
 *
 * Shortcuts registered via `useShortcut` are scoped to the component
 * tree's lifetime; unmount removes the binding. A keydown handler at
 * the document root dispatches to all matching registered shortcuts.
 *
 * Shortcuts DO NOT fire when an INPUT / TEXTAREA / SELECT or
 * contentEditable element is focused — operators can type into a
 * filter field without accidentally triggering F5 or Alt+1. The two
 * exceptions are Escape (always fires; needed to close modals from
 * inside their own inputs) and Ctrl+K (always fires; the palette is
 * the dedicated escape hatch when something is focused).
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import { useLocation } from 'react-router-dom';

export interface Shortcut {
  /** Display name, e.g. "Refresh", "Jump to Topics". */
  label: string;
  /**
   * Canonical key combo. Use lowercase letters; special keys keep
   * their KeyboardEvent.key value (Escape, F5, ArrowDown, etc.).
   * Modifier order: ctrl > alt > shift.
   *
   * Examples: "ctrl+k", "alt+1", "f5", "ctrl+slash", "escape".
   */
  combo: string;
  /** Action to invoke on match. */
  run: () => void;
  /** When the user opens the cheat sheet, group rows under this. */
  group?: 'navigation' | 'data' | 'global';
}

interface KeyboardContextValue {
  // NOTE: deliberately does NOT include the `shortcuts` array. If we
  // exposed it via context, every change to the list would re-render
  // every consumer of `useKeyboardContext` — and each `useShortcut`
  // call would see a "new" context and re-register, looping forever
  // (React error #185). The cheat sheet reads the list inside the
  // provider where mounting on the same render cycle is fine.
  register: (s: Shortcut) => () => void;
  showCheatSheet: () => void;
}

const KeyboardContext = createContext<KeyboardContextValue | null>(null);

/** Normalize a KeyboardEvent into the same string format used by
 *  `Shortcut.combo`. */
function eventToCombo(e: KeyboardEvent): string {
  const parts: string[] = [];
  if (e.ctrlKey || e.metaKey) parts.push('ctrl');
  if (e.altKey) parts.push('alt');
  if (e.shiftKey) parts.push('shift');
  // Normalize printable keys to lowercase letters; special keys keep
  // their `key` value (with a few aliases so users can write
  // "ctrl+slash" instead of "ctrl+/" which is hard to read).
  let k = e.key;
  if (k === '/') k = 'slash';
  else if (k === '?') k = 'question';
  else if (k.length === 1) k = k.toLowerCase();
  // Don't double-count the modifier keys themselves.
  if (k === 'Control' || k === 'Meta' || k === 'Alt' || k === 'Shift') {
    return '';
  }
  parts.push(k);
  return parts.join('+');
}

function isTypingTarget(el: EventTarget | null): boolean {
  if (!(el instanceof HTMLElement)) return false;
  const tag = el.tagName;
  if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true;
  if (el.isContentEditable) return true;
  return false;
}

/** Combos that should fire even when an input is focused. */
const ALWAYS_FIRES = new Set(['escape', 'ctrl+k']);

export function KeyboardProvider({ children }: { children: ReactNode }) {
  const [shortcuts, setShortcuts] = useState<Shortcut[]>([]);
  const [cheatOpen, setCheatOpen] = useState(false);
  // Use a ref so the document handler always sees the latest list
  // without re-binding on every register/unregister.
  const shortcutsRef = useRef<Shortcut[]>([]);
  shortcutsRef.current = shortcuts;

  // Auto-close cheat sheet on route change so it doesn't linger
  // when an operator triggered a navigation via Alt+N while the
  // sheet was open.
  const location = useLocation();
  useEffect(() => {
    setCheatOpen(false);
  }, [location.pathname]);

  const register = useCallback((s: Shortcut) => {
    setShortcuts((cur) => [...cur, s]);
    return () => {
      setShortcuts((cur) => cur.filter((x) => x !== s));
    };
  }, []);

  const showCheatSheet = useCallback(() => setCheatOpen(true), []);

  // Document-level handler. Single listener regardless of how many
  // useShortcut calls are active.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      // Ignore composing IMEs.
      if (e.isComposing) return;
      const combo = eventToCombo(e);
      if (!combo) return;
      if (isTypingTarget(e.target) && !ALWAYS_FIRES.has(combo)) return;
      // Built-in: Ctrl+/ opens the cheat sheet.
      if (combo === 'ctrl+slash') {
        e.preventDefault();
        setCheatOpen(true);
        return;
      }
      if (combo === 'escape' && cheatOpen) {
        e.preventDefault();
        setCheatOpen(false);
        return;
      }
      for (const s of shortcutsRef.current) {
        if (s.combo === combo) {
          e.preventDefault();
          s.run();
          return;
        }
      }
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [cheatOpen]);

  const value = useMemo<KeyboardContextValue>(
    () => ({ register, showCheatSheet }),
    [register, showCheatSheet],
  );

  return (
    <KeyboardContext.Provider value={value}>
      {children}
      {cheatOpen ? <CheatSheet onClose={() => setCheatOpen(false)} shortcuts={shortcuts} /> : null}
    </KeyboardContext.Provider>
  );
}

/** Register a shortcut for the lifetime of the calling component. */
export function useShortcut(s: Shortcut) {
  const ctx = useContext(KeyboardContext);
  // Re-register when the combo or run-fn changes; identity-stable
  // shortcut prevents thrash.
  const key = `${s.combo}|${s.label}`;
  const sRef = useRef(s);
  sRef.current = s;
  useEffect(() => {
    if (!ctx) return;
    const unreg = ctx.register({
      ...sRef.current,
      // Wrap so the registered function picks up the latest
      // callback identity without forcing a re-register.
      run: () => sRef.current.run(),
    });
    return unreg;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ctx, key]);
}

export function useKeyboardContext(): KeyboardContextValue {
  const ctx = useContext(KeyboardContext);
  if (!ctx) throw new Error('useKeyboardContext requires <KeyboardProvider>');
  return ctx;
}

// ─── Cheat sheet modal ────────────────────────────────────────────

interface CheatSheetProps {
  onClose: () => void;
  shortcuts: Shortcut[];
}

function CheatSheet({ onClose, shortcuts }: CheatSheetProps) {
  // Always-available shortcuts that aren't registered by individual
  // components: cheat sheet itself.
  const built_in: Shortcut[] = [
    {
      label: 'Show this cheat sheet',
      combo: 'ctrl+slash',
      run: () => {},
      group: 'global',
    },
    { label: 'Close modal / palette', combo: 'escape', run: () => {}, group: 'global' },
  ];

  const all = [...built_in, ...shortcuts];

  const grouped: Record<string, Shortcut[]> = {
    navigation: [],
    data: [],
    global: [],
  };
  for (const s of all) {
    const g = s.group ?? 'global';
    grouped[g].push(s);
  }

  return (
    <div
      className="fixed inset-0 z-50 bg-background/70 backdrop-blur-sm flex items-start justify-center pt-[15vh]"
      onClick={onClose}
    >
      <div
        className="w-[560px] max-w-[92vw] rounded-md border border-border bg-card shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="px-4 py-3 border-b border-border flex items-center justify-between">
          <div>
            <div className="text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground font-medium">
              Keyboard
            </div>
            <div className="text-[14px] font-semibold leading-none mt-1">
              Shortcuts
            </div>
          </div>
          <kbd className="font-mono text-[10px] text-muted-foreground border border-border rounded px-1.5 py-0.5">
            Esc to close
          </kbd>
        </div>
        <div className="p-4 max-h-[60vh] overflow-y-auto">
          {(['global', 'navigation', 'data'] as const).map((g) =>
            grouped[g].length === 0 ? null : (
              <div key={g} className="mb-4 last:mb-0">
                <div className="text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground font-medium mb-1.5">
                  {g}
                </div>
                <ul className="space-y-1">
                  {grouped[g].map((s, i) => (
                    <li
                      key={`${s.combo}-${i}`}
                      className="flex items-center justify-between gap-3 text-[12.5px]"
                    >
                      <span className="text-foreground">{s.label}</span>
                      <KeyCombo combo={s.combo} />
                    </li>
                  ))}
                </ul>
              </div>
            ),
          )}
        </div>
      </div>
    </div>
  );
}

export function KeyCombo({ combo }: { combo: string }) {
  const parts = combo.split('+').map(prettyKey);
  return (
    <span className="inline-flex items-center gap-0.5">
      {parts.map((p, i) => (
        <kbd
          key={i}
          className="font-mono text-[10.5px] text-muted-foreground border border-border rounded px-1.5 py-0.5 bg-muted"
        >
          {p}
        </kbd>
      ))}
    </span>
  );
}

function prettyKey(k: string): string {
  switch (k) {
    case 'ctrl':
      return 'Ctrl';
    case 'alt':
      return 'Alt';
    case 'shift':
      return 'Shift';
    case 'slash':
      return '/';
    case 'question':
      return '?';
    case 'escape':
      return 'Esc';
    default:
      // F-keys + single chars
      if (k.length === 1) return k.toUpperCase();
      return k;
  }
}
