import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react';

export type AtlasThemeMode = 'dark' | 'light';

const STORAGE_KEY = 'atlas-theme';

interface ThemeContextValue {
  theme: AtlasThemeMode;
  setTheme: (mode: AtlasThemeMode) => void;
  toggleTheme: () => void;
}

const ThemeContext = createContext<ThemeContextValue | null>(null);

function readStoredTheme(): AtlasThemeMode {
  if (typeof window === 'undefined') return 'dark';
  const stored = window.localStorage.getItem(STORAGE_KEY);
  if (stored === 'light' || stored === 'dark') return stored;
  return window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
}

function applyThemeToDocument(mode: AtlasThemeMode): void {
  document.documentElement.dataset.theme = mode;
  document.documentElement.style.colorScheme = mode;
  const meta = document.querySelector('meta[name="theme-color"]');
  if (meta) meta.setAttribute('content', mode === 'dark' ? '#0e0e10' : '#f7f5f0');
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setThemeState] = useState<AtlasThemeMode>(readStoredTheme);

  useEffect(() => {
    applyThemeToDocument(theme);
    window.localStorage.setItem(STORAGE_KEY, theme);
  }, [theme]);

  const setTheme = useCallback((mode: AtlasThemeMode) => {
    setThemeState(mode);
  }, []);

  const toggleTheme = useCallback(() => {
    setThemeState((prev) => (prev === 'dark' ? 'light' : 'dark'));
  }, []);

  const value = useMemo(
    () => ({ theme, setTheme, toggleTheme }),
    [theme, setTheme, toggleTheme],
  );

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useAtlasTheme(): ThemeContextValue {
  const ctx = useContext(ThemeContext);
  if (!ctx) {
    throw new Error('useAtlasTheme must be used within ThemeProvider');
  }
  return ctx;
}

export function ThemeToggle() {
  const { theme, toggleTheme } = useAtlasTheme();

  return (
    <button
      type="button"
      onClick={toggleTheme}
      aria-label={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
      title={theme === 'dark' ? 'Light mode' : 'Dark mode'}
      style={{
        all: 'unset',
        cursor: 'pointer',
        display: 'inline-flex',
        alignItems: 'center',
        gap: 8,
        padding: '6px 12px',
        borderRadius: 4,
        border: '1px solid var(--atlas-rule)',
        background: 'var(--atlas-surface)',
        color: 'var(--atlas-fg-dim)',
        fontSize: 10,
        letterSpacing: '.16em',
        transition: 'border-color .15s, color .15s',
      }}
      onMouseEnter={(e) => {
        (e.currentTarget as HTMLElement).style.color = 'var(--atlas-fg)';
        (e.currentTarget as HTMLElement).style.borderColor = 'var(--atlas-amber)';
      }}
      onMouseLeave={(e) => {
        (e.currentTarget as HTMLElement).style.color = 'var(--atlas-fg-dim)';
        (e.currentTarget as HTMLElement).style.borderColor = 'var(--atlas-rule)';
      }}
    >
      <span aria-hidden style={{ fontSize: 13 }}>
        {theme === 'dark' ? '☀' : '☾'}
      </span>
      {theme === 'dark' ? 'LIGHT' : 'DARK'}
    </button>
  );
}

/** Call once before React mounts to avoid a wrong-theme flash. */
export function bootstrapAtlasTheme(): void {
  applyThemeToDocument(readStoredTheme());
}
