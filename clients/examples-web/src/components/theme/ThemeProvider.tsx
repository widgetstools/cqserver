import { createContext, useContext, useEffect, useState, type ReactNode } from 'react';

type Theme = 'dark' | 'light';
export type Palette = 'teal' | 'indigo' | 'amber' | 'slate' | 'grey';

export const PALETTES: Palette[] = ['teal', 'indigo', 'amber', 'slate', 'grey'];

interface ThemeContextValue {
  theme: Theme;
  palette: Palette;
  setTheme: (t: Theme) => void;
  setPalette: (p: Palette) => void;
  toggleTheme: () => void;
}

const THEME_KEY = 'cqserver-atlas-theme';
const PALETTE_KEY = 'cqserver-atlas-palette';

const ThemeContext = createContext<ThemeContextValue>({
  theme: 'dark',
  palette: 'teal',
  setTheme: () => {},
  setPalette: () => {},
  toggleTheme: () => {},
});

function initialTheme(): Theme {
  if (typeof window === 'undefined') return 'dark';
  const stored = window.localStorage.getItem(THEME_KEY);
  if (stored === 'dark' || stored === 'light') return stored;
  return 'dark';
}

function initialPalette(): Palette {
  if (typeof window === 'undefined') return 'teal';
  const stored = window.localStorage.getItem(PALETTE_KEY);
  if (stored && (PALETTES as string[]).includes(stored)) return stored as Palette;
  return 'teal';
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setTheme] = useState<Theme>(initialTheme);
  const [palette, setPalette] = useState<Palette>(initialPalette);

  useEffect(() => {
    // Stockflux tokens key off `[data-theme]` and `[data-palette]`
    // on the documentElement, not a `.dark` class.
    const root = document.documentElement;
    root.dataset.theme = theme;
    root.dataset.palette = palette;
    window.localStorage.setItem(THEME_KEY, theme);
    window.localStorage.setItem(PALETTE_KEY, palette);
  }, [theme, palette]);

  const toggleTheme = () => setTheme((t) => (t === 'dark' ? 'light' : 'dark'));

  return (
    <ThemeContext.Provider value={{ theme, palette, setTheme, setPalette, toggleTheme }}>
      {children}
    </ThemeContext.Provider>
  );
}

export const useTheme = (): ThemeContextValue => useContext(ThemeContext);
