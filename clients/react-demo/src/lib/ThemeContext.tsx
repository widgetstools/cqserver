import { createContext, useContext, type ReactNode } from 'react';
import type { Palette, ThemeMode } from './agGridTheme';

interface ThemeValue {
  palette: Palette;
  mode: ThemeMode;
}

const ThemeContext = createContext<ThemeValue>({ palette: 'teal', mode: 'dark' });

export function ThemeProvider({
  palette,
  mode,
  children,
}: ThemeValue & { children: ReactNode }) {
  return (
    <ThemeContext.Provider value={{ palette, mode }}>{children}</ThemeContext.Provider>
  );
}

export function useThemePrefs(): ThemeValue {
  return useContext(ThemeContext);
}
