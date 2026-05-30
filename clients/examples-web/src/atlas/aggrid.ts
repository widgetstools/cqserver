/**
 * AG-Grid themes — fiery orange-red accent per mode.
 */
import { themeQuartz, iconSetQuartzBold, type Theme } from 'ag-grid-community';
import type { AtlasThemeMode } from './theme/ThemeContext';

const sharedParams = {
  fontFamily: { googleFont: 'JetBrains Mono' } as unknown as string,
  headerFontFamily: { googleFont: 'JetBrains Mono' } as unknown as string,
  fontSize: 11,
  headerFontSize: 9,
  headerFontWeight: 500,
  rowHeight: 26,
  headerHeight: 28,
  spacing: 6,
  cellHorizontalPadding: 12,
  accentColor: '#ff5722',
  invalidColor: '#ff6062',
  columnBorder: false,
};

const ATLAS_THEME_DARK: Theme = themeQuartz
  .withPart(iconSetQuartzBold)
  .withParams({
    ...sharedParams,
    browserColorScheme: 'dark',
    backgroundColor: '#0e0e10',
    foregroundColor: '#e6e6e6',
    chromeBackgroundColor: '#0e0e10',
    borderColor: 'rgba(255, 255, 255, 0.08)',
    rowBorder: { style: 'dashed', color: 'rgba(255, 255, 255, 0.06)', width: 1 },
    headerBackgroundColor: '#0e0e10',
    headerTextColor: 'rgba(230, 230, 230, 0.55)',
    headerColumnBorder: { style: 'solid', color: 'transparent' },
    rowHoverColor: 'rgba(255, 87, 34, 0.08)',
    selectedRowBackgroundColor: 'rgba(255, 87, 34, 0.12)',
    rangeSelectionBackgroundColor: 'rgba(255, 87, 34, 0.14)',
    rangeSelectionBorderColor: '#ff5722',
    valueChangeValueHighlightBackgroundColor: 'rgba(255, 87, 34, 0.45)',
    wrapperBorder: { style: 'solid', color: 'rgba(255, 255, 255, 0.08)', width: 1 },
  });

const ATLAS_THEME_LIGHT: Theme = themeQuartz
  .withPart(iconSetQuartzBold)
  .withParams({
    ...sharedParams,
    accentColor: '#ea580c',
    invalidColor: '#ff4757',
    browserColorScheme: 'light',
    backgroundColor: '#faf8f4',
    foregroundColor: '#2a2722',
    chromeBackgroundColor: '#f7f5f0',
    borderColor: 'rgba(42, 38, 32, 0.14)',
    rowBorder: { style: 'dashed', color: 'rgba(42, 38, 32, 0.09)', width: 1 },
    headerBackgroundColor: '#f7f5f0',
    headerTextColor: 'rgba(42, 39, 34, 0.82)',
    headerColumnBorder: { style: 'solid', color: 'rgba(42, 38, 32, 0.08)' },
    rowHoverColor: 'rgba(234, 88, 12, 0.16)',
    selectedRowBackgroundColor: 'rgba(234, 88, 12, 0.22)',
    rangeSelectionBackgroundColor: 'rgba(234, 88, 12, 0.24)',
    rangeSelectionBorderColor: '#ea580c',
    valueChangeValueHighlightBackgroundColor: 'rgba(234, 88, 12, 0.52)',
    wrapperBorder: { style: 'solid', color: 'rgba(42, 38, 32, 0.14)', width: 1 },
  });

const THEMES: Record<AtlasThemeMode, Theme> = {
  dark: ATLAS_THEME_DARK,
  light: ATLAS_THEME_LIGHT,
};

export function getAtlasGridTheme(mode: AtlasThemeMode): Theme {
  return THEMES[mode];
}
