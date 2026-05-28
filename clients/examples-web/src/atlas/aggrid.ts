/**
 * AG-Grid theme for the Atlas redesign — Modernist Mono · Amber.
 *
 * Built on the v33+ Theming API (`themeQuartz.withParams(...)`). Returns
 * a singleton theme object; safe to use as the value of `theme={...}`
 * on `<AgGridReact>`.
 */
import { themeQuartz, iconSetQuartzBold, type Theme } from 'ag-grid-community';

const ATLAS_THEME: Theme = themeQuartz
  .withPart(iconSetQuartzBold)
  .withParams({
    // ── chrome ─────────────────────────────────────────────────
    backgroundColor: '#0e0e10',
    foregroundColor: '#e6e6e6',
    chromeBackgroundColor: '#0e0e10',
    borderColor: 'rgba(255, 255, 255, 0.08)',
    rowBorder: { style: 'dashed', color: 'rgba(255, 255, 255, 0.06)', width: 1 },
    headerBackgroundColor: '#0e0e10',
    headerTextColor: 'rgba(230, 230, 230, 0.55)',
    headerColumnBorder: { style: 'solid', color: 'transparent' },
    // ── selection & range ──────────────────────────────────────
    rowHoverColor: 'rgba(244, 165, 43, 0.06)',
    selectedRowBackgroundColor: 'rgba(244, 165, 43, 0.10)',
    rangeSelectionBackgroundColor: 'rgba(244, 165, 43, 0.12)',
    rangeSelectionBorderColor: '#f4a52b',
    // ── flash on value change (signature motion; duration default) ─
    // v35 renames the theme-level cell-flash color; the duration knob
    // isn't exposed in the theming API and stays at the grid default.
    valueChangeValueHighlightBackgroundColor: 'rgba(244, 165, 43, 0.42)',
    // ── typography ─────────────────────────────────────────────
    fontFamily: { googleFont: 'JetBrains Mono' } as unknown as string,
    headerFontFamily: { googleFont: 'JetBrains Mono' } as unknown as string,
    fontSize: 11,
    headerFontSize: 9,
    headerFontWeight: 500,
    // ── density ────────────────────────────────────────────────
    rowHeight: 26,
    headerHeight: 28,
    spacing: 6,
    cellHorizontalPadding: 12,
    // ── visual flourishes ──────────────────────────────────────
    accentColor: '#f4a52b',
    invalidColor: '#ff6062',
    columnBorder: false,
    wrapperBorder: { style: 'solid', color: 'rgba(255, 255, 255, 0.08)', width: 1 },
  });

/**
 * Get the singleton Atlas grid theme. Stable identity across renders —
 * safe to use directly as `<AgGridReact theme={getAtlasGridTheme()} />`.
 */
export function getAtlasGridTheme(): Theme {
  return ATLAS_THEME;
}
