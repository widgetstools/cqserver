/**
 * AG Grid v33+ Theming API factory.
 *
 * Ported from /Users/develop/wfh/staruidesign1/aggrid-theme.js. Builds a
 * (palette × mode) theme object using themeQuartz.withParams. Cached so
 * downstream useMemo callers can rely on referential equality.
 */
import { themeQuartz, iconSetQuartzBold, type Theme } from 'ag-grid-community';

export type Palette = 'teal' | 'amber' | 'slate' | 'indigo' | 'grey';
export type ThemeMode = 'dark' | 'light';

interface ColorPack {
  bg: string;
  fg: string;
  chrome: string;
  header: string;
  headerText: string;
  odd: string;
  hover: string;
  sel: string;
  border: string;
  rowBorder: string;
  accent: string;
  accentSoft: string;
  inputBg: string;
  inputBorder: string;
  inputFocus: string;
  menu: string;
  menuText: string;
  menuBorder: string;
  tooltip: string;
  tooltipText: string;
  toggleOff: string;
}

const SHARED = {
  iconSize: 13,
  fontFamily: 'Inter, system-ui, sans-serif',
  fontSize: 12,
  headerFontFamily: 'Inter, system-ui, sans-serif',
  headerFontSize: 10,
  headerFontWeight: 700,
  cellFontFamily: "'JetBrains Mono','IBM Plex Mono', ui-monospace, monospace",
  headerHeight: 32,
  rowHeight: 30,
  spacing: 6,
  wrapperBorder: true,
  wrapperBorderRadius: 3,
  borderRadius: 3,
  cellHorizontalPadding: 10,
} as const;

const PALETTES: Record<Palette, Record<ThemeMode, ColorPack>> = {
  teal: {
    dark: {
      bg: '#0d1219', fg: '#f1f4f6',
      chrome: '#161c25', header: '#1a212a', headerText: '#8c969d',
      odd: '#11151c', hover: 'rgba(45,212,191,0.06)', sel: 'rgba(45,212,191,0.12)',
      border: '#2d3540', rowBorder: '#1c232c',
      accent: '#14b8a6', accentSoft: 'rgba(45,212,191,0.10)',
      inputBg: '#161c25', inputBorder: '#3f4854', inputFocus: '#2dd4bf',
      menu: '#1a212a', menuText: '#f1f4f6', menuBorder: '#3f4854',
      tooltip: '#2a323d', tooltipText: '#f1f4f6',
      toggleOff: '#363e49',
    },
    light: {
      bg: '#ffffff', fg: '#0c1d30',
      chrome: '#f3f7f9', header: '#eef3f6', headerText: '#506876',
      odd: '#f8fbfc', hover: 'rgba(13,148,136,0.06)', sel: 'rgba(13,148,136,0.10)',
      border: '#d6dfe5', rowBorder: '#e6edf0',
      accent: '#0f766e', accentSoft: 'rgba(13,148,136,0.08)',
      inputBg: '#ffffff', inputBorder: '#b9c6cf', inputFocus: '#0f766e',
      menu: '#ffffff', menuText: '#0c1d30', menuBorder: '#b9c6cf',
      tooltip: '#0c1d30', tooltipText: '#ffffff',
      toggleOff: '#d4e0e7',
    },
  },
  amber: {
    dark: {
      bg: '#110f0c', fg: '#f0ede6',
      chrome: '#1a1814', header: '#211e17', headerText: '#9b958a',
      odd: '#14110e', hover: 'rgba(245,158,11,0.06)', sel: 'rgba(245,158,11,0.13)',
      border: '#2b2820', rowBorder: '#1f1c16',
      accent: '#f59e0b', accentSoft: 'rgba(245,158,11,0.10)',
      inputBg: '#1a1814', inputBorder: '#3b372d', inputFocus: '#fbbf24',
      menu: '#211e17', menuText: '#f0ede6', menuBorder: '#3b372d',
      tooltip: '#28251e', tooltipText: '#f0ede6',
      toggleOff: '#343026',
    },
    light: {
      bg: '#ffffff', fg: '#271f15',
      chrome: '#f6f1e8', header: '#efe9db', headerText: '#6c5d44',
      odd: '#fbf8f0', hover: 'rgba(146,64,14,0.05)', sel: 'rgba(146,64,14,0.10)',
      border: '#d8ccae', rowBorder: '#e8dec1',
      accent: '#92400e', accentSoft: 'rgba(146,64,14,0.07)',
      inputBg: '#ffffff', inputBorder: '#bcae8b', inputFocus: '#92400e',
      menu: '#ffffff', menuText: '#271f15', menuBorder: '#bcae8b',
      tooltip: '#271f15', tooltipText: '#ffffff',
      toggleOff: '#e3dbc9',
    },
  },
  slate: {
    dark: {
      bg: '#171a1d', fg: '#ebedef',
      chrome: '#1e2125', header: '#252830', headerText: '#8f939a',
      odd: '#1a1d21', hover: 'rgba(59,130,246,0.06)', sel: 'rgba(59,130,246,0.12)',
      border: '#2c2f34', rowBorder: '#222529',
      accent: '#3b82f6', accentSoft: 'rgba(59,130,246,0.10)',
      inputBg: '#1e2125', inputBorder: '#3e4148', inputFocus: '#60a5fa',
      menu: '#252830', menuText: '#ebedef', menuBorder: '#3e4148',
      tooltip: '#2c2f34', tooltipText: '#ebedef',
      toggleOff: '#383c42',
    },
    light: {
      bg: '#ffffff', fg: '#18222f',
      chrome: '#f1f3f6', header: '#e9ecf0', headerText: '#525d6c',
      odd: '#f8fafc', hover: 'rgba(37,99,235,0.05)', sel: 'rgba(37,99,235,0.10)',
      border: '#d4d8de', rowBorder: '#e6e9ed',
      accent: '#2563eb', accentSoft: 'rgba(37,99,235,0.07)',
      inputBg: '#ffffff', inputBorder: '#b6bbc4', inputFocus: '#2563eb',
      menu: '#ffffff', menuText: '#18222f', menuBorder: '#b6bbc4',
      tooltip: '#18222f', tooltipText: '#ffffff',
      toggleOff: '#dde1e7',
    },
  },
  indigo: {
    dark: {
      bg: '#181a1d', fg: '#ecedee',
      chrome: '#1f2125', header: '#25272c', headerText: '#92949a',
      odd: '#1b1d20', hover: 'rgba(99,102,241,0.05)', sel: 'rgba(99,102,241,0.10)',
      border: '#2c2e33', rowBorder: '#222428',
      accent: '#6366f1', accentSoft: 'rgba(99,102,241,0.08)',
      inputBg: '#1f2125', inputBorder: '#3d3f45', inputFocus: '#818cf8',
      menu: '#25272c', menuText: '#ecedee', menuBorder: '#3d3f45',
      tooltip: '#2c2e33', tooltipText: '#ecedee',
      toggleOff: '#383a40',
    },
    light: {
      bg: '#ffffff', fg: '#181a1f',
      chrome: '#f5f5f7', header: '#edeef1', headerText: '#51545c',
      odd: '#fafafb', hover: 'rgba(79,70,229,0.04)', sel: 'rgba(79,70,229,0.07)',
      border: '#d8dade', rowBorder: '#ececef',
      accent: '#4f46e5', accentSoft: 'rgba(79,70,229,0.06)',
      inputBg: '#ffffff', inputBorder: '#bbbdc4', inputFocus: '#4f46e5',
      menu: '#ffffff', menuText: '#181a1f', menuBorder: '#bbbdc4',
      tooltip: '#181a1f', tooltipText: '#ffffff',
      toggleOff: '#e2e3e8',
    },
  },
  grey: {
    dark: {
      bg: '#0e0f11', fg: '#f8f9fa',
      chrome: '#141517', header: '#1a1b1d', headerText: '#9298a0',
      odd: '#111214', hover: 'rgba(248,249,250,0.04)', sel: 'rgba(248,249,250,0.09)',
      border: '#25262a', rowBorder: '#1c1c1f',
      accent: '#e3e4e6', accentSoft: 'rgba(248,249,250,0.06)',
      inputBg: '#141517', inputBorder: '#3e3f43', inputFocus: '#f8f9fa',
      menu: '#1a1b1d', menuText: '#f8f9fa', menuBorder: '#3e3f43',
      tooltip: '#25262a', tooltipText: '#f8f9fa',
      toggleOff: '#2a2b2d',
    },
    light: {
      bg: '#ffffff', fg: '#0c0d0e',
      chrome: '#f8f9fa', header: '#f2f3f4', headerText: '#515257',
      odd: '#fbfbfc', hover: 'rgba(12,13,14,0.04)', sel: 'rgba(12,13,14,0.07)',
      border: '#e3e4e6', rowBorder: '#eeeff0',
      accent: '#171819', accentSoft: 'rgba(12,13,14,0.05)',
      inputBg: '#ffffff', inputBorder: '#d2d3d5', inputFocus: '#171819',
      menu: '#ffffff', menuText: '#0c0d0e', menuBorder: '#d2d3d5',
      tooltip: '#0c0d0e', tooltipText: '#ffffff',
      toggleOff: '#e6e7e9',
    },
  },
};

function hexToRgba(hex: string, a: number): string {
  const m = hex.replace('#', '').match(/.{1,2}/g);
  if (!m || m.length < 3) return hex;
  const [r, g, b] = m.slice(0, 3).map((x) => parseInt(x.length === 1 ? x + x : x, 16));
  return `rgba(${r}, ${g}, ${b}, ${a})`;
}

function buildTheme(p: ColorPack): Theme {
  return themeQuartz.withPart(iconSetQuartzBold).withParams({
    ...SHARED,
    backgroundColor: p.bg,
    foregroundColor: p.fg,
    chromeBackgroundColor: p.chrome,
    headerBackgroundColor: p.header,
    headerTextColor: p.headerText,
    rowHoverColor: p.hover,
    selectedRowBackgroundColor: p.sel,
    oddRowBackgroundColor: p.odd,
    borderColor: p.border,
    rowBorder: { color: p.rowBorder, style: 'solid', width: 1 },
    headerColumnBorder: false,
    headerColumnResizeHandleColor: hexToRgba(p.accent, 0.5),
    accentColor: p.accent,
    rangeSelectionBorderColor: p.accent,
    rangeSelectionBackgroundColor: p.accentSoft,
    inputFocusBorder: { color: p.inputFocus, style: 'solid', width: 1 },
    inputBackgroundColor: p.inputBg,
    inputBorder: { color: p.inputBorder, style: 'solid', width: 1 },
    menuBackgroundColor: p.menu,
    menuTextColor: p.menuText,
    menuBorder: { color: p.menuBorder, style: 'solid', width: 1 },
    tooltipBackgroundColor: p.tooltip,
    tooltipTextColor: p.tooltipText,
    cellTextColor: p.fg,
    checkboxCheckedBackgroundColor: p.accent,
    checkboxCheckedBorderColor: p.accent,
    checkboxUncheckedBackgroundColor: p.inputBg,
    checkboxUncheckedBorderColor: p.inputBorder,
    toggleButtonOnBackgroundColor: p.accent,
    toggleButtonOffBackgroundColor: p.toggleOff,
  });
}

const cache = new Map<string, Theme>();

export const ALL_PALETTES: Palette[] = ['teal', 'amber', 'slate', 'indigo', 'grey'];

export function getAgGridTheme(palette: Palette, mode: ThemeMode): Theme {
  const key = `${palette}-${mode}`;
  const cached = cache.get(key);
  if (cached) return cached;
  const built = buildTheme(PALETTES[palette][mode]);
  cache.set(key, built);
  return built;
}
