# Phase 1 — Atlas Design Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the *Modernist Mono · Amber* design system — tokens, fonts, AG-Grid theme, and base chapter components — as a self-contained module under `clients/examples-web/src/atlas/`, with a hash-routed preview (`#atlas`) of the redesigned Pulse chapter wired to placeholder data.

**Architecture:** Build in parallel. The existing app at `/` is untouched. A `#atlas` hash route swaps the root to `<AtlasPreviewApp>`, which renders the new shell (top wordmark + stations nav + chapter content) using only new components and new tokens. All new styles are scoped under `.atlas-root` to avoid bleeding into the existing app. Phase 2 will swap the placeholder data for the SharedWorker.

**Tech Stack:** React 19 + TypeScript + Vite, JetBrains Mono Variable via `@fontsource-variable/jetbrains-mono` (already a workspace dep), AG-Grid v33+ Theming API (`themeQuartz.withParams(…)`), Tailwind 4 (for class utilities where helpful; design system itself is plain CSS variables).

This is **Phase 1 of 6** of the redesign in `docs/superpowers/specs/2026-05-27-examples-web-redesign-design.md`. Phases 2–6 (worker data layer, then chapter migrations) follow.

**Verification note:** `clients/examples-web` has **no unit test runner**. Every task verifies with `npm run typecheck` (which runs `tsc -b`) and `npm run build` (`tsc -b && vite build`). A final manual task opens `http://localhost:5175/#atlas` in the browser to see the chapter live.

---

## File Structure

All under `clients/examples-web/`. New files only; no existing file is modified except `src/App.tsx` (one hash-route guard at the top) and `src/main.tsx` (one CSS import).

```
src/
  atlas/
    tokens.css              ← C·amber palette + type + spacing tokens (scoped to .atlas-root)
    aggrid.ts               ← getAtlasGridTheme(): v33+ themeQuartz with atlas params
    icons.tsx               ← inline SVG icons used by the design (only 2: chevron, dot)
    types.ts                ← ChapterId, ChapterScope, ChapterMeta, KpiSpec types
    chapters.ts             ← The 8 chapter meta (id, num, name, kicker, scope stub)
    preview/
      placeholderData.ts    ← Static rows + KPI values + chip values for the Pulse preview
      PulsePreview.tsx      ← The Pulse chapter composed from base components
      AtlasPreviewApp.tsx   ← Preview root: AppShell + StationsNav + hash-routed chapter
    components/
      AppShell.tsx          ← Top wordmark + connection summary
      StationsNav.tsx       ← 8-station horizontal rail, keyboard 1–8 shortcuts
      ChapterHead.tsx       ← Eyebrow + amber title + sub + hero metric pulled right
      FilterRail.tsx        ← Chip rail; opens ChipPicker on click
      ChipPicker.tsx        ← Compact mono dropdown
      KpiStrip.tsx          ← 6-slot grid-lined KPI band
      DataTable.tsx         ← AG-Grid wrapper using the atlas theme
      Footer.tsx            ← LIVE status + tick stats + keyboard hints
src/App.tsx                 ← Add hash guard: render <AtlasPreviewApp/> when location.hash === '#atlas'
src/main.tsx                ← Add `import '@/atlas/tokens.css'`
```

---

## Task 1: Design tokens (CSS variables, fonts, base resets)

**Files:**
- Create: `clients/examples-web/src/atlas/tokens.css`
- Modify: `clients/examples-web/src/main.tsx` — add one import.

- [ ] **Step 1: Write the tokens file**

Create `clients/examples-web/src/atlas/tokens.css`:

```css
/**
 * Atlas design tokens — Modernist Mono · Amber.
 *
 * Scoped under `.atlas-root` so this file can be imported app-wide
 * without bleeding into the existing examples (which still use
 * tokens.css + globals.css). Every Atlas component lives inside an
 * `.atlas-root` container.
 */
@import "@fontsource-variable/jetbrains-mono/wght.css";

.atlas-root {
  /* ── palette ───────────────────────────────────────────────── */
  --atlas-ink: #0e0e10;
  --atlas-ink-2: #14141a;
  --atlas-surface: rgba(255, 255, 255, .02);
  --atlas-rule: rgba(255, 255, 255, .08);
  --atlas-rule-soft: rgba(255, 255, 255, .035);
  --atlas-fg: #e6e6e6;
  --atlas-fg-dim: rgba(230, 230, 230, .55);
  --atlas-fg-faint: rgba(230, 230, 230, .35);
  --atlas-amber: #f4a52b;
  --atlas-amber-soft: rgba(244, 165, 43, .08);
  --atlas-amber-glow: 0 0 12px rgba(244, 165, 43, .45);
  --atlas-neg: #ff6062;

  /* ── type ──────────────────────────────────────────────────── */
  --atlas-font: 'JetBrains Mono Variable', ui-monospace, 'SF Mono', monospace;

  /* ── spacing scale (4px base) ──────────────────────────────── */
  --atlas-sp-1: 4px;
  --atlas-sp-2: 8px;
  --atlas-sp-3: 12px;
  --atlas-sp-4: 16px;
  --atlas-sp-5: 22px;
  --atlas-sp-6: 32px;

  background: var(--atlas-ink);
  color: var(--atlas-fg);
  font-family: var(--atlas-font);
  font-feature-settings: 'tnum';
  -webkit-font-smoothing: antialiased;
}

/* Background grid — structural rhythm, masked to the focal area. */
.atlas-root.atlas-app {
  position: relative;
  min-height: 100vh;
}
.atlas-root.atlas-app::before {
  content: "";
  position: absolute;
  inset: 0;
  pointer-events: none;
  background-image:
    linear-gradient(to right, var(--atlas-rule-soft) 1px, transparent 1px),
    linear-gradient(to bottom, var(--atlas-rule-soft) 1px, transparent 1px);
  background-size: 32px 32px;
  -webkit-mask-image: radial-gradient(1400px 800px at 25% 0%, #000 50%, transparent 100%);
          mask-image: radial-gradient(1400px 800px at 25% 0%, #000 50%, transparent 100%);
}

/* The single signature motion — live tick pulse on the amber dot. */
@keyframes atlas-pulse { 0%,100% {opacity:1} 50% {opacity:.4} }
.atlas-root .atlas-live-dot {
  display: inline-block;
  width: 6px;
  height: 6px;
  background: var(--atlas-amber);
  border-radius: 99px;
  box-shadow: var(--atlas-amber-glow);
  animation: atlas-pulse 1.6s infinite;
}
```

- [ ] **Step 2: Wire the tokens into the app entry**

In `clients/examples-web/src/main.tsx`, add the import alongside the existing `globals.css` import (the existing imports stay):

```ts
import '@/atlas/tokens.css';
```

- [ ] **Step 3: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -4`
Expected: typecheck clean, build succeeds. (CSS-only change + one import; no type effects.)

- [ ] **Step 4: Commit**

```bash
git add clients/examples-web/src/atlas/tokens.css clients/examples-web/src/main.tsx
git commit -m "feat(atlas): design tokens — C·amber palette + JetBrains Mono Variable

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: AG-Grid Atlas theme

**Files:**
- Create: `clients/examples-web/src/atlas/aggrid.ts`

This builds the v33+ themed grid object matching the design — dark ink background, JetBrains Mono Variable, amber row flash, dashed row rules, no zebra.

- [ ] **Step 1: Write the theme module**

Create `clients/examples-web/src/atlas/aggrid.ts`:

```ts
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
    // ── flash on value change (single signature motion) ───────
    cellChangeFlashColor: 'rgba(244, 165, 43, 0.42)',
    cellChangeFlashDuration: 380,
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
```

(The `googleFont` cast is needed because `themeQuartz`'s `withParams` typing accepts either a string or a `{ googleFont }` object but the union isn't exposed in `Theme`'s public types in this version.)

- [ ] **Step 2: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -4`
Expected: typecheck clean, build succeeds.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/aggrid.ts
git commit -m "feat(atlas): AG-Grid v33 theme — dark ink, mono, amber row flash

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Shared types and chapter meta

**Files:**
- Create: `clients/examples-web/src/atlas/types.ts`
- Create: `clients/examples-web/src/atlas/chapters.ts`

Locks the chapter set and the shape of the per-chapter scope, used by every component built next.

- [ ] **Step 1: Write `types.ts`**

```ts
/**
 * Atlas shared types. The data-layer hooks (Phase 2) and chapter
 * components (Phase 1+) both consume these.
 */

export type ChapterId =
  | 'pulse'
  | 'tape'
  | 'lens'
  | 'heat'
  | 'view'
  | 'join'
  | 'slip'
  | 'query';

export interface ChapterMeta {
  id: ChapterId;
  num: string;       // '01' .. '08' — typeset in the stations rail
  name: string;      // 'PULSE' — uppercase mono label
  kicker: string;    // 'LIVE BOOK' — eyebrow text on the chapter head
}

/** One chip in a chapter's filter rail. Phase 1 uses these for the
 *  visual chip rail; Phase 2 wires them to subscription-driven values. */
export interface ChipSpec {
  key: string;                  // 'BOOK', 'SECTOR' — the chip label
  column: string;               // 'book_name' — the source column
  source?: string;              // '/v_pnl_by_book' — view that supplies values (Phase 2)
  default?: string;             // first-paint scope (e.g. 'RATES-US')
}

export interface ChapterScope {
  primary: {
    topic: string;              // '/positions'
    rowIdKey: string;           // 'position_id'
    filter?: (s: Record<string, string>) => string | null;
  };
  views?: string[];             // '/v_book_totals' etc., subscribed for KPIs
  chips: ChipSpec[];
}

export interface KpiSpec {
  label: string;                // 'NET MV'
  format: 'ccy' | 'signed-ccy' | 'count' | 'pct';
  source: string;               // '/v_book_totals' — the view this reads from
  column: string;               // 'market_value'
  caption?: string;             // 'market_value · sum'
}
```

- [ ] **Step 2: Write `chapters.ts`**

```ts
/**
 * The eight chapters of the Atlas. Order matters — drives the
 * stations rail layout and the `1`–`8` keyboard shortcuts.
 */
import type { ChapterMeta } from './types';

export const CHAPTERS: readonly ChapterMeta[] = [
  { id: 'pulse', num: '01', name: 'PULSE', kicker: 'LIVE BOOK' },
  { id: 'tape',  num: '02', name: 'TAPE',  kicker: 'FILTERED TRADE STREAM' },
  { id: 'lens',  num: '03', name: 'LENS',  kicker: 'CROSS-ASSET PIVOT' },
  { id: 'heat',  num: '04', name: 'HEAT',  kicker: 'SECTOR × REGION' },
  { id: 'view',  num: '05', name: 'VIEW',  kicker: 'MATERIALIZED VIEW' },
  { id: 'join',  num: '06', name: 'JOIN',  kicker: 'TRADES × POSITIONS' },
  { id: 'slip',  num: '07', name: 'SLIP',  kicker: 'SLIPPAGE AGGREGATION' },
  { id: 'query', num: '08', name: 'QUERY', kicker: 'AD-HOC SQL' },
] as const;

export function chapterById(id: string): ChapterMeta | undefined {
  return CHAPTERS.find((c) => c.id === id);
}
```

- [ ] **Step 3: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add clients/examples-web/src/atlas/types.ts clients/examples-web/src/atlas/chapters.ts
git commit -m "feat(atlas): shared types + chapter meta

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `<AppShell>` — top wordmark + connection summary

**Files:**
- Create: `clients/examples-web/src/atlas/components/AppShell.tsx`

- [ ] **Step 1: Write the component**

```tsx
/**
 * AppShell — the topmost band: cq · atlas wordmark on the left,
 * connection summary on the right. Sits above the stations rail.
 */
import type { ReactNode } from 'react';

interface AppShellProps {
  /** e.g. 'ws://127.0.0.1:9008'. Falls back to a sensible default. */
  connection?: string;
  /** Optional right-side hint, e.g. '40,000 / 340,130'. */
  hint?: string;
  /** The rest of the page (stations + chapter content). */
  children: ReactNode;
}

export function AppShell({ connection = 'ws://127.0.0.1:9008', hint, children }: AppShellProps) {
  return (
    <div className="atlas-root atlas-app" style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      <header
        style={{
          position: 'relative',
          zIndex: 1,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '14px 24px',
          borderBottom: '1px solid var(--atlas-rule)',
          fontSize: 11,
          letterSpacing: '.22em',
        }}
      >
        <div>
          <span style={{ color: 'var(--atlas-amber)', fontWeight: 700 }}>cq</span> · atlas
        </div>
        <div style={{ color: 'var(--atlas-fg-dim)', fontSize: 10 }}>
          cqserver · {connection}
          {hint ? ` · ${hint}` : null}
        </div>
      </header>
      {children}
    </div>
  );
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/components/AppShell.tsx
git commit -m "feat(atlas): AppShell — wordmark + connection summary

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: `<StationsNav>` — horizontal stations rail + keyboard 1–8

**Files:**
- Create: `clients/examples-web/src/atlas/components/StationsNav.tsx`

- [ ] **Step 1: Write the component**

```tsx
import { useEffect } from 'react';
import { CHAPTERS } from '../chapters';
import type { ChapterId } from '../types';

interface StationsNavProps {
  active: ChapterId;
  onChange: (id: ChapterId) => void;
}

export function StationsNav({ active, onChange }: StationsNavProps) {
  // Keyboard 1–8 jump to the corresponding chapter.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      const target = e.target as HTMLElement | null;
      if (target && (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.isContentEditable)) return;
      const n = Number(e.key);
      if (Number.isInteger(n) && n >= 1 && n <= CHAPTERS.length) {
        e.preventDefault();
        onChange(CHAPTERS[n - 1]!.id);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onChange]);

  return (
    <nav
      style={{
        position: 'relative',
        zIndex: 1,
        display: 'flex',
        padding: '0 18px',
        borderBottom: '1px solid var(--atlas-rule)',
        fontSize: 10.5,
      }}
    >
      {CHAPTERS.map((c, i) => {
        const isActive = c.id === active;
        return (
          <button
            key={c.id}
            onClick={() => onChange(c.id)}
            style={{
              all: 'unset',
              cursor: 'pointer',
              padding: '14px 16px 10px',
              position: 'relative',
              display: 'inline-flex',
              alignItems: 'baseline',
              gap: 8,
              opacity: isActive ? 1 : 0.5,
              transition: 'opacity .15s',
            }}
            onMouseEnter={(e) => {
              if (!isActive) (e.currentTarget as HTMLElement).style.opacity = '0.85';
            }}
            onMouseLeave={(e) => {
              if (!isActive) (e.currentTarget as HTMLElement).style.opacity = '0.5';
            }}
          >
            {i > 0 ? (
              <span
                aria-hidden
                style={{ position: 'absolute', left: -7, top: '50%', transform: 'translateY(-50%)', opacity: 0.25 }}
              >
                ─
              </span>
            ) : null}
            <span style={{ fontSize: 9, letterSpacing: '.22em', opacity: 0.65 }}>{c.num}</span>
            <span
              style={{
                fontSize: 12,
                letterSpacing: '.12em',
                fontWeight: 500,
                color: isActive ? 'var(--atlas-amber)' : undefined,
              }}
            >
              {c.name}
            </span>
            {isActive ? (
              <span
                aria-hidden
                style={{
                  position: 'absolute',
                  left: 14,
                  right: 14,
                  bottom: -1,
                  height: 2,
                  background: 'var(--atlas-amber)',
                  boxShadow: '0 0 10px var(--atlas-amber)',
                }}
              />
            ) : null}
          </button>
        );
      })}
    </nav>
  );
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/components/StationsNav.tsx
git commit -m "feat(atlas): StationsNav — horizontal rail + 1–8 shortcuts

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: `<ChapterHead>` — eyebrow + amber title + sub + hero metric

**Files:**
- Create: `clients/examples-web/src/atlas/components/ChapterHead.tsx`

- [ ] **Step 1: Write the component**

```tsx
import type { ReactNode } from 'react';

interface ChapterHeadProps {
  /** e.g. 'CHAPTER 01 — LIVE BOOK' (already uppercased). */
  kicker: string;
  /** The amber title — lowercase, mono, large. */
  title: string;
  /** One-line description. */
  sub: string;
  /** Right-aligned hero metric, e.g. <Hero label="UNREALISED PnL" value="+$3.21M" detail="vs prev close" /> */
  hero?: ReactNode;
}

export function ChapterHead({ kicker, title, sub, hero }: ChapterHeadProps) {
  return (
    <section
      style={{
        position: 'relative',
        zIndex: 1,
        padding: '22px 24px 14px',
        display: 'grid',
        gridTemplateColumns: '1.4fr 1fr',
        gap: 24,
        alignItems: 'end',
      }}
    >
      <div>
        <div style={{ fontSize: 9.5, letterSpacing: '.26em', color: 'var(--atlas-fg-dim)' }}>{kicker}</div>
        <h1
          style={{
            margin: '6px 0 0',
            fontSize: 40,
            fontWeight: 700,
            letterSpacing: '-.02em',
            lineHeight: 1,
            color: 'var(--atlas-amber)',
          }}
        >
          {title}
        </h1>
        <p
          style={{
            margin: '8px 0 0',
            fontSize: 12,
            color: 'var(--atlas-fg-dim)',
            maxWidth: 460,
            lineHeight: 1.55,
          }}
        >
          {sub}
        </p>
      </div>
      <div style={{ textAlign: 'right', paddingBottom: 4 }}>{hero}</div>
    </section>
  );
}

interface HeroMetricProps {
  label: string;
  value: string;
  detail?: string;
}

export function HeroMetric({ label, value, detail }: HeroMetricProps) {
  return (
    <>
      <div style={{ fontSize: 9, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)' }}>{label}</div>
      <div
        style={{
          fontSize: 48,
          fontWeight: 700,
          color: 'var(--atlas-amber)',
          fontFeatureSettings: '"tnum"',
          lineHeight: 1,
          marginTop: 8,
          textShadow: '0 0 28px rgba(244, 165, 43, .25)',
        }}
      >
        {value}
      </div>
      {detail ? <div style={{ fontSize: 10.5, color: 'var(--atlas-fg-dim)', marginTop: 6 }}>{detail}</div> : null}
    </>
  );
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/components/ChapterHead.tsx
git commit -m "feat(atlas): ChapterHead + HeroMetric

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: `<ChipPicker>` — compact mono dropdown

**Files:**
- Create: `clients/examples-web/src/atlas/components/ChipPicker.tsx`

A simple dropdown anchored under its trigger; closes on outside click or Escape.

- [ ] **Step 1: Write the component**

```tsx
import { useEffect, useRef } from 'react';

interface ChipPickerProps {
  open: boolean;
  options: string[];
  selected?: string;
  onSelect: (value: string) => void;
  onClose: () => void;
}

export function ChipPicker({ open, options, selected, onSelect, onClose }: ChipPickerProps) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('mousedown', onDown);
    window.addEventListener('keydown', onKey);
    return () => {
      window.removeEventListener('mousedown', onDown);
      window.removeEventListener('keydown', onKey);
    };
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div
      ref={ref}
      style={{
        position: 'absolute',
        top: 'calc(100% + 4px)',
        left: 0,
        zIndex: 20,
        minWidth: 180,
        maxHeight: 280,
        overflowY: 'auto',
        background: '#14141a',
        border: '1px solid var(--atlas-rule)',
        borderRadius: 6,
        boxShadow: '0 16px 40px -10px rgba(0,0,0,.6)',
        padding: 4,
      }}
    >
      {options.length === 0 ? (
        <div style={{ padding: '8px 10px', fontSize: 10.5, color: 'var(--atlas-fg-faint)' }}>(no values)</div>
      ) : (
        options.map((opt) => {
          const isSelected = opt === selected;
          return (
            <button
              key={opt}
              onClick={() => {
                onSelect(opt);
                onClose();
              }}
              style={{
                all: 'unset',
                display: 'block',
                width: '100%',
                padding: '6px 10px',
                fontFamily: 'var(--atlas-font)',
                fontSize: 11,
                color: isSelected ? 'var(--atlas-amber)' : 'var(--atlas-fg)',
                background: isSelected ? 'var(--atlas-amber-soft)' : 'transparent',
                borderRadius: 4,
                cursor: 'pointer',
              }}
              onMouseEnter={(e) => {
                if (!isSelected) (e.currentTarget as HTMLElement).style.background = 'rgba(255,255,255,.04)';
              }}
              onMouseLeave={(e) => {
                if (!isSelected) (e.currentTarget as HTMLElement).style.background = 'transparent';
              }}
            >
              {opt}
            </button>
          );
        })
      )}
    </div>
  );
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/components/ChipPicker.tsx
git commit -m "feat(atlas): ChipPicker — compact mono dropdown

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: `<FilterRail>` — chip rail

**Files:**
- Create: `clients/examples-web/src/atlas/components/FilterRail.tsx`

Renders the chips and the active-subscription summary. Phase 1 uses static option lists from the chapter's preview data; Phase 2 will wire `chip.source` to live view subscriptions.

- [ ] **Step 1: Write the component**

```tsx
import { useState } from 'react';
import type { ChipSpec } from '../types';
import { ChipPicker } from './ChipPicker';

interface FilterRailProps {
  chips: ChipSpec[];
  /** chip.key → currently selected value (undefined / 'All' = no constraint). */
  state: Record<string, string | undefined>;
  /** chip.key → option list (Phase 2 will populate from chip.source). */
  options: Record<string, string[]>;
  onChange: (next: Record<string, string | undefined>) => void;
  /** Human-readable summary of the active subscription, e.g. "book_name = 'RATES-US'". */
  subscriptionSummary?: string;
}

export function FilterRail({ chips, state, options, onChange, subscriptionSummary }: FilterRailProps) {
  const [openKey, setOpenKey] = useState<string | null>(null);

  return (
    <div
      style={{
        position: 'relative',
        zIndex: 1,
        padding: '14px 24px',
        borderTop: '1px solid var(--atlas-rule)',
        borderBottom: '1px solid var(--atlas-rule)',
        display: 'flex',
        alignItems: 'center',
        gap: 10,
        fontSize: 10.5,
      }}
    >
      <div
        style={{
          color: 'var(--atlas-fg-faint)',
          letterSpacing: '.22em',
          textTransform: 'uppercase',
          fontSize: 9.5,
          marginRight: 4,
        }}
      >
        FILTER
      </div>
      {chips.map((c) => {
        const value = state[c.key];
        const isActive = value != null && value !== '' && value !== 'All';
        return (
          <div key={c.key} style={{ position: 'relative' }}>
            <button
              onClick={() => setOpenKey(openKey === c.key ? null : c.key)}
              style={{
                all: 'unset',
                cursor: 'pointer',
                display: 'inline-flex',
                alignItems: 'center',
                gap: 8,
                padding: '5px 10px 5px 8px',
                border: `1px solid ${isActive ? 'rgba(244,165,43,.65)' : 'var(--atlas-rule)'}`,
                background: isActive ? 'var(--atlas-amber-soft)' : 'transparent',
                borderRadius: 999,
                fontSize: 10.5,
                color: 'var(--atlas-fg)',
              }}
            >
              <span
                style={{
                  color: 'var(--atlas-fg-faint)',
                  fontSize: 9.5,
                  letterSpacing: '.14em',
                  textTransform: 'uppercase',
                }}
              >
                {c.key}
              </span>
              <span style={{ fontWeight: 600, color: isActive ? 'var(--atlas-amber)' : undefined }}>
                {value ?? 'All'}
              </span>
              {isActive ? (
                <span
                  aria-label="clear"
                  onClick={(e) => {
                    e.stopPropagation();
                    onChange({ ...state, [c.key]: undefined });
                  }}
                  style={{ color: 'var(--atlas-fg-faint)', fontSize: 10, cursor: 'pointer' }}
                >
                  ×
                </span>
              ) : (
                <span style={{ color: 'var(--atlas-fg-faint)', fontSize: 10 }}>▾</span>
              )}
            </button>
            <ChipPicker
              open={openKey === c.key}
              options={options[c.key] ?? []}
              selected={value}
              onSelect={(v) => onChange({ ...state, [c.key]: v })}
              onClose={() => setOpenKey(null)}
            />
          </div>
        );
      })}
      <div style={{ flex: 1 }} />
      {subscriptionSummary ? (
        <div
          style={{
            fontSize: 9.5,
            letterSpacing: '.18em',
            color: 'var(--atlas-fg-dim)',
            display: 'flex',
            alignItems: 'center',
            gap: 8,
          }}
        >
          <span className="atlas-live-dot" />
          SUBSCRIBED · {subscriptionSummary}
        </div>
      ) : null}
    </div>
  );
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/components/FilterRail.tsx
git commit -m "feat(atlas): FilterRail — chip rail + subscription summary

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: `<KpiStrip>` — 6-slot grid-lined KPI band

**Files:**
- Create: `clients/examples-web/src/atlas/components/KpiStrip.tsx`

- [ ] **Step 1: Write the component**

```tsx
export interface Kpi {
  label: string;
  value: string;
  caption?: string;
  /** Apply amber colour to the value when true (default false). */
  emphasis?: boolean;
}

interface KpiStripProps {
  kpis: readonly Kpi[];
}

export function KpiStrip({ kpis }: KpiStripProps) {
  return (
    <div
      style={{
        position: 'relative',
        zIndex: 1,
        display: 'grid',
        gridTemplateColumns: `repeat(${kpis.length}, 1fr)`,
        borderBottom: '1px solid var(--atlas-rule)',
      }}
    >
      {kpis.map((k, i) => (
        <div
          key={k.label}
          style={{
            padding: '14px 16px',
            borderRight: i < kpis.length - 1 ? '1px solid var(--atlas-rule)' : 'none',
          }}
        >
          <div style={{ fontSize: 9, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)' }}>{k.label}</div>
          <div
            style={{
              fontSize: 22,
              fontWeight: 600,
              marginTop: 6,
              fontFeatureSettings: '"tnum"',
              color: k.emphasis ? 'var(--atlas-amber)' : 'var(--atlas-fg)',
            }}
          >
            {k.value}
          </div>
          {k.caption ? (
            <div style={{ fontSize: 10, color: 'var(--atlas-fg-faint)', marginTop: 4 }}>{k.caption}</div>
          ) : null}
        </div>
      ))}
    </div>
  );
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/components/KpiStrip.tsx
git commit -m "feat(atlas): KpiStrip — 6-slot KPI band

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: `<DataTable>` — AG-Grid wrapper using the Atlas theme

**Files:**
- Create: `clients/examples-web/src/atlas/components/DataTable.tsx`

Phase 1 just renders rowData/colDefs with the Atlas theme. Phase 2 will switch this to consume worker port chunks via `applyTransactionAsync`. Keep the prop surface minimal so the migration is trivial.

- [ ] **Step 1: Write the component**

```tsx
import { useMemo } from 'react';
import { AgGridReact } from 'ag-grid-react';
import {
  AllCommunityModule,
  ModuleRegistry,
  type ColDef,
} from 'ag-grid-community';
import { AllEnterpriseModule } from 'ag-grid-enterprise';
import { getAtlasGridTheme } from '../aggrid';

ModuleRegistry.registerModules([AllCommunityModule, AllEnterpriseModule]);

interface DataTableProps<T extends Record<string, unknown>> {
  /** Title strip above the grid, e.g. 'POSITIONS · 23 of 207 cols'. */
  title?: string;
  /** Right-aligned status, e.g. '4,827 rows · ticking'. */
  status?: string;
  rows: T[];
  colDefs: ColDef[];
  getRowId?: (row: T) => string;
}

export function DataTable<T extends Record<string, unknown>>({
  title,
  status,
  rows,
  colDefs,
  getRowId,
}: DataTableProps<T>) {
  const theme = useMemo(() => getAtlasGridTheme(), []);
  const agGetRowId = useMemo(
    () => (getRowId ? ({ data }: { data: T }) => getRowId(data) : undefined),
    [getRowId],
  );

  return (
    <div
      style={{
        position: 'relative',
        zIndex: 1,
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        minHeight: 0,
        padding: '18px 24px 0',
      }}
    >
      {(title || status) && (
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            paddingBottom: 12,
          }}
        >
          {title ? (
            <div style={{ fontSize: 11, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)' }}>{title}</div>
          ) : (
            <div />
          )}
          {status ? <div style={{ fontSize: 10, color: 'var(--atlas-fg-faint)' }}>{status}</div> : null}
        </div>
      )}
      <div style={{ flex: 1, minHeight: 280 }}>
        <AgGridReact<T>
          theme={theme}
          rowData={rows}
          columnDefs={colDefs}
          rowHeight={26}
          headerHeight={28}
          animateRows={false}
          getRowId={agGetRowId}
        />
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -4`
Expected: typecheck clean, build succeeds (AG-Grid bundle is the same chunk as before).

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/components/DataTable.tsx
git commit -m "feat(atlas): DataTable — AG-Grid wrapper with atlas theme

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: `<Footer>` — live status + keyboard hints

**Files:**
- Create: `clients/examples-web/src/atlas/components/Footer.tsx`

- [ ] **Step 1: Write the component**

```tsx
interface FooterProps {
  /** 'LIVE' or 'CONNECTING' etc. */
  status?: string;
  cadence?: string;     // '250ms cadence'
  tickStats?: string;   // '4,820 ticks · 0 drops'
}

export function Footer({ status = 'LIVE', cadence, tickStats }: FooterProps) {
  return (
    <div
      style={{
        position: 'relative',
        zIndex: 1,
        display: 'flex',
        justifyContent: 'space-between',
        padding: '10px 24px',
        borderTop: '1px solid var(--atlas-rule)',
        fontSize: 9.5,
        letterSpacing: '.18em',
        color: 'var(--atlas-fg-dim)',
      }}
    >
      <div style={{ display: 'flex', gap: 24 }}>
        <span>
          <span className="atlas-live-dot" style={{ marginRight: 8 }} />
          {status}
        </span>
        {cadence ? <span>{cadence}</span> : null}
        {tickStats ? <span>{tickStats}</span> : null}
      </div>
      <div style={{ display: 'flex', gap: 18 }}>
        <span>
          ⌘ <KeyBadge>K</KeyBadge> palette
        </span>
        <span>
          ⌘ <KeyBadge>F</KeyBadge> filter
        </span>
      </div>
    </div>
  );
}

function KeyBadge({ children }: { children: string }) {
  return (
    <span
      style={{
        background: 'rgba(255,255,255,.06)',
        padding: '1px 6px',
        borderRadius: 4,
        letterSpacing: '.04em',
      }}
    >
      {children}
    </span>
  );
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/components/Footer.tsx
git commit -m "feat(atlas): Footer — live status + keyboard hints

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 12: Placeholder data + `<PulsePreview>`

**Files:**
- Create: `clients/examples-web/src/atlas/preview/placeholderData.ts`
- Create: `clients/examples-web/src/atlas/preview/PulsePreview.tsx`

Composes every component built so far into the locked Pulse layout with static data. Phase 2 will swap the placeholder hook for the worker.

- [ ] **Step 1: Write the placeholder data**

```ts
// clients/examples-web/src/atlas/preview/placeholderData.ts
import type { ColDef } from 'ag-grid-community';
import type { Kpi } from '../components/KpiStrip';

export interface PulseRow {
  position_id: string;
  issuer: string;
  market_value: number;
  day_pnl: number;
  var_1d: number;
  util_pct: number;
  status: 'OK' | 'BREACH';
}

const ISSUERS = [
  'US Treasury 10Y', 'FNMA 30Y MBS', 'JPMC Sr Unsec', 'Apple 2031', 'Hertz HY 6.25',
  'Ford Mtr Co', 'UST Bill 3M', 'Microsoft 2029', 'Verizon 5.0', 'Bank of America Sub',
  'Caterpillar 2027', 'Comcast Cable 6.0', 'Goldman 5.5', 'Pfizer 2030', 'Intel 2032',
];

export function makePulseRows(n = 80): PulseRow[] {
  const rows: PulseRow[] = [];
  for (let i = 0; i < n; i++) {
    const mv = 0.5 + ((i * 173) % 1000) / 60;
    const pnl = (((i * 211) % 200) - 100) * 500;
    const util = 20 + ((i * 41) % 90);
    rows.push({
      position_id: `P-${String(481 + i).padStart(5, '0')}`,
      issuer: ISSUERS[i % ISSUERS.length]!,
      market_value: mv,
      day_pnl: pnl,
      var_1d: 1000 + ((i * 79) % 18000),
      util_pct: Math.round(util),
      status: util > 100 ? 'BREACH' : 'OK',
    });
  }
  return rows;
}

const fmtCcy = (n: number) =>
  n.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 });
const fmtSigned = (n: number) => (n >= 0 ? '+' : '−') + fmtCcy(Math.abs(n));
const fmtK = (n: number) => `${(n / 1000).toFixed(1)}k`;

export const PULSE_COL_DEFS: ColDef<PulseRow>[] = [
  {
    field: 'position_id',
    headerName: 'position_id',
    width: 110,
    cellStyle: { color: '#f4a52b' },
  },
  { field: 'issuer', headerName: 'issuer', flex: 1 },
  {
    field: 'market_value',
    headerName: 'market_value',
    width: 130,
    type: 'numericColumn',
    valueFormatter: (p) => `${(p.value as number).toFixed(2)}M`,
    cellClass: 'ag-right-aligned-cell',
  },
  {
    field: 'day_pnl',
    headerName: 'day_pnl',
    width: 120,
    type: 'numericColumn',
    valueFormatter: (p) => fmtSigned(p.value as number),
    cellClassRules: {
      'ag-pnl-pos': (p) => (p.value as number) >= 0,
      'ag-pnl-neg': (p) => (p.value as number) < 0,
    },
  },
  {
    field: 'var_1d',
    headerName: 'var_1d',
    width: 100,
    type: 'numericColumn',
    valueFormatter: (p) => fmtK(p.value as number),
  },
  {
    field: 'util_pct',
    headerName: 'util_%',
    width: 90,
    type: 'numericColumn',
    valueFormatter: (p) => `${p.value}`,
  },
  {
    field: 'status',
    headerName: 'status',
    width: 100,
    cellStyle: (p) => ({
      color: p.value === 'BREACH' ? '#ff6062' : '#f4a52b',
      letterSpacing: '.1em',
    }),
  },
];

export const PULSE_KPIS: readonly Kpi[] = [
  { label: 'NET MV', value: '$82.1M', caption: 'market_value · sum', emphasis: true },
  { label: 'EXPOSURE', value: '$248.6M', caption: 'gross · sum' },
  { label: 'DAY PnL', value: '+$0.41M', caption: 'today', emphasis: true },
  { label: 'YTD PnL', value: '+$8.92M', caption: 'inception', emphasis: true },
  { label: 'VaR (1d)', value: '$0.96M', caption: '95% confidence' },
  { label: 'POSITIONS', value: '4,827', caption: 'in scope' },
];

export const BOOK_OPTIONS = ['RATES-US', 'CREDIT-IG', 'EQTY-VOL', 'FX-MACRO'];
export const SECTOR_OPTIONS = ['All', 'Technology', 'Financials', 'Energy', 'Industrials'];
export const COMPLIANCE_OPTIONS = ['All', 'OK', 'BREACH'];
```

- [ ] **Step 2: Write `PulsePreview.tsx`**

```tsx
// clients/examples-web/src/atlas/preview/PulsePreview.tsx
import { useMemo, useState } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import type { ChipSpec } from '../types';
import {
  makePulseRows,
  PULSE_COL_DEFS,
  PULSE_KPIS,
  BOOK_OPTIONS,
  SECTOR_OPTIONS,
  COMPLIANCE_OPTIONS,
  type PulseRow,
} from './placeholderData';

const PULSE_CHIPS: readonly ChipSpec[] = [
  { key: 'BOOK', column: 'book_name', default: 'RATES-US' },
  { key: 'SECTOR', column: 'issuer_sector' },
  { key: 'COMPLIANCE', column: 'compliance_status' },
];

export function PulsePreview() {
  const [scope, setScope] = useState<Record<string, string | undefined>>({ BOOK: 'RATES-US' });
  const rows = useMemo(() => makePulseRows(80), []);
  const chipOptions = useMemo(
    () => ({ BOOK: BOOK_OPTIONS, SECTOR: SECTOR_OPTIONS, COMPLIANCE: COMPLIANCE_OPTIONS }),
    [],
  );
  const summary = scope.BOOK ? `book_name = '${scope.BOOK}'` : '(unfiltered — would stream ~40k rows)';
  const positionRowId = (r: PulseRow): string => r.position_id;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 01 — LIVE BOOK"
        title="pulse."
        sub="A continuous read of the firm’s book — KPIs, sector ladder, book contribution, breaches. Every figure server-computed by a materialized view; nothing aggregated in the browser."
        hero={<HeroMetric label="UNREALISED PnL" value="+$3.21M" detail="vs prev close · 4,820 ticks" />}
      />
      <FilterRail
        chips={[...PULSE_CHIPS]}
        state={scope}
        options={chipOptions}
        onChange={setScope}
        subscriptionSummary={summary}
      />
      <KpiStrip kpis={PULSE_KPIS} />
      <DataTable<PulseRow>
        title="POSITIONS · 23 of 207 cols"
        status={`${rows.length.toLocaleString()} rows · placeholder data (Phase 1)`}
        rows={rows}
        colDefs={PULSE_COL_DEFS}
        getRowId={positionRowId}
      />
    </>
  );
}
```

- [ ] **Step 3: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add clients/examples-web/src/atlas/preview/placeholderData.ts clients/examples-web/src/atlas/preview/PulsePreview.tsx
git commit -m "feat(atlas): Pulse preview with placeholder data

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 13: `<AtlasPreviewApp>` + hash route + App.tsx guard

**Files:**
- Create: `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx`
- Modify: `clients/examples-web/src/App.tsx` — hash guard at the top of the component.

- [ ] **Step 1: Write `AtlasPreviewApp.tsx`**

```tsx
import { useState } from 'react';
import { AppShell } from '../components/AppShell';
import { StationsNav } from '../components/StationsNav';
import { Footer } from '../components/Footer';
import { PulsePreview } from './PulsePreview';
import type { ChapterId } from '../types';

/** Stub for any chapter that hasn't been migrated yet (Phase 1 = Pulse only). */
function ComingSoon({ id }: { id: ChapterId }) {
  return (
    <div
      style={{
        flex: 1,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: 'var(--atlas-fg-faint)',
        fontSize: 11,
        letterSpacing: '.3em',
      }}
    >
      {id.toUpperCase()} · arriving in a later phase
    </div>
  );
}

export function AtlasPreviewApp() {
  const [active, setActive] = useState<ChapterId>('pulse');

  return (
    <AppShell hint="phase 1 preview · placeholder data">
      <StationsNav active={active} onChange={setActive} />
      <main style={{ position: 'relative', zIndex: 1, flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        {active === 'pulse' ? <PulsePreview /> : <ComingSoon id={active} />}
      </main>
      <Footer cadence="250ms cadence" tickStats="placeholder" />
    </AppShell>
  );
}
```

- [ ] **Step 2: Wire the hash route into `App.tsx`**

Open `clients/examples-web/src/App.tsx`. At the very top of the `App()` function body, ABOVE the existing `const [active, setActive] = useState<ExampleId>('live-pnl');` line, add:

```tsx
  // Phase 1 redesign preview — opt-in via #atlas hash. Existing app
  // continues to render at every other URL.
  if (typeof window !== 'undefined' && window.location.hash === '#atlas') {
    const { AtlasPreviewApp } = require('@/atlas/preview/AtlasPreviewApp') as typeof import('@/atlas/preview/AtlasPreviewApp');
    return <AtlasPreviewApp />;
  }
```

Alternative if `require` isn't available in the project's ESM config: import statically at the top of the file (`import { AtlasPreviewApp } from '@/atlas/preview/AtlasPreviewApp';`) and use the guard with a direct return. Use whichever the project's existing import style prefers — both are typecheck-equivalent. Confirm by reading the file's existing import list before deciding.

- [ ] **Step 3: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -4`
Expected: typecheck clean, build succeeds.

- [ ] **Step 4: Commit**

```bash
git add clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx clients/examples-web/src/App.tsx
git commit -m "feat(atlas): AtlasPreviewApp + #atlas hash route

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 14: Manual verification

**Files:** none.

- [ ] **Step 1: Launch the dev server**

If the demo isn't already up, run from the repo root:
```bash
./stop-demo.sh 2>/dev/null
POSITIONS=40000 TRADES_PER_POSITION=8 TICK_PCT=0.01 TICK_MS=250 ./start-atlas-demo.sh
```
(The Atlas preview doesn't need the publisher running, but the dev server is part of the demo orchestration. If only the Vite dev server is wanted, `cd clients/examples-web && npm run dev`.)

- [ ] **Step 2: Open the preview**

Browse to `http://localhost:5175/#atlas`. Expected to see:
- Top bar: `cq · atlas` (with `cq` in amber) on the left, connection summary on the right.
- Stations rail: `01 PULSE — 02 TAPE — … — 08 QUERY`. Click each chapter or press 1–8; only **PULSE** renders content; all others show the `ARRIVING IN A LATER PHASE` stub.
- Pulse chapter: amber `pulse.` headline, hero metric "+$3.21M" on the right, filter chip rail (Book = RATES-US active), 6-column KPI strip, mono data table with ~80 placeholder rows including one BREACH row in red.
- Footer: pulsing amber dot + status, ⌘K / ⌘F hints on the right.

- [ ] **Step 3: Verify the existing app is untouched**

Browse to `http://localhost:5175/` (no hash). Expected: the existing 8-tab dock-based demo renders exactly as before — no styling regression, no layout shift, no console errors specific to Atlas.

- [ ] **Step 4: Verify keyboard shortcuts**

In `#atlas`, press `1` through `8` — the active station should update immediately. Press a letter key — nothing should happen. Click a chip; the picker dropdown should open and close on outside click / Escape.

If any of these fail, fix the relevant earlier task before proceeding to Phase 2.

---

## Self-Review (completed by author)

**Spec coverage** (against the master spec's Phase 1 row):
- New tokens (`tokens-atlas.css` → built as `src/atlas/tokens.css`) — Task 1. ✅
- JetBrains Mono Variable only — Task 1 (font-family token + `@fontsource-variable` import). ✅
- AG-Grid v33+ theme rebuilt — Task 2. ✅
- Base components: `<AppShell>`, `<StationsNav>`, `<ChapterHead>`, `<FilterRail>`, `<KpiStrip>`, `<DataTable>`, `<Footer>`, `<ChipPicker>` — Tasks 4–11. ✅
- Smoke‑demonstrable via `/atlas` (using `#atlas` hash, which has zero routing dep) — Task 13. ✅
- "Built in parallel under `src/atlas/`" — every file lives under that path; no existing module is restructured. ✅
- No data‑layer changes — confirmed; the preview uses static placeholder rows. ✅
- No chapter migration — confirmed; the existing `/` app at any non‑`#atlas` URL renders the old chapters unchanged. ✅

**Placeholder scan:** no TBD/TODO/"handle errors"/"add validation". The fallback note in Task 13 Step 2 (`require` vs static import) gives both concrete options, not vague guidance. ✅

**Type/name consistency:** `ChapterId`/`ChapterMeta`/`ChipSpec`/`ChapterScope`/`KpiSpec` defined in Task 3 are consumed identically in Tasks 4–12. `Kpi` (`KpiStrip`'s row type) is the only locally‑named type outside `types.ts`; it's referenced from `KpiStrip.tsx` (define) and `placeholderData.ts` (consume) consistently. `getAtlasGridTheme` (Task 2) is the only theme symbol referenced (Task 10). The `cellClassRules` keys `ag-pnl-pos` / `ag-pnl-neg` referenced in `placeholderData.ts` are styled by AG‑Grid's own param colour overrides through the theme (the rule names just guard which cells get the colour); no separate CSS class is missing. ✅

**Scope:** focused, parallel, no behaviour change to the existing app. Ready to plan Phase 2 once this lands and you've eyeballed the preview.
