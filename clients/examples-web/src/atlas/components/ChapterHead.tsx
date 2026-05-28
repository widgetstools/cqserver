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
