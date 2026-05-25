import type { ExampleEntry } from '@/examples/registry';

interface ContextBarProps {
  example: ExampleEntry;
}

/**
 * ContextBar — the thin (32px) band above the dock canvas that names
 * the active example. Replaces the previous 120px hero rubric.
 *
 * Anatomy (left → right):
 *
 *   [4px teal accent bar]
 *   "04"   in JetBrains Mono · mint-teal · 12px
 *   "Ticking Heatmap — Sector × Region"  in Inter · 13px semibold
 *   "● join  ● pivot  ● agg  ● stream"   tiny color-coded dots
 *   [right side] "● LIVE"  pulsing pill (only if the example streams)
 *   "/heatmap"  in mono · muted · the cqserver-style URL hint
 */
export function ContextBar({ example }: ContextBarProps) {
  const isLive = example.features.includes('stream');
  return (
    <div className="atlas-context">
      <span className="atlas-context-serial">{example.serial}</span>
      <h1 className="atlas-context-title">{example.title}</h1>
      <div className="atlas-context-tags">
        {example.features.map((f) => (
          <span key={f} className="atlas-context-tag" data-kind={f}>
            {f}
          </span>
        ))}
      </div>
      <div className="atlas-context-right">
        {isLive ? <span className="atlas-live-pill">LIVE</span> : null}
        <span className="atlas-context-path">/{example.id}</span>
      </div>
    </div>
  );
}
