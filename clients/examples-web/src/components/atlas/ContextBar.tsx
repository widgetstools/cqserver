import type { ExampleEntry } from '@/examples/registry';
import { FEATURE_META, headlineFeature, orderFeatures } from '@/lib/features';

interface ContextBarProps {
  example: ExampleEntry;
}

/**
 * ContextBar — example header strip rendered above the dock canvas.
 *
 * Anatomy (left → right):
 *
 *   LEFT (lede)
 *     Eyebrow:   EX.04  ·  PIVOT          mono · teal · all-caps
 *     Title:     Ticking Heatmap — Sector × Region   Inter 15px / 600
 *
 *   MIDDLE (taxonomy)
 *     One pill per feature primitive (Stream / Join / View / Pivot /
 *     Agg / Filter / Window). Each pill is its taxonomy color; the
 *     letter glyph sits in a circle on the left.
 *
 *   RIGHT (telemetry)
 *     Live pulse + path chip. In Phase 2 the throughput / rows / ticks
 *     readouts are stubbed to "—" because the global cq-store mirror is
 *     gone; Phase 3 wires per-subscription stats through the chapter
 *     scope.
 */
export function ContextBar({ example }: ContextBarProps) {
  const features = orderFeatures(example.features);
  const headline = headlineFeature(example.features);
  const headlineMeta = FEATURE_META[headline];
  const isLive = example.features.includes('stream');

  return (
    <div className="atlas-context">
      <div className="atlas-context-lede">
        <div className="atlas-context-eyebrow">
          <span className="atlas-context-serial">EX.{example.serial}</span>
          <span className="atlas-context-clause">{headlineMeta.clause}</span>
        </div>
        <h1 className="atlas-context-title">{example.title}</h1>
      </div>

      <div className="atlas-context-tags">
        {features.map((f) => {
          const meta = FEATURE_META[f];
          return (
            <span
              key={f}
              className="atlas-context-tag"
              style={{ ['--feature-color' as never]: meta.colorVar }}
              title={meta.blurb}
            >
              <span className="atlas-context-tag-glyph">{meta.glyph}</span>
              {meta.name}
            </span>
          );
        })}
      </div>

      <div className="atlas-context-right">
        {isLive && (
          <div className="atlas-context-stats">
            <div className="atlas-context-stat">
              <span className="atlas-context-stat-value atlas-context-stat-value--accent tabular-nums">
                —
              </span>
              <span className="atlas-context-stat-label">rows/sec</span>
            </div>
            <div className="atlas-context-stat">
              <span className="atlas-context-stat-value tabular-nums">—</span>
              <span className="atlas-context-stat-label">ticks</span>
            </div>
            <div className="atlas-context-stat">
              <span className="atlas-context-stat-value tabular-nums">—</span>
              <span className="atlas-context-stat-label">rows</span>
            </div>
          </div>
        )}
        {isLive && <span className="atlas-live-pill">LIVE</span>}
        <span className="atlas-context-path">/{example.id}</span>
      </div>
    </div>
  );
}
