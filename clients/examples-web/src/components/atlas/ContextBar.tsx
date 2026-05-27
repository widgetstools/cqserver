import { useEffect, useRef, useState } from 'react';
import type { ExampleEntry } from '@/examples/registry';
import { FEATURE_META, headlineFeature, orderFeatures } from '@/lib/features';
import { cqStore, useTickCount } from '@/lib/cq-store';

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
 *     Live pulse + tick-rate readout (deltas/sec, ticks total). Hooked
 *     directly into the cq-store so the header acts as a system meter
 *     for whichever topic is most central to the visible example.
 *     Trailing path chip: `/heatmap`.
 */
export function ContextBar({ example }: ContextBarProps) {
  const features = orderFeatures(example.features);
  const headline = headlineFeature(example.features);
  const headlineMeta = FEATURE_META[headline];
  const isLive = example.features.includes('stream');

  // Pick the topic this example leans on hardest so the throughput
  // readout reflects what's actually visible. Trade-centric examples
  // surface trade deltas; everything else uses positions.
  const tradeCentric = example.id === 'trade-blotter' || example.id === 'slippage-agg';
  const topic = tradeCentric ? 'trades' : 'positions';
  const ticks = useTickCount(topic);
  const rate = useTickRate(topic, ticks);

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
                {Math.round(rate).toLocaleString()}
              </span>
              <span className="atlas-context-stat-label">{topic}/sec</span>
            </div>
            <div className="atlas-context-stat">
              <span className="atlas-context-stat-value tabular-nums">
                {ticks.toLocaleString()}
              </span>
              <span className="atlas-context-stat-label">ticks</span>
            </div>
            <div className="atlas-context-stat">
              <span className="atlas-context-stat-value tabular-nums">
                {cqStore[topic].getSize().toLocaleString()}
              </span>
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

/**
 * Sample tick counts every second to derive a deltas/sec rate. We hold
 * the previous reading + timestamp in a ref so React renders don't
 * reset the smoothing window. The hook still re-runs on every parent
 * render driven by `useTickCount`, but the rate maths is cheap and the
 * UI only re-renders the chrome strip — not the grid — so it's free.
 */
function useTickRate(topic: 'positions' | 'trades', ticks: number): number {
  const last = useRef({ t: Date.now(), ticks });
  const [rate, setRate] = useState(0);
  useEffect(() => {
    const id = setInterval(() => {
      const now = Date.now();
      const dt = (now - last.current.t) / 1000;
      if (dt <= 0) return;
      const observed = cqStore[topic].getTickCount();
      const dn = observed - last.current.ticks;
      setRate(dn / dt);
      last.current = { t: now, ticks: observed };
    }, 1000);
    return () => clearInterval(id);
  }, [topic]);
  return rate;
}
