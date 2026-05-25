import { useEffect, useRef, useState, type ReactNode } from 'react';
import { Sparkline } from './Sparkline';
import { cn } from '@/lib/utils';

interface MetricCardProps {
  label: string;
  value: ReactNode;
  /** Optional second-line annotation, e.g. "+18 (5m)". */
  delta?: ReactNode;
  /** Trend direction for the delta tint. */
  trend?: 'up' | 'down' | 'flat';
  /** Sparkline data. Pass `undefined` to suppress the chart. */
  series?: number[];
  /** Override the sparkline color (defaults to primary). */
  sparkColor?: string;
  /** A short qualifier line beneath the label, e.g. "process RSS". */
  sub?: string;
  /** Numeric value for pulse detection. When this changes, value
   *  briefly tints up/down. */
  pulseKey?: number;
  className?: string;
}

export function MetricCard({
  label,
  value,
  delta,
  trend,
  series,
  sparkColor,
  sub,
  pulseKey,
  className,
}: MetricCardProps) {
  const [pulse, setPulse] = useState<'up' | 'down' | null>(null);
  const last = useRef<number | undefined>(undefined);

  useEffect(() => {
    if (pulseKey == null) return;
    const prev = last.current;
    last.current = pulseKey;
    if (prev == null) return;
    if (pulseKey > prev) setPulse('up');
    else if (pulseKey < prev) setPulse('down');
    const t = setTimeout(() => setPulse(null), 600);
    return () => clearTimeout(t);
  }, [pulseKey]);

  return (
    <div
      className={cn(
        'flex flex-col gap-1.5 rounded-md border border-border bg-card px-4 py-3 transition-colors',
        className,
      )}
    >
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground font-medium">
          {label}
        </span>
        {series && series.length > 0 ? (
          <Sparkline
            data={series}
            width={88}
            height={22}
            stroke={sparkColor ?? 'var(--primary)'}
          />
        ) : null}
      </div>
      <div
        className={cn(
          'font-mono tabular text-[26px] font-semibold leading-none -mt-0.5 rounded-sm transition-colors px-0.5 -mx-0.5',
          pulse === 'up' && 'pulse-up',
          pulse === 'down' && 'pulse-down',
        )}
      >
        {value}
      </div>
      {(delta || sub) && (
        <div className="flex items-baseline justify-between gap-2 mt-0.5">
          {sub ? (
            <span className="text-[11px] text-muted-foreground">{sub}</span>
          ) : (
            <span />
          )}
          {delta ? (
            <span
              className={cn(
                'text-[11px] font-mono tabular',
                trend === 'up'
                  ? 'text-ok'
                  : trend === 'down'
                  ? 'text-err'
                  : 'text-muted-foreground',
              )}
            >
              {delta}
            </span>
          ) : null}
        </div>
      )}
    </div>
  );
}
