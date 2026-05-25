import { useMemo } from 'react';
import { cn } from '@/lib/utils';

interface SparklineProps {
  data: number[];
  width?: number;
  height?: number;
  stroke?: string;
  fill?: string;
  className?: string;
  /** When provided, fixes the y-domain. Otherwise auto-scales to data. */
  domain?: [number, number];
}

export function Sparkline({
  data,
  width = 96,
  height = 24,
  stroke = 'var(--primary)',
  fill,
  className,
  domain,
}: SparklineProps) {
  const { d, area } = useMemo(() => {
    if (data.length < 2) return { d: '', area: '' };
    const lo = domain?.[0] ?? Math.min(...data);
    const hi = domain?.[1] ?? Math.max(...data);
    const span = hi - lo || 1;
    const stepX = width / (data.length - 1);

    const points = data.map((v, i) => {
      const x = i * stepX;
      // pad 1px top & bottom so the stroke isn't clipped
      const y = height - 1 - ((v - lo) / span) * (height - 2);
      return [x, y] as const;
    });

    const path = points
      .map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`)
      .join(' ');

    const areaPath = `${path} L${(width).toFixed(2)},${height} L0,${height} Z`;

    return { d: path, area: areaPath };
  }, [data, width, height, domain]);

  if (!d) {
    return (
      <svg width={width} height={height} className={className} aria-hidden="true">
        <line
          x1={0}
          y1={height / 2}
          x2={width}
          y2={height / 2}
          stroke="var(--border)"
          strokeWidth="1"
          strokeDasharray="2 3"
        />
      </svg>
    );
  }

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className={cn('block', className)}
      aria-hidden="true"
    >
      {fill ? <path d={area} fill={fill} /> : null}
      <path
        d={d}
        className="sparkline-path"
        fill="none"
        stroke={stroke}
        strokeWidth="1.25"
        strokeLinecap="round"
        strokeLinejoin="round"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  );
}
