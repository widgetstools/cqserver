import type { ReactNode } from 'react';
import { cn } from '@/lib/utils';

interface SectionProps {
  title: string;
  count?: string;
  className?: string;
  children: ReactNode;
  /** Fixed height for the section content area. */
  bodyHeight?: number | string;
}

export function Section({ title, count, className, children, bodyHeight = 360 }: SectionProps) {
  return (
    <section
      className={cn('flex flex-col rounded-md border overflow-hidden', className)}
      style={{ background: 'var(--sf-bg-2)', borderColor: 'var(--sf-border)' }}
    >
      <div
        className="flex items-center justify-between px-3 py-2 text-xs font-semibold"
        style={{ borderBottom: '1px solid var(--sf-border)', color: 'var(--sf-t-0)' }}
      >
        <span>{title}</span>
        {count !== undefined && (
          <span className="font-normal" style={{ color: 'var(--sf-t-2)' }}>
            {count}
          </span>
        )}
      </div>
      <div className="min-h-0" style={{ height: bodyHeight }}>
        {children}
      </div>
    </section>
  );
}
