import type { ReactNode } from 'react';
import { cn } from '@/lib/utils';

interface PanelChromeProps {
  title: string;
  right?: ReactNode;
  marker?: boolean;
  className?: string;
  children: ReactNode;
}

export function PanelChrome({ title, right, marker = true, className, children }: PanelChromeProps) {
  return (
    <div className={cn('w-full h-full flex flex-col bg-card', className)}>
      <div className="panel-header">
        <div className="panel-title">
          {marker ? <span className="marker" /> : null}
          {title}
        </div>
        {right}
      </div>
      <div className="panel-body">{children}</div>
    </div>
  );
}
