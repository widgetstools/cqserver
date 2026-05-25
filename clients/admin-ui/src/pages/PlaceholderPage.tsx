import { Construction } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';

interface PlaceholderPageProps {
  title: string;
  description: string;
  worklogRef: string;
}

export function PlaceholderPage({ title, description, worklogRef }: PlaceholderPageProps) {
  return (
    <div className="px-6 py-5 max-w-[1400px] mx-auto">
      <div className="mb-4">
        <h1 className="text-[18px] font-semibold tracking-tight leading-none">
          {title}
        </h1>
        <p className="text-[11.5px] text-muted-foreground mt-1.5">
          {description}
        </p>
      </div>

      <Card>
        <CardContent className="py-12 flex flex-col items-center justify-center text-center">
          <div className="size-9 rounded-md bg-warn-muted border border-warn/30 flex items-center justify-center mb-3">
            <Construction size={16} className="text-warn" />
          </div>
          <div className="text-[13px] font-medium mb-1">Coming in a follow-up session</div>
          <div className="text-[11.5px] text-muted-foreground font-mono">
            See <span className="text-foreground">{worklogRef}</span> in{' '}
            <span className="text-foreground">ADMIN_UI_WORKLOG.md</span>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
