import type { ExampleEntry } from '@/examples/registry';
import { Badge } from '@/components/ui/badge';

interface ExampleRubricProps {
  example: ExampleEntry;
  /** Optional eyebrow text (e.g. "ATLAS · LIVE"). */
  eyebrow?: string;
}

/**
 * The visual hallmark of the Atlas: huge "EX.NN" serial, ALL-CAPS
 * title, synopsis, capsule feature tags. Rendered atop every example
 * canvas.
 */
export function ExampleRubric({ example, eyebrow }: ExampleRubricProps) {
  return (
    <div className="px-6 py-4 border-b border-border bg-card flex items-start gap-6 relative overflow-hidden">
      {/* Decorative crosshair behind the serial — only visible in dark.  */}
      <div
        aria-hidden
        className="absolute -left-3 -top-3 w-32 h-32 rounded-full pointer-events-none"
        style={{
          background:
            'radial-gradient(circle at 30% 30%, color-mix(in oklab, var(--signal) 12%, transparent) 0%, transparent 60%)',
        }}
      />
      <div className="atlas-serial relative shrink-0" style={{ lineHeight: 0.9 }}>
        {example.serial}
      </div>
      <div className="hairline-vert h-12 self-center" />
      <div className="flex-1 min-w-0">
        {eyebrow ? (
          <div className="atlas-eyebrow mb-1">
            <span className="dot">▸ </span>
            {eyebrow}
          </div>
        ) : null}
        <h1 className="atlas-title leading-tight">{example.title}</h1>
        <p className="atlas-subtitle mt-1 line-clamp-2 max-w-3xl">{example.synopsis}</p>
        <div className="mt-2 flex gap-1.5 flex-wrap">
          {example.features.map((f) => (
            <span key={f} className="feature-tag" data-kind={f}>
              {f}
            </span>
          ))}
        </div>
      </div>
      <div className="shrink-0 self-start flex flex-col items-end gap-2">
        <Badge variant="outline" className="!text-[9.5px]">
          {example.category.toUpperCase()}
        </Badge>
        <span className="font-mono text-[10px] text-muted-foreground tracking-[0.1em]">
          /{example.id}
        </span>
      </div>
    </div>
  );
}
