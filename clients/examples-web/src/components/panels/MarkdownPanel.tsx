import { marked } from 'marked';
import { useMemo } from 'react';
import { PanelChrome } from './PanelChrome';
import { Badge } from '@/components/ui/badge';

// Trusted, in-repo markdown → no sanitizer pass.
marked.use({ gfm: true, breaks: false });

interface MarkdownPanelProps {
  title?: string;
  source: string;
  /** Optional sub-label (e.g. "ex01.md") shown in the panel header. */
  filename?: string;
}

export function MarkdownPanel({ title = 'Notes', source, filename }: MarkdownPanelProps) {
  const html = useMemo(() => marked.parse(source, { async: false }) as string, [source]);
  return (
    <PanelChrome
      title={title}
      right={filename ? <Badge variant="muted" className="!text-[9px]">{filename}</Badge> : undefined}
    >
      <div className="px-5 py-4">
        <article className="cq-prose" dangerouslySetInnerHTML={{ __html: html }} />
      </div>
    </PanelChrome>
  );
}
