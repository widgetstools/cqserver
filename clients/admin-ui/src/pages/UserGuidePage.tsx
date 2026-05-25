import { useQuery } from '@tanstack/react-query';
import { useEffect, useMemo, useRef, useState } from 'react';
import { marked } from 'marked';
import {
  BookOpen,
  ChevronDown,
  ChevronRight,
  ExternalLink,
  Search,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { cn } from '@/lib/utils';

/**
 * UserGuidePage — renders `docs/USER_GUIDE.md` from the same-origin
 * `/docs/USER_GUIDE.md` static-file endpoint.
 *
 * We trust the markdown source because:
 *   - It's a file we ship in the same repo as the binary
 *   - The static-file endpoint serves *only* files under
 *     `CQSERVER_DOCS_DIR` (default `./docs`); no user-provided content
 *     reaches the renderer
 *   - Operators with admin access already have superuser power
 *
 * For the same reason we render `marked.parse()` output via
 * dangerouslySetInnerHTML without an extra sanitization pass. If the
 * doc source were ever opened to untrusted contributors, swap in
 * DOMPurify on the parsed HTML before assigning to innerHTML.
 */

// Slug-ify a heading's source text into a URL-safe id. Stable
// between renders: a heading "1. What cqserver is" → "1-what-cqserver-is".
function slugify(text: string): string {
  return text
    .toLowerCase()
    .trim()
    .replace(/[^\w\s-]/g, '')   // strip punctuation
    .replace(/\s+/g, '-')        // collapse whitespace to '-'
    .replace(/^-+|-+$/g, '');    // trim leading/trailing dashes
}

// Configure marked once at module load. GFM = tables + strikethrough
// + autolinking. marked v18 no longer auto-generates heading IDs, so
// we install a custom renderer that does — without IDs the in-page
// anchor links (and our sidebar TOC) can't resolve targets.
marked.use({
  gfm: true,
  breaks: false,
  renderer: {
    // The `text` field is the inline-rendered HTML for the heading;
    // `raw` is the source line including the `## ` prefix. We slug
    // from `text` since it matches what the user sees + what the
    // markdown's own `[link](#anchor)` references encode.
    heading({ tokens, depth }) {
      // Recover the plain-text form by joining token raws.
      const plain = tokens.map((t) => ('raw' in t ? t.raw : '')).join('');
      const id = slugify(plain);
      const inner = this.parser.parseInline(tokens);
      return `<h${depth} id="${id}">${inner}</h${depth}>\n`;
    },
  },
});

interface TocEntry {
  id: string;
  text: string;
  level: number;
}

export function UserGuidePage() {
  const [query, setQuery] = useState('');
  const [tocOpen, setTocOpen] = useState(true);
  const articleRef = useRef<HTMLDivElement>(null);

  const md = useQuery({
    queryKey: ['user-guide'],
    queryFn: async () => {
      // `/docs/USER_GUIDE.md` is served by the cqserver admin port
      // via tower-http ServeDir (CQSERVER_DOCS_DIR, default ./docs).
      // Same-origin → no CORS, no proxy.
      const r = await fetch('/docs/USER_GUIDE.md');
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      return await r.text();
    },
    staleTime: 60_000,
  });

  const html = useMemo(() => {
    if (!md.data) return '';
    return marked.parse(md.data, { async: false }) as string;
  }, [md.data]);

  // Build a TOC by walking the headings post-render. Done after the
  // article is mounted so we read the real `id` attributes marked
  // produced (which match the URL anchors).
  const [toc, setToc] = useState<TocEntry[]>([]);
  useEffect(() => {
    if (!articleRef.current) return;
    const hs = articleRef.current.querySelectorAll('h2, h3');
    const out: TocEntry[] = [];
    hs.forEach((h) => {
      const id = h.id;
      if (!id) return;
      out.push({
        id,
        text: h.textContent ?? '',
        level: h.tagName === 'H2' ? 2 : 3,
      });
    });
    setToc(out);
    // Re-run when html changes.
  }, [html]);

  // Filtered TOC for the search-as-you-type box.
  const filteredToc = useMemo(() => {
    if (!query) return toc;
    const q = query.toLowerCase();
    return toc.filter((e) => e.text.toLowerCase().includes(q));
  }, [toc, query]);

  // Anchor scrolling within the SPA — react-router doesn't auto-
  // scroll to `#hash` on click since we're rendering raw HTML.
  // Intercept clicks on internal anchors and scroll manually.
  useEffect(() => {
    const el = articleRef.current;
    if (!el) return;
    const onClick = (e: Event) => {
      const target = e.target as HTMLElement | null;
      const a = target?.closest('a') as HTMLAnchorElement | null;
      if (!a) return;
      const href = a.getAttribute('href');
      if (href && href.startsWith('#')) {
        e.preventDefault();
        const id = href.slice(1);
        document.getElementById(id)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        // Update the URL hash for shareability without re-routing.
        history.replaceState(null, '', `#${id}`);
      }
    };
    el.addEventListener('click', onClick);
    return () => el.removeEventListener('click', onClick);
  }, [html]);

  const onTocClick = (id: string) => {
    document.getElementById(id)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    history.replaceState(null, '', `#${id}`);
  };

  return (
    <div className="px-6 py-5 max-w-[1400px] mx-auto">
      <div className="flex items-baseline justify-between mb-4">
        <div>
          <h1 className="text-[18px] font-semibold tracking-tight leading-none flex items-center gap-2">
            <BookOpen size={16} className="text-primary" />
            User Guide
          </h1>
          <p className="text-[11.5px] text-muted-foreground mt-1.5">
            Operator + developer reference. Source:{' '}
            <code className="font-mono">docs/USER_GUIDE.md</code> served live
            from this cqserver instance.
          </p>
        </div>
        <a
          href="/docs/USER_GUIDE.md"
          target="_blank"
          rel="noreferrer"
          className="text-[11px] text-primary hover:underline inline-flex items-center gap-1 font-mono"
        >
          Open raw <ExternalLink size={10} />
        </a>
      </div>

      {md.isLoading ? (
        <Card>
          <CardContent className="py-10 text-center text-[12px] text-muted-foreground">
            Loading user guide…
          </CardContent>
        </Card>
      ) : md.isError ? (
        <Card>
          <CardContent className="py-10 text-center text-[12px] text-err">
            Failed to load <code className="font-mono">docs/USER_GUIDE.md</code>.
            Is the cqserver process running with{' '}
            <code className="font-mono">CQSERVER_DOCS_DIR</code> pointing at the
            docs directory?
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-12 gap-4">
          {/* TOC */}
          <aside className="col-span-12 lg:col-span-3 lg:sticky lg:top-4 lg:self-start lg:max-h-[calc(100vh-6rem)] lg:overflow-y-auto">
            <Card>
              <div className="px-3 py-2 border-b border-border flex items-center justify-between gap-2">
                <button
                  onClick={() => setTocOpen((o) => !o)}
                  className="text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground font-medium flex items-center gap-1 hover:text-foreground"
                >
                  {tocOpen ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
                  Contents
                </button>
                <span className="text-[10.5px] text-muted-foreground font-mono">
                  {toc.length}
                </span>
              </div>
              {tocOpen ? (
                <>
                  <div className="px-2 py-1.5 border-b border-border">
                    <div className="relative">
                      <Search
                        size={11}
                        className="absolute left-1.5 top-1/2 -translate-y-1/2 text-muted-foreground"
                      />
                      <input
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        placeholder="Filter…"
                        data-page-filter
                        className="w-full h-6 pl-6 pr-1.5 rounded-sm border border-border bg-input text-[11px] text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
                      />
                    </div>
                  </div>
                  <CardContent className="px-1 py-1 max-h-[60vh] lg:max-h-none overflow-y-auto">
                    {filteredToc.length === 0 ? (
                      <div className="text-[11px] text-muted-foreground text-center py-4">
                        No matches.
                      </div>
                    ) : (
                      <ul>
                        {filteredToc.map((e) => (
                          <li key={e.id}>
                            <button
                              onClick={() => onTocClick(e.id)}
                              className={cn(
                                'block w-full text-left text-[11.5px] py-1 px-2 rounded-sm hover:bg-accent/50 transition-colors',
                                e.level === 3 ? 'pl-5 text-muted-foreground' : 'text-foreground',
                              )}
                            >
                              {e.text}
                            </button>
                          </li>
                        ))}
                      </ul>
                    )}
                  </CardContent>
                </>
              ) : null}
            </Card>
            <div className="mt-2 flex">
              <Button
                variant="secondary"
                size="sm"
                onClick={() => md.refetch()}
                className="w-full"
              >
                Refetch
              </Button>
            </div>
          </aside>

          {/* Article */}
          <Card className="col-span-12 lg:col-span-9 overflow-hidden">
            <CardContent className="px-6 py-5">
              <article
                ref={articleRef}
                className="cq-prose"
                // See the comment at the top of this file for why
                // dangerouslySetInnerHTML is safe here.
                dangerouslySetInnerHTML={{ __html: html }}
              />
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}
