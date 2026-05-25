import { useQuery } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import { Copy, FileText, RefreshCw, Search } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { adminApi } from '@/lib/admin';
import { cn } from '@/lib/utils';

export function ConfigPage() {
  const [filter, setFilter] = useState('');
  const [copied, setCopied] = useState(false);

  const cfg = useQuery({
    queryKey: ['config-toml'],
    queryFn: adminApi.configToml,
    // Config rarely changes after startup; poll every minute just to
    // catch a live reload (when that ever lands).
    refetchInterval: 60_000,
  });

  const text = cfg.data ?? '';
  const lines = useMemo(() => text.split('\n'), [text]);

  const filteredLines = useMemo(() => {
    if (!filter) return lines.map((line, idx) => ({ line, idx, match: false }));
    const lower = filter.toLowerCase();
    return lines.map((line, idx) => ({
      line,
      idx,
      match: line.toLowerCase().includes(lower),
    }));
  }, [lines, filter]);

  const matchCount = useMemo(
    () => filteredLines.filter((l) => l.match).length,
    [filteredLines],
  );

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // Older browsers: select-all the pre via a textarea fallback.
      const ta = document.createElement('textarea');
      ta.value = text;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    }
  };

  return (
    <div className="px-6 py-5 max-w-[1400px] mx-auto">
      <div className="flex items-baseline justify-between mb-4">
        <div>
          <h1 className="text-[18px] font-semibold tracking-tight leading-none flex items-center gap-2">
            <FileText size={16} className="text-primary" />
            Config
          </h1>
          <p className="text-[11.5px] text-muted-foreground mt-1.5">
            Live render of <code className="font-mono">cqserver.toml</code> with
            any{' '}
            <code className="font-mono text-foreground">${'{VAR:-default}'}</code>{' '}
            substitutions already applied. Read-only; edit on disk + restart to
            change.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Search
              size={12}
              className="absolute left-2 top-1/2 -translate-y-1/2 text-muted-foreground"
            />
            <input
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="Find in config…"
              className="h-7 w-52 pl-7 pr-2 rounded-md border border-border bg-input text-[12px] font-mono text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
            />
          </div>
          <Button variant="secondary" size="sm" onClick={copy}>
            <Copy size={11} />
            {copied ? 'Copied' : 'Copy'}
          </Button>
          <Button
            variant="secondary"
            size="sm"
            onClick={() => cfg.refetch()}
            disabled={cfg.isFetching}
          >
            <RefreshCw size={11} className={cfg.isFetching ? 'animate-spin' : ''} />
            Refresh
          </Button>
        </div>
      </div>

      <Card>
        <CardHeader className="flex flex-row items-baseline justify-between pb-2 border-b border-border">
          <CardTitle>cqserver.toml</CardTitle>
          <span className="text-[11px] text-muted-foreground font-mono">
            {lines.length} lines
            {filter
              ? ` · ${matchCount} match${matchCount === 1 ? '' : 'es'} for "${filter}"`
              : ''}
          </span>
        </CardHeader>
        <CardContent className="p-0">
          <div className="font-mono text-[11.5px] leading-relaxed overflow-x-auto">
            {cfg.isLoading ? (
              <div className="py-10 text-center text-muted-foreground">
                Loading config…
              </div>
            ) : cfg.isError ? (
              <div className="py-10 text-center text-err">
                Failed to load config — is /admin/config exposed?
              </div>
            ) : (
              <table className="w-full">
                <tbody>
                  {filteredLines.map(({ line, idx, match }) => (
                    <tr
                      key={idx}
                      className={cn(
                        'group',
                        match && 'bg-warn-muted/50',
                      )}
                    >
                      <td className="select-none text-right px-3 py-0.5 text-muted-foreground/50 w-[3.5em] border-r border-border">
                        {idx + 1}
                      </td>
                      <td className="px-3 py-0.5 whitespace-pre">
                        <TomlLine line={line} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

/** Cheap TOML-ish line highlighter: comments, table headers, keys, strings. */
function TomlLine({ line }: { line: string }) {
  // comment
  const commentIdx = line.indexOf('#');
  if (line.trim().startsWith('#')) {
    return <span className="text-muted-foreground/60">{line}</span>;
  }
  // table header [foo] or [[foo]]
  const tableMatch = line.match(/^(\s*)(\[\[?[^\]]+\]\]?)/);
  if (tableMatch) {
    return (
      <>
        <span>{tableMatch[1]}</span>
        <span className="text-primary font-medium">{tableMatch[2]}</span>
        <span>{line.slice(tableMatch[0].length)}</span>
      </>
    );
  }
  // key = value
  const kvMatch = line.match(/^(\s*)([A-Za-z_][A-Za-z0-9_-]*)(\s*=\s*)(.*)$/);
  if (kvMatch) {
    const [, lead, key, eq, valRest] = kvMatch;
    const inlineComment =
      commentIdx >= 0 && commentIdx > lead.length + key.length + eq.length
        ? line.slice(commentIdx)
        : null;
    const value = inlineComment
      ? valRest.slice(0, valRest.length - inlineComment.length).trimEnd()
      : valRest;
    return (
      <>
        <span>{lead}</span>
        <span className="text-foreground">{key}</span>
        <span className="text-muted-foreground">{eq}</span>
        <ValueSpan v={value} />
        {inlineComment ? (
          <span className="text-muted-foreground/60"> {inlineComment}</span>
        ) : null}
      </>
    );
  }
  return <span>{line}</span>;
}

function ValueSpan({ v }: { v: string }) {
  if (v.startsWith('"') && v.endsWith('"')) {
    return <span className="text-ok">{v}</span>;
  }
  if (v === 'true' || v === 'false') {
    return <span className="text-warn">{v}</span>;
  }
  if (/^-?\d+(\.\d+)?$/.test(v.trim())) {
    return <span className="text-info">{v}</span>;
  }
  return <span>{v}</span>;
}
