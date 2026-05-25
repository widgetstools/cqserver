import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { EditorView } from '@codemirror/view';
import { useTheme } from '@/components/theme/ThemeProvider';
import { PanelChrome } from './PanelChrome';
import { Badge } from '@/components/ui/badge';
import { useMemo, useState } from 'react';

interface SqlPanelProps {
  title: string;
  value: string;
  readOnly?: boolean;
  onChange?: (v: string) => void;
  /** Optional execution hook — when present a Run button appears. */
  onRun?: (sql: string) => void;
  /** Optional plan summary line shown next to Run. */
  planSummary?: string;
}

const lightExtensions = [EditorView.theme({
  '&': {
    backgroundColor: 'transparent',
    color: 'var(--foreground)',
  },
  '.cm-content': { caretColor: 'var(--signal)' },
  '.cm-activeLine': { backgroundColor: 'var(--accent)' },
  '.cm-keyword': { color: 'var(--signal)', fontWeight: '600' },
  '.cm-comment': { color: 'var(--muted-foreground)', fontStyle: 'italic' },
  '.cm-string': { color: 'var(--ok)' },
  '.cm-number': { color: 'var(--primary)' },
  '.cm-operator': { color: 'var(--foreground)' },
}, { dark: false })];

const darkExtensions = [EditorView.theme({
  '&': {
    backgroundColor: 'transparent',
    color: 'var(--foreground)',
  },
  '.cm-content': { caretColor: 'var(--signal)' },
  '.cm-activeLine': { backgroundColor: 'var(--accent)' },
  '.cm-keyword': { color: 'var(--signal)', fontWeight: '600' },
  '.cm-comment': { color: 'var(--muted-foreground)', fontStyle: 'italic' },
  '.cm-string': { color: 'var(--ok)' },
  '.cm-number': { color: '#60a5fa' },
  '.cm-operator': { color: 'var(--foreground)' },
}, { dark: true })];

export function SqlPanel({ title, value, readOnly, onChange, onRun, planSummary }: SqlPanelProps) {
  const { theme } = useTheme();
  const [text, setText] = useState(value);
  // Sync if value prop changes from outside (e.g. clicking a different
  // saved query in the library): re-seed local state.
  useMemo(() => setText(value), [value]);

  const exts = useMemo(() => {
    return [sql({ upperCaseKeywords: true }), ...(theme === 'dark' ? darkExtensions : lightExtensions)];
  }, [theme]);

  return (
    <PanelChrome
      title={title}
      right={
        <div className="flex items-center gap-2">
          {planSummary ? (
            <span className="font-mono text-[10px] text-muted-foreground truncate max-w-[260px]">
              {planSummary}
            </span>
          ) : null}
          <Badge variant="muted" className="!text-[9px]">SQL</Badge>
          {onRun ? (
            <button
              type="button"
              onClick={() => onRun(text)}
              className="text-[10px] font-mono uppercase tracking-[0.1em] px-2 h-5 rounded-sm border border-signal/40 text-signal hover:bg-signal hover:text-signal-foreground transition-colors"
            >
              Run ▸
            </button>
          ) : null}
        </div>
      }
    >
      <div className="h-full">
        <CodeMirror
          value={text}
          height="100%"
          theme={theme === 'dark' ? 'dark' : 'light'}
          extensions={exts}
          readOnly={readOnly}
          basicSetup={{
            lineNumbers: true,
            foldGutter: false,
            highlightActiveLine: true,
            indentOnInput: true,
            bracketMatching: true,
          }}
          onChange={(v) => {
            setText(v);
            onChange?.(v);
          }}
        />
      </div>
    </PanelChrome>
  );
}
