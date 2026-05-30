/**
 * SqlEditor — controlled-textarea SQL editor for Chapter 08. Plain
 * <textarea> rather than CodeMirror/Monaco so the bundle stays flat;
 * the legacy ex08 used the same approach. Run button hands the
 * current text to the chapter's onRun handler. Status strip beneath
 * shows the active run's elapsed time, row count, or any error.
 */
import { useEffect, useRef } from 'react';

interface SqlEditorProps {
  value: string;
  onChange: (v: string) => void;
  onRun: () => void;
  status?: string;
  error?: string | null;
  busy?: boolean;
  /** Tighter layout for the Query tab — shorter editor, less chrome. */
  compact?: boolean;
}

export function SqlEditor({ value, onChange, onRun, status, error, busy, compact }: SqlEditorProps) {
  const taRef = useRef<HTMLTextAreaElement | null>(null);

  // Cmd/Ctrl+Enter to run.
  useEffect(() => {
    const ta = taRef.current;
    if (!ta) return;
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
        e.preventDefault();
        onRun();
      }
    };
    ta.addEventListener('keydown', onKey);
    return () => ta.removeEventListener('keydown', onKey);
  }, [onRun]);

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        flexShrink: compact ? 0 : undefined,
        minHeight: 0,
        maxHeight: compact ? 'min(28vh, 168px)' : undefined,
        borderBottom: '1px solid var(--atlas-rule)',
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: compact ? '6px 14px' : '10px 18px',
          borderBottom: '1px solid var(--atlas-rule)',
          gap: 12,
        }}
      >
        <div style={{ fontSize: compact ? 10 : 11, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)', flexShrink: 0 }}>
          SQL · ⌘↩ run
        </div>
        {status ? (
          <div
            style={{
              flex: 1,
              textAlign: compact ? 'left' : 'right',
              fontSize: 10,
              color: error ? 'var(--atlas-neg)' : 'var(--atlas-fg-faint)',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
              minWidth: 0,
            }}
          >
            {error ?? status}
          </div>
        ) : null}
        <button
          onClick={onRun}
          disabled={busy}
          style={{
            background: 'var(--atlas-amber)',
            color: 'var(--atlas-ink)',
            border: 'none',
            padding: '5px 14px',
            fontFamily: 'var(--atlas-font)',
            fontSize: 11,
            fontWeight: 700,
            letterSpacing: '.18em',
            cursor: busy ? 'wait' : 'pointer',
            opacity: busy ? 0.6 : 1,
          }}
        >
          {busy ? 'RUNNING…' : 'RUN'}
        </button>
      </div>
      <textarea
        ref={taRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        spellCheck={false}
        style={{
          flex: compact ? '1 1 auto' : 1,
          minHeight: compact ? 72 : 180,
          width: '100%',
          padding: compact ? '8px 14px' : '12px 18px',
          background: 'var(--atlas-ink-2)',
          color: 'var(--atlas-fg)',
          border: 'none',
          outline: 'none',
          resize: 'none',
          fontFamily: 'var(--atlas-font)',
          fontSize: 12,
          lineHeight: 1.55,
          tabSize: 2,
        }}
      />
      {!compact ? (
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            padding: '8px 18px',
            fontSize: 10,
            borderTop: '1px solid var(--atlas-rule)',
            background: 'var(--atlas-surface)',
          }}
        >
          <div style={{ color: error ? 'var(--atlas-neg)' : 'var(--atlas-fg-faint)' }}>
            {error ?? status ?? '—'}
          </div>
        </div>
      ) : null}
    </div>
  );
}
