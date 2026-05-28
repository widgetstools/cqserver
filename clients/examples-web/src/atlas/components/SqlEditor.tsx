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
  /** Right-side status, e.g. '5,210 rows · 32 ms · live' or '—'. */
  status?: string;
  /** Error message; renders red when set. */
  error?: string | null;
  /** Disables the Run button while a query is opening. */
  busy?: boolean;
}

export function SqlEditor({ value, onChange, onRun, status, error, busy }: SqlEditorProps) {
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
        minHeight: 0,
        borderBottom: '1px solid var(--atlas-rule)',
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '10px 18px',
          borderBottom: '1px solid var(--atlas-rule)',
        }}
      >
        <div style={{ fontSize: 11, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)' }}>
          SQL · editable · ⌘↩ to run
        </div>
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
          flex: 1,
          minHeight: 180,
          width: '100%',
          padding: '12px 18px',
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
    </div>
  );
}
