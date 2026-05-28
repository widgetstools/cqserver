import { useEffect } from 'react';
import { CHAPTERS } from '../chapters';
import type { ChapterId } from '../types';

interface StationsNavProps {
  active: ChapterId;
  onChange: (id: ChapterId) => void;
}

export function StationsNav({ active, onChange }: StationsNavProps) {
  // Keyboard 1–8 jump to the corresponding chapter.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      const target = e.target as HTMLElement | null;
      if (target && (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.isContentEditable)) return;
      const n = Number(e.key);
      if (Number.isInteger(n) && n >= 1 && n <= CHAPTERS.length) {
        e.preventDefault();
        onChange(CHAPTERS[n - 1]!.id);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onChange]);

  return (
    <nav
      style={{
        position: 'relative',
        zIndex: 1,
        display: 'flex',
        padding: '0 18px',
        borderBottom: '1px solid var(--atlas-rule)',
        fontSize: 10.5,
      }}
    >
      {CHAPTERS.map((c, i) => {
        const isActive = c.id === active;
        return (
          <button
            key={c.id}
            onClick={() => onChange(c.id)}
            style={{
              all: 'unset',
              cursor: 'pointer',
              padding: '14px 16px 10px',
              position: 'relative',
              display: 'inline-flex',
              alignItems: 'baseline',
              gap: 8,
              opacity: isActive ? 1 : 0.5,
              transition: 'opacity .15s',
            }}
            onMouseEnter={(e) => {
              if (!isActive) (e.currentTarget as HTMLElement).style.opacity = '0.85';
            }}
            onMouseLeave={(e) => {
              if (!isActive) (e.currentTarget as HTMLElement).style.opacity = '0.5';
            }}
          >
            {i > 0 ? (
              <span
                aria-hidden
                style={{ position: 'absolute', left: -7, top: '50%', transform: 'translateY(-50%)', opacity: 0.25 }}
              >
                ─
              </span>
            ) : null}
            <span style={{ fontSize: 9, letterSpacing: '.22em', opacity: 0.65 }}>{c.num}</span>
            <span
              style={{
                fontSize: 12,
                letterSpacing: '.12em',
                fontWeight: 500,
                color: isActive ? 'var(--atlas-amber)' : undefined,
              }}
            >
              {c.name}
            </span>
            {isActive ? (
              <span
                aria-hidden
                style={{
                  position: 'absolute',
                  left: 14,
                  right: 14,
                  bottom: -1,
                  height: 2,
                  background: 'var(--atlas-amber)',
                  boxShadow: '0 0 10px var(--atlas-amber)',
                }}
              />
            ) : null}
          </button>
        );
      })}
    </nav>
  );
}
