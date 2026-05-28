import { useEffect, useRef } from 'react';

interface ChipPickerProps {
  open: boolean;
  options: string[];
  selected?: string;
  onSelect: (value: string) => void;
  onClose: () => void;
}

export function ChipPicker({ open, options, selected, onSelect, onClose }: ChipPickerProps) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('mousedown', onDown);
    window.addEventListener('keydown', onKey);
    return () => {
      window.removeEventListener('mousedown', onDown);
      window.removeEventListener('keydown', onKey);
    };
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div
      ref={ref}
      style={{
        position: 'absolute',
        top: 'calc(100% + 4px)',
        left: 0,
        zIndex: 20,
        minWidth: 180,
        maxHeight: 280,
        overflowY: 'auto',
        background: '#14141a',
        border: '1px solid var(--atlas-rule)',
        borderRadius: 6,
        boxShadow: '0 16px 40px -10px rgba(0,0,0,.6)',
        padding: 4,
      }}
    >
      {options.length === 0 ? (
        <div style={{ padding: '8px 10px', fontSize: 10.5, color: 'var(--atlas-fg-faint)' }}>(no values)</div>
      ) : (
        options.map((opt) => {
          const isSelected = opt === selected;
          return (
            <button
              key={opt}
              onClick={() => {
                onSelect(opt);
                onClose();
              }}
              style={{
                all: 'unset',
                display: 'block',
                width: '100%',
                padding: '6px 10px',
                fontFamily: 'var(--atlas-font)',
                fontSize: 11,
                color: isSelected ? 'var(--atlas-amber)' : 'var(--atlas-fg)',
                background: isSelected ? 'var(--atlas-amber-soft)' : 'transparent',
                borderRadius: 4,
                cursor: 'pointer',
              }}
              onMouseEnter={(e) => {
                if (!isSelected) (e.currentTarget as HTMLElement).style.background = 'rgba(255,255,255,.04)';
              }}
              onMouseLeave={(e) => {
                if (!isSelected) (e.currentTarget as HTMLElement).style.background = 'transparent';
              }}
            >
              {opt}
            </button>
          );
        })
      )}
    </div>
  );
}
