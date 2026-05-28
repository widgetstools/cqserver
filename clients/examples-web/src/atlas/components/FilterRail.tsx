import { useState } from 'react';
import type { ChipSpec } from '../types';
import { ChipPicker } from './ChipPicker';

interface FilterRailProps {
  chips: ChipSpec[];
  /** chip.key → currently selected value (undefined / 'All' = no constraint). */
  state: Record<string, string | undefined>;
  /** chip.key → option list (Phase 2 will populate from chip.source). */
  options: Record<string, string[]>;
  onChange: (next: Record<string, string | undefined>) => void;
  /** Human-readable summary of the active subscription, e.g. "book_name = 'RATES-US'". */
  subscriptionSummary?: string;
}

export function FilterRail({ chips, state, options, onChange, subscriptionSummary }: FilterRailProps) {
  const [openKey, setOpenKey] = useState<string | null>(null);

  return (
    <div
      style={{
        position: 'relative',
        zIndex: 1,
        padding: '14px 24px',
        borderTop: '1px solid var(--atlas-rule)',
        borderBottom: '1px solid var(--atlas-rule)',
        display: 'flex',
        alignItems: 'center',
        gap: 10,
        fontSize: 10.5,
      }}
    >
      <div
        style={{
          color: 'var(--atlas-fg-faint)',
          letterSpacing: '.22em',
          textTransform: 'uppercase',
          fontSize: 9.5,
          marginRight: 4,
        }}
      >
        FILTER
      </div>
      {chips.map((c) => {
        const value = state[c.key];
        const isActive = value != null && value !== '' && value !== 'All';
        return (
          <div key={c.key} style={{ position: 'relative' }}>
            <button
              onClick={() => setOpenKey(openKey === c.key ? null : c.key)}
              style={{
                all: 'unset',
                cursor: 'pointer',
                display: 'inline-flex',
                alignItems: 'center',
                gap: 8,
                padding: '5px 10px 5px 8px',
                border: `1px solid ${isActive ? 'rgba(244,165,43,.65)' : 'var(--atlas-rule)'}`,
                background: isActive ? 'var(--atlas-amber-soft)' : 'transparent',
                borderRadius: 999,
                fontSize: 10.5,
                color: 'var(--atlas-fg)',
              }}
            >
              <span
                style={{
                  color: 'var(--atlas-fg-faint)',
                  fontSize: 9.5,
                  letterSpacing: '.14em',
                  textTransform: 'uppercase',
                }}
              >
                {c.key}
              </span>
              <span style={{ fontWeight: 600, color: isActive ? 'var(--atlas-amber)' : undefined }}>
                {value ?? 'All'}
              </span>
              {isActive ? (
                <span
                  aria-label="clear"
                  onClick={(e) => {
                    e.stopPropagation();
                    onChange({ ...state, [c.key]: undefined });
                  }}
                  style={{ color: 'var(--atlas-fg-faint)', fontSize: 10, cursor: 'pointer' }}
                >
                  ×
                </span>
              ) : (
                <span style={{ color: 'var(--atlas-fg-faint)', fontSize: 10 }}>▾</span>
              )}
            </button>
            <ChipPicker
              open={openKey === c.key}
              options={options[c.key] ?? []}
              selected={value}
              onSelect={(v) => onChange({ ...state, [c.key]: v })}
              onClose={() => setOpenKey(null)}
            />
          </div>
        );
      })}
      <div style={{ flex: 1 }} />
      {subscriptionSummary ? (
        <div
          style={{
            fontSize: 9.5,
            letterSpacing: '.18em',
            color: 'var(--atlas-fg-dim)',
            display: 'flex',
            alignItems: 'center',
            gap: 8,
          }}
        >
          <span className="atlas-live-dot" />
          SUBSCRIBED · {subscriptionSummary}
        </div>
      ) : null}
    </div>
  );
}
