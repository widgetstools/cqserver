interface FooterProps {
  /** 'LIVE' or 'CONNECTING' etc. */
  status?: string;
  cadence?: string;     // '250ms cadence'
  tickStats?: string;   // '4,820 ticks · 0 drops'
}

export function Footer({ status = 'LIVE', cadence, tickStats }: FooterProps) {
  return (
    <div
      style={{
        position: 'relative',
        zIndex: 1,
        display: 'flex',
        justifyContent: 'space-between',
        padding: '10px 24px',
        borderTop: '1px solid var(--atlas-rule)',
        fontSize: 9.5,
        letterSpacing: '.18em',
        color: 'var(--atlas-fg-dim)',
      }}
    >
      <div style={{ display: 'flex', gap: 24 }}>
        <span>
          <span className="atlas-live-dot" style={{ marginRight: 8 }} />
          {status}
        </span>
        {cadence ? <span>{cadence}</span> : null}
        {tickStats ? <span>{tickStats}</span> : null}
      </div>
      <div style={{ display: 'flex', gap: 18 }}>
        <span>
          ⌘ <KeyBadge>K</KeyBadge> palette
        </span>
        <span>
          ⌘ <KeyBadge>F</KeyBadge> filter
        </span>
      </div>
    </div>
  );
}

function KeyBadge({ children }: { children: string }) {
  return (
    <span
      style={{
        background: 'rgba(255,255,255,.06)',
        padding: '1px 6px',
        borderRadius: 4,
        letterSpacing: '.04em',
      }}
    >
      {children}
    </span>
  );
}
