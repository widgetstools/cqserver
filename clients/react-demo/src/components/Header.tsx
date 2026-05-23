import { Moon, Sun } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Select } from '@/components/ui/select';
import type { Palette, ThemeMode } from '@/lib/agGridTheme';

interface HeaderProps {
  palette: Palette;
  mode: ThemeMode;
  onPaletteChange: (p: Palette) => void;
  onModeChange: (m: ThemeMode) => void;
  wsUrl: string;
}

const PALETTE_OPTIONS: { value: Palette; label: string }[] = [
  { value: 'teal', label: 'Teal' },
  { value: 'amber', label: 'Amber' },
  { value: 'slate', label: 'Slate' },
  { value: 'indigo', label: 'Indigo' },
  { value: 'grey', label: 'Grey' },
];

export function Header({ palette, mode, onPaletteChange, onModeChange, wsUrl }: HeaderProps) {
  return (
    <header
      className="flex items-center justify-between px-4 py-3"
      style={{
        background: 'var(--sf-bg-2)',
        borderBottom: '1px solid var(--sf-border)',
      }}
    >
      <div className="flex items-center gap-3">
        <div
          className="h-7 w-7 rounded-md grid place-items-center text-sm font-bold"
          style={{
            background: 'var(--sf-teal, var(--sf-up))',
            color: '#ffffff',
          }}
        >
          C
        </div>
        <div>
          <h1 className="text-sm font-semibold leading-tight">cqserver · FI Positions</h1>
          <p className="text-[11px]" style={{ color: 'var(--sf-t-2)' }}>
            {wsUrl}
          </p>
        </div>
      </div>

      <div className="flex items-center gap-2">
        <label className="flex items-center gap-1.5 text-xs" style={{ color: 'var(--sf-t-2)' }}>
          Palette
          <Select
            value={palette}
            onChange={(e) => onPaletteChange(e.target.value as Palette)}
          >
            {PALETTE_OPTIONS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </Select>
        </label>
        <Button
          variant="outline"
          size="icon"
          aria-label="Toggle dark / light"
          onClick={() => onModeChange(mode === 'dark' ? 'light' : 'dark')}
        >
          {mode === 'dark' ? <Sun size={14} /> : <Moon size={14} />}
        </Button>
      </div>
    </header>
  );
}
