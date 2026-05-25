import { Moon, Sun, Github, Activity } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { useTheme } from '@/components/theme/ThemeProvider';

export function AtlasHeader() {
  const { theme, toggleTheme } = useTheme();
  return (
    <header className="h-12 border-b border-border bg-card flex items-center px-4 gap-3 shrink-0">
      <div className="flex items-center gap-2.5">
        <div className="size-6 rounded-sm bg-signal-muted border border-signal/40 flex items-center justify-center">
          <span className="text-signal text-[10px] font-mono font-bold tracking-tight">cq</span>
        </div>
        <div className="flex flex-col leading-none">
          <span className="text-[13.5px] font-semibold tracking-tight">cqserver</span>
          <span className="atlas-eyebrow !text-[9px] mt-0.5">
            <span className="dot">●</span> ATLAS
          </span>
        </div>
      </div>
      <div className="hairline-vert h-6 mx-2" />
      <div className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
        <Activity size={11} className="text-ok" />
        <span className="font-mono">examples-web @ :5175</span>
      </div>
      <div className="flex-1" />
      <Button variant="ghost" size="iconSm" onClick={toggleTheme} aria-label="Toggle theme">
        {theme === 'dark' ? <Sun size={13} /> : <Moon size={13} />}
      </Button>
      <a
        href="https://github.com/widgetstools/cqserver"
        target="_blank"
        rel="noreferrer"
        className="inline-flex items-center justify-center h-7 w-7 rounded-md text-foreground hover:bg-accent transition-colors"
        aria-label="GitHub"
      >
        <Github size={13} />
      </a>
    </header>
  );
}
