import { useQuery } from '@tanstack/react-query';
import { Moon, Sun, Wifi, WifiOff } from 'lucide-react';
import { useTheme } from '@/components/theme/ThemeProvider';
import { Button } from '@/components/ui/button';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { adminApi, adminBase } from '@/lib/admin';
import { cn } from '@/lib/utils';

export function Header() {
  const { theme, toggleTheme } = useTheme();

  const health = useQuery({
    queryKey: ['healthz'],
    queryFn: () => adminApi.healthz(),
    refetchInterval: 2_000,
    retry: false,
  });

  const ok = health.data === 'ok';
  const failed = health.isError;

  return (
    <header className="h-12 shrink-0 flex items-center gap-3 px-4 border-b border-border bg-card">
      {/* Health pill */}
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            className={cn(
              'inline-flex items-center gap-1.5 h-7 rounded-md px-2 border text-[11px] font-mono uppercase tracking-[0.06em] transition-colors',
              ok
                ? 'bg-ok-muted border-ok/30 text-ok'
                : failed
                ? 'bg-err-muted border-err/30 text-err'
                : 'bg-muted border-border text-muted-foreground',
            )}
          >
            <span
              className={cn(
                'size-[7px] rounded-full',
                ok ? 'bg-ok' : failed ? 'bg-err blink-soft' : 'bg-muted-foreground',
              )}
            />
            {ok ? (
              <>
                <Wifi size={11} /> connected
              </>
            ) : failed ? (
              <>
                <WifiOff size={11} /> unreachable
              </>
            ) : (
              <>connecting…</>
            )}
          </button>
        </TooltipTrigger>
        <TooltipContent side="bottom">
          <code className="font-mono">{adminBase}</code>
        </TooltipContent>
      </Tooltip>

      {/* Endpoint label */}
      <div className="hidden md:flex items-baseline gap-1.5 text-[11px] text-muted-foreground">
        <span className="uppercase tracking-[0.08em]">admin</span>
        <code className="font-mono text-foreground">{adminBase || 'same origin'}</code>
      </div>

      <div className="flex-1" />

      {/* Theme toggle */}
      <Tooltip>
        <TooltipTrigger asChild>
          <Button
            variant="ghost"
            size="icon"
            onClick={toggleTheme}
            aria-label="toggle theme"
          >
            {theme === 'dark' ? <Sun size={14} /> : <Moon size={14} />}
          </Button>
        </TooltipTrigger>
        <TooltipContent side="bottom">
          {theme === 'dark' ? 'Switch to light' : 'Switch to dark'}
        </TooltipContent>
      </Tooltip>
    </header>
  );
}
