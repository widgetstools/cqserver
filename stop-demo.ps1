# Stop the cqserver demo on Windows.
#
# Mirrors stop-demo.sh:
#   Phase 1 - graceful: kill PIDs recorded by start-demo.ps1 in
#             .demo-run\*.pid and their direct children.
#   Phase 2 - sweep: anything still listening on the demo ports or
#             matching the cqserver binary path, just to be sure.

$ErrorActionPreference = 'Continue'

$Root   = Split-Path -Parent $MyInvocation.MyCommand.Path
$RunDir = Join-Path $Root '.demo-run'

# Ports the demo BINDS. Anything listening here is presumed demo state.
$ListenPorts = @(9007, 9008, 8085, 5173)

function Info([string]$Msg) { Write-Host "  $Msg" -ForegroundColor DarkGray }
function Ok  ([string]$Msg) { Write-Host "  + $Msg" -ForegroundColor Green }
function Err ([string]$Msg) { Write-Host "  x $Msg" -ForegroundColor Red }
function Step([string]$Msg) { Write-Host "> $Msg" -ForegroundColor Cyan }

# Kill helper: TERM the PID + its direct children, escalate to
# Stop-Process -Force after a short grace period.
function Stop-PidTree {
    param([int]$ProcessId, [string]$Description)

    $proc = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if (-not $proc) { return }

    Write-Host "  stopping $Description (pid=$ProcessId)"

    # Find direct children via WMI (ParentProcessId). Bounded so we
    # don't touch unrelated processes.
    $children = Get-CimInstance Win32_Process `
        -Filter "ParentProcessId=$ProcessId" `
        -ErrorAction SilentlyContinue
    foreach ($c in $children) {
        try { Stop-Process -Id $c.ProcessId -ErrorAction SilentlyContinue } catch {}
    }
    try { $proc.CloseMainWindow() | Out-Null } catch {}

    # Wait up to ~900ms for graceful exit, then SIGKILL-equivalent.
    for ($i = 0; $i -lt 3; $i++) {
        if (-not (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)) { break }
        Start-Sleep -Milliseconds 300
    }
    if (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue) {
        try { Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue } catch {}
        Err "$Description force-killed (pid=$ProcessId)"
    } else {
        Ok "$Description stopped (pid=$ProcessId)"
    }
}

# ── Phase 1: tracked PIDs from start-demo.ps1 ──────────────────
if (Test-Path $RunDir) {
    Step 'Phase 1: tracked processes'
    foreach ($name in 'react-demo','publisher','server') {
        $pidFile = Join-Path $RunDir "$name.pid"
        if (Test-Path $pidFile) {
            $raw = (Get-Content $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
            $existing = 0
            $null = [int]::TryParse(($raw -as [string]).Trim(), [ref]$existing)
            if ($existing -gt 0 -and (Get-Process -Id $existing -ErrorAction SilentlyContinue)) {
                Stop-PidTree -ProcessId $existing -Description $name
            } else {
                Info "$name already stopped"
            }
            Remove-Item $pidFile -ErrorAction SilentlyContinue
        }
    }
} else {
    Info 'No .demo-run dir; skipping tracked-process phase.'
}

# ── Phase 2: sweep ports + binary paths ────────────────────────
Step 'Phase 2: port + binary sweep'

# 2a) Kill cqserver by binary path. Anchored to this repo's target\
# so we never touch cqserver instances from other checkouts. Catches
# servers started without start-demo.ps1.
$serverPaths = @(
    (Join-Path $Root 'target\release\cqserver.exe'),
    (Join-Path $Root 'target\debug\cqserver.exe')
)
foreach ($p in $serverPaths) {
    $matches = Get-CimInstance Win32_Process `
        -Filter "Name='cqserver.exe'" `
        -ErrorAction SilentlyContinue |
        Where-Object { $_.ExecutablePath -and ($_.ExecutablePath -ieq $p) }
    foreach ($m in $matches) {
        Stop-PidTree -ProcessId $m.ProcessId -Description "cqserver ($p)"
    }
}

# 2b) Listeners on every demo port (catches vite + anything else).
foreach ($port in $ListenPorts) {
    $listeners = Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue
    foreach ($l in $listeners) {
        Stop-PidTree -ProcessId $l.OwningProcess -Description "listener on :$port"
    }
}

# ── Final state summary ────────────────────────────────────────
$stillBound = 0
foreach ($port in $ListenPorts) {
    if (Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue) {
        $stillBound++
    }
}
if ($stillBound -eq 0) {
    Write-Host 'Done - all demo ports free.' -ForegroundColor Green
} else {
    Write-Host "Done - $stillBound demo port(s) still bound (check manually)." -ForegroundColor Yellow
}
