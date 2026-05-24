# Start the cqserver FI demo end-to-end on Windows.
#
# Mirrors start-demo.sh:
#   1. cqserver (Rust release binary)
#   2. Generate JSON tables (idempotent)
#   3. Load JSON into the server
#   4. Live publisher (market-data ticks + trades)
#   5. React demo dev server
#
# PIDs + logs are written under .demo-run\ so stop-demo.ps1 can shut
# everything down cleanly. Requires PowerShell 5.1+ (ships with
# Windows 10/11) and Node.js on PATH.

# Stop on uncaught errors; show them clearly.
$ErrorActionPreference = 'Stop'

$Root      = Split-Path -Parent $MyInvocation.MyCommand.Path
$RunDir    = Join-Path $Root '.demo-run'
$ServerBin = Join-Path $Root 'target\release\cqserver.exe'
$ServerCfg = Join-Path $Root 'config\cqserver.toml'
$AdminUrl  = 'http://127.0.0.1:8085'

New-Item -ItemType Directory -Force -Path $RunDir | Out-Null

function Step([string]$Msg) { Write-Host "> $Msg" -ForegroundColor Cyan }
function Info([string]$Msg) { Write-Host "  $Msg" -ForegroundColor DarkGray }
function Ok  ([string]$Msg) { Write-Host "  + $Msg" -ForegroundColor Green }
function Die ([string]$Msg) { Write-Host "  x $Msg" -ForegroundColor Red; exit 1 }

# ──────────────────────────────────────────────────────────────────
# Pre-flight checks
# ──────────────────────────────────────────────────────────────────

Step 'Pre-flight'

# Refuse to start on top of a previous run.
foreach ($name in 'server','publisher','react-demo') {
    $pidFile = Join-Path $RunDir "$name.pid"
    if (Test-Path $pidFile) {
        $existing = Get-Content $pidFile -ErrorAction SilentlyContinue
        if ($existing -and (Get-Process -Id $existing -ErrorAction SilentlyContinue)) {
            Die "$name already running (pid=$existing); run .\stop-demo.ps1 first"
        }
    }
}

# Ports must be free — only LISTENing sockets count. CLOSE_WAIT
# stragglers (browser leftovers) don't block a fresh bind.
foreach ($port in 9007, 9008, 8085, 5173) {
    $listener = Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue
    if ($listener) {
        Die "Port $port already in use (pid=$($listener[0].OwningProcess))"
    }
}
Ok 'Ports 9007 9008 8085 5173 free'

# Build the server release binary if missing.
if (-not (Test-Path $ServerBin)) {
    Info 'cqserver binary not found - building release...'
    Push-Location $Root
    try { cargo build --release -p cq-server } finally { Pop-Location }
}
Ok 'cqserver binary present'

# Make sure JS deps are installed for both clients/ts and clients/react-demo.
foreach ($d in 'clients\ts','clients\react-demo') {
    $nm = Join-Path $Root "$d\node_modules"
    if (-not (Test-Path $nm)) {
        Info "Installing JS deps in $d..."
        Push-Location (Join-Path $Root $d)
        try { npm install --silent } finally { Pop-Location }
    }
}
Ok 'JS deps installed'

# ──────────────────────────────────────────────────────────────────
# 1. cqserver
# ──────────────────────────────────────────────────────────────────

Step 'Starting cqserver'
$serverLog = Join-Path $RunDir 'server.log'
$serverErr = Join-Path $RunDir 'server.err.log'
$serverProc = Start-Process -FilePath $ServerBin `
    -ArgumentList @('--config', $ServerCfg) `
    -WorkingDirectory $Root `
    -RedirectStandardOutput $serverLog `
    -RedirectStandardError  $serverErr `
    -WindowStyle Hidden -PassThru
$serverProc.Id | Out-File -Encoding ASCII (Join-Path $RunDir 'server.pid')
Info "pid=$($serverProc.Id)  log=$serverLog"

# Wait for healthz.
$ready = $false
for ($i = 0; $i -lt 60; $i++) {
    try {
        Invoke-WebRequest -Uri "$AdminUrl/healthz" -UseBasicParsing -TimeoutSec 1 -ErrorAction Stop | Out-Null
        $ready = $true; break
    } catch { Start-Sleep -Milliseconds 250 }
}
if (-not $ready) {
    Die "cqserver did not come up - check $serverLog"
}
Ok 'cqserver healthy'

# ──────────────────────────────────────────────────────────────────
# 2. Generate JSON data
# ──────────────────────────────────────────────────────────────────

Step 'Generating FI demo JSON'
Push-Location (Join-Path $Root 'clients\ts')
try {
    & npm run --silent generate-fi-data > (Join-Path $RunDir 'generate.log') 2>&1
    if ($LASTEXITCODE -ne 0) { Die "generate-fi-data failed; see $RunDir\generate.log" }
} finally { Pop-Location }
Ok 'JSON written to clients\ts\examples\data\'

# ──────────────────────────────────────────────────────────────────
# 3. Load JSON into server
# ──────────────────────────────────────────────────────────────────

Step 'Loading data into cqserver'
Push-Location (Join-Path $Root 'clients\ts')
try {
    & npm run --silent load-fi-data > (Join-Path $RunDir 'load.log') 2>&1
    if ($LASTEXITCODE -ne 0) { Die "load-fi-data failed; see $RunDir\load.log" }
} finally { Pop-Location }
$loadLine = Select-String -Path (Join-Path $RunDir 'load.log') -Pattern '^Loaded in' -SimpleMatch -ErrorAction SilentlyContinue
Ok ($loadLine ? $loadLine.Line : 'loaded')

# ──────────────────────────────────────────────────────────────────
# 4. Live publisher
# ──────────────────────────────────────────────────────────────────

Step 'Starting live publisher'
$pubLog = Join-Path $RunDir 'publisher.log'
$pubProc = Start-Process -FilePath 'npx.cmd' `
    -ArgumentList @('--no-install', 'tsx', 'examples/fi-publisher.ts') `
    -WorkingDirectory (Join-Path $Root 'clients\ts') `
    -RedirectStandardOutput $pubLog `
    -RedirectStandardError  (Join-Path $RunDir 'publisher.err.log') `
    -WindowStyle Hidden -PassThru
$pubProc.Id | Out-File -Encoding ASCII (Join-Path $RunDir 'publisher.pid')
Info "pid=$($pubProc.Id)  log=$pubLog"

# Wait for the publisher to reach the streaming phase.
$streaming = $false
for ($i = 0; $i -lt 60; $i++) {
    if (Test-Path $pubLog) {
        if (Select-String -Path $pubLog -Pattern 'Streaming:' -SimpleMatch -Quiet -ErrorAction SilentlyContinue) {
            $streaming = $true; break
        }
    }
    Start-Sleep -Milliseconds 250
}
if (-not $streaming) {
    Die "Publisher did not reach streaming phase - check $pubLog"
}
Ok 'Publisher streaming'

# ──────────────────────────────────────────────────────────────────
# 5. React demo dev server
# ──────────────────────────────────────────────────────────────────

Step 'Starting React blotter dev server'
$viteLog = Join-Path $RunDir 'react-demo.log'
$viteProc = Start-Process -FilePath 'npx.cmd' `
    -ArgumentList @('--no-install', 'vite') `
    -WorkingDirectory (Join-Path $Root 'clients\react-demo') `
    -RedirectStandardOutput $viteLog `
    -RedirectStandardError  (Join-Path $RunDir 'react-demo.err.log') `
    -WindowStyle Hidden -PassThru
$viteProc.Id | Out-File -Encoding ASCII (Join-Path $RunDir 'react-demo.pid')
Info "pid=$($viteProc.Id)  log=$viteLog"

$viteReady = $false
for ($i = 0; $i -lt 40; $i++) {
    if (Test-Path $viteLog) {
        if (Select-String -Path $viteLog -Pattern 'Local:' -SimpleMatch -Quiet -ErrorAction SilentlyContinue) {
            $viteReady = $true; break
        }
    }
    Start-Sleep -Milliseconds 250
}
if (-not $viteReady) {
    Die "Vite did not start - check $viteLog"
}
Ok 'React dev server up'

# ──────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────

Write-Host ''
Write-Host 'Demo running.' -ForegroundColor Green
Write-Host ''
Write-Host "  Admin UI       $AdminUrl/"
Write-Host "  FI dashboard   $AdminUrl/fi-demo"
Write-Host '  React blotter  http://127.0.0.1:5173/'
Write-Host ''
Write-Host "  Logs           $RunDir"
Write-Host '  Stop           .\stop-demo.ps1'
