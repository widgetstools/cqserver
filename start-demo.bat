@echo off
REM Start the cqserver FI demo end-to-end on Windows.
REM
REM Mirrors start-demo.sh:
REM   1. cqserver (Rust release binary)
REM   2. Generate JSON tables (idempotent)
REM   3. Load JSON into the server
REM   4. Live publisher (market-data ticks + trades)
REM   5. React demo dev server
REM
REM PIDs + logs are written under .demo-run\ so stop-demo.bat can shut
REM everything down cleanly. Pure cmd.exe + wmic, no PowerShell, so
REM corporate execution-policy / AppLocker rules don't apply.
REM
REM Tools required on PATH: cmd.exe, curl.exe, netstat.exe, wmic.exe,
REM tasklist.exe, taskkill.exe, timeout.exe, findstr.exe, cargo, node,
REM npm, npx. wmic is being deprecated by Microsoft and is NOT installed
REM by default on Windows 11 24H2+; on those builds, add the optional
REM feature "WMIC" via Settings > Apps > Optional Features.

setlocal enabledelayedexpansion

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
set "RUN_DIR=%ROOT%\.demo-run"
set "SERVER_BIN=%ROOT%\target\release\cqserver.exe"
set "SERVER_CFG=%ROOT%\config\cqserver.toml"
set "ADMIN_URL=http://127.0.0.1:8085"

if not exist "%RUN_DIR%" mkdir "%RUN_DIR%"

echo ^> Pre-flight

REM Refuse to start on top of a previous run.
for %%N in (server publisher react-demo) do (
  if exist "%RUN_DIR%\%%N.pid" (
    set "PREV_PID="
    set /p PREV_PID=<"%RUN_DIR%\%%N.pid"
    if defined PREV_PID (
      set "PREV_PID=!PREV_PID: =!"
      tasklist /fi "PID eq !PREV_PID!" 2>nul | findstr /B "!PREV_PID! " >nul
      if !errorlevel! equ 0 (
        echo   x %%N already running ^(pid=!PREV_PID!^); run .\stop-demo.bat first
        exit /b 1
      )
    )
  )
)

REM Ports must be free. Only LISTENING sockets count -- CLOSE_WAIT
REM stragglers (browser leftovers) don't block a fresh bind.
for %%P in (9007 9008 8085 5173) do (
  for /f "tokens=5" %%i in ('netstat -ano -p TCP ^| findstr /R /C:":%%P *LISTENING"') do (
    echo   x Port %%P already in use ^(pid=%%i^)
    exit /b 1
  )
)
echo   + Ports 9007 9008 8085 5173 free

REM Build cqserver if missing.
if not exist "%SERVER_BIN%" (
  echo     cqserver binary not found - building release...
  pushd "%ROOT%"
  cargo build --release -p cq-server
  if !errorlevel! neq 0 ( popd & exit /b 1 )
  popd
)
echo   + cqserver binary present

REM npm install if needed.
for %%D in (clients\ts clients\react-demo) do (
  if not exist "%ROOT%\%%D\node_modules" (
    echo     Installing JS deps in %%D...
    pushd "%ROOT%\%%D"
    call npm install --silent
    if !errorlevel! neq 0 ( popd & exit /b 1 )
    popd
  )
)
echo   + JS deps installed

REM 1. cqserver
echo ^> Starting cqserver
REM `start /b cmd /c "..."` is the canonical pattern for a backgrounded
REM child whose stdout/stderr needs to land in a log file.
pushd "%ROOT%"
start "" /b cmd /c ""%SERVER_BIN%" --config "%SERVER_CFG%" > "%RUN_DIR%\server.log" 2>&1"
popd

REM Capture cqserver PID via wmic, anchored to binary path so we don't
REM catch cqserver instances from other checkouts. Sleep briefly so
REM the process is registered before we query.
timeout /t 1 /nobreak >nul
set "SERVER_PID="
REM wmic's /value output: `Key=Value` on its own line. We tokenize on `=`
REM and take the second token. Backslashes in the path need escaping for
REM the WMI WHERE clause (\\ -> \\\\).
set "WMIC_PATH=%SERVER_BIN:\=\\%"
for /f "tokens=2 delims==" %%i in ('wmic process where "ExecutablePath='%WMIC_PATH%'" get ProcessId /value 2^>nul ^| findstr /R "^ProcessId="') do (
  if not defined SERVER_PID set "SERVER_PID=%%i"
)
REM Strip any trailing CR (wmic emits CRLF; for /f keeps the CR).
if defined SERVER_PID set "SERVER_PID=!SERVER_PID:~0,-1!"
if not defined SERVER_PID (
  echo   x cqserver failed to start - check %RUN_DIR%\server.log
  exit /b 1
)
> "%RUN_DIR%\server.pid" echo !SERVER_PID!
echo     pid=!SERVER_PID!  log=%RUN_DIR%\server.log

REM Wait for healthz.
set "READY=0"
for /l %%n in (1,1,60) do (
  if "!READY!"=="0" (
    curl -fsS -m 1 %ADMIN_URL%/healthz >nul 2>&1
    if !errorlevel! equ 0 set "READY=1"
    if "!READY!"=="0" timeout /t 1 /nobreak >nul
  )
)
if "!READY!"=="0" (
  echo   x cqserver did not come up - check %RUN_DIR%\server.log
  exit /b 1
)
echo   + cqserver healthy

REM 2. Generate JSON
echo ^> Generating FI demo JSON
pushd "%ROOT%\clients\ts"
call npm run --silent generate-fi-data > "%RUN_DIR%\generate.log" 2>&1
if !errorlevel! neq 0 ( popd & echo   x generate-fi-data failed - see %RUN_DIR%\generate.log & exit /b 1 )
popd
echo   + JSON written to clients\ts\examples\data\

REM 3. Load JSON
echo ^> Loading data into cqserver
pushd "%ROOT%\clients\ts"
call npm run --silent load-fi-data > "%RUN_DIR%\load.log" 2>&1
if !errorlevel! neq 0 ( popd & echo   x load-fi-data failed - see %RUN_DIR%\load.log & exit /b 1 )
popd
echo   + Data loaded

REM 4. Live publisher
echo ^> Starting live publisher
pushd "%ROOT%\clients\ts"
start "" /b cmd /c "npx --no-install tsx examples/fi-publisher.ts > "%RUN_DIR%\publisher.log" 2>&1"
popd

REM Capture publisher PID -- the node.exe whose CommandLine includes
REM the publisher script. wmic LIKE uses % for wildcards.
timeout /t 2 /nobreak >nul
set "PUBLISHER_PID="
for /f "tokens=2 delims==" %%i in ('wmic process where "Name='node.exe' AND CommandLine LIKE '%%fi-publisher.ts%%'" get ProcessId /value 2^>nul ^| findstr /R "^ProcessId="') do (
  if not defined PUBLISHER_PID set "PUBLISHER_PID=%%i"
)
if defined PUBLISHER_PID set "PUBLISHER_PID=!PUBLISHER_PID:~0,-1!"
if defined PUBLISHER_PID > "%RUN_DIR%\publisher.pid" echo !PUBLISHER_PID!
echo     pid=!PUBLISHER_PID!  log=%RUN_DIR%\publisher.log

REM Wait for "Streaming:" line.
set "STREAMING=0"
for /l %%n in (1,1,60) do (
  if "!STREAMING!"=="0" (
    findstr /C:"Streaming:" "%RUN_DIR%\publisher.log" >nul 2>&1
    if !errorlevel! equ 0 set "STREAMING=1"
    if "!STREAMING!"=="0" timeout /t 1 /nobreak >nul
  )
)
if "!STREAMING!"=="0" (
  echo   x Publisher did not reach streaming phase - check %RUN_DIR%\publisher.log
  exit /b 1
)
echo   + Publisher streaming

REM 5. React dev server
echo ^> Starting React blotter dev server
pushd "%ROOT%\clients\react-demo"
start "" /b cmd /c "npx --no-install vite > "%RUN_DIR%\react-demo.log" 2>&1"
popd

timeout /t 2 /nobreak >nul
set "VITE_PID="
for /f "tokens=2 delims==" %%i in ('wmic process where "Name='node.exe' AND CommandLine LIKE '%%vite%%' AND CommandLine LIKE '%%react-demo%%'" get ProcessId /value 2^>nul ^| findstr /R "^ProcessId="') do (
  if not defined VITE_PID set "VITE_PID=%%i"
)
if defined VITE_PID set "VITE_PID=!VITE_PID:~0,-1!"
if defined VITE_PID > "%RUN_DIR%\react-demo.pid" echo !VITE_PID!
echo     pid=!VITE_PID!  log=%RUN_DIR%\react-demo.log

REM Wait for "Local:" line.
set "VITE_READY=0"
for /l %%n in (1,1,40) do (
  if "!VITE_READY!"=="0" (
    findstr /C:"Local:" "%RUN_DIR%\react-demo.log" >nul 2>&1
    if !errorlevel! equ 0 set "VITE_READY=1"
    if "!VITE_READY!"=="0" timeout /t 1 /nobreak >nul
  )
)
if "!VITE_READY!"=="0" (
  echo   x Vite did not start - check %RUN_DIR%\react-demo.log
  exit /b 1
)
echo   + React dev server up

echo.
echo Demo running.
echo.
echo   Admin UI       %ADMIN_URL%/
echo   FI dashboard   %ADMIN_URL%/fi-demo
echo   React blotter  http://127.0.0.1:5173/
echo.
echo   Logs           %RUN_DIR%
echo   Stop           .\stop-demo.bat

endlocal
