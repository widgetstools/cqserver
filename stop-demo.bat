@echo off
REM Stop the cqserver demo on Windows.
REM
REM Mirrors stop-demo.sh:
REM   Phase 1 - graceful: kill PIDs recorded by start-demo.bat in
REM             .demo-run\*.pid (with tree-kill).
REM   Phase 2 - sweep: cqserver by binary path + anything still
REM             listening on the demo ports.
REM
REM Pure cmd.exe + wmic. No PowerShell, no execution-policy concerns.
REM See start-demo.bat header for the wmic deprecation note on 24H2+.

setlocal enabledelayedexpansion

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
set "RUN_DIR=%ROOT%\.demo-run"

echo ^> Phase 1: tracked processes
if exist "%RUN_DIR%" (
  for %%N in (react-demo publisher server) do (
    if exist "%RUN_DIR%\%%N.pid" (
      set "PID="
      set /p PID=<"%RUN_DIR%\%%N.pid"
      if defined PID (
        REM Strip any trailing whitespace / CR.
        set "PID=!PID: =!"
        tasklist /fi "PID eq !PID!" 2>nul | findstr /B "!PID! " >nul
        if !errorlevel! equ 0 (
          echo   stopping %%N ^(pid=!PID!^)
          taskkill /pid !PID! /t /f >nul 2>&1
          if !errorlevel! equ 0 (
            echo   + %%N stopped
          ) else (
            echo   x %%N taskkill returned non-zero
          )
        ) else (
          echo   - %%N already stopped
        )
      )
      del "%RUN_DIR%\%%N.pid" 2>nul
    )
  )
) else (
  echo   - no .demo-run dir; skipping tracked-process phase
)

echo ^> Phase 2: port + binary sweep

REM Kill cqserver by binary path. Anchored to this repo's target\ so we
REM never touch cqserver instances from other checkouts. wmic WHERE
REM needs backslashes doubled.
for %%P in ("%ROOT%\target\release\cqserver.exe" "%ROOT%\target\debug\cqserver.exe") do (
  set "WMIC_PATH=%%~P"
  set "WMIC_PATH=!WMIC_PATH:\=\\!"
  for /f "tokens=2 delims==" %%i in ('wmic process where "ExecutablePath='!WMIC_PATH!'" get ProcessId /value 2^>nul ^| findstr /R "^ProcessId="') do (
    set "FOUND_PID=%%i"
    set "FOUND_PID=!FOUND_PID:~0,-1!"
    if defined FOUND_PID (
      echo   stopping cqserver ^(pid=!FOUND_PID!^)
      taskkill /pid !FOUND_PID! /t /f >nul 2>&1
    )
  )
)

REM Kill anything still listening on the demo ports (catches Vite +
REM publisher + cqserver missed by the binary-path sweep).
for %%P in (9007 9008 8085 5173) do (
  for /f "tokens=5" %%i in ('netstat -ano -p TCP ^| findstr /R /C:":%%P *LISTENING"') do (
    if not "%%i"=="0" (
      echo   stopping listener on :%%P ^(pid=%%i^)
      taskkill /pid %%i /t /f >nul 2>&1
    )
  )
)

REM Final state summary.
set /a STILL=0
for %%P in (9007 9008 8085 5173) do (
  netstat -ano -p TCP | findstr /R /C:":%%P *LISTENING" >nul
  if !errorlevel! equ 0 set /a STILL+=1
)
if !STILL! equ 0 (
  echo Done -- all demo ports free.
) else (
  echo Done -- !STILL! demo port^(s^) still bound ^(check manually^).
)

endlocal
