@echo off
setlocal enabledelayedexpansion

echo.
echo ========================================
echo   UMAnager v2.0 — Full System Launch
echo ========================================
echo.
echo Launching Sidecar and Nexus in separate windows...
echo.

cd /d "%~dp0" || (
    echo ERROR: Failed to change directory to root.
    pause
    exit /b 1
)

REM Launch Sidecar in a new window
start "UMAnager Sidecar" cmd /k "call run-sidecar.bat"

REM Give Sidecar a moment to initialize before Nexus tries to connect
timeout /t 2 /nobreak

REM Launch Nexus in a new window
start "UMAnager Nexus" cmd /k "call run-nexus.bat"

echo.
echo ========================================
echo Both processes launched in separate windows.
echo.
echo Sidecar terminal: Watch for "[Sidecar] Waiting for Nexus connection..."
echo Nexus terminal:   Watch for "[Nexus] Sidecar responded..."
echo.
echo Once both are ready, open your browser:
echo   Local:  http://localhost:5000
echo   Network: http://100.109.190.38:5000
echo.
echo To stop: Close both terminal windows.
echo ========================================
echo.
pause
