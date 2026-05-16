@echo off
setlocal enabledelayedexpansion

echo.
echo ========================================
echo   UMAnager Sidecar (x86, JV-Link COM)
echo ========================================
echo.

cd /d "%~dp0src\UMAnager.Sidecar" || (
    echo ERROR: Failed to change directory to Sidecar folder.
    pause
    exit /b 1
)

echo Starting Sidecar...
echo Expected: "[Sidecar] JVInit returned: 0" then "[Sidecar] Waiting for Nexus connection..."
echo.
echo Open another terminal and run: run-nexus.bat
echo.

dotnet run

echo.
echo Sidecar stopped.
pause
