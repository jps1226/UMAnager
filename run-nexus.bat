@echo off
setlocal enabledelayedexpansion

echo.
echo ========================================
echo   UMAnager Nexus (x64, ASP.NET Core)
echo ========================================
echo.

cd /d "%~dp0src\UMAnager.Nexus" || (
    echo ERROR: Failed to change directory to Nexus folder.
    pause
    exit /b 1
)

echo Starting Nexus...
echo Expected: "[Nexus] Sidecar responded: Version=..."
echo.
echo Once running, open: http://localhost:5000
echo.

dotnet run

echo.
echo Nexus stopped.
pause
