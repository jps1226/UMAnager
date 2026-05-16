@echo off
setlocal enabledelayedexpansion

echo.
echo ========================================
echo   Kill Hanging Processes
echo ========================================
echo.

REM Kill any running dotnet processes
echo Killing any running dotnet.exe processes...
taskkill /F /IM dotnet.exe >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo  OK - Killed dotnet processes
) else (
    echo  (no dotnet processes found)
)

REM Kill any Command Prompt windows that might be running our services
echo Killing any Command Prompt windows...
taskkill /F /IM cmd.exe >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo  OK - Killed cmd.exe windows
) else (
    echo  (no cmd.exe windows found)
)

REM Wait a moment for cleanup
timeout /t 2 /nobreak

echo.
echo ========================================
echo Ready to restart. Run: run-all.bat
echo ========================================
echo.
pause
