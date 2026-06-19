@echo off
REM Double-click to open the JV-Link settings dialog and disable the blocking "notice" prompt.
REM In the dialog: UNCHECK "JRA-VANからのお知らせを表示する" (and "払戻速報を表示する"), then click OK.
REM See tools\configure-jvlink-ui.ps1 for details.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0configure-jvlink-ui.ps1"
echo.
pause
