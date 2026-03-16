@echo off
title UMAnager Server
echo =========================================
echo Starting UMAnager...
echo =========================================

:: Get the directory where this batch file is located
set "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"

:: Open the default web browser to the local server address
:: Using a slight delay to ensure the server is ready
start http://127.0.0.1:8000

:: Start the FastAPI server with live-reloading
uvicorn server:app --reload --host 127.0.0.1 --port 8000 --log-level info > server.log 2>&1
