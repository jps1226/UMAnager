@echo off
title Netkeiba Command Center Server
echo =========================================
echo Starting Netkeiba Command Center...
echo =========================================

:: Navigate to your project directory
cd /d C:\Users\ITSAdm\Documents\projects\UMAnager

:: Open the default web browser to the local server address
start http://127.0.0.1:8000

:: Start the FastAPI server with live-reloading
uvicorn server:app --reload

pause