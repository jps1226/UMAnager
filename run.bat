@echo off
title Netkeiba Command Center Server
echo =========================================
echo Starting Netkeiba Command Center...
echo =========================================

:: Navigate to your project directory
cd /d C:\Users\ITSAdm\Documents\projects\api-20260313T123628Z-3-001\api

:: Open the default web browser to the local server address
start http://127.0.0.1:8000

:: Start the FastAPI server with live-reloading
uvicorn server:app --reload

pause