from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import asyncio
import os
import subprocess
import signal
import threading
import logging
from typing import Literal
from pydantic import BaseModel
import data_manager
import config
from routers.maintenance import router as maintenance_router
from routers.lists_config import router as lists_config_router
from routers.races import router as races_router, set_progress_logger
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(maintenance_router)
app.include_router(lists_config_router)
app.include_router(races_router)

# --- NEW: CONSOLE LOGGING MEMORY ---
scrape_logs = []
scrape_logs_lock = threading.Lock()
scrape_job_lock = asyncio.Lock()


class ScrapeRequest(BaseModel):
    mode: Literal["new", "all"] = "new"


def log_progress(msg):
    """Pushes new messages to the frontend console."""
    with scrape_logs_lock:
        scrape_logs.append(msg)
        # Keep the console from using too much memory on massive scrapes
        if len(scrape_logs) > config.MAX_CONSOLE_LOGS:
            scrape_logs.pop(0)


set_progress_logger(log_progress)

def find_listening_pids(port=8000):
    """Find PIDs listening on a TCP port (Windows and POSIX best effort)."""
    pids = set()
    try:
        res = subprocess.run(["netstat", "-ano", "-p", "tcp"], capture_output=True, text=True, check=False)
        for line in res.stdout.splitlines():
            line_u = line.upper()
            if f":{port}" not in line or "LISTEN" not in line_u:
                continue
            parts = line.split()
            if parts and parts[-1].isdigit():
                pids.add(int(parts[-1]))
    except Exception as e:
        logger.warning(f"Could not inspect netstat for port {port}: {e}")
    return pids

def terminate_pid(pid):
    """Terminate a process by PID with platform-specific commands."""
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, text=True, check=False)
        else:
            os.kill(pid, signal.SIGTERM)
    except Exception as e:
        logger.warning(f"Failed to terminate PID {pid}: {e}")

def shutdown_server_instances(port=8000):
    """Best-effort shutdown for server instances on the configured port."""
    pids = find_listening_pids(port=port)
    pids.add(os.getpid())
    for pid in pids:
        terminate_pid(pid)

@app.get("/")
def read_root():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

# --- DATA MANAGEMENT ENDPOINTS ---
@app.post("/api/scrape")
async def run_scrape(payload: ScrapeRequest):
    global scrape_logs

    if scrape_job_lock.locked():
        return {"status": "busy", "message": "A scrape job is already running."}

    with scrape_logs_lock:
        scrape_logs = ["Initializing Netkeiba Scraper..."]

    mode = payload.mode

    async with scrape_job_lock:
        # Run scraper in a worker thread so the server can keep serving logs.
        await asyncio.to_thread(data_manager.fetch_weekend_timeline, mode=mode, progress_callback=log_progress)

    with scrape_logs_lock:
        scrape_logs.append("Done! Refreshing schedule...")
    return {"status": "success"}

@app.get("/api/scrape/log")
def get_scrape_log():
    with scrape_logs_lock:
        return {"logs": list(scrape_logs)}

@app.post("/api/server/shutdown")
def shutdown_server(background_tasks: BackgroundTasks):
    background_tasks.add_task(shutdown_server_instances, 8000)
    return {"status": "success", "message": "Shutdown signal sent."}
