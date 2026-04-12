from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import os
import subprocess
import signal
import logging
import re
from html import unescape
from dotenv import load_dotenv
import requests

load_dotenv()

from routers.maintenance import router as maintenance_router
from routers.lists_config import router as lists_config_router
from routers.races import router as races_router, set_progress_logger
from routers.scrape import router as scrape_router, log_progress
from routers.orepro import router as orepro_router
from routers.jvlink import router as jvlink_router
from storage import init_storage_foundation

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
app.include_router(scrape_router)
app.include_router(orepro_router)
app.include_router(jvlink_router)


set_progress_logger(log_progress)


@app.on_event("startup")
def initialize_storage_foundation():
    init_storage_foundation()

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

@app.get("/tv")
def tv_mode():
    with open("tv.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


@app.get("/api/gch/free-embed-url")
def get_gch_free_embed_url():
    """Extract the current free-player iframe URL from sp.gch.jp/jra."""
    source_url = "https://sp.gch.jp/jra"
    try:
        response = requests.get(
            source_url,
            timeout=10,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36"
                )
            },
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch {source_url}: {exc}") from exc

    if not response.ok:
        raise HTTPException(status_code=502, detail=f"Upstream returned HTTP {response.status_code}")

    html_text = response.text or ""
    # Free video iframe observed in HAR/MHTML as players.streaks.jp/.../index.html?m=...
    match = re.search(
        r'<iframe[^>]+src=["\'](https://players\.streaks\.jp/[^"\']+/index\.html\?m=[^"\']+)["\']',
        html_text,
        flags=re.IGNORECASE,
    )
    if not match:
        raise HTTPException(status_code=404, detail="Could not find free embed iframe URL in upstream HTML")

    embed_url = unescape(match.group(1))
    return {"embedUrl": embed_url, "source": source_url}


@app.get("/api/gch/live-playback-json")
def get_gch_live_playback_json():
    """Resolve current live playback metadata from gch APIs used by jra_player.js."""
    session_url = "https://sp.gch.jp/api/vij"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Referer": "https://sp.gch.jp/jra",
        "Accept": "application/json,text/plain,*/*",
    }
    try:
        vij_resp = requests.get(session_url, timeout=10, headers=headers)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch {session_url}: {exc}") from exc

    if not vij_resp.ok:
        raise HTTPException(status_code=502, detail=f"Session endpoint returned HTTP {vij_resp.status_code}")

    try:
        vij_json = vij_resp.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Session endpoint returned invalid JSON") from exc

    project_id = str(vij_json.get("project_id") or "").strip()
    media_id = str(vij_json.get("id") or "").strip()
    api_key = str(vij_json.get("api_key") or "").strip()
    if not project_id or not media_id or not api_key:
        raise HTTPException(status_code=502, detail="Session JSON missing project_id/id/api_key")

    playback_url = f"https://playback.api.streaks.jp/v1/projects/{project_id}/medias/{media_id}"
    try:
        playback_resp = requests.get(
            playback_url,
            timeout=12,
            headers={
                "X-Streaks-Api-Key": api_key,
                "Content-Type": "application/json",
                "User-Agent": headers["User-Agent"],
                "Referer": "https://sp.gch.jp/",
                "Origin": "https://sp.gch.jp",
                "Accept": "application/json,text/plain,*/*",
            },
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch playback metadata: {exc}") from exc

    if not playback_resp.ok:
        raise HTTPException(
            status_code=502,
            detail=f"Playback endpoint returned HTTP {playback_resp.status_code}",
        )

    try:
        playback_json = playback_resp.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Playback endpoint returned invalid JSON") from exc

    return {
        "session": {
            "project_id": project_id,
            "id": media_id,
        },
        "playback": playback_json,
    }

@app.post("/api/server/shutdown")
def shutdown_server(background_tasks: BackgroundTasks):
    background_tasks.add_task(shutdown_server_instances, 8000)
    return {"status": "success", "message": "Shutdown signal sent."}
