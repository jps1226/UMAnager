from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import asyncio
import pickle
import json
import os
import re
import datetime
import subprocess
import signal
import tempfile
import threading
from pathlib import Path
import pandas as pd
import logging
from typing import Any, Literal
from pydantic import BaseModel
import data_manager
import config
from routers.maintenance import router as maintenance_router
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

# Use paths from config
CACHE_FILE = config.CACHE_FILE
MARKS_FILE = config.MARKS_FILE
TRACKING_FILE = config.TRACKING_FILE
WATCHLIST_FILE = config.WATCHLIST_FILE
HORSE_DICT_FILE = config.HORSE_DICT_FILE

# --- NEW: CONSOLE LOGGING MEMORY ---
scrape_logs = []
scrape_logs_lock = threading.Lock()
scrape_job_lock = asyncio.Lock()


class ScrapeRequest(BaseModel):
    mode: Literal["new", "all"] = "new"


class ListsPayload(BaseModel):
    favorites: str = ""
    watchlist: str = ""


class SnipeRequest(BaseModel):
    url: str = ""
    id: str = ""
    list_type: Literal["favorites", "watchlist"] = "favorites"


class DeleteDayPayload(BaseModel):
    date: str
    scope: Literal["marks", "entries", "all"]


def atomic_write_json(path, payload):
    """Write JSON atomically to avoid partial/corrupt files on interruption."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=target.parent) as tmp:
        json.dump(payload, tmp, ensure_ascii=False, indent=4)
        tmp_path = tmp.name
    os.replace(tmp_path, target)


def atomic_write_pickle(path, payload):
    """Write pickle atomically to avoid partial/corrupt files on interruption."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", delete=False, dir=target.parent) as tmp:
        pickle.dump(payload, tmp)
        tmp_path = tmp.name
    os.replace(tmp_path, target)


def atomic_write_text(path, text):
    """Write text atomically to avoid partial/corrupt files on interruption."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=target.parent) as tmp:
        tmp.write(text)
        tmp_path = tmp.name
    os.replace(tmp_path, target)


def safe_read_json(path, default):
    """Load JSON with fallback to defaults when file is missing/corrupt."""
    target = Path(path)
    if not target.exists():
        return default
    try:
        with open(target, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Failed to read JSON from {target}: {e}")
        return default

def log_progress(msg):
    """Pushes new messages to the frontend console."""
    with scrape_logs_lock:
        scrape_logs.append(msg)
        # Keep the console from using too much memory on massive scrapes
        if len(scrape_logs) > config.MAX_CONSOLE_LOGS:
            scrape_logs.pop(0)

def validate_horse_id(horse_id):
    """Validate that horse_id matches Netkeiba ID format (10 alphanumeric)."""
    if not horse_id:
        return False
    horse_id_str = str(horse_id).strip()
    return bool(re.match(config.HORSE_ID_PATTERN, horse_id_str))

def validate_list_type(list_type):
    """Validate that list_type is one of the allowed values."""
    return list_type in ["favorites", "watchlist"]

def validate_url(url_str):
    """Extract and validate horse ID from a URL."""
    if not url_str:
        return None
    # Look for Netkeiba horse ID pattern in URL
    match = re.search(r'/([a-zA-Z0-9]{10})', url_str)
    return match.group(1) if match else None

def load_text_file(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f: return f.read()
    return ""

def load_ids(filepath):
    ids = set()
    text = load_text_file(filepath)
    for line in text.split('\n'):
        clean = line.split('#')[0].strip()
        if clean: ids.add(clean)
    return ids

def force_str(val):
    if not val or str(val) == 'nan' or str(val) == '---': return ""
    return str(val).split('.')[0].strip()

def load_cached_races():
    if not os.path.exists(CACHE_FILE):
        return []
    with open(CACHE_FILE, "rb") as f:
        return pickle.load(f)

def save_cached_races(races):
    atomic_write_pickle(CACHE_FILE, races)

def load_marks_data():
    return safe_read_json(MARKS_FILE, {})

def save_marks_data(marks):
    atomic_write_json(MARKS_FILE, marks)

def load_horse_dict_data():
    return safe_read_json(HORSE_DICT_FILE, {})

def save_horse_dict_data(horse_dict):
    atomic_write_json(HORSE_DICT_FILE, horse_dict)

def parse_sort_time(sort_time_str):
    """Parse a race sort_time string (YYYY-MM-DD HH:MM) safely."""
    if not sort_time_str:
        return None
    try:
        return datetime.datetime.strptime(str(sort_time_str), "%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        return None

def split_races_by_day_completion(races_by_date):
    """Classify each race day as upcoming or past; days move only as a full group."""
    now = datetime.datetime.now()
    today = now.date()
    upcoming = {}
    past = {}

    for date_str, races in races_by_date.items():
        day_datetimes = []
        day_date = None

        for race in races:
            info = race.get("info", {})
            dt_val = parse_sort_time(info.get("sort_time"))
            if dt_val:
                day_datetimes.append(dt_val)
                if day_date is None:
                    day_date = dt_val.date()

        # Move only when the whole day is considered complete.
        if day_datetimes:
            is_day_complete = now >= max(day_datetimes)
        else:
            # Fallback for missing times: move day after calendar day has passed.
            if day_date is None:
                try:
                    day_date = datetime.datetime.strptime(str(date_str), "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    day_date = today
            is_day_complete = day_date < today

        target = past if is_day_complete else upcoming
        target[date_str] = races

    return upcoming, past

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

# --- PEDIGREE LISTS & SNIPER ENDPOINTS ---
@app.get("/api/lists")
def get_lists():
    return {
        "favorites": load_text_file(TRACKING_FILE),
        "watchlist": load_text_file(WATCHLIST_FILE)
    }

@app.post("/api/lists")
async def save_lists(payload: ListsPayload):
    atomic_write_text(TRACKING_FILE, payload.favorites)
    atomic_write_text(WATCHLIST_FILE, payload.watchlist)
    return {"status": "success"}

@app.post("/api/snipe")
async def snipe_horse(payload: SnipeRequest):
    """Add a horse to favorites or watchlist."""
    url = payload.url.strip()
    direct_id = payload.id.strip()
    list_type = payload.list_type.strip()
    
    # Validate list_type
    if not validate_list_type(list_type):
        logger.warning(f"Invalid list_type: {list_type}")
        return {"status": "error", "message": "Invalid list type"}
    
    # Extract and validate horse ID
    new_id = None
    if direct_id:
        if validate_horse_id(direct_id):
            new_id = direct_id
        else:
            logger.warning(f"Invalid horse ID format: {direct_id}")
            return {"status": "error", "message": "Invalid horse ID format"}
    elif url:
        new_id = validate_url(url)
        if not new_id:
            logger.warning(f"Could not extract horse ID from URL: {url}")
            return {"status": "error", "message": "Invalid URL or horse ID format"}
    else:
        return {"status": "error", "message": "Either URL or ID must be provided"}
    
    # Check if horse is already tracked
    current_favorites = load_text_file(TRACKING_FILE)
    current_watchlist = load_text_file(WATCHLIST_FILE)
    
    if new_id in current_favorites or new_id in current_watchlist:
        logger.info(f"Horse {new_id} already tracked")
        return {"status": "error", "message": "ID already tracked."}
    
    # Add horse to list
    target_f = TRACKING_FILE if list_type == "favorites" else WATCHLIST_FILE
    current_c = load_text_file(target_f)
    
    try:
        h_data = data_manager.get_horse_data(new_id, "Unknown")
        h_name = h_data.get("name", "Unknown")
        
        prefix = "\n" if current_c and not current_c.endswith('\n') else ""
        with open(target_f, "a", encoding="utf-8") as f:
            f.write(f"{prefix}{new_id} # {h_name}\n")
        logger.info(f"Added horse {h_name} ({new_id}) to {list_type}")
        return {"status": "success", "message": f"Added {h_name}!"}
    except Exception as e:
        logger.error(f"Error adding horse: {e}")
        return {"status": "error", "message": "Failed to add horse"}

# --- CONFIG ENDPOINTS ---
CONFIG_FILE = "data/config.json"

def load_config():
    """Load config from file, return defaults if missing."""
    defaults = {
        "sidebarTabs": {"favorites": True, "watchlist": True, "weekendWatchlist": True},
        "ui": {"riskSlider": 50, "autoFetchPastResults": True}
    }
    return safe_read_json(CONFIG_FILE, defaults)

def save_config(config_data):
    """Save config to file."""
    atomic_write_json(CONFIG_FILE, config_data)

@app.get("/api/config")
def get_config():
    return load_config()

@app.post("/api/config")
async def update_config(config_data: dict[str, Any]):
    save_config(config_data)
    return {"status": "success"}

# --- MARKS & SCHEDULE ENDPOINTS ---
@app.get("/api/marks")
def get_marks():
    return safe_read_json(MARKS_FILE, {})

@app.post("/api/marks")
async def save_marks(marks: dict[str, str]):
    atomic_write_json(MARKS_FILE, marks)
    return {"status": "success"}

@app.get("/api/races")
def get_races():
    if not os.path.exists(CACHE_FILE):
        return {
            "top_picks": [],
            "races_by_date": {},
            "upcoming_races_by_date": {},
            "past_races_by_date": {}
        }

    app_cfg = load_config()
    weekend_races = load_cached_races()
    auto_history_enabled = app_cfg.get("ui", {}).get("autoFetchPastResults", True)
    if auto_history_enabled:
        auto_refreshed_races, auto_refreshed_entries = refresh_missing_past_race_history(weekend_races)
        if auto_refreshed_races:
            logger.info(
                "Auto history refresh complete: refreshed %s races and %s entries",
                auto_refreshed_races,
                auto_refreshed_entries,
            )
    tracked_ids = load_ids(TRACKING_FILE)
    watchlist_ids = load_ids(WATCHLIST_FILE)
    
    races_by_date = {}
    top_picks = []
    
    for race in weekend_races:
        info = race["info"]
        df = race["entries"].copy()
        date_str = info.get("clean_date", "Unknown Date")
        
        if date_str not in races_by_date: races_by_date[date_str] = []
        
        scores, icons, status = [], [], []
        for _, row in df.iterrows():
            fam = [force_str(row.get('Horse_ID')), force_str(row.get('Sire_ID')), force_str(row.get('Dam_ID')), force_str(row.get('BMS_ID'))]
            f_score = config.SCORE_TRACKED_HORSE if fam[0] in tracked_ids else (config.SCORE_TRACKED_SIRE if fam[1] in tracked_ids else 0.0)
            f_score += (config.SCORE_TRACKED_DAM if fam[2] in tracked_ids else 0.0) + (config.SCORE_TRACKED_BMS if fam[3] in tracked_ids else 0.0)
            w_score = config.SCORE_WATCHLIST_HORSE if fam[0] in watchlist_ids else (config.SCORE_WATCHLIST_SIRE if fam[1] in watchlist_ids else 0.0)
            w_score += (config.SCORE_WATCHLIST_DAM if fam[2] in watchlist_ids else 0.0) + (config.SCORE_WATCHLIST_BMS if fam[3] in watchlist_ids else 0.0)

            if f_score > 0:
                s, stat = min(f_score, config.SCORE_MAX), "FAV"
                icon = "⭐⭐⭐" if f_score >= config.ICON_THRESHOLD_3STAR else ("⭐⭐" if f_score >= config.ICON_THRESHOLD_2STAR else "⭐")
                if f_score >= config.ICON_THRESHOLD_3STAR: top_picks.append((date_str, info.get('time'), info.get('place'), row.get('Horse'), icon, info.get('race_id')))
            elif w_score > 0:
                s, stat = min(w_score, config.SCORE_MAX), "WATCH"
                icon = "👁️👁️" if w_score >= config.ICON_THRESHOLD_3STAR else "👁️"
                if w_score >= config.ICON_THRESHOLD_3STAR: top_picks.append((date_str, info.get('time'), info.get('place'), row.get('Horse'), icon, info.get('race_id')))
            else:
                s, stat, icon = 0.0, "", ""
                
            scores.append(s); status.append(stat); icons.append(icon)
            
        df['Match'], df['Score'], df['Status'] = icons, scores, status
        
        races_by_date[date_str].append({"info": info, "entries": df.to_dict(orient="records")})
        
    upcoming_races_by_date, past_races_by_date = split_races_by_day_completion(races_by_date)
    upcoming_dates = set(upcoming_races_by_date.keys())
    filtered_top_picks = [pick for pick in top_picks if pick[0] in upcoming_dates]

    return {
        "top_picks": filtered_top_picks,
        "races_by_date": upcoming_races_by_date,
        "upcoming_races_by_date": upcoming_races_by_date,
        "past_races_by_date": past_races_by_date
    }

def race_has_history_data(entries_df):
    if entries_df is None or entries_df.empty or "Finish" not in entries_df.columns:
        return False
    return any(force_str(val) for val in entries_df["Finish"].tolist())

def apply_history_map_to_race_entries(entries_df, history_map):
    if "Finish" not in entries_df.columns:
        entries_df["Finish"] = ""

    updated_count = 0
    for idx, row in entries_df.iterrows():
        horse_id = force_str(row.get("Horse_ID"))
        if horse_id not in history_map:
            continue

        hist = history_map[horse_id]
        row_changed = False

        if hist.get("odds", "") != "" and force_str(row.get("Odds")) != force_str(hist.get("odds", "")):
            entries_df.at[idx, "Odds"] = hist.get("odds", "")
            row_changed = True
        if hist.get("fav", "") != "" and force_str(row.get("Fav")) != force_str(hist.get("fav", "")):
            entries_df.at[idx, "Fav"] = hist.get("fav", "")
            row_changed = True
        if hist.get("finish", "") != "" and force_str(row.get("Finish")) != force_str(hist.get("finish", "")):
            entries_df.at[idx, "Finish"] = hist.get("finish", "")
            row_changed = True

        if row_changed:
            updated_count += 1

    return entries_df, updated_count

def refresh_cached_race_history(weekend_races, race_id: str, reason: str = "manual"):
    target_index = None
    for i, race in enumerate(weekend_races):
        if str(race.get("info", {}).get("race_id", "")) == str(race_id):
            target_index = i
            break

    if target_index is None:
        raise HTTPException(status_code=404, detail="Race not found in cache")

    history_map = data_manager.fetch_race_history_by_id(race_id)
    if not history_map:
        raise HTTPException(status_code=404, detail="No history data found for this race")

    race_obj = weekend_races[target_index]
    entries_df = race_obj["entries"].copy()
    entries_df, updated_count = apply_history_map_to_race_entries(entries_df, history_map)
    has_history_data = race_has_history_data(entries_df)

    race_obj["entries"] = entries_df
    race_obj["info"]["history_refreshed"] = has_history_data
    race_obj["info"]["history_refresh_reason"] = reason
    race_obj["info"]["history_refreshed_at"] = datetime.datetime.now().isoformat(timespec="seconds")
    return updated_count

def refresh_missing_past_race_history(weekend_races):
    races_by_date = {}
    for race in weekend_races:
        info = race.get("info", {})
        date_str = info.get("clean_date", "Unknown Date")
        races_by_date.setdefault(date_str, []).append({"info": info, "entries": []})

    _, past_races_by_date = split_races_by_day_completion(races_by_date)
    past_dates = set(past_races_by_date.keys())
    refreshed_races = 0
    refreshed_entries = 0
    changed = False

    for race in weekend_races:
        info = race.get("info", {})
        date_str = info.get("clean_date", "Unknown Date")
        race_id = str(info.get("race_id", "")).strip()
        entries_df = race.get("entries")

        if date_str not in past_dates or not race_id or entries_df is None or entries_df.empty:
            continue
        if info.get("history_refreshed") or race_has_history_data(entries_df):
            continue

        try:
            updated_count = refresh_cached_race_history(weekend_races, race_id, reason="auto")
        except HTTPException as exc:
            logger.info("Auto history refresh skipped for race %s: %s", race_id, exc.detail)
            continue

        refreshed_races += 1
        refreshed_entries += updated_count
        changed = True

    if changed:
        save_cached_races(weekend_races)

    return refreshed_races, refreshed_entries

@app.post("/api/races/{race_id}/refresh-history")
def refresh_race_history(race_id: str):
    weekend_races = load_cached_races()
    if not weekend_races:
        raise HTTPException(status_code=404, detail="No cached races found")

    updated_count = refresh_cached_race_history(weekend_races, race_id, reason="manual")
    save_cached_races(weekend_races)
    return {"status": "success", "updated_entries": updated_count}

@app.post("/api/races/upcoming/refresh")
def refresh_upcoming_races():
    weekend_races = load_cached_races()
    if not weekend_races:
        raise HTTPException(status_code=404, detail="No cached races found")

    races_by_date = {}
    for race in weekend_races:
        info = race.get("info", {})
        date_str = info.get("clean_date", "Unknown Date")
        races_by_date.setdefault(date_str, []).append({"info": info, "entries": []})

    upcoming_races_by_date, _ = split_races_by_day_completion(races_by_date)
    upcoming_dates = set(upcoming_races_by_date.keys())

    updated_races = 0
    updated_rows = 0
    failed_races = []

    for race in weekend_races:
        info = race.get("info", {})
        date_str = info.get("clean_date", "Unknown Date")
        if date_str not in upcoming_dates:
            continue

        race_id = str(info.get("race_id", "")).strip()
        if not race_id:
            continue

        snap = data_manager.fetch_upcoming_race_snapshot(race_id)
        if not snap:
            failed_races.append(race_id)
            continue

        old_df = race.get("entries")
        new_df = snap.get("entries")
        if not isinstance(old_df, pd.DataFrame) or not isinstance(new_df, pd.DataFrame):
            failed_races.append(race_id)
            continue

        new_map = {}
        for _, n_row in new_df.iterrows():
            h_id = force_str(n_row.get("Horse_ID"))
            if h_id:
                new_map[h_id] = n_row

        row_updates_in_race = 0
        for idx, o_row in old_df.iterrows():
            h_id = force_str(o_row.get("Horse_ID"))
            if h_id not in new_map:
                continue

            n_row = new_map[h_id]
            for col in ["BK", "PP", "Odds", "Fav"]:
                new_val = str(n_row.get(col, "")).strip()
                if new_val and new_val.lower() != "nan":
                    old_df.at[idx, col] = new_val
            row_updates_in_race += 1

        race["entries"] = old_df

        fresh_info = snap.get("info", {})
        for key in ["time", "sort_time", "clean_date", "race_name", "place", "date", "race_number"]:
            if key in fresh_info and str(fresh_info.get(key, "")).strip() != "":
                info[key] = fresh_info[key]
        info["upcoming_refreshed_at"] = datetime.datetime.now().isoformat(timespec="seconds")

        updated_races += 1
        updated_rows += row_updates_in_race

    save_cached_races(weekend_races)
    return {
        "status": "success",
        "updated_races": updated_races,
        "updated_rows": updated_rows,
        "failed_races": failed_races
    }

@app.post("/api/day/delete")
async def delete_day_data(payload: DeleteDayPayload):
    target_date = payload.date.strip()
    scope = payload.scope

    if not target_date:
        raise HTTPException(status_code=400, detail="Missing day/date")

    weekend_races = load_cached_races()
    target_race_ids = set()
    target_horse_ids = set()

    for race in weekend_races:
        info = race.get("info", {})
        if str(info.get("clean_date", "")).strip() != target_date:
            continue
        target_race_ids.add(str(info.get("race_id", "")).strip())

        entries = race.get("entries")
        if isinstance(entries, pd.DataFrame):
            for _, row in entries.iterrows():
                h_id = force_str(row.get("Horse_ID"))
                if h_id:
                    target_horse_ids.add(h_id)

    removed_races = 0
    removed_marks = 0
    removed_horses = 0

    if scope in {"entries", "all"}:
        filtered_races = [
            race for race in weekend_races
            if str(race.get("info", {}).get("clean_date", "")).strip() != target_date
        ]
        removed_races = len(weekend_races) - len(filtered_races)
        save_cached_races(filtered_races)

    if scope in {"marks", "all"} and target_race_ids:
        marks = load_marks_data()
        new_marks = {}
        for key, val in marks.items():
            race_prefix = key.split("_", 1)[0]
            if race_prefix in target_race_ids:
                removed_marks += 1
                continue
            new_marks[key] = val
        save_marks_data(new_marks)

    if scope == "all" and target_horse_ids:
        horse_dict = load_horse_dict_data()
        for h_id in target_horse_ids:
            if h_id in horse_dict:
                del horse_dict[h_id]
                removed_horses += 1
        save_horse_dict_data(horse_dict)

    return {
        "status": "success",
        "date": target_date,
        "scope": scope,
        "removed_races": removed_races,
        "removed_marks": removed_marks,
        "removed_horse_entries": removed_horses,
        "matched_races": len(target_race_ids)
    }

@app.post("/api/server/shutdown")
def shutdown_server(background_tasks: BackgroundTasks):
    background_tasks.add_task(shutdown_server_instances, 8000)
    return {"status": "success", "message": "Shutdown signal sent."}
