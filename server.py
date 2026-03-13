from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import asyncio # NEW: Needed to unblock the server during scraping
import pickle
import json
import os
import re
import pandas as pd
import data_manager

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

CACHE_FILE = "race_cache.pkl"
MARKS_FILE = "saved_marks.json"
TRACKING_FILE = "tracked_horses.txt"
WATCHLIST_FILE = "watchlist_horses.txt"

# --- NEW: CONSOLE LOGGING MEMORY ---
scrape_logs = []

def log_progress(msg):
    """Pushes new messages to the frontend console."""
    scrape_logs.append(msg)
    # Keep the console from using too much memory on massive scrapes
    if len(scrape_logs) > 100:
        scrape_logs.pop(0)

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

@app.get("/")
def read_root():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

# --- DATA MANAGEMENT ENDPOINTS ---
@app.post("/api/scrape")
async def run_scrape(request: Request):
    global scrape_logs
    scrape_logs = ["Initializing Netkeiba Scraper..."]
    
    data = await request.json()
    mode = data.get("mode", "new")
    
    # NEW: Run the scraper in a background thread so the server can still serve logs!
    await asyncio.to_thread(data_manager.fetch_weekend_timeline, mode=mode, progress_callback=log_progress)
    
    scrape_logs.append("Done! Refreshing schedule...")
    return {"status": "success"}

@app.get("/api/scrape/log")
def get_scrape_log():
    return {"logs": scrape_logs}

@app.post("/api/cache/clear")
def clear_cache():
    if os.path.exists(CACHE_FILE): os.remove(CACHE_FILE)
    return {"status": "success"}

@app.post("/api/dict/wipe")
def wipe_dict():
    if os.path.exists("horse_names.json"): os.remove("horse_names.json")
    return {"status": "success"}

# --- PEDIGREE LISTS & SNIPER ENDPOINTS ---
@app.get("/api/lists")
def get_lists():
    return {
        "favorites": load_text_file(TRACKING_FILE),
        "watchlist": load_text_file(WATCHLIST_FILE)
    }

@app.post("/api/lists")
async def save_lists(request: Request):
    data = await request.json()
    with open(TRACKING_FILE, "w", encoding="utf-8") as f: f.write(data.get("favorites", ""))
    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f: f.write(data.get("watchlist", ""))
    return {"status": "success"}

@app.post("/api/snipe")
async def snipe_horse(request: Request):
    data = await request.json()
    url = data.get("url", "")
    direct_id = data.get("id", "") 
    list_type = data.get("list_type", "favorites")
    
    new_id = ""
    if direct_id:
        new_id = str(direct_id).strip()
    else:
        id_match = re.search(r'/([a-zA-Z0-9]{10})', url)
        if id_match: new_id = id_match.group(1)
            
    if not new_id or len(new_id) != 10:
        return {"status": "error", "message": "Invalid ID or URL"}
    
    target_f = TRACKING_FILE if list_type == "favorites" else WATCHLIST_FILE
    current_c = load_text_file(target_f)
    
    if new_id not in load_text_file(TRACKING_FILE) and new_id not in load_text_file(WATCHLIST_FILE):
        h_data = data_manager.get_horse_data(new_id, "Unknown")
        h_name = h_data.get("name", "Unknown")
        
        prefix = "\n" if current_c and not current_c.endswith('\n') else ""
        with open(target_f, "a", encoding="utf-8") as f:
            f.write(f"{prefix}{new_id} # {h_name}\n")
        return {"status": "success", "message": f"Added {h_name}!"}
    
    return {"status": "error", "message": "ID already tracked."}

# --- MARKS & SCHEDULE ENDPOINTS ---
@app.get("/api/marks")
def get_marks():
    if os.path.exists(MARKS_FILE):
        with open(MARKS_FILE, "r", encoding="utf-8") as f: return json.load(f)
    return {}

@app.post("/api/marks")
async def save_marks(request: Request):
    marks = await request.json()
    with open(MARKS_FILE, "w", encoding="utf-8") as f: json.dump(marks, f, ensure_ascii=False, indent=4)
    return {"status": "success"}

@app.get("/api/races")
def get_races():
    if not os.path.exists(CACHE_FILE): return {"top_picks": [], "races_by_date": {}}
        
    with open(CACHE_FILE, "rb") as f: weekend_races = pickle.load(f)
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
            f_score = 1.0 if fam[0] in tracked_ids else (0.5 if fam[1] in tracked_ids else 0.0)
            f_score += (0.5 if fam[2] in tracked_ids else 0.0) + (0.25 if fam[3] in tracked_ids else 0.0)
            w_score = 1.0 if fam[0] in watchlist_ids else (0.5 if fam[1] in watchlist_ids else 0.0)
            w_score += (0.5 if fam[2] in watchlist_ids else 0.0) + (0.25 if fam[3] in watchlist_ids else 0.0)

            if f_score > 0:
                s, stat = min(f_score, 1.0), "FAV"
                icon = "⭐⭐⭐" if f_score >= 1.0 else ("⭐⭐" if f_score >= 0.5 else "⭐")
                if f_score >= 1.0: top_picks.append((date_str, info.get('time'), info.get('place'), row.get('Horse'), icon, info.get('race_id')))
            elif w_score > 0:
                s, stat = min(w_score, 1.0), "WATCH"
                icon = "👁️👁️" if w_score >= 1.0 else "👁️"
                if w_score >= 1.0: top_picks.append((date_str, info.get('time'), info.get('place'), row.get('Horse'), icon, info.get('race_id')))
            else:
                s, stat, icon = 0.0, "", ""
                
            scores.append(s); status.append(stat); icons.append(icon)
            
        df['Match'], df['Score'], df['Status'] = icons, scores, status
        
        races_by_date[date_str].append({"info": info, "entries": df.to_dict(orient="records")})
        
    return {"top_picks": top_picks, "races_by_date": races_by_date}
