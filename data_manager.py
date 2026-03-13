import keibascraper
import pandas as pd
import datetime
from zoneinfo import ZoneInfo
import requests
from bs4 import BeautifulSoup
import re
import os
import pickle
import pykakasi
import json
import time
import logging

logger = logging.getLogger(__name__)

# 1. Setup Offline Translators & Caches
kks = pykakasi.kakasi()

TRACK_TRANSLATIONS = {
    "札幌": "Sapporo", "函館": "Hakodate", "福島": "Fukushima",
    "新潟": "Niigata", "東京": "Tokyo", "中山": "Nakayama",
    "中京": "Chukyo", "京都": "Kyoto", "阪神": "Hanshin", "小倉": "Kokura"
}

CACHE_FILE = "race_cache.pkl"
HORSE_DICT_FILE = "horse_names.json"

if os.path.exists(HORSE_DICT_FILE):
    with open(HORSE_DICT_FILE, "r", encoding="utf-8") as f:
        HORSE_CACHE = json.load(f)
else:
    HORSE_CACHE = {}

def safe_request(url, timeout=5, retries=2):
    """Make HTTP request with automatic retry and error handling."""
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
            response.raise_for_status()
            return response
        except requests.Timeout:
            logger.warning(f"Timeout on attempt {attempt + 1}/{retries} for {url}")
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)
        except requests.RequestException as e:
            logger.error(f"Request failed on attempt {attempt + 1}/{retries} for {url}: {e}")
            if attempt == retries - 1:
                return None
            time.sleep(2 ** attempt)
    return None

def save_horse_dict():
    with open(HORSE_DICT_FILE, "w", encoding="utf-8") as f:
        json.dump(HORSE_CACHE, f, indent=4, ensure_ascii=False)

def romanize(text):
    if not text or pd.isna(text): return ""
    text = re.sub(r'\s+', ' ', str(text)).strip()
    
    eng_match = re.match(r'^([A-Za-z0-9\s\-\.\']+)', text)
    if eng_match:
        clean_eng = eng_match.group(1).strip()
        words = clean_eng.split()
        if len(words) >= 2:
            for seq_len in range(1, len(words) // 2 + 1):
                seq = " ".join(words[:seq_len])
                repeats = clean_eng.count(seq)
                if repeats >= 2 and clean_eng.replace(seq, "").strip() == "":
                    if repeats >= 3 or len(seq.split()) > 1:
                        clean_eng = seq
                        break
        if len(clean_eng) > 2: return clean_eng.title()
            
    result = kks.convert(text)
    return " ".join([item['hepburn'].title() for item in result]).replace("  ", " ").strip()

def fetch_official_name_by_id(horse_id, jp_fallback):
    if not horse_id: return romanize(jp_fallback)
    str_id = str(horse_id)
    if str_id in HORSE_CACHE and isinstance(HORSE_CACHE[str_id], dict) and HORSE_CACHE[str_id].get("name"):
        return HORSE_CACHE[str_id]["name"]
        
    logger.info(f"Sniping parent profile: {jp_fallback}...")
    time.sleep(0.3) 
    official_name = romanize(jp_fallback)
    url = f"https://db.netkeiba.com/horse/ped/{str_id}/"
    
    try:
        response = safe_request(url)
        if response:
            response.encoding = 'euc-jp'
            soup = BeautifulSoup(response.text, 'html.parser')
        
        eng_link = soup.find('a', href=re.compile(r'en\.netkeiba\.com/db/horse/'))
        if eng_link and eng_link.text.strip(): official_name = eng_link.text.strip()
        else:
            eng_p = soup.find('p', class_='eng_name')
            if eng_p and eng_p.text.strip(): official_name = eng_p.text.strip()
            else:
                h1_tag = soup.find('div', class_='horse_title')
                if h1_tag and h1_tag.find('h1'): official_name = romanize(h1_tag.find('h1').text.strip())
    except Exception: pass
        
    HORSE_CACHE[str_id] = {
        "name": official_name,
        "sire": "", "dam": "", "bms": "",
        "sire_id": "", "dam_id": "", "bms_id": ""
    }
    return official_name

def get_horse_data(horse_id, jp_name):
    str_id = str(horse_id).replace('.0', '').strip()
    if str_id in HORSE_CACHE and isinstance(HORSE_CACHE[str_id], dict):
        cached = HORSE_CACHE[str_id]
        if cached.get("sire_id") and cached.get("sire_id") != "":
            if "record" not in cached or cached["record"] == "0/0" or cached["record"] == "":
                try:
                    res = requests.get(f"https://db.netkeiba.com/horse/{str_id}/", headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
                    res.encoding = 'euc-jp'
                    m = re.search(r'(\d+)戦(\d+)勝', res.text)
                    if m: cached["record"] = f"{m.group(2)}/{m.group(1)}"
                except: pass
                if "record" not in cached: cached["record"] = "0/0"
                HORSE_CACHE[str_id] = cached
            return cached

    data = {
        "name": romanize(jp_name),
        "sire": "", "dam": "", "bms": "",
        "sire_id": "", "dam_id": "", "bms_id": "",
        "record": "0/0"
    }
    
    if not str_id or str_id == 'nan' or str_id == '---': return data
        
    logger.info(f"Deep Scraping {jp_name} (ID: {str_id})...")
    time.sleep(0.3) 
    
    try:
        res_main = safe_request(f"https://db.netkeiba.com/horse/{str_id}/")
        if res_main:
            res_main.encoding = 'euc-jp'
            m = re.search(r'(\d+)戦(\d+)勝', res_main.text)
            if m: data["record"] = f"{m.group(2)}/{m.group(1)}"
    except Exception as e:
        logger.warning(f"Failed to fetch main horse data: {e}")

    try:
        res_ped = safe_request(f"https://db.netkeiba.com/horse/ped/{str_id}/")
        if res_ped:
            res_ped.encoding = 'euc-jp' 
            soup_ped = BeautifulSoup(res_ped.text, 'html.parser')
            
            eng_link = soup_ped.find('a', href=re.compile(r'en\.netkeiba\.com/db/horse/'))
            if eng_link and eng_link.text.strip(): data["name"] = eng_link.text.strip()
            else:
                eng_p = soup_ped.find('p', class_='eng_name')
                if eng_p and eng_p.text.strip(): data["name"] = eng_p.text.strip()
                else:
                    h1_tag = soup_ped.find('div', class_='horse_title')
                    if h1_tag and h1_tag.find('h1'): data["name"] = romanize(h1_tag.find('h1').text.strip())
                
            blood_table = soup_ped.find('table', class_='blood_table')
            if blood_table:
                td_16s = blood_table.find_all('td', rowspan="16")
                if len(td_16s) >= 2:
                    sire_a = td_16s[0].find('a')
                    dam_a = td_16s[1].find('a')
                    bms_td = td_16s[1].find_next_sibling('td', rowspan="8")
                    bms_a = bms_td.find('a') if bms_td else None

                    def get_id(a_tag):
                        if not a_tag: return ""
                        match = re.search(r'/([a-zA-Z0-9]{10})/?', a_tag.get('href', ''))
                        return match.group(1) if match else ""
                    
                    data["sire_id"] = get_id(sire_a)
                    data["dam_id"] = get_id(dam_a)
                    data["bms_id"] = get_id(bms_a)
                    
                    data["sire"] = fetch_official_name_by_id(data["sire_id"], sire_a.text.strip() if sire_a else "")
                    data["dam"] = fetch_official_name_by_id(data["dam_id"], dam_a.text.strip() if dam_a else "")
                    data["bms"] = fetch_official_name_by_id(data["bms_id"], bms_a.text.strip() if bms_a else "")
    except Exception as e: logger.error(f"Error fetching pedigree: {e}")

    HORSE_CACHE[str_id] = data
    return data

def get_next_weekend_dates():
    today = datetime.date.today()
    days_ahead_sat = (5 - today.weekday()) % 7
    if days_ahead_sat == 0: days_ahead_sat = 7
    next_sat = today + datetime.timedelta(days=days_ahead_sat)
    next_sun = next_sat + datetime.timedelta(days=1)
    return next_sat, next_sun

def fetch_real_post_time(race_id):
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    try:
        response = safe_request(url)
        if response:
            soup = BeautifulSoup(response.text, 'html.parser')
            data_div = soup.find('div', class_='RaceData01')
            if data_div:
                match = re.search(r'(\d{2}:\d{2})', data_div.text)
                if match: return match.group(1)
    except Exception as e:
        logger.warning(f"Failed to fetch post time for {race_id}: {e}")
    return None

# --- NEW: HTML Sniper for Predicted Odds/Fav ---
def fetch_predictions(race_id):
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    api_url = f"https://race.netkeiba.com/api/api_get_jra_odds.html?race_id={race_id}&type=1&action=init"
    predictions = {}
    
    try:
        res = safe_request(url)
        if not res:
            logger.warning(f"Failed to fetch HTML for race {race_id}")
            return predictions
            
        res.encoding = 'euc-jp'
        soup = BeautifulSoup(res.text, 'html.parser')
        
        horse_indexes = {} 
        umaban_to_horse = {} 
        
        logger.debug(f"Debugging race {race_id}")
        
        for tr in soup.find_all('tr', class_=re.compile(r'HorseList')):
            row_html = str(tr)
            id_match = re.search(r'/horse/(\d{10})', row_html) or re.search(r'myhorse_(\d{10})', row_html)
            if not id_match: continue
            
            h_id = id_match.group(1)
            
            tr_id = tr.get('id', '')
            if tr_id.startswith('tr_'):
                horse_indexes[tr_id.replace('tr_', '').strip()] = h_id
                
            td_umaban = tr.find('td', class_=re.compile(r'Umaban'))
            if td_umaban:
                u_text = re.sub(r'\D', '', td_umaban.text)
                if u_text:
                    umaban_to_horse[str(int(u_text))] = h_id

        logger.debug(f"Found {len(horse_indexes)} Internal IDs and {len(umaban_to_horse)} Umaban IDs")
                    
        headers = {"User-Agent": "Mozilla/5.0", "Referer": url, "X-Requested-With": "XMLHttpRequest"}
        api_res = safe_request(api_url, retries=1)
        
        if not api_res:
            logger.warning(f"Failed to fetch API for race {race_id}")
            return predictions
        
        logger.debug(f"API Status Code: {api_res.status_code}")
        
        try:
            data = api_res.json()
            status = data.get("status")
            logger.debug(f"API JSON Status: '{status}'")
            
            if status in ["success", "middle", "yoso"]: 
                odds_root = data.get("data", {})
                
                odds_level_1 = odds_root.get("odds", {})
                logger.debug(f"Keys in 'data.odds': {list(odds_level_1.keys())[:5]}")
                
                odds_data = odds_level_1.get("1", {})
                logger.debug(f"Keys in 'data.odds.1': {list(odds_data.keys())[:5]}")
                
                if not odds_data:
                    logger.error("'odds_data' is empty! The API structure may have changed.")
                
                match_count = 0
                for key_str, values in odds_data.items():
                    clean_key = str(int(key_str)) if key_str.isdigit() else key_str
                    
                    h_id = None
                    if status in ["success", "middle"]: 
                        h_id = umaban_to_horse.get(clean_key)
                    elif status == "yoso":
                        h_id = horse_indexes.get(clean_key)
                        
                    if h_id:
                        o_val = str(values[0]) if len(values) > 0 else ""
                        f_val = str(values[2]) if len(values) > 2 else ""
                        if o_val and o_val != "0.0":
                            predictions[h_id] = {"odds": o_val, "fav": f_val}
                            match_count += 1
                
                logger.debug(f"Successfully mapped {match_count} horses.")
            else:
                logger.warning(f"API returned unexpected status: {str(data)[:200]}")
                
        except Exception as json_err:
            logger.error(f"Failed to parse API JSON: {json_err}")
            logger.error(f"Raw text snippet: {api_res.text[:200]}")
                        
    except Exception as e:
        logger.error(f"Fatal Prediction fetch error: {e}", exc_info=True)
        
    return predictions

def format_entry_data(entry_list, predictions=None):
    if not entry_list: return pd.DataFrame()
    df = pd.DataFrame(entry_list)
    
    # --- NEW: Drop phantom extra rows (like empty footers) immediately ---
    if 'horse_id' in df.columns:
        df = df[~df['horse_id'].astype(str).str.lower().isin(['nan', 'none', ''])]
    
    formatted = pd.DataFrame()
    
    def get_col(options, default=""):
        for opt in options:
            if opt in df.columns:
                return df[opt].fillna(default).astype(str).str.replace(r'\.0$', '', regex=True).replace('nan', default).tolist()
        return [default] * len(df)

    formatted['BK'] = get_col(['bracket_number', 'bracket', 'bk', '枠番'])
    formatted['PP'] = get_col(['horse_number', 'pp', 'num', '馬番'])
    
    horse_ids = get_col(['horse_id'])
    jp_names = get_col(['horse_name', '馬名'])
    
    names, sires, dams, bms_list, records = [], [], [], [], []
    s_ids, d_ids, b_ids, h_ids = [], [], [], []
    
    for h_id, j_name in zip(horse_ids, jp_names):
        clean_id = str(h_id).replace('.0', '').strip()
        h_data = get_horse_data(clean_id, j_name)
        
        names.append(h_data["name"])
        sires.append(h_data["sire"])
        dams.append(h_data["dam"])
        bms_list.append(h_data["bms"])
        records.append(h_data.get("record", "0/0"))
        
        h_ids.append(clean_id) 
        s_ids.append(h_data.get("sire_id", ""))
        d_ids.append(h_data.get("dam_id", ""))
        b_ids.append(h_data.get("bms_id", ""))

    formatted['Horse'] = names
    formatted['Record'] = records
    formatted['Sire'] = sires
    formatted['Dam'] = dams
    formatted['BMS'] = bms_list
    
    if predictions:
        formatted['Odds'] = [predictions.get(h, {}).get('odds', "") for h in h_ids]
        formatted['Fav'] = [predictions.get(h, {}).get('fav', "") for h in h_ids]
    else:
        formatted['Odds'] = get_col(['win_odds', 'odds', '単勝オッズ'], "")
        formatted['Fav'] = get_col(['popularity', 'pop', 'fav', '人気'], "")
        
    formatted['Horse_ID'] = h_ids
    formatted['Sire_ID'] = s_ids
    formatted['Dam_ID'] = d_ids
    formatted['BMS_ID'] = b_ids
    
    return formatted

def fetch_weekend_timeline(mode="load", progress_callback=None):
    next_sat, next_sun = get_next_weekend_dates()
    target_year, target_month = next_sat.year, next_sat.month
    
    cached_races = []
    if mode in ["load", "new"] and os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "rb") as f:
            cached_races = pickle.load(f)
            
    if mode == "load" and cached_races: return cached_races

    print(f"\n--- SCRAPE MODE: {mode.upper()} ---")
    all_race_ids = keibascraper.race_list(target_year, target_month)
    total_races = len(all_race_ids)
    
    existing_ids = [r["info"]["race_id"] for r in cached_races] if mode == "new" else []
    weekend_races = list(cached_races) if mode == "new" else []
    skip_prefixes = set() 
    
    jst_zone = ZoneInfo("Asia/Tokyo")
    ct_zone = ZoneInfo("America/Chicago") 
    
    for i, race_id in enumerate(all_race_ids):
        # --- NEW: Test Mode Safety Brake ---
        if mode == "test" and len(weekend_races) >= 3:
            msg = f"🧪 Test Mode Complete: Stopped at 3 races to protect IP."
            print(msg)
            if progress_callback: progress_callback(msg)
            break
        # -----------------------------------
        
        str_id = str(race_id)
        if str_id in existing_ids:
            msg = f"[{i + 1}/{total_races}] {str_id}... Cached."
            print(msg)
            if progress_callback: progress_callback(msg)
            continue
            
        prefix = str_id[:-2] 
        if prefix in skip_prefixes: continue
            
        msg = f"[{i + 1}/{total_races}] Checking {str_id}..."
        print(msg, end=" ")
        if progress_callback: progress_callback(msg)
        
        try:
            result = keibascraper.load("entry", race_id)
            if isinstance(result, tuple) and len(result) == 2 and result[0]:
                race_info = result[0][0]
                entry_list = result[1]
                date_str = race_info.get('date', race_info.get('race_date', ''))
                
                if date_str:
                    clean_date_str = date_str.split(' ')[0] 
                    race_date = pd.to_datetime(clean_date_str).date()
                    
                    if race_date in [next_sat, next_sun]:
                        place = race_info.get('place', race_info.get('course', ''))
                        race_info['place'] = TRACK_TRANSLATIONS.get(place, place)
                        race_info['race_name'] = romanize(race_info.get('race_name', ''))
                        race_info['race_id'] = str_id
                        
                        jst_time = fetch_real_post_time(race_id)
                        if jst_time:
                            dt_str = f"{clean_date_str} {jst_time}"
                            try:
                                race_info['clean_date'] = str(race_date) 
                                dt_jst = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=jst_zone)
                                dt_ct = dt_jst.astimezone(ct_zone)
                                race_info['time'] = dt_ct.strftime("%I:%M %p")
                                race_info['sort_time'] = dt_ct.strftime("%Y-%m-%d %H:%M")
                            except Exception as e:
                                race_info['clean_date'] = str(race_date)
                                race_info['time'] = jst_time
                                race_info['sort_time'] = f"{race_date} {jst_time}"
                        else:
                            race_info['clean_date'] = str(race_date)
                            race_info['time'] = "TBA"
                            race_info['sort_time'] = f"{race_date} 00:00"                        
                        
                        msg2 = f"MATCH! -> Scraping Data for Race {str_id}..."
                        print(msg2)
                        if progress_callback: progress_callback(msg2)
                        
                        # Fetch the predicted odds & fav right now
                        preds = fetch_predictions(str_id)
                        formatted_entries = format_entry_data(entry_list, preds)
                        
                        weekend_races.append({
                            "info": race_info,
                            "entries": formatted_entries
                        })
                        save_horse_dict() 
                    else: skip_prefixes.add(prefix) 
        except Exception as e:
            print(f"Failed to parse. Error: {e}")
            continue
            
    def safe_int(val):
        try: return int(val)
        except: return 99

    weekend_races.sort(key=lambda x: (
        x["info"].get("clean_date", "2099-12-31"),
        safe_int(x["info"].get("race_number", 99)),
        x["info"].get("sort_time", "23:59"),         
        x["info"].get("place", "")
    ))
    
    with open(CACHE_FILE, "wb") as f: pickle.dump(weekend_races, f)
    return weekend_races
