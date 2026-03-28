import keibascraper
import pandas as pd
import datetime
from zoneinfo import ZoneInfo
import requests
from bs4 import BeautifulSoup
import re
import os
import pykakasi
import json
import time
import logging
import config
from storage import (
    load_horse_cache_map,
    load_race_cache,
    save_race_cache,
    upsert_horse_cache_entries,
    normalize_data_engine,
)
from jra_van_storage import get_race_keys_in_date_range, get_cache_dates_in_range

logger = logging.getLogger(__name__)

# 1. Setup Offline Translators & Caches
kks = pykakasi.kakasi()

HORSE_CACHE = load_horse_cache_map()
HORSE_CACHE_DIRTY_IDS = set()


def clear_horse_runtime_cache():
    cleared_count = len(HORSE_CACHE)
    HORSE_CACHE.clear()
    HORSE_CACHE_DIRTY_IDS.clear()
    return cleared_count

def safe_request(url, timeout=None, retries=None):
    """Make HTTP request with automatic retry and error handling."""
    if timeout is None:
        timeout = config.REQUEST_TIMEOUT
    if retries is None:
        retries = config.REQUEST_RETRIES
        
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
    if not HORSE_CACHE_DIRTY_IDS:
        return
    payload = {
        horse_id: HORSE_CACHE[horse_id]
        for horse_id in list(HORSE_CACHE_DIRTY_IDS)
        if horse_id in HORSE_CACHE
    }
    upsert_horse_cache_entries(payload)
    HORSE_CACHE_DIRTY_IDS.clear()


def _set_horse_cache_entry(horse_id, data):
    clean_horse_id = str(horse_id or "").replace('.0', '').strip()
    if not clean_horse_id:
        return
    HORSE_CACHE[clean_horse_id] = data
    HORSE_CACHE_DIRTY_IDS.add(clean_horse_id)

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
        if len(clean_eng) > config.MIN_NAME_LENGTH: return clean_eng.title()
            
    result = kks.convert(text)
    return " ".join([item['hepburn'].title() for item in result]).replace("  ", " ").strip()

def fetch_official_name_by_id(horse_id, jp_fallback):
    if not horse_id: return romanize(jp_fallback)
    str_id = str(horse_id)
    if str_id in HORSE_CACHE and isinstance(HORSE_CACHE[str_id], dict) and HORSE_CACHE[str_id].get("name"):
        return HORSE_CACHE[str_id]["name"]
        
    logger.info(f"Sniping parent profile: {jp_fallback}...")
    time.sleep(config.SCRAPE_DELAY) 
    official_name = romanize(jp_fallback)
    url = f"{config.NETKEIBA_PEDIGREE_URL}/{str_id}/"
    
    try:
        response = safe_request(url)
        if not response:
            raise RuntimeError("No response for pedigree profile request")

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
    except Exception as e:
        logger.debug(f"Failed to fetch official name for {str_id}: {e}")
        
    _set_horse_cache_entry(str_id, {
        "name": official_name,
        "sire": "", "dam": "", "bms": "",
        "sire_id": "", "dam_id": "", "bms_id": ""
    })
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
                except Exception as e:
                    logger.debug(f"Failed to refresh cached record for {str_id}: {e}")
                if "record" not in cached: cached["record"] = "0/0"
                _set_horse_cache_entry(str_id, cached)
            return cached

    data = {
        "name": romanize(jp_name),
        "sire": "", "dam": "", "bms": "",
        "sire_id": "", "dam_id": "", "bms_id": "",
        "record": "0/0"
    }
    
    if not str_id or str_id == 'nan' or str_id == '---': return data
        
    logger.info(f"Deep Scraping {jp_name} (ID: {str_id})...")
    time.sleep(config.SCRAPE_DELAY) 
    
    try:
        res_main = safe_request(f"{config.NETKEIBA_HORSE_URL}/{str_id}/")
        if res_main:
            res_main.encoding = 'euc-jp'
            m = re.search(r'(\d+)戦(\d+)勝', res_main.text)
            if m: data["record"] = f"{m.group(2)}/{m.group(1)}"
    except Exception as e:
        logger.warning(f"Failed to fetch main horse data: {e}")

    try:
        res_ped = safe_request(f"{config.NETKEIBA_PEDIGREE_URL}/{str_id}/")
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

    _set_horse_cache_entry(str_id, data)
    return data

def get_upcoming_weekend_dates(weeks_ahead=None):
    """Return upcoming Sat/Sun dates starting from the next available Saturday."""
    if weeks_ahead is None:
        weeks_ahead = max(1, int(getattr(config, "UPCOMING_WEEKEND_WEEKS", 3)))

    today = datetime.datetime.now(ZoneInfo("Asia/Tokyo")).date()
    days_until_sat = (5 - today.weekday()) % 7
    first_sat = today + datetime.timedelta(days=days_until_sat)

    target_dates = set()
    for week_offset in range(weeks_ahead):
        sat = first_sat + datetime.timedelta(days=7 * week_offset)
        sun = sat + datetime.timedelta(days=1)
        target_dates.add(sat)
        target_dates.add(sun)

    return target_dates


def extract_race_date(race_info):
    date_str = race_info.get('date', race_info.get('race_date', ''))
    if not date_str:
        return None
    clean_date_str = str(date_str).split(' ')[0]
    try:
        return pd.to_datetime(clean_date_str).date()
    except Exception:
        return None


def _get_month_race_ids(year: int, month: int):
    try:
        return [str(rid) for rid in keibascraper.race_list(year, month)]
    except Exception as e:
        # Future month calendars are often unavailable until closer to race days.
        logger.info(f"Race list unavailable for {year}-{month:02d}: {e}")
        return []


def get_month_race_ids(year: int, month: int):
    """Public wrapper for month race-list discovery used by check-only prefetch scans."""
    race_ids = _get_month_race_ids(year, month)
    return sorted({str(rid).strip() for rid in race_ids if str(rid).strip()})


def _get_race_ids_from_daily_list(target_date):
    """Fallback race-id discovery from Netkeiba daily race list page."""
    if isinstance(target_date, datetime.datetime):
        target_date = target_date.date()
    if isinstance(target_date, str):
        target_date = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()

    kaisai_date = target_date.strftime("%Y%m%d")
    url = f"https://race.netkeiba.com/top/race_list_sub.html?kaisai_date={kaisai_date}"
    resp = safe_request(url, timeout=15, retries=2)
    if not resp or not resp.text:
        return []

    ids = sorted(set(re.findall(r"race_id=(\d{12})", resp.text)))
    return ids


def get_race_ids_for_date(target_date):
    """Find all race IDs whose entry metadata matches the given date."""
    if isinstance(target_date, str):
        target_date = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()

    race_ids = set(_get_race_ids_from_daily_list(target_date))

    if race_ids:
        return sorted(race_ids)

    all_race_ids = _get_month_race_ids(target_date.year, target_date.month)

    for race_id in all_race_ids:
        try:
            result = keibascraper.load("entry", race_id)
        except Exception:
            continue
        if not (isinstance(result, tuple) and len(result) == 2 and result[0]):
            continue

        race_info = result[0][0]
        race_date = extract_race_date(race_info)
        if race_date == target_date:
            race_ids.add(str(race_id))

    return sorted(race_ids)


def _normalize_source_mode(source_mode):
    mode = str(source_mode or "").strip().lower()
    if mode in {"nk", "jv", "auto"}:
        return mode
    cfg_mode = str(getattr(config, "SCRAPE_SOURCE_MODE", "nk") or "nk").strip().lower()
    return cfg_mode if cfg_mode in {"nk", "jv", "auto"} else "nk"


def _race_id12_from_jv_key16(race_key_16):
    key = str(race_key_16 or "").strip()
    if not re.fullmatch(r"\d{16}", key):
        return ""
    # Preserve existing internal race-id shape (12 digits): YYYY + JYO(2) + KAIJI(2) + NICHIJI(2) + RACE(2)
    return f"{key[0:4]}{key[8:16]}"


def _get_jv_race_ids_for_window(start_date, end_date, allow_nk_assist=True):
    start_text = str(start_date)
    end_text = str(end_date)
    try:
        keys = get_race_keys_in_date_range(start_text, end_text)
    except Exception as exc:
        logger.warning(f"JV race-key lookup failed for {start_text}..{end_text}: {exc}")
        return []

    race_ids = []
    for key in keys:
        rid = _race_id12_from_jv_key16(key)
        if rid:
            race_ids.append(rid)

    race_ids = sorted(set(race_ids))
    if race_ids:
        return race_ids

    # Fallback path: infer active dates from JV cache index metadata and
    # expand those dates to race_ids using the existing daily list discovery.
    if not allow_nk_assist:
        return []

    # The lookback tolerance (start_date may be before today) is intentional for
    # the race_keys table but must NOT be applied to the daily-list expansion —
    # past dates yield completed races whose date check will always fail in the
    # scrape loop.  Clamp to today so we only discover upcoming race IDs.
    today_dt = datetime.date.today()
    daily_list_start = max(today_dt, start_date if isinstance(start_date, datetime.date)
                          else datetime.datetime.strptime(start_text, "%Y-%m-%d").date())
    try:
        covered_dates = get_cache_dates_in_range(str(daily_list_start), end_text, specs=["RA", "SE", "BN", "JG", "TK"])
    except Exception as exc:
        logger.warning(f"JV cache-date lookup failed for {start_text}..{end_text}: {exc}")
        return []

    for day_text in covered_dates:
        try:
            day_dt = datetime.datetime.strptime(day_text, "%Y-%m-%d").date()
        except Exception:
            continue
        race_ids.extend(_get_race_ids_from_daily_list(day_dt))

    # Always also try upcoming weekend dates that fall inside the query window.
    # This ensures the current/next weekend races are found even when the JV cache
    # hasn't been refreshed yet for those specific dates.
    try:
        covered_date_set = set(covered_dates)
        end_dt = datetime.datetime.strptime(end_text, "%Y-%m-%d").date()
        for wd in sorted(get_upcoming_weekend_dates()):
            if daily_list_start <= wd <= end_dt:
                wd_text = wd.strftime("%Y-%m-%d")
                if wd_text not in covered_date_set:
                    race_ids.extend(_get_race_ids_from_daily_list(wd))
    except Exception as exc:
        logger.debug(f"Weekend date supplement failed: {exc}")

    # If daily-list expansion yields nothing, try month-level discovery for covered dates.
    if not race_ids and covered_dates:
        year_months = set()
        for day_text in covered_dates:
            try:
                d = datetime.datetime.strptime(day_text, "%Y-%m-%d").date()
            except Exception:
                continue
            year_months.add((d.year, d.month))
        for y, m in sorted(year_months):
            race_ids.extend(_get_month_race_ids(y, m))

    return sorted(set(str(r).strip() for r in race_ids if str(r).strip()))

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


def _build_jv_placeholder_race_snapshot(race_id, resolved_source_mode, resolved_data_engine):
    race_id_text = str(race_id or "").strip()
    if not re.fullmatch(r"\d{12}", race_id_text):
        return None

    year = race_id_text[0:4]
    jyo_cd = race_id_text[4:6]
    kaiji = race_id_text[6:8]
    nichiji = race_id_text[8:10]
    race_num = race_id_text[10:12]

    jyo_map = {
        "01": "Sapporo",
        "02": "Hakodate",
        "03": "Fukushima",
        "04": "Niigata",
        "05": "Tokyo",
        "06": "Nakayama",
        "07": "Chukyo",
        "08": "Kyoto",
        "09": "Hanshin",
        "10": "Kokura",
    }
    place = jyo_map.get(jyo_cd, f"JYO-{jyo_cd}")

    info = {
        "race_id": race_id_text,
        "clean_date": "",
        "place": place,
        "race_name": f"{place} R{int(race_num)}",
        "race_number": int(race_num),
        "sort_time": "",
        "time": "TBA",
        "kaisai_id": f"{year}{jyo_cd}{kaiji}{nichiji}",
        "discovery_sources": ["jv"],
        "discovery_source": "jv",
        "scrape_source_mode": resolved_source_mode,
        "data_engine": resolved_data_engine,
    }

    # Try to infer month/day from the JV key table if present.
    try:
        start_d = datetime.date(int(year), 1, 1)
        end_d = datetime.date(int(year), 12, 31)
        keys = get_race_keys_in_date_range(str(start_d), str(end_d))
        key_prefix = f"{year}"
        for key in keys:
            k = str(key or "")
            if len(k) != 16 or not k.startswith(key_prefix):
                continue
            if k[8:16] == race_id_text[4:12]:
                month_day = k[4:8]
                try:
                    guessed_date = datetime.datetime.strptime(f"{year}{month_day}", "%Y%m%d").date()
                    info["clean_date"] = str(guessed_date)
                    info["sort_time"] = f"{guessed_date} 00:00"
                except Exception:
                    pass
                break
    except Exception:
        pass

    return {
        "info": info,
        "entries": pd.DataFrame([], columns=["BK", "PP", "Horse", "Record", "Sire", "Dam", "BMS", "Odds", "Fav", "Horse_ID", "Sire_ID", "Dam_ID", "BMS_ID"]),
    }

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

def fetch_weekend_timeline(mode="load", progress_callback=None, source_mode=None, data_engine=None):
    today = datetime.datetime.now(ZoneInfo("Asia/Tokyo")).date()
    lookahead_days = max(1, int(getattr(config, "UPCOMING_LOOKAHEAD_DAYS", 28)))
    end_date = today + datetime.timedelta(days=lookahead_days)
    resolved_data_engine = normalize_data_engine(data_engine)
    resolved_source_mode = _normalize_source_mode(source_mode)
    if resolved_data_engine == "jv" and resolved_source_mode != "jv":
        resolved_source_mode = "jv"
    if resolved_data_engine == "nk" and resolved_source_mode == "jv":
        resolved_source_mode = "nk"
    target_year_months = sorted({
        (today.year, today.month),
        (end_date.year, end_date.month),
    })
    
    cached_races = []
    if mode in ["load", "new"]:
        cached_races = load_race_cache(data_engine=resolved_data_engine)
            
    if mode == "load" and cached_races: return cached_races

    print(
        f"\n--- SCRAPE MODE: {mode.upper()} / ENGINE: {resolved_data_engine.upper()} / "
        f"SOURCE: {resolved_source_mode.upper()} ---"
    )
    nk_discovered: list = []
    jv_discovered: list = []

    if resolved_source_mode in {"nk", "auto"}:
        for year_val, month_val in target_year_months:
            nk_discovered.extend(str(r) for r in _get_month_race_ids(year_val, month_val))

        # Calendar fallback: discover upcoming weekend race IDs from daily race list pages.
        weekend_dates = sorted(
            d for d in get_upcoming_weekend_dates()
            if today <= d <= end_date
        )
        for weekend_date in weekend_dates:
            nk_discovered.extend(str(r) for r in _get_race_ids_from_daily_list(weekend_date))

    if resolved_source_mode in {"jv", "auto"}:
        lookback_days = max(0, int(getattr(config, "JV_DISCOVERY_LOOKBACK_DAYS", 7)))
        jv_start_date = today - datetime.timedelta(days=lookback_days)
        jv_ids_raw = _get_jv_race_ids_for_window(
            jv_start_date,
            end_date,
            allow_nk_assist=(resolved_data_engine != "jv"),
        )
        if jv_ids_raw:
            jv_discovered.extend(str(r) for r in jv_ids_raw)
            msg = f"JV discovery added {len(jv_discovered)} race IDs for {jv_start_date}..{end_date}."
            print(msg)
            if progress_callback:
                progress_callback(msg)
        elif (
            resolved_source_mode == "jv"
            and resolved_data_engine != "jv"
            and getattr(config, "SCRAPE_SOURCE_FALLBACK_TO_NK", True)
        ):
            msg = "JV discovery returned no race IDs; falling back to NK discovery."
            print(msg)
            if progress_callback:
                progress_callback(msg)
            for year_val, month_val in target_year_months:
                nk_discovered.extend(str(r) for r in _get_month_race_ids(year_val, month_val))
            # Also include upcoming weekend dates — mirrors what full NK mode does.
            weekend_dates = sorted(
                d for d in get_upcoming_weekend_dates()
                if today <= d <= end_date
            )
            for weekend_date in weekend_dates:
                nk_discovered.extend(str(r) for r in _get_race_ids_from_daily_list(weekend_date))

    nk_id_set = set(nk_discovered)
    jv_id_set = set(jv_discovered)

    def _discovery_sources_for(rid: str) -> list:
        sources = []
        if rid in nk_id_set:
            sources.append("nk")
        if rid in jv_id_set:
            sources.append("jv")
        return sources or [resolved_source_mode]

    # Preserve order while dropping duplicates; NK IDs first, then JV-only additions.
    all_race_ids = list(dict.fromkeys(nk_discovered + jv_discovered))
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
            # In "new" mode, augment provenance if this scrape found the race via a new source.
            if mode == "new":
                disc_sources = _discovery_sources_for(str_id)
                for cached_race in weekend_races:
                    if cached_race["info"].get("race_id") == str_id:
                        prev = set(cached_race["info"].get("discovery_sources") or [])
                        merged = sorted(prev | set(disc_sources))
                        if merged != sorted(prev):
                            cached_race["info"]["discovery_sources"] = merged
                            cached_race["info"]["discovery_source"] = "both" if len(merged) > 1 else merged[0]
                        break
            msg = f"[{i + 1}/{total_races}] {str_id}... Cached."
            print(msg)
            if progress_callback: progress_callback(msg)
            continue
            
        prefix = str_id[:-2] 
        if prefix in skip_prefixes: continue
            
        msg = f"[{i + 1}/{total_races}] Checking {str_id}..."
        print(msg, end=" ")
        if progress_callback: progress_callback(msg)

        if resolved_data_engine == "jv":
            jv_snapshot = _build_jv_placeholder_race_snapshot(
                str_id,
                resolved_source_mode=resolved_source_mode,
                resolved_data_engine=resolved_data_engine,
            )
            if jv_snapshot is not None:
                weekend_races.append(jv_snapshot)
                msg_jv = f"JV snapshot staged for {str_id}."
                print(msg_jv)
                if progress_callback:
                    progress_callback(msg_jv)
            continue
        
        try:
            result = keibascraper.load("entry", race_id)
            if isinstance(result, tuple) and len(result) == 2 and result[0]:
                race_info = result[0][0]
                entry_list = result[1]
                date_str = race_info.get('date', race_info.get('race_date', ''))

                if date_str:
                    clean_date_str = str(date_str).split(' ')[0]
                    race_date = pd.to_datetime(clean_date_str).date()

                    if today <= race_date <= end_date:
                        place = race_info.get('place', race_info.get('course', ''))
                        race_info['place'] = config.TRACK_TRANSLATIONS.get(place, place)
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

                        # Tag race with provenance: which source(s) discovered this race ID.
                        disc_sources = _discovery_sources_for(str_id)
                        race_info["discovery_sources"] = disc_sources
                        race_info["discovery_source"] = "both" if len(disc_sources) > 1 else disc_sources[0]
                        race_info["scrape_source_mode"] = resolved_source_mode
                        race_info["data_engine"] = resolved_data_engine

                        weekend_races.append({
                            "info": race_info,
                            "entries": formatted_entries
                        })
                        save_horse_dict() 
                    else:
                        skip_prefixes.add(prefix)
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
    
    save_race_cache(weekend_races, data_engine=resolved_data_engine)
    return weekend_races

def fetch_race_history_by_id(race_id):
    """Fetch finalized race result data and map it by horse_id.

    Scrapes the race.netkeiba.com result page directly because the keibascraper
    parser/config no longer matches the live result page structure.
    """
    history_map = {}

    def clean_text(node):
        if node is None:
            return ""
        value = node.get_text(strip=True)
        return "" if value.lower() == "nan" else value

    result_urls = [
        f"https://race.netkeiba.com/race/result.html?race_id={race_id}",
        f"https://race.netkeiba.com/race/result.html?race_id={race_id}&rf=race_list",
    ]

    logger.info(f"History fetch start for race {race_id}; trying {len(result_urls)} result URLs")

    for url in result_urls:
        try:
            logger.info(f"History fetch request for race {race_id}: {url}")
            resp = safe_request(url, timeout=20, retries=2)
            if resp is None:
                logger.warning(f"History fetch request returned no response for race {race_id}: {url}")
                continue
            resp.encoding = resp.apparent_encoding
            logger.info(
                "History fetch response for race %s: status=%s final_url=%s bytes=%s",
                race_id,
                getattr(resp, "status_code", "?"),
                getattr(resp, "url", url),
                len(resp.text or "")
            )
        except Exception as e:
            logger.warning(f"History fetch failed for race {race_id}: {e}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        page_title = soup.title.get_text(" ", strip=True) if soup.title else ""
        all_tables = soup.find_all("table")
        logger.info(
            "History fetch parse summary for race %s: title=%r tables=%s all_result=%s race_table=%s",
            race_id,
            page_title,
            len(all_tables),
            bool(soup.select_one("table#All_Result_Table")),
            bool(soup.select_one("table.RaceTable01.RaceCommon_Table.ResultRefund.Table_Show_All"))
        )
        if all_tables:
            sample_classes = [".".join(tbl.get("class", [])) or "<no-class>" for tbl in all_tables[:5]]
            logger.info(f"History fetch table samples for race {race_id}: {sample_classes}")

        result_table = soup.select_one("table#All_Result_Table")
        if result_table is None:
            result_table = soup.select_one("table.RaceTable01.RaceCommon_Table.ResultRefund.Table_Show_All")
        if result_table is None:
            body_preview = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))[:240]
            logger.info(f"History fetch no matching result table for race {race_id}; page preview: {body_preview}")
            continue

        rows = result_table.select("tbody tr.HorseList")
        logger.info(f"History fetch row count for race {race_id}: {len(rows)} rows")
        if not rows:
            tbody_preview = re.sub(r"\s+", " ", result_table.get_text(" ", strip=True))[:240]
            logger.info(f"History fetch found table but no HorseList rows for race {race_id}; table preview: {tbody_preview}")
            continue

        for row in rows:
            horse_link = row.select_one("td.Horse_Info a[href*='/horse/']")
            if horse_link is None:
                continue

            href = horse_link.get("href", "")
            match = re.search(r"/horse/([a-zA-Z0-9]+)", href)
            if not match:
                continue

            horse_id = match.group(1)
            finish_val = clean_text(row.select_one("td.Result_Num .Rank"))
            fav_val = clean_text(row.select_one("td.Odds.Txt_C .OddsPeople"))
            odds_val = clean_text(row.select_one("td.Odds.Txt_R .Odds_Ninki, td.Odds.Txt_R span"))

            history_map[horse_id] = {
                "finish": finish_val,
                "odds": odds_val,
                "fav": fav_val,
            }

        if history_map:
            logger.info(f"History fetch succeeded for race {race_id}: parsed {len(history_map)} horses")
            return history_map

    logger.warning(f"History fetch failed for race {race_id}: no result table found on race.netkeiba.com")

    return history_map


def fetch_history_table_map_by_race_id(race_id):
    """Best-effort placeholder for history-source support (not exposed by keibascraper API)."""
    # keibascraper currently does not expose a 'history' data_type in load().
    # Keep this as a no-op so the call chain can prefer official APIs first.
    return {}


def fetch_result_table_map_by_race_id(race_id):
    """Read finalized race data from keibascraper result loader."""
    try:
        raw = keibascraper.load("result", race_id)
    except Exception as e:
        logger.info(f"Result table fetch unavailable for race {race_id}: {e}")
        return {}

    if raw is None:
        return {}

    if isinstance(raw, pd.DataFrame):
        df = raw
    elif isinstance(raw, tuple):
        frames = [item for item in raw if isinstance(item, pd.DataFrame)]
        df = frames[0] if frames else None
    elif isinstance(raw, list) and raw and isinstance(raw[0], dict):
        df = pd.DataFrame(raw)
    else:
        df = None

    if df is None or df.empty:
        return {}

    possible_horse_cols = ["horse_id", "Horse_ID", "id"]
    possible_rank_cols = ["rank", "Rank", "finish", "Finish"]
    possible_odds_cols = ["win_odds", "odds", "Odds"]
    possible_fav_cols = ["popularity", "fav", "Fav"]

    horse_col = next((c for c in possible_horse_cols if c in df.columns), None)
    rank_col = next((c for c in possible_rank_cols if c in df.columns), None)
    odds_col = next((c for c in possible_odds_cols if c in df.columns), None)
    fav_col = next((c for c in possible_fav_cols if c in df.columns), None)

    if horse_col is None or rank_col is None:
        return {}

    result_map = {}
    for _, row in df.iterrows():
        horse_id = str(row.get(horse_col, "")).split(".")[0].strip()
        if not horse_id or horse_id.lower() == "nan":
            continue

        finish_val = str(row.get(rank_col, "")).strip()
        if finish_val.lower() == "nan":
            finish_val = ""

        odds_val = ""
        if odds_col is not None:
            odds_val = str(row.get(odds_col, "")).strip()
            if odds_val.lower() == "nan":
                odds_val = ""

        fav_val = ""
        if fav_col is not None:
            fav_val = str(row.get(fav_col, "")).strip()
            if fav_val.lower() == "nan":
                fav_val = ""

        result_map[horse_id] = {
            "finish": finish_val,
            "odds": odds_val,
            "fav": fav_val,
        }

    return result_map


def fetch_race_result_map_prefer_history(race_id):
    """Prefer 'history' source and fall back to result-page scraping."""
    history_map = fetch_history_table_map_by_race_id(race_id)
    if history_map:
        return history_map, "history"

    result_map = fetch_result_table_map_by_race_id(race_id)
    if result_map:
        return result_map, "result"

    direct_map = fetch_race_history_by_id(race_id)
    if direct_map:
        return direct_map, "result-direct"

    return {}, "none"

def fetch_upcoming_race_snapshot(race_id):
    """Fetch latest entry snapshot for an upcoming race (posts/brackets/odds/fav/time)."""
    try:
        result = keibascraper.load("entry", race_id)
    except Exception as e:
        logger.warning(f"Upcoming snapshot fetch failed for race {race_id}: {e}")
        return None

    if not (isinstance(result, tuple) and len(result) == 2 and result[0]):
        return None

    race_info = result[0][0]
    entry_list = result[1]
    str_id = str(race_id)

    place = race_info.get('place', race_info.get('course', ''))
    race_info['place'] = config.TRACK_TRANSLATIONS.get(place, place)
    race_info['race_name'] = romanize(race_info.get('race_name', ''))
    race_info['race_id'] = str_id

    jst_zone = ZoneInfo("Asia/Tokyo")
    ct_zone = ZoneInfo("America/Chicago")
    date_str = race_info.get('date', race_info.get('race_date', ''))

    if date_str:
        clean_date_str = str(date_str).split(' ')[0]
        try:
            race_date = pd.to_datetime(clean_date_str).date()
            race_info['clean_date'] = str(race_date)
        except Exception:
            race_date = None
            race_info['clean_date'] = clean_date_str
    else:
        race_date = None

    jst_time = fetch_real_post_time(race_id)
    if jst_time and race_date:
        try:
            dt_jst = datetime.datetime.strptime(f"{race_date} {jst_time}", "%Y-%m-%d %H:%M").replace(tzinfo=jst_zone)
            dt_ct = dt_jst.astimezone(ct_zone)
            race_info['time'] = dt_ct.strftime("%I:%M %p")
            race_info['sort_time'] = dt_ct.strftime("%Y-%m-%d %H:%M")
        except Exception:
            race_info['time'] = jst_time
            race_info['sort_time'] = f"{race_info.get('clean_date', '')} {jst_time}".strip()
    elif jst_time:
        race_info['time'] = jst_time
    elif race_date and not race_info.get('sort_time'):
        race_info['sort_time'] = f"{race_date} 00:00"

    preds = fetch_predictions(str_id)
    formatted_entries = format_entry_data(entry_list, preds)
    return {
        "info": race_info,
        "entries": formatted_entries
    }


def fetch_entry_horse_ids_quick(race_id):
    """Lightweight entry check for race-card changes.
    Returns a set of horse IDs from the current entry list without fetching odds/predictions.
    """
    horse_ids, _ = fetch_entry_quick_data(race_id)
    return horse_ids


def fetch_entry_quick_data(race_id):
    """Lightweight entry check. Returns (horse_ids, pp_set).
    horse_ids: set of all horse IDs in the current live entry list.
    pp_set: subset of horse_ids where a post position (horse_number / 馬番) is already assigned.
    """
    try:
        result = keibascraper.load("entry", race_id)
    except Exception as e:
        logger.info(f"Quick entry data fetch unavailable for race {race_id}: {e}")
        return set(), set()

    if not (isinstance(result, tuple) and len(result) == 2):
        return set(), set()

    entry_list = result[1]
    if not isinstance(entry_list, list) or not entry_list:
        return set(), set()

    _pp_keys = ["horse_number", "pp", "num", "馬番"]
    horse_ids = set()
    pp_set = set()

    for row in entry_list:
        if not isinstance(row, dict):
            continue
        raw_horse_id = row.get("horse_id") or row.get("Horse_ID") or row.get("id")
        horse_id = str(raw_horse_id or "").split(".")[0].strip()
        if not horse_id or horse_id.lower() == "nan":
            continue
        horse_ids.add(horse_id)
        for pp_key in _pp_keys:
            raw_pp = str(row.get(pp_key, "")).split(".")[0].strip()
            if raw_pp and raw_pp not in ("", "0", "nan", "none"):
                pp_set.add(horse_id)
                break

    return horse_ids, pp_set
