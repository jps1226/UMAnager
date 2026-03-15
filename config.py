"""
Configuration and constants for UMAnager.
Centralized file paths, timeouts, and magic numbers.
"""

import os
from pathlib import Path

# File paths
DATA_DIR = Path(__file__).parent / "data"
CACHE_FILE = DATA_DIR / "race_cache.pkl"
MARKS_FILE = DATA_DIR / "saved_marks.json"
TRACKING_FILE = DATA_DIR / "tracked_horses.txt"
WATCHLIST_FILE = DATA_DIR / "watchlist_horses.txt"
HORSE_DICT_FILE = DATA_DIR / "horse_names.json"

# API & Network settings
REQUEST_TIMEOUT = 5
REQUEST_RETRIES = 2
SCRAPE_DELAY = 0.3  # Delay between requests to avoid rate limiting
UPCOMING_WEEKEND_WEEKS = 3  # Number of upcoming weekend pairs (Sat/Sun) to scan
UPCOMING_LOOKAHEAD_DAYS = 28  # Rolling upcoming window used for race discovery

# Validation rules
HORSE_ID_PATTERN = r'^[a-zA-Z0-9]{10}$'  # Netkeiba horse IDs are 10 alphanumeric chars
MAX_CONSOLE_LOGS = 100
MIN_NAME_LENGTH = 2  # Minimum length for a cleaned English name

# Pedigree scoring (used in race matching)
SCORE_TRACKED_HORSE = 1.0
SCORE_TRACKED_SIRE = 0.5
SCORE_TRACKED_DAM = 0.5
SCORE_TRACKED_BMS = 0.25
SCORE_WATCHLIST_HORSE = 1.0
SCORE_WATCHLIST_SIRE = 0.5
SCORE_WATCHLIST_DAM = 0.5
SCORE_WATCHLIST_BMS = 0.25
SCORE_MAX = 1.0  # Maximum score for display

# UI thresholds
ICON_THRESHOLD_3STAR = 1.0      # ⭐⭐⭐
ICON_THRESHOLD_2STAR = 0.5      # ⭐⭐
ICON_THRESHOLD_1STAR = 0.0      # ⭐

# Track name translations (Japanese -> English)
TRACK_TRANSLATIONS = {
    "札幌": "Sapporo", "函館": "Hakodate", "福島": "Fukushima",
    "新潟": "Niigata", "東京": "Tokyo", "中山": "Nakayama",
    "中京": "Chukyo", "京都": "Kyoto", "阪神": "Hanshin", "小倉": "Kokura"
}

# Netkeiba URLs
NETKEIBA_BASE = "https://db.netkeiba.com"
NETKEIBA_HORSE_URL = f"{NETKEIBA_BASE}/horse"
NETKEIBA_PEDIGREE_URL = f"{NETKEIBA_BASE}/horse/ped"
NETKEIBA_RACE_SHUTUBA = "https://race.netkeiba.com/race/shutuba.html"
NETKEIBA_API_ODDS = "https://race.netkeiba.com/api/api_get_jra_odds.html"
NETKEIBA_EN_HORSE = "https://en.netkeiba.com/db/horse"
