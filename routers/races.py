from fastapi import APIRouter, HTTPException
import datetime
import json
import logging
import os
import pickle
import tempfile
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel

import config
import data_manager

router = APIRouter(tags=["races"])
logger = logging.getLogger(__name__)

CACHE_FILE = config.CACHE_FILE
MARKS_FILE = config.MARKS_FILE
TRACKING_FILE = config.TRACKING_FILE
WATCHLIST_FILE = config.WATCHLIST_FILE
HORSE_DICT_FILE = config.HORSE_DICT_FILE
CONFIG_FILE = config.DATA_DIR / "config.json"

_progress_logger = None


class DayResultsImportPayload(BaseModel):
    date: str


class DeleteDayPayload(BaseModel):
    date: str
    scope: Literal["marks", "entries", "all"]


def set_progress_logger(callback):
    global _progress_logger
    _progress_logger = callback


def log_progress(msg):
    if _progress_logger is not None:
        _progress_logger(msg)


def atomic_write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=target.parent) as tmp:
        json.dump(payload, tmp, ensure_ascii=False, indent=4)
        tmp_path = tmp.name
    os.replace(tmp_path, target)


def atomic_write_pickle(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", delete=False, dir=target.parent) as tmp:
        pickle.dump(payload, tmp)
        tmp_path = tmp.name
    os.replace(tmp_path, target)


def safe_read_json(path, default):
    target = Path(path)
    if not target.exists():
        return default
    try:
        with open(target, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Failed to read JSON from {target}: {e}")
        return default


def load_text_file(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()
    return ""


def load_ids(filepath):
    ids = set()
    text = load_text_file(filepath)
    for line in text.split("\n"):
        clean = line.split("#")[0].strip()
        if clean:
            ids.add(clean)
    return ids


def force_str(val):
    if not val or str(val) == "nan" or str(val) == "---":
        return ""
    return str(val).split(".")[0].strip()


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


def load_config():
    defaults = {
        "sidebarTabs": {"favorites": True, "watchlist": True, "weekendWatchlist": True},
        "ui": {"riskSlider": 50, "autoFetchPastResults": True},
    }
    return safe_read_json(CONFIG_FILE, defaults)


def parse_sort_time(sort_time_str):
    if not sort_time_str:
        return None
    try:
        return datetime.datetime.strptime(str(sort_time_str), "%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        return None


def split_races_by_day_completion(races_by_date):
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

        if day_datetimes:
            is_day_complete = now >= max(day_datetimes)
        else:
            if day_date is None:
                try:
                    day_date = datetime.datetime.strptime(str(date_str), "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    day_date = today
            is_day_complete = day_date < today

        target = past if is_day_complete else upcoming
        target[date_str] = races

    return upcoming, past


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


@router.get("/api/marks")
def get_marks():
    return safe_read_json(MARKS_FILE, {})


@router.post("/api/marks")
async def save_marks(marks: dict):
    atomic_write_json(MARKS_FILE, marks)
    return {"status": "success"}


@router.get("/api/races")
def get_races():
    if not os.path.exists(CACHE_FILE):
        return {
            "top_picks": [],
            "races_by_date": {},
            "upcoming_races_by_date": {},
            "past_races_by_date": {},
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

        if date_str not in races_by_date:
            races_by_date[date_str] = []

        scores, icons, status = [], [], []
        for _, row in df.iterrows():
            fam = [
                force_str(row.get("Horse_ID")),
                force_str(row.get("Sire_ID")),
                force_str(row.get("Dam_ID")),
                force_str(row.get("BMS_ID")),
            ]
            f_score = config.SCORE_TRACKED_HORSE if fam[0] in tracked_ids else (config.SCORE_TRACKED_SIRE if fam[1] in tracked_ids else 0.0)
            f_score += (config.SCORE_TRACKED_DAM if fam[2] in tracked_ids else 0.0) + (config.SCORE_TRACKED_BMS if fam[3] in tracked_ids else 0.0)
            w_score = config.SCORE_WATCHLIST_HORSE if fam[0] in watchlist_ids else (config.SCORE_WATCHLIST_SIRE if fam[1] in watchlist_ids else 0.0)
            w_score += (config.SCORE_WATCHLIST_DAM if fam[2] in watchlist_ids else 0.0) + (config.SCORE_WATCHLIST_BMS if fam[3] in watchlist_ids else 0.0)

            if f_score > 0:
                s, stat = min(f_score, config.SCORE_MAX), "FAV"
                icon = "⭐⭐⭐" if f_score >= config.ICON_THRESHOLD_3STAR else ("⭐⭐" if f_score >= config.ICON_THRESHOLD_2STAR else "⭐")
                if f_score >= config.ICON_THRESHOLD_3STAR:
                    top_picks.append((date_str, info.get("time"), info.get("place"), row.get("Horse"), icon, info.get("race_id")))
            elif w_score > 0:
                s, stat = min(w_score, config.SCORE_MAX), "WATCH"
                icon = "👁️👁️" if w_score >= config.ICON_THRESHOLD_3STAR else "👁️"
                if w_score >= config.ICON_THRESHOLD_3STAR:
                    top_picks.append((date_str, info.get("time"), info.get("place"), row.get("Horse"), icon, info.get("race_id")))
            else:
                s, stat, icon = 0.0, "", ""

            scores.append(s)
            status.append(stat)
            icons.append(icon)

        df["Match"], df["Score"], df["Status"] = icons, scores, status
        races_by_date[date_str].append({"info": info, "entries": df.to_dict(orient="records")})

    upcoming_races_by_date, past_races_by_date = split_races_by_day_completion(races_by_date)
    upcoming_dates = set(upcoming_races_by_date.keys())
    filtered_top_picks = [pick for pick in top_picks if pick[0] in upcoming_dates]

    return {
        "top_picks": filtered_top_picks,
        "races_by_date": upcoming_races_by_date,
        "upcoming_races_by_date": upcoming_races_by_date,
        "past_races_by_date": past_races_by_date,
    }


@router.post("/api/races/{race_id}/refresh-history")
def refresh_race_history(race_id: str):
    weekend_races = load_cached_races()
    if not weekend_races:
        raise HTTPException(status_code=404, detail="No cached races found")

    updated_count = refresh_cached_race_history(weekend_races, race_id, reason="manual")
    save_cached_races(weekend_races)
    return {"status": "success", "updated_entries": updated_count}


@router.post("/api/races/upcoming/refresh")
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
        "failed_races": failed_races,
    }


@router.post("/api/races/day/import-results")
def import_day_results(payload: DayResultsImportPayload):
    try:
        target_date = datetime.datetime.strptime(payload.date.strip(), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    log_progress(f"[Import] Starting day results import for {target_date}...")

    race_ids = data_manager.get_race_ids_for_date(target_date)
    if not race_ids:
        log_progress(f"[Import] No races found for {target_date}.")
        raise HTTPException(status_code=404, detail="No races found for the selected day")

    weekend_races = load_cached_races()
    race_index = {
        str(r.get("info", {}).get("race_id", "")): i
        for i, r in enumerate(weekend_races)
        if str(r.get("info", {}).get("race_id", "")).strip()
    }

    imported_races = 0
    updated_entries = 0
    source_counts = {"history": 0, "result": 0, "result_direct": 0, "none": 0}
    failed_races = []

    for race_id in race_ids:
        race_key = str(race_id).strip()
        if not race_key:
            continue

        if race_key in race_index:
            race_obj = weekend_races[race_index[race_key]]
        else:
            snap = data_manager.fetch_upcoming_race_snapshot(race_key)
            if not snap:
                failed_races.append(race_key)
                log_progress(f"[Import] Failed to fetch race snapshot for {race_key}.")
                continue

            race_obj = {
                "info": snap.get("info", {}),
                "entries": snap.get("entries", pd.DataFrame()),
            }
            weekend_races.append(race_obj)
            race_index[race_key] = len(weekend_races) - 1
            imported_races += 1
            log_progress(f"[Import] Added race {race_key} to cache.")

        entries_df = race_obj.get("entries")
        if not isinstance(entries_df, pd.DataFrame) or entries_df.empty:
            failed_races.append(race_key)
            log_progress(f"[Import] Race {race_key} has no entry data to update.")
            continue

        history_map, source = data_manager.fetch_race_result_map_prefer_history(race_key)
        source_key = "result_direct" if source == "result-direct" else source
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        if not history_map:
            failed_races.append(race_key)
            log_progress(f"[Import] No history/result rows found for race {race_key}.")
            continue

        entries_df, changed_rows = apply_history_map_to_race_entries(entries_df.copy(), history_map)
        race_obj["entries"] = entries_df
        race_obj.setdefault("info", {})["clean_date"] = str(target_date)
        race_obj.setdefault("info", {})["history_refreshed"] = race_has_history_data(entries_df)
        race_obj["info"]["history_refresh_reason"] = "calendar-import"
        race_obj["info"]["history_refresh_source"] = source
        race_obj["info"]["history_refreshed_at"] = datetime.datetime.now().isoformat(timespec="seconds")
        updated_entries += changed_rows
        log_progress(f"[Import] Race {race_key} updated via {source_key}: {changed_rows} rows.")

    if imported_races or updated_entries:
        save_cached_races(weekend_races)

    log_progress(
        f"[Import] Completed {target_date}: found={len(race_ids)} imported={imported_races} "
        f"updated_rows={updated_entries} history={source_counts.get('history', 0)} "
        f"result={source_counts.get('result', 0)} result_direct={source_counts.get('result_direct', 0)}"
    )

    return {
        "status": "success",
        "date": str(target_date),
        "races_found": len(race_ids),
        "races_imported": imported_races,
        "updated_entries": updated_entries,
        "sources": source_counts,
        "failed_races": failed_races,
    }


@router.post("/api/day/delete")
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
        "matched_races": len(target_race_ids),
    }