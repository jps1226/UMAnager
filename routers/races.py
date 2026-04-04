from fastapi import APIRouter, HTTPException
import datetime
import itertools
import logging
import math
import os
from typing import Any, Dict, List, Literal, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import BaseModel, Field
import requests

import config
import data_manager
from storage import (
    delete_horse_cache_entries_by_ids,
    delete_marks_for_races,
    get_active_data_engine,
    load_app_config,
    load_horse_list,
    load_marks_store,
    load_race_cache,
    save_marks_store,
    save_race_cache,
)

router = APIRouter(tags=["races"])
logger = logging.getLogger(__name__)

_progress_logger = None
_PREFETCH_CHECK_MAX_FUTURE_RACES = 8
_PREFETCH_CHECK_MAX_PAST_RACES = 5
_PREFETCH_CHECK_CACHE_TTL_SECONDS = 240
_prefetch_check_cache = {
    "raceSignature": "",
    "expiresAt": None,
    "result": None,
}

MARKS_SCHEMA_VERSION = 2
DEFAULT_MARKS_STORE = {
    "version": MARKS_SCHEMA_VERSION,
    "marks": {},
    "raceMeta": {},
}

WIN_STAKE_YEN = 5000
QUINELLA_STAKE_YEN = 500
TRIO_STAKE_YEN = 500


class DayResultsImportPayload(BaseModel):
    date: str


class DeleteDayPayload(BaseModel):
    date: str
    scope: Literal["marks", "entries", "all"]


class StrategySnapshotPayload(BaseModel):
    riskSlider: Optional[int] = None
    riskLabel: Optional[str] = None
    formulaWeights: Dict[str, Any] = Field(default_factory=dict)


class RaceMetaPayload(BaseModel):
    savedAt: Optional[str] = None
    updatedAt: Optional[str] = None
    markSource: Optional[str] = None
    strategySnapshot: StrategySnapshotPayload = Field(default_factory=StrategySnapshotPayload)
    manualAdjustments: int = 0
    lockStateAtSave: Optional[bool] = None
    activeSymbols: List[str] = Field(default_factory=list)


class MarksSavePayload(BaseModel):
    version: Optional[int] = MARKS_SCHEMA_VERSION
    marks: Dict[str, Optional[str]] = Field(default_factory=dict)
    raceMeta: Dict[str, RaceMetaPayload] = Field(default_factory=dict)


class RaceBetEstimateRequestItem(BaseModel):
    race_id: str
    honmei_post: int
    box_posts: List[int] = Field(default_factory=list)


class RaceBetEstimateBatchRequest(BaseModel):
    races: List[RaceBetEstimateRequestItem] = Field(default_factory=list)


def set_progress_logger(callback):
    global _progress_logger
    _progress_logger = callback


def log_progress(msg):
    if _progress_logger is not None:
        _progress_logger(msg)


def load_ids(list_type):
    return {h for h, _n in load_horse_list(list_type)}


def force_str(val):
    if not val or str(val) == "nan" or str(val) == "---":
        return ""
    return str(val).split(".")[0].strip()


def load_cached_races():
    return load_race_cache(data_engine=get_active_data_engine())


def save_cached_races(races):
    save_race_cache(races, data_engine=get_active_data_engine())


def _clean_mark_symbol(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_marks_map(raw_marks):
    normalized = {}
    if not isinstance(raw_marks, dict):
        return normalized

    for key, value in raw_marks.items():
        clean_key = str(key).strip()
        clean_value = _clean_mark_symbol(value)
        if clean_key and clean_value:
            normalized[clean_key] = clean_value
    return normalized


def _normalize_strategy_snapshot(raw_snapshot):
    if not isinstance(raw_snapshot, dict):
        raw_snapshot = {}

    risk_slider = raw_snapshot.get("riskSlider")
    try:
        risk_slider = int(risk_slider) if risk_slider is not None else None
    except (TypeError, ValueError):
        risk_slider = None

    risk_label = raw_snapshot.get("riskLabel")
    risk_label = str(risk_label).strip() if risk_label is not None else None
    if risk_label == "":
        risk_label = None

    formula_weights = raw_snapshot.get("formulaWeights")
    if not isinstance(formula_weights, dict):
        formula_weights = {}

    return {
        "riskSlider": risk_slider,
        "riskLabel": risk_label,
        "formulaWeights": formula_weights,
    }


def _normalize_race_meta_map(raw_race_meta):
    normalized = {}
    if not isinstance(raw_race_meta, dict):
        return normalized

    for race_id, meta in raw_race_meta.items():
        clean_race_id = str(race_id).strip()
        if not clean_race_id or not isinstance(meta, dict):
            continue

        manual_adjustments = meta.get("manualAdjustments", 0)
        try:
            manual_adjustments = max(0, int(manual_adjustments))
        except (TypeError, ValueError):
            manual_adjustments = 0

        active_symbols = meta.get("activeSymbols")
        if not isinstance(active_symbols, list):
            active_symbols = []
        active_symbols = [str(symbol).strip() for symbol in active_symbols if str(symbol).strip()]

        normalized[clean_race_id] = {
            "savedAt": str(meta.get("savedAt") or "").strip() or None,
            "updatedAt": str(meta.get("updatedAt") or "").strip() or None,
            "markSource": str(meta.get("markSource") or "").strip() or None,
            "strategySnapshot": _normalize_strategy_snapshot(meta.get("strategySnapshot") or {}),
            "manualAdjustments": manual_adjustments,
            "lockStateAtSave": bool(meta.get("lockStateAtSave")) if meta.get("lockStateAtSave") is not None else None,
            "activeSymbols": active_symbols,
        }

    return normalized


def normalize_marks_store(raw_data):
    if not isinstance(raw_data, dict):
        return DEFAULT_MARKS_STORE.copy()

    is_versioned = any(key in raw_data for key in ("version", "marks", "raceMeta"))
    raw_marks = raw_data.get("marks", {}) if is_versioned else raw_data
    raw_race_meta = raw_data.get("raceMeta", {}) if is_versioned else {}

    return {
        "version": MARKS_SCHEMA_VERSION,
        "marks": _normalize_marks_map(raw_marks),
        "raceMeta": _normalize_race_meta_map(raw_race_meta),
    }


def load_marks_data():
    return load_marks_store()["marks"]


def save_marks_data(marks):
    current_store = load_marks_store()
    current_store["marks"] = _normalize_marks_map(marks)
    save_marks_store(current_store)


def load_config():
    return load_app_config()


def parse_sort_time(sort_time_str):
    if not sort_time_str:
        return None

    text = str(sort_time_str).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.datetime.strptime(text, fmt).replace(tzinfo=ZoneInfo("Asia/Tokyo"))
        except (ValueError, TypeError):
            continue

    return None


def _normalize_post_number(value):
    try:
        post = int(value)
    except (TypeError, ValueError):
        return None
    if post <= 0:
        return None
    return post


def _build_combo_key(posts):
    return "".join(f"{int(p):02d}" for p in sorted(posts))


def _parse_odds_to_float(value):
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", "--", "---"}:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    if parsed <= 0:
        return None
    return parsed


def _parse_primary_odds(entry):
    if isinstance(entry, list) and entry:
        return _parse_odds_to_float(entry[0])
    return _parse_odds_to_float(entry)


def _fetch_netkeiba_odds_map(race_id: str, odds_type: int):
    referer = f"{config.NETKEIBA_RACE_SHUTUBA}?race_id={race_id}"
    params = {"race_id": race_id, "type": str(odds_type), "action": "init"}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": referer,
        "X-Requested-With": "XMLHttpRequest",
    }
    response = requests.get(config.NETKEIBA_API_ODDS, params=params, headers=headers, timeout=10)
    response.raise_for_status()
    payload = response.json()

    data = payload.get("data") if isinstance(payload, dict) else {}
    odds_root = data.get("odds") if isinstance(data, dict) else {}
    odds_map = odds_root.get(str(odds_type)) if isinstance(odds_root, dict) else {}
    if not isinstance(odds_map, dict):
        return {}
    return odds_map


def _fetch_netkeiba_odds_map_safe(race_id: str, odds_type: int):
    try:
        return _fetch_netkeiba_odds_map(race_id, odds_type), None
    except requests.RequestException as exc:
        return {}, str(exc)
    except Exception as exc:
        return {}, str(exc)


def _estimate_range_from_combos(combo_posts, odds_map, total_purchase, stake_per_ticket):
    if not combo_posts:
        return {
            "tickets": 0,
            "resolvedTickets": 0,
            "missingTickets": 0,
            "minOdds": None,
            "maxOdds": None,
            "minPayout": None,
            "maxPayout": None,
            "minNet": None,
            "maxNet": None,
        }

    odds_values = []
    missing = 0
    for combo in combo_posts:
        key = _build_combo_key(combo)
        odds = _parse_primary_odds(odds_map.get(key))
        if odds is None:
            missing += 1
            continue
        odds_values.append(odds)

    if not odds_values:
        return {
            "tickets": len(combo_posts),
            "resolvedTickets": 0,
            "missingTickets": len(combo_posts),
            "minOdds": None,
            "maxOdds": None,
            "minPayout": None,
            "maxPayout": None,
            "minNet": None,
            "maxNet": None,
        }

    min_odds = min(odds_values)
    max_odds = max(odds_values)
    min_payout = int(round(min_odds * stake_per_ticket))
    max_payout = int(round(max_odds * stake_per_ticket))
    return {
        "tickets": len(combo_posts),
        "resolvedTickets": len(odds_values),
        "missingTickets": missing,
        "minOdds": min_odds,
        "maxOdds": max_odds,
        "minPayout": min_payout,
        "maxPayout": max_payout,
        "minNet": min_payout - total_purchase,
        "maxNet": max_payout - total_purchase,
    }


def _build_box_bet_estimate(race_id: str, honmei_post: int, box_posts: List[int]):
    clean_race_id = str(race_id or "").strip()
    if not clean_race_id:
        raise ValueError("race_id is required")

    clean_honmei = _normalize_post_number(honmei_post)
    if clean_honmei is None:
        raise ValueError("honmei_post must be a positive integer")

    normalized_posts = []
    for post in box_posts:
        clean_post = _normalize_post_number(post)
        if clean_post is not None and clean_post not in normalized_posts:
            normalized_posts.append(clean_post)
    normalized_posts.sort()

    if clean_honmei not in normalized_posts:
        normalized_posts.append(clean_honmei)
        normalized_posts.sort()

    if len(normalized_posts) < 2:
        raise ValueError("At least two unique posts are required for box estimates")

    win_ticket_cost = WIN_STAKE_YEN
    quinella_ticket_count = math.comb(len(normalized_posts), 2)
    trio_ticket_count = math.comb(len(normalized_posts), 3) if len(normalized_posts) >= 3 else 0
    quinella_cost = quinella_ticket_count * QUINELLA_STAKE_YEN
    trio_cost = trio_ticket_count * TRIO_STAKE_YEN
    total_purchase = win_ticket_cost + quinella_cost + trio_cost

    win_map, win_fetch_error = _fetch_netkeiba_odds_map_safe(clean_race_id, 1)
    # Netkeiba type=4 is horse-number Quinella (Umaren); type=3 is bracket quinella.
    quinella_map, quinella_fetch_error = _fetch_netkeiba_odds_map_safe(clean_race_id, 4)
    trio_map, trio_fetch_error = _fetch_netkeiba_odds_map_safe(clean_race_id, 7)
    fetch_errors = [err for err in [win_fetch_error, quinella_fetch_error, trio_fetch_error] if err]

    win_key = f"{clean_honmei:02d}"
    win_odds = _parse_primary_odds(win_map.get(win_key))
    win_payout = int(round(win_odds * WIN_STAKE_YEN)) if win_odds is not None else None

    quinella_combos = list(itertools.combinations(normalized_posts, 2))
    trio_combos = list(itertools.combinations(normalized_posts, 3)) if len(normalized_posts) >= 3 else []
    quinella_range = _estimate_range_from_combos(
        quinella_combos,
        quinella_map,
        total_purchase,
        QUINELLA_STAKE_YEN,
    )
    trio_range = _estimate_range_from_combos(
        trio_combos,
        trio_map,
        total_purchase,
        TRIO_STAKE_YEN,
    )

    all_hit_min_net = None
    all_hit_max_net = None
    if (
        win_payout is not None
        and quinella_range.get("minPayout") is not None
        and trio_range.get("minPayout") is not None
    ):
        all_hit_min_net = win_payout + quinella_range["minPayout"] + trio_range["minPayout"] - total_purchase
        all_hit_max_net = win_payout + quinella_range["maxPayout"] + trio_range["maxPayout"] - total_purchase

    return {
        "status": "partial" if fetch_errors else "ok",
        "raceId": clean_race_id,
        "boxPosts": normalized_posts,
        "honmeiPost": clean_honmei,
        "purchase": {
            "total": total_purchase,
            "win": win_ticket_cost,
            "quinellaBox": quinella_cost,
            "trioBox": trio_cost,
            "quinellaTickets": quinella_ticket_count,
            "trioTickets": trio_ticket_count,
            "winStake": WIN_STAKE_YEN,
            "quinellaStake": QUINELLA_STAKE_YEN,
            "trioStake": TRIO_STAKE_YEN,
        },
        "win": {
            "odds": win_odds,
            "payout": win_payout,
            "net": (win_payout - total_purchase) if win_payout is not None else None,
        },
        "quinellaBox": quinella_range,
        "trioBox": trio_range,
        "allHit": {
            "minNet": all_hit_min_net,
            "maxNet": all_hit_max_net,
        },
        "warnings": fetch_errors,
    }


def split_races_by_day_completion(races_by_date):
    now = datetime.datetime.now(ZoneInfo("Asia/Tokyo"))
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


def _parse_clean_date(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _extract_entry_horse_ids(entries_df):
    horse_ids = set()
    if entries_df is None or entries_df.empty:
        return horse_ids
    if "Horse_ID" not in entries_df.columns:
        return horse_ids

    for raw_val in entries_df["Horse_ID"].tolist():
        horse_id = force_str(raw_val)
        if horse_id:
            horse_ids.add(horse_id)
    return horse_ids


def _race_id_to_date_str(race_id: str) -> Optional[str]:
    """Extract YYYY-MM-DD from a race ID whose first 8 chars are YYYYMMDD."""
    s = str(race_id).strip()
    if len(s) >= 8 and s[:8].isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return None


def run_prefetch_race_check(weekend_races):
    now_jst = datetime.datetime.now(ZoneInfo("Asia/Tokyo"))
    today_jst = now_jst.date()
    lookahead_days = max(1, int(getattr(config, "UPCOMING_LOOKAHEAD_DAYS", 28)))
    end_date = today_jst + datetime.timedelta(days=lookahead_days)

    updates_by_date: Dict[str, List[str]] = {}

    def _add_update(date_str: str, update_type: str) -> None:
        updates_by_date.setdefault(date_str, [])
        if update_type not in updates_by_date[date_str]:
            updates_by_date[date_str].append(update_type)

    # --- 1. New races: actionable (today..lookahead) IDs not present in local cache ---
    cached_race_ids = {
        str(race.get("info", {}).get("race_id", "")).strip()
        for race in weekend_races
        if str(race.get("info", {}).get("race_id", "")).strip()
    }
    month_targets = sorted({(today_jst.year, today_jst.month), (end_date.year, end_date.month)})
    actionable_ids = set()
    for y, m in month_targets:
        actionable_ids.update(data_manager.get_month_race_ids(y, m))

    for rid in actionable_ids - cached_race_ids:
        date_str = _race_id_to_date_str(rid)
        if not date_str:
            continue
        race_date = _parse_clean_date(date_str)
        if race_date is None or race_date < today_jst or race_date > end_date:
            continue
        _add_update(date_str, "new_race")

    # --- 2. Future races: new entries and newly-published post positions ---
    eligible_future: List = []
    for race in weekend_races:
        info = race.get("info", {})
        race_id = str(info.get("race_id", "")).strip()
        if not race_id:
            continue
        race_date = _parse_clean_date(info.get("clean_date"))
        if race_date is None or race_date < today_jst:
            continue
        sort_time = parse_sort_time(info.get("sort_time"))
        eligible_future.append((race_date, sort_time, race))

    future_sort_max = datetime.datetime.max.replace(tzinfo=ZoneInfo("Asia/Tokyo"))
    eligible_future.sort(key=lambda item: (item[0], item[1] or future_sort_max))
    limited_future = eligible_future[:_PREFETCH_CHECK_MAX_FUTURE_RACES]

    new_entry_races = 0
    post_position_races = 0

    for race_date, _sort_time, race in limited_future:
        info = race.get("info", {})
        race_id = str(info.get("race_id", "")).strip()
        date_str = str(race_date)
        if not race_id:
            continue

        live_horse_ids, live_pp_set = data_manager.fetch_entry_quick_data(race_id)
        if not live_horse_ids:
            continue

        cached_entries = race.get("entries")
        cached_horse_ids = _extract_entry_horse_ids(cached_entries)

        # Build the set of horses that already have a PP stored in the cache.
        cached_pp_set: set = set()
        if cached_entries is not None and not cached_entries.empty and "PP" in cached_entries.columns:
            for _, row in cached_entries.iterrows():
                pp_val = force_str(row.get("PP", ""))
                horse_id = force_str(row.get("Horse_ID", ""))
                if horse_id and pp_val and pp_val.lower() not in ("", "0", "nan", "none"):
                    cached_pp_set.add(horse_id)

        # New horse entries not yet in the local cache.
        if live_horse_ids - cached_horse_ids:
            _add_update(date_str, "new_entries")
            new_entry_races += 1

        # Post positions now assigned for horses already in the cache.
        newly_pp = (live_pp_set & cached_horse_ids) - cached_pp_set
        if newly_pp:
            _add_update(date_str, "post_positions")
            post_position_races += 1

    # --- 3. Past races: finish positions now available from live results ---
    eligible_past: List = []
    for race in weekend_races:
        info = race.get("info", {})
        race_id = str(info.get("race_id", "")).strip()
        if not race_id:
            continue
        race_date = _parse_clean_date(info.get("clean_date"))
        if race_date is None or race_date >= today_jst:
            continue
        entries_df = race.get("entries")
        if race_has_history_data(entries_df):
            continue  # Already has finish data stored.
        sort_time = parse_sort_time(info.get("sort_time"))
        eligible_past.append((race_date, sort_time, race))

    # Check most recent past races first.
    past_sort_max = datetime.datetime.max.replace(tzinfo=ZoneInfo("Asia/Tokyo"))
    eligible_past.sort(key=lambda item: (item[0], item[1] or past_sort_max), reverse=True)
    limited_past = eligible_past[:_PREFETCH_CHECK_MAX_PAST_RACES]

    finish_position_races = 0
    for race_date, _sort_time, race in limited_past:
        info = race.get("info", {})
        race_id = str(info.get("race_id", "")).strip()
        date_str = str(race_date)
        if not race_id:
            continue
        try:
            history_map = data_manager.fetch_race_history_by_id(race_id)
        except Exception:
            continue
        if history_map:
            _add_update(date_str, "finish_positions")
            finish_position_races += 1

    return {
        "enabled": True,
        "checkedAt": now_jst.isoformat(timespec="seconds"),
        "hasUpdates": bool(updates_by_date),
        "updatesByDate": updates_by_date,
        "summary": {
            "newRaceDates": len([d for d, ts in updates_by_date.items() if "new_race" in ts]),
            "newEntryRaces": new_entry_races,
            "postPositionRaces": post_position_races,
            "finishPositionRaces": finish_position_races,
            "checkedFutureRaces": len(limited_future),
            "checkedPastRaces": len(limited_past),
        },
    }


def run_prefetch_race_check_cached(weekend_races):
    now = datetime.datetime.now(datetime.timezone.utc)
    cached_race_ids = sorted(
        {
            str(race.get("info", {}).get("race_id", "")).strip()
            for race in weekend_races
            if str(race.get("info", {}).get("race_id", "")).strip()
        }
    )
    race_signature = "|".join(cached_race_ids)

    cache_expiry = _prefetch_check_cache.get("expiresAt")
    if (
        _prefetch_check_cache.get("result") is not None
        and _prefetch_check_cache.get("raceSignature") == race_signature
        and isinstance(cache_expiry, datetime.datetime)
        and cache_expiry > now
    ):
        return _prefetch_check_cache.get("result")

    result = run_prefetch_race_check(weekend_races)
    _prefetch_check_cache["raceSignature"] = race_signature
    _prefetch_check_cache["result"] = result
    _prefetch_check_cache["expiresAt"] = now + datetime.timedelta(seconds=_PREFETCH_CHECK_CACHE_TTL_SECONDS)
    return result


@router.get("/api/marks")
def get_marks():
    return load_marks_store()


@router.post("/api/marks")
async def save_marks(payload: MarksSavePayload):
    save_marks_store(
        {
            "version": payload.version or MARKS_SCHEMA_VERSION,
            "marks": payload.marks,
            "raceMeta": {race_id: meta.dict() for race_id, meta in payload.raceMeta.items()},
        }
    )
    return {"status": "success"}


@router.post("/api/races/bet-estimate")
def get_bet_estimates(payload: RaceBetEstimateBatchRequest):
    estimates: Dict[str, Dict[str, Any]] = {}

    requests_batch = payload.races[:50] if isinstance(payload.races, list) else []
    for item in requests_batch:
        race_id = str(item.race_id or "").strip()
        if not race_id:
            continue

        try:
            estimates[race_id] = _build_box_bet_estimate(
                race_id=race_id,
                honmei_post=item.honmei_post,
                box_posts=item.box_posts,
            )
        except requests.RequestException as exc:
            estimates[race_id] = {
                "status": "error",
                "raceId": race_id,
                "message": f"Odds fetch failed: {exc}",
            }
        except Exception as exc:
            estimates[race_id] = {
                "status": "partial",
                "raceId": race_id,
                "message": str(exc),
                "warnings": [str(exc)],
                "purchase": {
                    "total": None,
                    "win": None,
                    "quinellaBox": None,
                    "trioBox": None,
                    "quinellaTickets": None,
                    "trioTickets": None,
                    "winStake": WIN_STAKE_YEN,
                    "quinellaStake": QUINELLA_STAKE_YEN,
                    "trioStake": TRIO_STAKE_YEN,
                },
                "win": {"odds": None, "payout": None, "net": None},
                "quinellaBox": {
                    "tickets": 0,
                    "resolvedTickets": 0,
                    "missingTickets": 0,
                    "minOdds": None,
                    "maxOdds": None,
                    "minPayout": None,
                    "maxPayout": None,
                    "minNet": None,
                    "maxNet": None,
                },
                "trioBox": {
                    "tickets": 0,
                    "resolvedTickets": 0,
                    "missingTickets": 0,
                    "minOdds": None,
                    "maxOdds": None,
                    "minPayout": None,
                    "maxPayout": None,
                    "minNet": None,
                    "maxNet": None,
                },
                "allHit": {"minNet": None, "maxNet": None},
            }

    return {"status": "ok", "estimates": estimates}


@router.get("/api/prefetch-check")
def get_prefetch_check():
    """Background prefetch check — called asynchronously after race data loads."""
    app_cfg = load_config()
    if not bool(app_cfg.get("ui", {}).get("prefetchRaceCheck", False)):
        return {"enabled": False, "hasUpdates": False, "updatesByDate": {}}
    try:
        weekend_races = load_cached_races()
        return run_prefetch_race_check_cached(weekend_races)
    except Exception as exc:
        logger.warning("Prefetch race check failed: %s", exc)
        return {
            "enabled": True,
            "checkedAt": datetime.datetime.now(ZoneInfo("Asia/Tokyo")).isoformat(timespec="seconds"),
            "error": str(exc),
            "hasUpdates": False,
            "updatesByDate": {},
        }


@router.get("/api/races")
def get_races():
    app_cfg = load_config()

    weekend_races = load_cached_races()

    if not weekend_races:
        return {
            "top_picks": [],
            "races_by_date": {},
            "upcoming_races_by_date": {},
            "past_races_by_date": {},
        }

    auto_history_enabled = app_cfg.get("ui", {}).get("autoFetchPastResults", True)
    if auto_history_enabled:
        auto_refreshed_races, auto_refreshed_entries = refresh_missing_past_race_history(weekend_races)
        if auto_refreshed_races:
            logger.info(
                "Auto history refresh complete: refreshed %s races and %s entries",
                auto_refreshed_races,
                auto_refreshed_entries,
            )
    tracked_ids = load_ids("favorites")
    watchlist_ids = load_ids("watchlist")

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


def _refresh_upcoming_races_in_memory(weekend_races):
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

    return {
        "updated_races": updated_races,
        "updated_rows": updated_rows,
        "failed_races": failed_races,
    }


@router.post("/api/races/upcoming/refresh")
def refresh_upcoming_races():
    weekend_races = load_cached_races()
    if not weekend_races:
        raise HTTPException(status_code=404, detail="No cached races found")
    result = _refresh_upcoming_races_in_memory(weekend_races)
    save_cached_races(weekend_races)
    return {
        "status": "success",
        "updated_races": result["updated_races"],
        "updated_rows": result["updated_rows"],
        "failed_races": result["failed_races"],
    }


@router.post("/api/races/enrich-horse-info")
def enrich_cached_horse_info():
    """Manually fill horse names / pedigree fields after a fast race scrape.

    This uses cached `Horse_ID` values from the current engine's race cache and resolves
    official names plus sire/dam/BMS info without changing the fast scrape path itself.
    """
    from routers import scrape as scrape_router

    if scrape_router.scrape_job_lock.locked():
        raise HTTPException(status_code=409, detail="A scrape job is already running.")

    with scrape_router.scrape_logs_lock:
        scrape_router.scrape_logs = ["Initializing horse info enrichment..."]

    def emit(msg: str):
        logger.info(msg)
        scrape_router.log_progress(msg)

    weekend_races = load_cached_races()
    if not weekend_races:
        raise HTTPException(status_code=404, detail="No cached races found")

    updated_races = 0
    updated_rows = 0
    unique_horses = set()
    fetched_candidates = 0
    horse_details = {}
    errors = []
    total_races = len(weekend_races)
    processed_unique_horses = 0
    network_fetches = 0

    for race in weekend_races:
        entries_df = race.get("entries")
        if not isinstance(entries_df, pd.DataFrame) or entries_df.empty:
            continue
        for row in entries_df.to_dict(orient="records"):
            horse_id = force_str(row.get("Horse_ID"))
            if horse_id:
                unique_horses.add(horse_id)

    emit(
        f"Horse enrichment: scanning {total_races} races | unique horses={len(unique_horses)}. "
        f"Checking cache and filling names/pedigree..."
    )

    for race_idx, race in enumerate(weekend_races, start=1):
        entries_df = race.get("entries")
        if not isinstance(entries_df, pd.DataFrame) or entries_df.empty:
            continue

        info = race.get("info") or {}
        race_id = str(info.get("race_id") or "").strip() or f"race-{race_idx}"
        rows = entries_df.to_dict(orient="records")
        race_changed = False

        emit(f"Horse enrichment: race {race_idx}/{total_races} {race_id} | entries={len(rows)}")

        for row in rows:
            horse_id = force_str(row.get("Horse_ID"))
            raw_name = str(row.get("Horse") or "").strip()
            if not horse_id:
                continue

            if horse_id not in horse_details:
                processed_unique_horses += 1
                cached_before = data_manager.HORSE_CACHE.get(horse_id)
                had_complete_cache = bool(
                    isinstance(cached_before, dict)
                    and str(cached_before.get("name") or "").strip()
                    and str(cached_before.get("sire_id") or "").strip()
                )
                if not had_complete_cache:
                    fetched_candidates += 1
                    network_fetches += 1
                    emit(
                        f"Horse enrichment fetch {processed_unique_horses}/{len(unique_horses)}: "
                        f"{horse_id} {raw_name or '(unnamed)'}"
                    )
                elif processed_unique_horses == 1 or processed_unique_horses % 25 == 0:
                    emit(
                        f"Horse enrichment progress: horses {processed_unique_horses}/{len(unique_horses)} processed | "
                        f"network fetches so far={network_fetches}"
                    )
                try:
                    horse_details[horse_id] = data_manager.get_horse_data(horse_id, raw_name)
                except Exception as exc:
                    logger.warning("Horse enrichment failed for %s: %s", horse_id, exc)
                    errors.append(f"{horse_id}: {exc}")
                    emit(f"Horse enrichment warning: failed for {horse_id} ({exc})")
                    horse_details[horse_id] = {}

            details = horse_details.get(horse_id) or {}
            updated_fields = {
                "Horse": data_manager.resolve_cached_or_romanized_horse_name(horse_id, raw_name),
                "Record": str(details.get("record") or row.get("Record") or "").strip(),
                "Sire": str(details.get("sire") or row.get("Sire") or "").strip(),
                "Dam": str(details.get("dam") or row.get("Dam") or "").strip(),
                "BMS": str(details.get("bms") or row.get("BMS") or "").strip(),
                "Sire_ID": str(details.get("sire_id") or row.get("Sire_ID") or "").strip(),
                "Dam_ID": str(details.get("dam_id") or row.get("Dam_ID") or "").strip(),
                "BMS_ID": str(details.get("bms_id") or row.get("BMS_ID") or "").strip(),
            }

            row_changed = False
            for key, new_value in updated_fields.items():
                if new_value and str(row.get(key) or "").strip() != new_value:
                    row[key] = new_value
                    row_changed = True

            if row_changed:
                updated_rows += 1
                race_changed = True

        if race_changed:
            race["entries"] = pd.DataFrame(rows)
            updated_races += 1
            emit(
                f"Horse enrichment: race {race_id} updated | updated_races={updated_races} updated_rows={updated_rows}"
            )

    emit("Horse enrichment: saving cache updates...")
    data_manager.save_horse_dict()
    save_cached_races(weekend_races)
    emit(
        f"Horse enrichment complete: updated_races={updated_races} updated_rows={updated_rows} "
        f"unique_horses={len(unique_horses)} network_fetches={network_fetches}"
    )

    return {
        "status": "success",
        "updated_races": updated_races,
        "updated_rows": updated_rows,
        "unique_horses": len(unique_horses),
        "fetch_candidates": fetched_candidates,
        "errors": errors[:10],
    }


@router.post("/api/races/prefetch/apply")
def apply_prefetch_updates():
    """Apply pending updates discovered by the prefetch check."""
    weekend_races = load_cached_races()
    if not weekend_races:
        raise HTTPException(status_code=404, detail="No cached races found")

    prefetch = run_prefetch_race_check_cached(weekend_races)
    if not prefetch or not prefetch.get("enabled"):
        return {
            "status": "success",
            "message": "Prefetch check is disabled.",
            "applied": {
                "newRaceCheckTriggered": False,
                "upcomingRefreshed": False,
                "pastHistoryRefreshed": False,
                "newRaceCachedCount": 0,
                "updatedUpcomingRaces": 0,
                "updatedUpcomingRows": 0,
                "updatedPastRaces": 0,
                "updatedPastEntries": 0,
            },
        }

    updates_by_date = prefetch.get("updatesByDate") or {}
    pending_types = {u for items in updates_by_date.values() for u in (items or [])}
    if not pending_types:
        return {
            "status": "success",
            "message": "No pending updates found.",
            "applied": {
                "newRaceCheckTriggered": False,
                "upcomingRefreshed": False,
                "pastHistoryRefreshed": False,
                "newRaceCachedCount": 0,
                "updatedUpcomingRaces": 0,
                "updatedUpcomingRows": 0,
                "updatedPastRaces": 0,
                "updatedPastEntries": 0,
            },
        }

    new_race_cached_count = 0
    upcoming_result = {"updated_races": 0, "updated_rows": 0, "failed_races": []}
    refreshed_past_races = 0
    refreshed_past_entries = 0

    if "new_race" in pending_types:
        refreshed = data_manager.fetch_weekend_timeline(mode="new") or []
        new_race_cached_count = len(refreshed)
        weekend_races = load_cached_races()

    if "new_entries" in pending_types or "post_positions" in pending_types:
        upcoming_result = _refresh_upcoming_races_in_memory(weekend_races)

    if "finish_positions" in pending_types:
        refreshed_past_races, refreshed_past_entries = refresh_missing_past_race_history(weekend_races)

    save_cached_races(weekend_races)
    _prefetch_check_cache["raceSignature"] = ""
    _prefetch_check_cache["result"] = None
    _prefetch_check_cache["expiresAt"] = None

    return {
        "status": "success",
        "applied": {
            "newRaceCheckTriggered": "new_race" in pending_types,
            "upcomingRefreshed": ("new_entries" in pending_types or "post_positions" in pending_types),
            "pastHistoryRefreshed": "finish_positions" in pending_types,
            "newRaceCachedCount": new_race_cached_count,
            "updatedUpcomingRaces": upcoming_result["updated_races"],
            "updatedUpcomingRows": upcoming_result["updated_rows"],
            "failedUpcomingRaceCount": len(upcoming_result["failed_races"]),
            "updatedPastRaces": refreshed_past_races,
            "updatedPastEntries": refreshed_past_entries,
            "pendingUpdateTypes": sorted(list(pending_types)),
        },
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
        removed_marks, _ = delete_marks_for_races(target_race_ids)

    if scope == "all" and target_horse_ids:
        removed_horses = delete_horse_cache_entries_by_ids(target_horse_ids)

    return {
        "status": "success",
        "date": target_date,
        "scope": scope,
        "removed_races": removed_races,
        "removed_marks": removed_marks,
        "removed_horse_entries": removed_horses,
        "matched_races": len(target_race_ids),
    }