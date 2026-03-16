from fastapi import APIRouter
import os
import re
from typing import Any, Literal

from pydantic import BaseModel

import config
import data_manager
from storage import atomic_write_text, load_app_config, load_text_file, save_app_config

router = APIRouter(tags=["lists-config"])

TRACKING_FILE = config.TRACKING_FILE
WATCHLIST_FILE = config.WATCHLIST_FILE


class ListsPayload(BaseModel):
    favorites: str = ""
    watchlist: str = ""


class SnipeRequest(BaseModel):
    url: str = ""
    id: str = ""
    list_type: Literal["favorites", "watchlist"] = "favorites"


def validate_horse_id(horse_id):
    if not horse_id:
        return False
    horse_id_str = str(horse_id).strip()
    return bool(re.match(config.HORSE_ID_PATTERN, horse_id_str))


def validate_url(url_str):
    if not url_str:
        return None
    candidate = str(url_str).strip()
    if validate_horse_id(candidate):
        return candidate
    match = re.search(r"/([a-zA-Z0-9]{10})", candidate)
    return match.group(1) if match else None


@router.get("/api/lists")
def get_lists():
    return {
        "favorites": load_text_file(TRACKING_FILE),
        "watchlist": load_text_file(WATCHLIST_FILE),
    }


@router.post("/api/lists")
async def save_lists(payload: ListsPayload):
    atomic_write_text(TRACKING_FILE, payload.favorites)
    atomic_write_text(WATCHLIST_FILE, payload.watchlist)
    return {"status": "success"}


@router.post("/api/snipe")
async def snipe_horse(payload: SnipeRequest):
    url = payload.url.strip()
    direct_id = payload.id.strip()
    list_type = payload.list_type.strip()

    new_id = None
    if direct_id:
        if validate_horse_id(direct_id):
            new_id = direct_id
        else:
            return {"status": "error", "message": "Invalid horse ID format"}
    elif url:
        new_id = validate_url(url)
        if not new_id:
            return {"status": "error", "message": "Invalid URL or horse ID format"}
    else:
        return {"status": "error", "message": "Either URL or ID must be provided"}

    current_favorites = load_text_file(TRACKING_FILE)
    current_watchlist = load_text_file(WATCHLIST_FILE)

    if new_id in current_favorites or new_id in current_watchlist:
        return {"status": "error", "message": "ID already tracked."}

    target_f = TRACKING_FILE if list_type == "favorites" else WATCHLIST_FILE
    current_c = load_text_file(target_f)

    try:
        h_data = data_manager.get_horse_data(new_id, "Unknown")
        h_name = h_data.get("name", "Unknown")

        prefix = "\n" if current_c and not current_c.endswith("\n") else ""
        with open(target_f, "a", encoding="utf-8") as f:
            f.write(f"{prefix}{new_id} # {h_name}\n")
        return {"status": "success", "message": f"Added {h_name}!"}
    except Exception:
        return {"status": "error", "message": "Failed to add horse"}


@router.get("/api/config")
def get_config():
    return load_app_config()


@router.post("/api/config")
async def update_config(config_data: dict[str, Any]):
    save_app_config(config_data)
    return {"status": "success"}
