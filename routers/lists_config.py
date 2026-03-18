from fastapi import APIRouter
import os
import re
from typing import Any, Literal

from pydantic import BaseModel

import config
import data_manager
from storage import (
    _parse_horse_lines_from_text,
    add_horse_to_list,
    horse_ids_to_text,
    load_app_config,
    load_horse_list,
    save_app_config,
    save_horse_list,
)

router = APIRouter(tags=["lists-config"])



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
        "favorites": horse_ids_to_text(load_horse_list("favorites")),
        "watchlist": horse_ids_to_text(load_horse_list("watchlist")),
    }


@router.post("/api/lists")
async def save_lists(payload: ListsPayload):
    save_horse_list("favorites", _parse_horse_lines_from_text(payload.favorites))
    save_horse_list("watchlist", _parse_horse_lines_from_text(payload.watchlist))
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

    tracked_ids = {h for h, _n in load_horse_list("favorites")}
    watchlist_ids = {h for h, _n in load_horse_list("watchlist")}

    if new_id in tracked_ids or new_id in watchlist_ids:
        return {"status": "error", "message": "ID already tracked."}

    try:
        h_data = data_manager.get_horse_data(new_id, "Unknown")
        h_name = h_data.get("name", "Unknown")
        add_horse_to_list(list_type, new_id, h_name)
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
