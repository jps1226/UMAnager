import copy
import json
import logging
import os
import pickle
import tempfile
from pathlib import Path

import config

logger = logging.getLogger(__name__)

APP_CONFIG_DEFAULTS = {
    "sidebarTabs": {
        "raceDatabase": True,
        "pedigreeLists": True,
        "autoPickStrategy": True,
        "weekendWatchlist": True,
    },
    "ui": {
        "riskSlider": 50,
        "betSafetyIndicator": True,
        "voteSortingTop": True,
        "autoFetchPastResults": True,
        "autoLockPastVotes": False,
        "showConsole": True,
    },
}


def atomic_write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=target.parent) as tmp:
        json.dump(payload, tmp, ensure_ascii=False, indent=4)
        tmp_path = tmp.name
    os.replace(tmp_path, target)


def atomic_write_text(path, text):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=target.parent) as tmp:
        tmp.write(text)
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
        return copy.deepcopy(default)
    try:
        with open(target, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to read JSON from %s: %s", target, exc)
        return copy.deepcopy(default)


def load_text_file(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()
    return ""


def load_pickle(path, default=None):
    if not os.path.exists(path):
        return [] if default is None else default
    with open(path, "rb") as f:
        return pickle.load(f)


def _merge_dicts(defaults, override):
    if not isinstance(defaults, dict):
        return copy.deepcopy(override) if override is not None else copy.deepcopy(defaults)
    merged = copy.deepcopy(defaults)
    if not isinstance(override, dict):
        return merged
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def load_app_config(path=None):
    config_path = path or (config.DATA_DIR / "config.json")
    stored = safe_read_json(config_path, {})
    return _merge_dicts(APP_CONFIG_DEFAULTS, stored)


def save_app_config(config_data, path=None):
    config_path = path or (config.DATA_DIR / "config.json")
    atomic_write_json(config_path, config_data)