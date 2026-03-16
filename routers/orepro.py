import datetime
import logging
import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter
from pydantic import BaseModel

import config
from storage import atomic_write_json, safe_read_json

router = APIRouter(tags=["orepro"])
logger = logging.getLogger(__name__)

OREPRO_URL = "https://orepro.netkeiba.com/bet/race_list.html"
SESSION_FILE = Path(config.DATA_DIR) / "orepro_session.json"
LAST_SYNC_FILE = Path(config.DATA_DIR) / "orepro_last_sync.json"


class OreProSessionPayload(BaseModel):
    nkauth: str = ""


def _read_session():
    return safe_read_json(SESSION_FILE, {"nkauth": "", "updatedAt": ""})


def _mask_cookie(value: str):
    if not value:
        return ""
    value = str(value)
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def _extract_summary_lines(text: str):
    keywords = ["払戻", "購入", "収支", "投票", "的中", "残高", "結果", "bets", "payout", "profit"]
    lines = []
    for raw in text.split("\n"):
        line = raw.strip()
        if not line:
            continue
        lower = line.lower()
        if any(k in line for k in keywords[:7]) or any(k in lower for k in keywords[7:]):
            lines.append(line)
        if len(lines) >= 30:
            break
    return lines


def _extract_yen_values(text: str):
    vals = []
    for match in re.findall(r"([+\-]?\d[\d,]*)\s*円", text):
        v = match.strip()
        if v not in vals:
            vals.append(v)
        if len(vals) >= 20:
            break
    return vals


@router.get("/api/orepro/session")
def get_orepro_session():
    session = _read_session()
    nkauth = str(session.get("nkauth", "")).strip()
    return {
        "configured": bool(nkauth),
        "masked": _mask_cookie(nkauth),
        "updatedAt": session.get("updatedAt", ""),
    }


@router.post("/api/orepro/session")
def save_orepro_session(payload: OreProSessionPayload):
    nkauth = str(payload.nkauth or "").strip()
    if not nkauth:
        return {"status": "error", "message": "nkauth is required"}

    atomic_write_json(
        SESSION_FILE,
        {
            "nkauth": nkauth,
            "updatedAt": datetime.datetime.now().isoformat(timespec="seconds"),
        },
    )
    return {"status": "success", "masked": _mask_cookie(nkauth)}


@router.post("/api/orepro/session/clear")
def clear_orepro_session():
    atomic_write_json(SESSION_FILE, {"nkauth": "", "updatedAt": ""})
    return {"status": "success"}


@router.post("/api/orepro/results/sync")
def sync_orepro_results():
    session = _read_session()
    nkauth = str(session.get("nkauth", "")).strip()
    if not nkauth:
        return {
            "status": "error",
            "loggedIn": False,
            "message": "No nkauth configured. Save your cookie first.",
        }

    try:
        resp = requests.get(
            OREPRO_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Referer": OREPRO_URL,
            },
            cookies={"nkauth": nkauth},
            timeout=12,
        )
    except requests.RequestException as exc:
        logger.warning("OrePro sync request failed: %s", exc)
        return {
            "status": "error",
            "loggedIn": False,
            "message": f"OrePro request failed: {exc}",
        }

    soup = BeautifulSoup(resp.text or "", "html.parser")
    text = soup.get_text("\n", strip=True)
    lowered = text.lower()
    login_markers = ["ログイン", "会員", "signin", "sign in", "login"]
    logged_in = not any(marker in lowered for marker in login_markers)

    summary_lines = _extract_summary_lines(text)
    yen_values = _extract_yen_values(text)

    payload = {
        "status": "success" if logged_in else "warn",
        "loggedIn": logged_in,
        "message": "OrePro results synced." if logged_in else "Cookie seems invalid or expired. Open OrePro and refresh nkauth.",
        "httpStatus": resp.status_code,
        "fetchedAt": datetime.datetime.now().isoformat(timespec="seconds"),
        "summaryLines": summary_lines,
        "yenValues": yen_values,
    }
    atomic_write_json(LAST_SYNC_FILE, payload)
    return payload


@router.get("/api/orepro/results/last")
def get_last_orepro_sync():
    return safe_read_json(
        LAST_SYNC_FILE,
        {
            "status": "idle",
            "loggedIn": False,
            "message": "No OrePro sync has been run yet.",
            "summaryLines": [],
            "yenValues": [],
        },
    )
