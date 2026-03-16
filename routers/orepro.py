import datetime
import json
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
BET_API_URL = "https://orepro.netkeiba.com/bet/api_get_bet_race.html"
RACE_YOSO_URL = "https://orepro.netkeiba.com/mydata/race_yoso_list.html"
GOODS_LIST_API_URL = "https://orepro.netkeiba.com/api/api_get_goods_list.html"
MYTOP_URL = "https://orepro.netkeiba.com/mydata/mytop.html"
YOSOKA_KAISAI_NAVI_URL = "https://orepro.netkeiba.com/mydata/api_get_yosoka_kaisai_navi.html"
MYDATA_API_URL = "https://orepro.netkeiba.com/mydata/api_get_mydata.html"
SESSION_FILE = Path(config.DATA_DIR) / "orepro_session.json"
LAST_SYNC_FILE = Path(config.DATA_DIR) / "orepro_last_sync.json"
HISTORY_FILE = Path(config.DATA_DIR) / "orepro_results_history.json"


class OreProSessionPayload(BaseModel):
    nkauth: str = ""


class OreProSyncRequest(BaseModel):
    kaisai_date: str = ""   # YYYYMMDD, e.g. "20260314"
    kaisai_id: str = ""     # optional venue ID, e.g. "2026060205"
    yosoka_id: str = ""     # optional public OrePro profile ID (e.g. 20021241)


def _read_session():
    return safe_read_json(SESSION_FILE, {"nkauth": "", "updatedAt": ""})


def _read_history():
    history = safe_read_json(HISTORY_FILE, {"entries": []})
    entries = history.get("entries", []) if isinstance(history, dict) else []
    entries = entries if isinstance(entries, list) else []
    if entries:
        return entries

    last_payload = safe_read_json(LAST_SYNC_FILE, {})
    seeded_entry = _build_history_entry(last_payload)
    if seeded_entry is None:
        return []

    atomic_write_json(HISTORY_FILE, {"entries": [seeded_entry]})
    return [seeded_entry]


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


def _extract_kaisai_ids_from_race_list(soup: BeautifulSoup):
    ids = []
    for a in soup.select('a[href*="race_list.html?kaisai_id="]'):
        href = str(a.get("href", ""))
        match = re.search(r"kaisai_id=([A-Za-z0-9]+)", href)
        if match:
            kid = match.group(1).strip()
            if kid and kid not in ids:
                ids.append(kid)
    return ids


def _extract_race_ids_from_race_list(soup: BeautifulSoup):
    race_ids = []
    for el in soup.find_all(id=re.compile(r"^myhorse_")):
        rid = str(el.get("id", "")).replace("myhorse_", "").strip()
        if rid and rid not in race_ids:
            race_ids.append(rid)
    return race_ids


def _parse_money_to_int(text: str):
    if text is None:
        return None
    raw = str(text).strip()
    if not raw:
        return None
    sign = -1 if raw.startswith("-") else 1
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return None
    return sign * int(digits)


def _format_yen(value):
    if value is None:
        return ""
    sign = "+" if value > 0 else ""
    return f"{sign}{value:,}円"


def _build_history_entry(payload):
    if not isinstance(payload, dict):
        return None
    summary = payload.get("myBetSummary") or {}
    date_key = str(payload.get("kaisai_date") or "").strip()
    profile_id = str(((payload.get("debug") or {}).get("yosokaIdUsed") or payload.get("memberId") or "")).strip()
    races = int(summary.get("races") or 0)
    if not date_key or not profile_id or races <= 0:
        return None

    purchase = int(summary.get("purchase") or 0)
    payout = int(summary.get("payout") or 0)
    profit = int(summary.get("profit") or 0)
    return {
        "date": date_key,
        "profileId": profile_id,
        "username": str(payload.get("username") or ((payload.get("debug") or {}).get("username") or "")).strip(),
        "kaisaiId": str(payload.get("kaisai_id") or "").strip(),
        "resolvedKaisaiIds": payload.get("resolvedKaisaiIds") or [],
        "isPartial": bool(payload.get("kaisai_id")),
        "races": races,
        "purchase": purchase,
        "purchaseLabel": _format_yen(purchase),
        "payout": payout,
        "payoutLabel": _format_yen(payout),
        "profit": profit,
        "profitLabel": _format_yen(profit),
        "fetchedAt": str(payload.get("fetchedAt") or ""),
        "myRaceResults": payload.get("myRaceResults") or [],
    }


def _should_replace_history_entry(existing, incoming):
    if not isinstance(existing, dict):
        return True
    existing_partial = bool(existing.get("isPartial"))
    incoming_partial = bool(incoming.get("isPartial"))
    if existing_partial and not incoming_partial:
        return True
    if not existing_partial and incoming_partial:
        return False

    existing_races = int(existing.get("races") or 0)
    incoming_races = int(incoming.get("races") or 0)
    if incoming_races != existing_races:
        return incoming_races > existing_races

    return str(incoming.get("fetchedAt") or "") >= str(existing.get("fetchedAt") or "")


def _summarize_history(entries):
    valid_entries = [entry for entry in entries if isinstance(entry, dict)]
    sorted_entries = sorted(valid_entries, key=lambda item: (str(item.get("date") or ""), str(item.get("fetchedAt") or "")), reverse=True)
    purchase = sum(int(entry.get("purchase") or 0) for entry in sorted_entries)
    payout = sum(int(entry.get("payout") or 0) for entry in sorted_entries)
    profit = sum(int(entry.get("profit") or 0) for entry in sorted_entries)
    races = sum(int(entry.get("races") or 0) for entry in sorted_entries)
    roi_pct = round((payout / purchase) * 100, 1) if purchase > 0 else 0.0
    best_day = max(sorted_entries, key=lambda entry: int(entry.get("profit") or 0), default=None)
    worst_day = min(sorted_entries, key=lambda entry: int(entry.get("profit") or 0), default=None)

    return {
        "status": "success",
        "entries": sorted_entries,
        "totals": {
            "days": len(sorted_entries),
            "races": races,
            "purchase": purchase,
            "purchaseLabel": _format_yen(purchase),
            "payout": payout,
            "payoutLabel": _format_yen(payout),
            "profit": profit,
            "profitLabel": _format_yen(profit),
            "roiPct": roi_pct,
            "bestDay": best_day,
            "worstDay": worst_day,
            "lastUpdatedAt": sorted_entries[0].get("fetchedAt", "") if sorted_entries else "",
        },
    }


def _upsert_history_from_payload(payload):
    history_entry = _build_history_entry(payload)
    current_entries = _read_history()
    if history_entry is None:
        return _summarize_history(current_entries)

    match_index = None
    for index, entry in enumerate(current_entries):
        if not isinstance(entry, dict):
            continue
        if str(entry.get("date") or "") == history_entry["date"] and str(entry.get("profileId") or "") == history_entry["profileId"]:
            match_index = index
            break

    if match_index is None:
        current_entries.append(history_entry)
    elif _should_replace_history_entry(current_entries[match_index], history_entry):
        current_entries[match_index] = history_entry

    atomic_write_json(HISTORY_FILE, {"entries": current_entries})
    return _summarize_history(current_entries)


def _decode_goods_list_payload(raw_text: str):
    txt = (raw_text or "").strip()
    # Expected wrapper: ("...escaped html...")
    m = re.match(r"^\((.*)\)$", txt, flags=re.DOTALL)
    if not m:
        return txt
    inner = m.group(1)
    try:
        # Inner is typically a JSON string literal
        return json.loads(inner)
    except Exception:
        # Fallback decode for escaped payload
        return bytes(inner.strip('"'), "utf-8").decode("unicode_escape", errors="ignore")


def _decode_jsonp_object(raw_text: str):
    txt = (raw_text or "").strip()
    if txt.startswith("(") and txt.endswith(")"):
        txt = txt[1:-1]
    try:
        return json.loads(txt)
    except Exception:
        return {}


def _extract_goods_entry_metrics(li_node):
    metric_map = {"payout": None, "purchase": None, "profit": None}
    for dl in li_node.select("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        if not dts or not dds:
            continue
        for dt, dd in zip(dts, dds):
            key = dt.get_text(" ", strip=True)
            val_text = dd.get_text(" ", strip=True)
            if "払戻" in key:
                metric_map["payout"] = _parse_money_to_int(val_text)
            elif "購入" in key:
                metric_map["purchase"] = _parse_money_to_int(val_text)
            elif "収支" in key:
                metric_map["profit"] = _parse_money_to_int(val_text)
    return metric_map


def _extract_member_id_from_race_page_html(html_text: str):
    m = re.search(r"memberId'\s*:\s*'?(\d+)'?", html_text or "")
    return m.group(1) if m else ""


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

    # Strip any non-ASCII characters — cookie header values must be latin-1 safe.
    # Non-ASCII can sneak in when copying from Japanese browser pages.
    sanitized = nkauth.encode("ascii", errors="ignore").decode("ascii").strip()
    if not sanitized:
        return {"status": "error", "message": "Cookie value contained only non-ASCII characters. Copy just the cookie value (alphanumeric/symbols only)."}
    if sanitized != nkauth:
        logger.warning("nkauth contained non-ASCII characters that were stripped.")

    atomic_write_json(
        SESSION_FILE,
        {
            "nkauth": sanitized,
            "updatedAt": datetime.datetime.now().isoformat(timespec="seconds"),
        },
    )
    return {"status": "success", "masked": _mask_cookie(sanitized), "stripped": sanitized != nkauth}


@router.post("/api/orepro/session/clear")
def clear_orepro_session():
    atomic_write_json(SESSION_FILE, {"nkauth": "", "updatedAt": ""})
    return {"status": "success"}


@router.post("/api/orepro/results/sync")
def sync_orepro_results(req: OreProSyncRequest = None):
    if req is None:
        req = OreProSyncRequest()
    session = _read_session()
    nkauth = str(session.get("nkauth", "")).strip()
    # Ensure the stored value is still ASCII-safe (guard against old bad values)
    nkauth = nkauth.encode("ascii", errors="ignore").decode("ascii").strip()
    if not nkauth:
        return {
            "status": "error",
            "loggedIn": False,
            "message": "No nkauth configured. Save your cookie first.",
            "raceIds": [],
            "betData": {},
            "summaryLines": [],
            "yenValues": [],
        }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": OREPRO_URL,
    }
    cookies = {"nkauth": nkauth}
    logger.warning(
        "OrePro sync start: date=%s kaisai_id=%s cookie=%s",
        req.kaisai_date or "(none)",
        req.kaisai_id or "(auto)",
        _mask_cookie(nkauth),
    )

    # Build URL params for the specific date/venue if provided
    params: dict = {}
    if req.kaisai_date:
        params["kaisai_date"] = req.kaisai_date
    if req.kaisai_id:
        params["kaisai_id"] = req.kaisai_id

    try:
        resp = requests.get(
            OREPRO_URL,
            params=params,
            headers=headers,
            cookies=cookies,
            timeout=12,
        )
    except requests.RequestException as exc:
        logger.warning("OrePro sync request failed: %s", exc)
        return {
            "status": "error",
            "loggedIn": False,
            "message": f"OrePro request failed: {exc}",
            "raceIds": [],
            "betData": {},
            "summaryLines": [],
            "yenValues": [],
        }

    primary_soup = BeautifulSoup(resp.content, "html.parser", from_encoding="euc-jp")

    # Parse with EUC-JP encoding (netkeiba uses EUC-JP)
    text = primary_soup.get_text("\n", strip=True)

    # Extract logged-in username from header_nickname span
    username = ""
    nickname_el = primary_soup.find("span", class_="header_nickname")
    if nickname_el:
        raw_nick = nickname_el.get_text(strip=True)
        # Strip trailing さん if present
        username = raw_nick.removesuffix("さん").strip()
    member_id = _extract_member_id_from_race_page_html(resp.text or "")

    # Check whether the same cookie is recognized as logged in on mydata pages.
    account_logged_in = None
    account_member_rank = ""
    try:
        mytop_resp = requests.get(
            MYTOP_URL,
            headers={**headers, "Referer": OREPRO_URL},
            cookies=cookies,
            timeout=12,
        )
        mytop_text = mytop_resp.text or ""
        rank_match = re.search(r"memberRank'\s*:\s*'([^']*)'", mytop_text)
        if rank_match:
            account_member_rank = rank_match.group(1).strip()
            account_logged_in = account_member_rank.lower() != "notlogin"
        else:
            account_logged_in = "pid=login" not in mytop_text.lower()
        logger.warning(
            "OrePro mytop auth check: loggedIn=%s memberRank=%s http=%s",
            account_logged_in,
            account_member_rank or "(unknown)",
            mytop_resp.status_code,
        )
    except requests.RequestException as exc:
        logger.warning("OrePro mytop auth check failed: %s", exc)
    logger.warning(
        "OrePro identity detect: username=%s memberId=%s",
        username or "(empty)",
        member_id or "(empty)",
    )

    # Determine venue list. If user supplied one, use it; otherwise discover all venues for the day.
    if req.kaisai_id:
        venue_ids = [req.kaisai_id]
    else:
        venue_ids = _extract_kaisai_ids_from_race_list(primary_soup)

    # Collect race IDs for all selected venues
    race_ids = []
    if venue_ids and req.kaisai_date:
        for venue_id in venue_ids:
            try:
                venue_resp = requests.get(
                    OREPRO_URL,
                    params={"kaisai_date": req.kaisai_date, "kaisai_id": venue_id},
                    headers=headers,
                    cookies=cookies,
                    timeout=12,
                )
                venue_soup = BeautifulSoup(venue_resp.content, "html.parser", from_encoding="euc-jp")
                for rid in _extract_race_ids_from_race_list(venue_soup):
                    if rid not in race_ids:
                        race_ids.append(rid)
            except requests.RequestException as exc:
                logger.warning("OrePro venue fetch failed for %s: %s", venue_id, exc)
    else:
        race_ids = _extract_race_ids_from_race_list(primary_soup)

    # Pull race-level pages where actual bet info lives
    race_pages = []
    my_race_results = []
    total_purchase = 0
    total_payout = 0
    total_profit = 0
    race_debug = []
    for rid in race_ids:
        try:
            race_resp = requests.get(
                RACE_YOSO_URL,
                params={"race_id": rid},
                headers=headers,
                cookies=cookies,
                timeout=12,
            )
            race_soup = BeautifulSoup(race_resp.content, "html.parser", from_encoding="euc-jp")
            race_text = race_soup.get_text("\n", strip=True)

            # Pull goods list (contains purchase/payout/profit cards)
            goods_html = ""
            goods_entries = []
            my_entry = None
            try:
                goods_resp = requests.post(
                    GOODS_LIST_API_URL,
                    data={
                        "input": "UTF-8",
                        "output": "jsonp",
                        "show_id": "yoso_view",
                        "cond_type": "r",
                        "race_id": rid,
                    },
                    headers={**headers, "Referer": f"{RACE_YOSO_URL}?race_id={rid}"},
                    cookies=cookies,
                    timeout=15,
                )
                goods_html = _decode_goods_list_payload(goods_resp.text or "")
                goods_soup = BeautifulSoup(goods_html, "html.parser")
                li_nodes = goods_soup.select("li.Selectable")
                for li in goods_soup.select("li.Selectable"):
                    profile_link = li.select_one('.Profile a[href*="/mydata/mytop.html?id="]')
                    member = ""
                    if profile_link:
                        href = str(profile_link.get("href", ""))
                        mm = re.search(r"id=(\d+)", href)
                        member = mm.group(1) if mm else ""
                    if not member:
                        hidden_member = li.select_one('input[id^="prop_"]')
                        if hidden_member:
                            member = str(hidden_member.get("value", "")).strip()

                    metrics = _extract_goods_entry_metrics(li)
                    if metrics["purchase"] is None and metrics["payout"] is None and metrics["profit"] is None:
                        continue
                    entry = {
                        "memberId": member,
                        "purchase": metrics["purchase"],
                        "payout": metrics["payout"],
                        "profit": metrics["profit"],
                    }
                    goods_entries.append(entry)

                profile_id = str(req.yosoka_id or member_id or "").strip()
                if profile_id:
                    my_entry = next((e for e in goods_entries if str(e.get("memberId") or "") == profile_id), None)

                unique_members = sorted({str(e.get("memberId") or "") for e in goods_entries if e.get("memberId")})
                race_debug_item = {
                    "raceId": rid,
                    "goodsHttp": goods_resp.status_code,
                    "goodsLen": len(goods_resp.text or ""),
                    "listItems": len(li_nodes),
                    "parsedEntries": len(goods_entries),
                    "memberIdsSample": unique_members[:8],
                    "myEntryFound": bool(my_entry),
                }
                race_debug.append(race_debug_item)
                if len(race_debug) <= 12:
                    logger.warning(
                        "OrePro race debug %s: goodsHttp=%s goodsLen=%s listItems=%s parsedEntries=%s myEntry=%s sampleMembers=%s",
                        rid,
                        goods_resp.status_code,
                        len(goods_resp.text or ""),
                        len(li_nodes),
                        len(goods_entries),
                        bool(my_entry),
                        ",".join(unique_members[:6]) if unique_members else "(none)",
                    )

            except requests.RequestException as exc:
                logger.warning("OrePro goods list fetch failed for %s: %s", rid, exc)

            race_pages.append(
                {
                    "raceId": rid,
                    "url": f"{RACE_YOSO_URL}?race_id={rid}",
                    "httpStatus": race_resp.status_code,
                    "yenValues": _extract_yen_values(race_text),
                    "summaryLines": _extract_summary_lines(race_text),
                    "goodsEntries": len(goods_entries),
                    "myEntry": my_entry,
                }
            )

            if my_entry:
                p = my_entry.get("purchase") or 0
                r = my_entry.get("payout") or 0
                s = my_entry.get("profit")
                if s is None:
                    s = r - p
                total_purchase += p
                total_payout += r
                total_profit += s
                my_race_results.append(
                    {
                        "raceId": rid,
                        "purchase": p,
                        "purchaseLabel": _format_yen(p),
                        "payout": r,
                        "payoutLabel": _format_yen(r),
                        "profit": s,
                        "profitLabel": _format_yen(s),
                    }
                )
        except requests.RequestException as exc:
            race_pages.append(
                {
                    "raceId": rid,
                    "url": f"{RACE_YOSO_URL}?race_id={rid}",
                    "error": str(exc),
                    "yenValues": [],
                    "summaryLines": [],
                }
            )

    # Call the bet data API to get per-race bet info (amounts, wins, payouts)
    bet_data: dict = {}
    api_raw: str = ""
    api_status = None
    if race_ids:
        try:
            # Build POST body: race_id[0]=..., race_id[1]=..., etc.
            api_post: dict = {"input": "UTF-8", "output": "json", "action": "my_bet_data"}
            for i, rid in enumerate(race_ids):
                api_post[f"race_id[{i}]"] = rid
            api_resp = requests.post(
                BET_API_URL,
                data=api_post,
                headers=headers,
                cookies=cookies,
                timeout=15,
            )
            api_status = api_resp.status_code
            api_raw = api_resp.text[:8000]
            try:
                bet_data = api_resp.json()
            except Exception:
                bet_data = {"raw": api_raw}
        except requests.RequestException as exc:
            logger.warning("OrePro bet API call failed: %s", exc)
            bet_data = {"error": str(exc)}

    # Fallback: public profile mode (mytop id) uses cond_type='y' and mydata APIs.
    profile_id = str(req.yosoka_id or member_id or "").strip()
    profile_daily = None
    if profile_id and req.kaisai_date and len(my_race_results) == 0:
        try:
            navi_resp = requests.post(
                YOSOKA_KAISAI_NAVI_URL,
                data={"input": "UTF-8", "output": "jsonp", "id": profile_id, "date": req.kaisai_date},
                headers=headers,
                cookies=cookies,
                timeout=15,
            )
            navi_json = _decode_jsonp_object(navi_resp.text)
            navi_data = (navi_json or {}).get("data", {}) if isinstance(navi_json, dict) else {}
            search_key = str(navi_data.get("mydata_search_key") or req.kaisai_date)

            # Daily totals for selected key
            mydata_resp = requests.post(
                MYDATA_API_URL,
                data={"action": "daily", "id": profile_id, "output": "jsonp"},
                headers=headers,
                cookies=cookies,
                timeout=15,
            )
            mydata_json = _decode_jsonp_object(mydata_resp.text)
            mydata_all = (mydata_json or {}).get("data", {}) if isinstance(mydata_json, dict) else {}
            profile_daily = mydata_all.get(search_key)

            # Per-race cards for selected tab
            goods_resp = requests.post(
                GOODS_LIST_API_URL,
                data={
                    "input": "UTF-8",
                    "output": "jsonp",
                    "show_id": "yoso_view",
                    "cond_type": "y",
                    "yosoka_id": profile_id,
                    "search_date_tab": search_key,
                },
                headers=headers,
                cookies=cookies,
                timeout=20,
            )
            goods_html = _decode_goods_list_payload(goods_resp.text or "")
            goods_soup = BeautifulSoup(goods_html, "html.parser")

            fallback_rows = []
            fallback_purchase = 0
            fallback_payout = 0
            fallback_profit = 0
            for li in goods_soup.select("li.Selectable"):
                metrics = _extract_goods_entry_metrics(li)
                p = metrics.get("purchase")
                r = metrics.get("payout")
                s = metrics.get("profit")
                if p is None and r is None and s is None:
                    continue
                race_btn = li.select_one('button[id^="orerace_"]')
                race_id = ""
                if race_btn:
                    race_id = str(race_btn.get("id", "")).replace("orerace_", "").strip()
                p = 0 if p is None else p
                r = 0 if r is None else r
                if s is None:
                    s = r - p
                fallback_purchase += p
                fallback_payout += r
                fallback_profit += s
                fallback_rows.append(
                    {
                        "raceId": race_id,
                        "purchase": p,
                        "purchaseLabel": _format_yen(p),
                        "payout": r,
                        "payoutLabel": _format_yen(r),
                        "profit": s,
                        "profitLabel": _format_yen(s),
                    }
                )

            if fallback_rows:
                my_race_results = fallback_rows
                total_purchase = fallback_purchase
                total_payout = fallback_payout
                total_profit = fallback_profit

            # Prefer mydata daily totals if available
            if isinstance(profile_daily, dict):
                daily_my = profile_daily.get("mydata", {})
                dp = _parse_money_to_int(daily_my.get("price", ""))
                dr = _parse_money_to_int(daily_my.get("payback", ""))
                dd = _parse_money_to_int(daily_my.get("diff", ""))
                if dp is not None:
                    total_purchase = dp
                if dr is not None:
                    total_payout = dr
                if dd is not None:
                    total_profit = dd

            logger.warning(
                "OrePro public-profile fallback: yosoka_id=%s search_key=%s rows=%d",
                profile_id,
                search_key,
                len(my_race_results),
            )
        except requests.RequestException as exc:
            logger.warning("OrePro public-profile fallback failed: %s", exc)

    # Determine auth state from concrete signals, not generic page text markers.
    # Generic "login" words can appear in scripts even when authenticated.
    has_username = bool(username)
    has_races = bool(race_ids)
    api_text = ""
    if isinstance(bet_data, dict):
        if "raw" in bet_data:
            api_text = str(bet_data.get("raw", ""))
        else:
            api_text = json.dumps(bet_data, ensure_ascii=False)
    invalid_markers = ["未ログイン", "ログイン", "login", "sign in", "signin", "unauthorized", "forbidden"]
    api_looks_invalid = any(marker in api_text.lower() for marker in [m.lower() for m in invalid_markers]) if api_text else False
    if isinstance(bet_data, dict):
        has_api_data = bool(bet_data) and not bet_data.get("error") and not api_looks_invalid
    else:
        has_api_data = bool(bet_data)
    logged_in = has_username or has_races or has_api_data

    # Prefer summary/money extracted from race pages (where bet info actually exists)
    summary_lines = []
    yen_values = []
    for page in race_pages:
        for line in page.get("summaryLines", []):
            if line not in summary_lines:
                summary_lines.append(line)
            if len(summary_lines) >= 60:
                break
        for v in page.get("yenValues", []):
            if v not in yen_values:
                yen_values.append(v)
            if len(yen_values) >= 40:
                break
    if not summary_lines:
        summary_lines = _extract_summary_lines(text)
    if not yen_values:
        yen_values = _extract_yen_values(text)

    payload = {
        "status": "success" if logged_in else "warn",
        "loggedIn": logged_in,
        "username": username,
        "memberId": member_id,
        "message": (
            f"OrePro results synced. Found {len(race_ids)} races."
            if logged_in
            else "Could not confirm OrePro login from response data. Refresh nkauth and retry."
        ),
        "httpStatus": resp.status_code,
        "betApiStatus": api_status,
        "fetchedAt": datetime.datetime.now().isoformat(timespec="seconds"),
        "kaisai_date": req.kaisai_date,
        "kaisai_id": req.kaisai_id,
        "resolvedKaisaiIds": venue_ids,
        "raceIds": race_ids,
        "racePages": race_pages,
        "myBetSummary": {
            "races": len(my_race_results),
            "purchase": total_purchase,
            "purchaseLabel": _format_yen(total_purchase),
            "payout": total_payout,
            "payoutLabel": _format_yen(total_payout),
            "profit": total_profit,
            "profitLabel": _format_yen(total_profit),
        },
        "myRaceResults": my_race_results,
        "debug": {
            "memberId": member_id,
            "username": username,
            "yosokaIdUsed": profile_id,
            "profileDailyFound": bool(profile_daily),
            "accountLoggedIn": account_logged_in,
            "accountMemberRank": account_member_rank,
            "raceDebug": race_debug,
            "myEntryMatches": len(my_race_results),
        },
        "betData": bet_data,
        "summaryLines": summary_lines,
        "yenValues": yen_values,
    }
    if payload["myBetSummary"]["races"] == 0 and account_logged_in is False:
        payload["message"] = (
            "No personal bet cards found: mydata account appears not logged in with current cookie. "
            "nkauth can load race pages but not your personal bet feed."
        )
    elif payload["myBetSummary"]["races"] == 0:
        payload["message"] = (
            "No personal bet cards found for the selected day/venue. "
            "Try the exact day/race where you know you placed bets."
        )
    logger.warning(
        "OrePro sync done: status=%s loggedIn=%s venues=%s races=%d betApiStatus=%s memberId=%s myMatches=%d",
        payload.get("status"),
        payload.get("loggedIn"),
        ",".join(venue_ids) if venue_ids else "(none)",
        len(race_ids),
        api_status,
        member_id or "(empty)",
        len(my_race_results),
    )
    payload["historySummary"] = _upsert_history_from_payload(payload)
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


@router.get("/api/orepro/results/history")
def get_orepro_history():
    return _summarize_history(_read_history())
