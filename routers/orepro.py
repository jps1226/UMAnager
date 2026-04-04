import datetime
import json
import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Dict, List

import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter
from pydantic import BaseModel

from storage import orepro_get_history_summary, orepro_get_last_sync_payload, orepro_upsert_history_from_payload

router = APIRouter(tags=["orepro"])
logger = logging.getLogger(__name__)

OREPRO_URL = "https://orepro.netkeiba.com/bet/race_list.html"
BET_API_URL = "https://orepro.netkeiba.com/bet/api_get_bet_race.html"
RACE_YOSO_URL = "https://orepro.netkeiba.com/mydata/race_yoso_list.html"
GOODS_LIST_API_URL = "https://orepro.netkeiba.com/api/api_get_goods_list.html"
MYTOP_URL = "https://orepro.netkeiba.com/mydata/mytop.html"
YOSOKA_KAISAI_NAVI_URL = "https://orepro.netkeiba.com/mydata/api_get_yosoka_kaisai_navi.html"
MYDATA_API_URL = "https://orepro.netkeiba.com/mydata/api_get_mydata.html"


class OreProSyncRequest(BaseModel):
    kaisai_date: str = ""   # YYYYMMDD, e.g. "20260314"
    kaisai_id: str = ""     # optional venue ID, e.g. "2026060205"
    yosoka_id: str = ""     # optional public OrePro profile ID (e.g. 20021241)


class OreProCompanionWindowRequest(BaseModel):
    action: str = "open"


class OreProVoteMark(BaseModel):
    symbol: str = ""
    post: int = 0
    mark_code: str = ""


class OreProRaceVotesRequest(BaseModel):
    race_id: str = ""
    marks: List[OreProVoteMark] = []


class OreProApplyVotesRequest(BaseModel):
    races: List[OreProRaceVotesRequest] = []
    dry_run: bool = False
    use_companion_session: bool = True
    force_refresh: bool = True
    submit_after_apply: bool = False
    go_next_race: bool = False


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


def _extract_seq_by_post_from_shutuba_html(html_text: str) -> Dict[str, str]:
    soup = BeautifulSoup(html_text or "", "html.parser", from_encoding="euc-jp")
    post_to_seq: Dict[str, str] = {}
    for row in soup.select("tr.HorseList[id^='tr_']"):
        row_id = str(row.get("id", "")).strip()
        m = re.match(r"tr_(\d+)$", row_id)
        if not m:
            continue
        seq = m.group(1)
        post_cell = row.select_one("td[id^='act_waku_']")
        if not post_cell:
            continue
        post_raw = re.sub(r"\D", "", post_cell.get_text(" ", strip=True) or "")
        if post_raw:
            post_to_seq[post_raw] = seq
    return post_to_seq


def _extract_plain_bet_summary_from_html(html_fragment: str) -> List[str]:
    if not html_fragment:
        return []
    soup = BeautifulSoup(html_fragment, "html.parser")
    lines = []
    for raw in soup.get_text("\n", strip=True).split("\n"):
        line = raw.strip()
        if not line:
            continue
        lines.append(line)
        if len(lines) >= 24:
            break
    return lines


def _orepro_window_helper_path() -> Path:
    return Path(__file__).resolve().parent.parent / "open_orepro_topmost.ps1"


def _orepro_companion_vote_helper_path() -> Path:
    return Path(__file__).resolve().parent.parent / "orepro_apply_votes_companion.ps1"


@router.post("/api/orepro/companion/window")
def control_orepro_companion_window(req: OreProCompanionWindowRequest = None):
    if req is None:
        req = OreProCompanionWindowRequest()

    action = str(req.action or "open").strip().lower()
    if action not in {"open", "focus"}:
        action = "open"

    if os.name != "nt":
        return {
            "status": "error",
            "action": action,
            "message": "Native OrePro always-on-top launching is only supported on Windows.",
        }

    script_path = _orepro_window_helper_path()
    if not script_path.exists():
        return {
            "status": "error",
            "action": action,
            "message": f"OrePro helper script is missing: {script_path}",
        }

    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(script_path),
                "-Action",
                action,
                "-Url",
                OREPRO_URL,
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except Exception as exc:
        logger.warning("OrePro native helper failed to launch: %s", exc)
        return {
            "status": "error",
            "action": action,
            "message": f"Failed to launch OrePro helper: {exc}",
        }

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    raw_output = stdout or stderr

    if result.returncode != 0:
        logger.warning("OrePro native helper returned %s: %s", result.returncode, raw_output or "(no output)")
        return {
            "status": "error",
            "action": action,
            "message": raw_output or "OrePro native helper failed.",
        }

    if raw_output:
        try:
            payload = json.loads(raw_output)
            if isinstance(payload, dict):
                payload.setdefault("status", "ok")
                payload.setdefault("action", action)
                return payload
        except json.JSONDecodeError:
            pass

    return {
        "status": "ok",
        "action": action,
        "message": raw_output or f"OrePro companion {action} request completed.",
    }


@router.post("/api/orepro/votes/apply")
def apply_orepro_votes(req: OreProApplyVotesRequest = None):
    if req is None:
        req = OreProApplyVotesRequest()

    if req.use_companion_session:
        if os.name != "nt":
            return {
                "status": "error",
                "message": "Companion-session vote apply is only supported on Windows.",
                "dryRun": bool(req.dry_run),
                "results": [],
            }

        helper_path = _orepro_companion_vote_helper_path()
        if not helper_path.exists():
            return {
                "status": "error",
                "message": f"Companion vote helper script is missing: {helper_path}",
                "dryRun": bool(req.dry_run),
                "results": [],
            }

        try:
            req_payload = req.model_dump() if hasattr(req, "model_dump") else req.dict()
            result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(helper_path),
                    "-PayloadJson",
                    json.dumps(req_payload, ensure_ascii=True),
                ],
                capture_output=True,
                text=True,
                timeout=90,
                check=False,
            )
        except Exception as exc:
            return {
                "status": "error",
                "message": f"Failed launching companion vote helper: {exc}",
                "dryRun": bool(req.dry_run),
                "results": [],
            }

        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        raw = stdout or stderr

        if result.returncode != 0:
            return {
                "status": "error",
                "message": raw or "Companion vote helper failed. Click Open OrePro once, then retry.",
                "dryRun": bool(req.dry_run),
                "results": [],
            }

        if raw:
            cleaned = raw.replace("\ufeff", "").strip()
            candidates = [cleaned]
            for line in cleaned.splitlines():
                stripped = line.strip()
                if stripped:
                    candidates.append(stripped)

            # Extract balanced JSON object fragments from noisy/mixed stdout.
            def _extract_json_object_fragments(text: str) -> List[str]:
                fragments: List[str] = []
                starts = [idx for idx, ch in enumerate(text) if ch == "{"]
                for start in starts:
                    depth = 0
                    in_string = False
                    escaped = False
                    for idx in range(start, len(text)):
                        ch = text[idx]
                        if escaped:
                            escaped = False
                            continue
                        if ch == "\\":
                            escaped = True
                            continue
                        if ch == '"':
                            in_string = not in_string
                            continue
                        if in_string:
                            continue
                        if ch == "{":
                            depth += 1
                        elif ch == "}":
                            depth -= 1
                            if depth == 0:
                                fragments.append(text[start:idx + 1].strip())
                                break
                return fragments

            candidates.extend(_extract_json_object_fragments(cleaned))

            for candidate in reversed(candidates):
                try:
                    parsed = json.loads(candidate)
                except json.JSONDecodeError:
                    continue

                if isinstance(parsed, str):
                    nested = parsed.replace("\ufeff", "").strip()
                    try:
                        parsed = json.loads(nested)
                    except json.JSONDecodeError:
                        continue

                if isinstance(parsed, dict):
                    parsed.setdefault("dryRun", bool(req.dry_run))
                    return parsed

        return {
            "status": "error",
            "message": f"Companion helper returned no parseable JSON output. Raw: {(raw or '')[:300]}",
            "dryRun": bool(req.dry_run),
            "results": [],
        }

    symbol_to_mark_code = {"◎": "1", "〇": "2", "▲": "3", "△": "4"}
    mark_sort = {"1": 1, "2": 2, "3": 3, "4": 4}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": OREPRO_URL,
    }

    session = requests.Session()
    race_results = []

    for race_item in req.races or []:
        race_id = str(race_item.race_id or "").strip()
        if not race_id:
            race_results.append({
                "raceId": race_id,
                "status": "error",
                "message": "race_id is required.",
            })
            continue

        single_by_mark: Dict[str, str] = {}
        extra_triangles: List[str] = []
        seen_pairs = set()
        for mark in race_item.marks or []:
            raw_code = str(getattr(mark, "mark_code", "") or "").strip()
            mark_code = raw_code if raw_code in {"1", "2", "3", "4"} else symbol_to_mark_code.get(str(mark.symbol or "").strip())
            post_num = str(int(mark.post or 0))
            if not mark_code or post_num == "0":
                continue

            pair_key = (mark_code, post_num)
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            if mark_code == "4":
                extra_triangles.append(post_num)
            elif mark_code not in single_by_mark:
                single_by_mark[mark_code] = post_num

        ordered_pairs = [
            (code, post) for code, post in sorted(single_by_mark.items(), key=lambda x: mark_sort.get(x[0], 99))
        ]
        ordered_pairs.extend(("4", post) for post in extra_triangles)

        requested = [
            {"symbol": {"1": "◎", "2": "〇", "3": "▲", "4": "△"}[code], "post": int(post)}
            for code, post in ordered_pairs
        ]
        if not requested:
            race_results.append({
                "raceId": race_id,
                "status": "skipped",
                "message": "No valid main marks (1-4) to apply for this race.",
                "requested": [],
                "resolved": [],
                "unmatchedPosts": [],
            })
            continue

        try:
            shutuba_resp = session.get(
                "https://orepro.netkeiba.com/bet/shutuba.html",
                params={"race_id": race_id},
                headers=headers,
                timeout=20,
            )
            shutuba_html = shutuba_resp.text or ""
            post_to_seq = _extract_seq_by_post_from_shutuba_html(shutuba_html)
        except requests.RequestException as exc:
            race_results.append({
                "raceId": race_id,
                "status": "error",
                "message": f"Failed fetching shutuba page: {exc}",
                "requested": requested,
                "resolved": [],
                "unmatchedPosts": [r["post"] for r in requested],
            })
            continue

        resolved = []
        unmatched = []
        for mark_code, post in ordered_pairs:
            seq = post_to_seq.get(post)
            if not seq:
                unmatched.append(int(post))
                continue
            resolved.append({
                "symbol": {"1": "◎", "2": "〇", "3": "▲", "4": "△"}[mark_code],
                "post": int(post),
                "seq": int(seq),
                "markCode": int(mark_code),
            })

        if not resolved:
            race_results.append({
                "raceId": race_id,
                "status": "error",
                "message": "None of the requested post numbers were found in OrePro shutuba rows.",
                "requested": requested,
                "resolved": [],
                "unmatchedPosts": unmatched,
            })
            continue

        if req.dry_run:
            race_results.append({
                "raceId": race_id,
                "status": "dry-run",
                "message": "Dry run only. No OrePro cart updates were sent.",
                "requested": requested,
                "resolved": resolved,
                "unmatchedPosts": unmatched,
            })
            continue

        cart_payload = [
            ("input", "UTF-8"),
            ("output", "json"),
            ("action", "replace"),
            ("group", f"oremark_{race_id}"),
        ]
        for row in resolved:
            cart_payload.append(("item_id[]", str(row["seq"])))
            cart_payload.append(("item_value[]", "1"))
            cart_payload.append(("item_price[]", "0"))
            cart_payload.append(("client_data[]", f"_{row['markCode']}"))

        try:
            cart_resp = session.post(
                "https://orepro.netkeiba.com/cart/",
                data=cart_payload,
                headers={**headers, "Referer": f"https://orepro.netkeiba.com/bet/shutuba.html?race_id={race_id}"},
                timeout=20,
            )
            try:
                cart_json = cart_resp.json()
            except Exception:
                cart_json = {"raw": (cart_resp.text or "")[:1000]}

            session.post(
                BET_API_URL,
                data={"input": "UTF-8", "output": "jsonp", "race_id": race_id},
                headers=headers,
                timeout=20,
            )
            bet_view_resp = session.post(
                "https://orepro.netkeiba.com/bet/api_get_bet_view.html",
                data={"input": "UTF-8", "output": "jsonp", "race_id": race_id, "src": "session"},
                headers=headers,
                timeout=20,
            )
            bet_view_json = _decode_jsonp_object(bet_view_resp.text or "")
            bet_view_html = ""
            if isinstance(bet_view_json, dict):
                bet_view_html = str(bet_view_json.get("data") or "")
            preview_lines = _extract_plain_bet_summary_from_html(bet_view_html)

            race_results.append({
                "raceId": race_id,
                "status": "ok",
                "message": "Marks applied to OrePro cart session (no money action).",
                "requested": requested,
                "resolved": resolved,
                "unmatchedPosts": unmatched,
                "cartResponse": cart_json,
                "betPreviewLines": preview_lines,
            })
        except requests.RequestException as exc:
            race_results.append({
                "raceId": race_id,
                "status": "error",
                "message": f"Failed applying marks via OrePro API: {exc}",
                "requested": requested,
                "resolved": resolved,
                "unmatchedPosts": unmatched,
            })

    ok_count = len([r for r in race_results if r.get("status") == "ok"])
    return {
        "status": "ok" if ok_count else "warn",
        "message": (
            f"Applied marks for {ok_count}/{len(race_results)} races. "
            "This endpoint only updates OrePro mark/cart state and does not submit paid bets."
        ),
        "dryRun": bool(req.dry_run),
        "results": race_results,
    }


@router.post("/api/orepro/results/sync")
def sync_orepro_results(req: OreProSyncRequest = None):
    if req is None:
        req = OreProSyncRequest()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": OREPRO_URL,
    }
    # Public profile endpoints can be read without storing a user cookie.
    cookies = {}
    logger.warning(
        "OrePro sync start: date=%s kaisai_id=%s yosoka_id=%s",
        req.kaisai_date or "(none)",
        req.kaisai_id or "(auto)",
        req.yosoka_id or "(auto)",
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

    # Check whether mydata pages indicate a logged-in account.
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
            "No personal bet cards found: mydata account does not appear logged in. "
            "Open OrePro and sign in, then sync again."
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
    payload["historySummary"] = orepro_upsert_history_from_payload(payload)
    return payload


@router.get("/api/orepro/results/last")
def get_last_orepro_sync():
    return orepro_get_last_sync_payload()


@router.get("/api/orepro/results/history")
def get_orepro_history():
    return orepro_get_history_summary()
