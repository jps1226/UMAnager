import datetime
import json
import re
import sqlite3
import zlib
from pathlib import Path

import config


def _utc_now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)


def _is_plausible_race_key16(token: str) -> bool:
    if not re.fullmatch(r"\d{16}", str(token or "")):
        return False
    text = str(token)
    y = int(text[0:4])
    mm = int(text[4:6])
    dd = int(text[6:8])
    jyo = int(text[8:10])
    kaiji = int(text[10:12])
    nichiji = int(text[12:14])
    race_num = int(text[14:16])
    if y < 1990 or y > 2099:
        return False
    if mm < 1 or mm > 12 or dd < 1 or dd > 31:
        return False
    if jyo < 1 or jyo > 10:
        return False
    if kaiji < 1 or kaiji > 12:
        return False
    if nichiji < 1 or nichiji > 12:
        return False
    if race_num < 1 or race_num > 12:
        return False
    return True


def _connect():
    db_path = Path(config.JRA_VAN_DB_FILE)
    _ensure_parent(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_jra_van_storage():
    conn = _connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jvlink_probe_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                sid TEXT,
                data_spec TEXT NOT NULL,
                from_date TEXT NOT NULL,
                max_read_calls INTEGER NOT NULL,
                ok INTEGER NOT NULL,
                version TEXT,
                init_code INTEGER,
                set_save_path_code INTEGER,
                set_service_key_code INTEGER,
                open_code INTEGER,
                close_code INTEGER,
                read_count INTEGER,
                download_count INTEGER,
                last_file_timestamp TEXT,
                used_service_key INTEGER NOT NULL,
                error TEXT,
                raw_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jvlink_cache_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                indexed_at TEXT NOT NULL,
                relative_path TEXT NOT NULL UNIQUE,
                file_name TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                file_mtime TEXT NOT NULL,
                prefix_code TEXT,
                record_spec2 TEXT,
                date_start TEXT,
                date_end TEXT,
                suffix TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jvlink_race_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ingested_at TEXT NOT NULL,
                source_relative_path TEXT NOT NULL,
                source_file_name TEXT NOT NULL,
                record_spec2 TEXT NOT NULL,
                race_key_16 TEXT NOT NULL,
                year TEXT NOT NULL,
                month_day TEXT NOT NULL,
                jyo_cd TEXT NOT NULL,
                kaiji TEXT NOT NULL,
                nichiji TEXT NOT NULL,
                race_num TEXT NOT NULL,
                UNIQUE(source_relative_path, record_spec2, race_key_16)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jvlink_stream_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                sid TEXT,
                data_spec TEXT NOT NULL,
                from_date TEXT NOT NULL,
                record_index INTEGER NOT NULL,
                ret_code INTEGER NOT NULL,
                data_size INTEGER NOT NULL,
                file_name TEXT,
                record_spec TEXT,
                data_kubun TEXT,
                race_key_16 TEXT,
                raw_json TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _parse_cache_file_name(file_name: str):
    stem = Path(file_name).stem.upper()
    # Example observed pattern: DMSW20250323202503241320006
    match = re.match(r"^([A-Z0-9]{4})(\d{8})(\d{8})(\d*)$", stem)
    if not match:
        return {
            "prefix_code": "",
            "record_spec2": "",
            "date_start": "",
            "date_end": "",
            "suffix": "",
        }
    prefix = match.group(1)
    return {
        "prefix_code": prefix,
        "record_spec2": prefix[:2],
        "date_start": match.group(2),
        "date_end": match.group(3),
        "suffix": match.group(4) or "",
    }


def index_cache_files(max_files: int = 5000):
    init_jra_van_storage()

    cache_root = Path(config.JVLINK_SAVE_DIR) / "cache"
    cache_root.mkdir(parents=True, exist_ok=True)

    all_files = [p for p in cache_root.rglob("*") if p.is_file()]
    all_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    safe_max = max(1, min(int(max_files), 100000))
    target_files = all_files[:safe_max]

    conn = _connect()
    inserted = 0
    try:
        for file_path in target_files:
            rel = str(file_path.relative_to(cache_root.parent)).replace("\\", "/")
            stat = file_path.stat()
            parsed = _parse_cache_file_name(file_path.name)

            conn.execute(
                """
                INSERT INTO jvlink_cache_files (
                    indexed_at, relative_path, file_name, file_size, file_mtime,
                    prefix_code, record_spec2, date_start, date_end, suffix
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(relative_path) DO UPDATE SET
                    indexed_at=excluded.indexed_at,
                    file_name=excluded.file_name,
                    file_size=excluded.file_size,
                    file_mtime=excluded.file_mtime,
                    prefix_code=excluded.prefix_code,
                    record_spec2=excluded.record_spec2,
                    date_start=excluded.date_start,
                    date_end=excluded.date_end,
                    suffix=excluded.suffix
                """,
                (
                    _utc_now_iso(),
                    rel,
                    file_path.name,
                    int(stat.st_size),
                    datetime.datetime.fromtimestamp(stat.st_mtime, datetime.timezone.utc).isoformat(),
                    parsed["prefix_code"],
                    parsed["record_spec2"],
                    parsed["date_start"],
                    parsed["date_end"],
                    parsed["suffix"],
                ),
            )
            inserted += 1
        conn.commit()

        total_rows = conn.execute("SELECT COUNT(*) FROM jvlink_cache_files").fetchone()[0]
        grouped = conn.execute(
            """
            SELECT prefix_code, COUNT(*) AS file_count
            FROM jvlink_cache_files
            GROUP BY prefix_code
            ORDER BY file_count DESC, prefix_code ASC
            LIMIT 20
            """
        ).fetchall()

        latest = conn.execute(
            """
            SELECT relative_path, file_name, file_size, file_mtime, prefix_code, record_spec2, date_start, date_end
            FROM jvlink_cache_files
            ORDER BY file_mtime DESC
            LIMIT 20
            """
        ).fetchall()

        return {
            "ok": True,
            "cacheRoot": str(cache_root),
            "scanned": len(target_files),
            "upserted": inserted,
            "totalIndexed": int(total_rows),
            "topPrefixes": [dict(row) for row in grouped],
            "latestFiles": [dict(row) for row in latest],
        }
    finally:
        conn.close()


def get_cache_max_date(specs=None) -> str:
    """Return the latest date_end across cache files for the given specs as YYYYMMDD000000.

    Used to auto-compute the JVOpen FromDate for incremental cache refreshes.
    Falls back to one year ago if no matching cache files exist.
    """
    init_jra_van_storage()

    spec_list = [str(s).upper() for s in (specs or ["RA", "SE"]) if str(s).strip()]
    placeholders = ",".join(["?"] * len(spec_list))
    conn = _connect()
    try:
        row = conn.execute(
            f"SELECT MAX(date_end) AS max_end FROM jvlink_cache_files "
            f"WHERE record_spec2 IN ({placeholders}) AND date_end != ''",
            tuple(spec_list),
        ).fetchone()
        max_end = str(row["max_end"] or "").strip() if row else ""
        if re.fullmatch(r"\d{8}", max_end):
            return max_end + "000000"
        # Fallback: one year ago
        fallback = (datetime.date.today() - datetime.timedelta(days=365)).strftime("%Y%m%d")
        return fallback + "000000"
    finally:
        conn.close()


def get_cache_index_summary(limit: int = 50):
    init_jra_van_storage()

    safe_limit = max(1, min(int(limit), 500))
    conn = _connect()
    try:
        total_rows = conn.execute("SELECT COUNT(*) FROM jvlink_cache_files").fetchone()[0]
        grouped = conn.execute(
            """
            SELECT prefix_code, record_spec2, COUNT(*) AS file_count,
                   MIN(date_start) AS min_date_start,
                   MAX(date_end) AS max_date_end
            FROM jvlink_cache_files
            GROUP BY prefix_code, record_spec2
            ORDER BY file_count DESC, prefix_code ASC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

        latest = conn.execute(
            """
            SELECT relative_path, file_name, file_size, file_mtime, prefix_code, record_spec2, date_start, date_end
            FROM jvlink_cache_files
            ORDER BY file_mtime DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

        return {
            "ok": True,
            "totalIndexed": int(total_rows),
            "groups": [dict(row) for row in grouped],
            "latestFiles": [dict(row) for row in latest],
        }
    finally:
        conn.close()


def _iter_payload_candidates(raw: bytes):
    """Yield byte payload candidates from raw file bytes.

    Many JV cache files are zlib-compressed (`...x\x9c...`) and are not directly
    searchable as plain Shift-JIS text.
    """
    if not raw:
        return
    yield raw

    idx = raw.find(b"\x78\x9c")
    if idx >= 0:
        try:
            yield zlib.decompress(raw[idx:])
        except Exception:
            pass


def _extract_race_keys_from_bytes(raw: bytes):
    # RACE_ID layout: YYYY + MMDD + JYO(2) + KAIJI(2) + NICHIJI(2) + RACE(2)
    pattern = re.compile(r"(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{8}")
    candidates = set()

    for payload in _iter_payload_candidates(raw):
        for enc in ("shift_jis", "cp932", "latin-1"):
            try:
                text = payload.decode(enc, errors="ignore")
            except Exception:
                continue
            candidates.update(pattern.findall(text))

    parsed = []
    for token in sorted(candidates):
        if not _is_plausible_race_key16(token):
            continue
        parsed.append(
            {
                "race_key_16": token,
                "year": token[0:4],
                "month_day": token[4:8],
                "jyo_cd": token[8:10],
                "kaiji": token[10:12],
                "nichiji": token[12:14],
                "race_num": token[14:16],
            }
        )
    return parsed


def cleanup_invalid_race_keys():
    """Delete rows that do not satisfy basic RACE_ID plausibility rules."""
    init_jra_van_storage()
    conn = _connect()
    try:
        before = conn.execute("SELECT COUNT(*) FROM jvlink_race_keys").fetchone()[0]
        conn.execute(
            """
            DELETE FROM jvlink_race_keys
            WHERE NOT (
                race_key_16 GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                AND year BETWEEN '1990' AND '2099'
                AND month_day BETWEEN '0101' AND '1231'
                AND jyo_cd BETWEEN '01' AND '10'
                AND kaiji BETWEEN '01' AND '12'
                AND nichiji BETWEEN '01' AND '12'
                AND race_num BETWEEN '01' AND '12'
            )
            """
        )
        conn.commit()
        after = conn.execute("SELECT COUNT(*) FROM jvlink_race_keys").fetchone()[0]
        return {
            "ok": True,
            "before": int(before),
            "after": int(after),
            "deleted": int(before) - int(after),
        }
    finally:
        conn.close()


def ingest_race_keys_from_cache(max_files: int = 500, max_keys_per_file: int = 200):
    init_jra_van_storage()
    cleanup_invalid_race_keys()

    cache_root = Path(config.JVLINK_SAVE_DIR) / "cache"
    cache_root.mkdir(parents=True, exist_ok=True)

    allowed_specs = {
        "RA", "SE", "HR", "O1", "O2", "O3", "O4", "O5", "O6", "H1", "H6",
        "BN", "JG", "TK", "BR", "CH", "RC", "UM", "KS", "HY", "HN", "SK",
    }
    safe_max_files = max(1, min(int(max_files), 50000))
    safe_max_keys = max(1, min(int(max_keys_per_file), 2000))

    all_files = [p for p in cache_root.rglob("*") if p.is_file()]
    all_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    scanned = 0
    candidate_files = 0
    files_with_keys = 0

    conn = _connect()
    try:
        total_before = conn.execute("SELECT COUNT(*) FROM jvlink_race_keys").fetchone()[0]
        for file_path in all_files[:safe_max_files]:
            scanned += 1
            parsed_name = _parse_cache_file_name(file_path.name)
            spec2 = parsed_name.get("record_spec2", "")
            if spec2 not in allowed_specs:
                continue

            candidate_files += 1
            rel = str(file_path.relative_to(cache_root.parent)).replace("\\", "/")

            try:
                raw = file_path.read_bytes()
            except Exception:
                continue

            keys = _extract_race_keys_from_bytes(raw)
            if not keys:
                continue

            files_with_keys += 1

            for key_info in keys[:safe_max_keys]:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO jvlink_race_keys (
                        ingested_at, source_relative_path, source_file_name, record_spec2,
                        race_key_16, year, month_day, jyo_cd, kaiji, nichiji, race_num
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _utc_now_iso(),
                        rel,
                        file_path.name,
                        spec2,
                        key_info["race_key_16"],
                        key_info["year"],
                        key_info["month_day"],
                        key_info["jyo_cd"],
                        key_info["kaiji"],
                        key_info["nichiji"],
                        key_info["race_num"],
                    ),
                )

        conn.commit()

        total = conn.execute("SELECT COUNT(*) FROM jvlink_race_keys").fetchone()[0]
        top_specs = conn.execute(
            """
            SELECT record_spec2, COUNT(*) AS race_key_count
            FROM jvlink_race_keys
            GROUP BY record_spec2
            ORDER BY race_key_count DESC, record_spec2 ASC
            LIMIT 20
            """
        ).fetchall()
        latest = conn.execute(
            """
            SELECT source_relative_path, record_spec2, race_key_16, year, month_day, jyo_cd, kaiji, nichiji, race_num
            FROM jvlink_race_keys
            ORDER BY id DESC
            LIMIT 50
            """
        ).fetchall()

        return {
            "ok": True,
            "cacheRoot": str(cache_root),
            "scannedFiles": scanned,
            "candidateFiles": candidate_files,
            "filesWithRaceKeys": files_with_keys,
            "inserted": int(total) - int(total_before),
            "totalRaceKeys": int(total),
            "topSpecs": [dict(row) for row in top_specs],
            "latestRaceKeys": [dict(row) for row in latest],
        }
    finally:
        conn.close()


def get_race_key_summary(limit: int = 100):
    init_jra_van_storage()

    safe_limit = max(1, min(int(limit), 5000))
    conn = _connect()
    try:
        total = conn.execute(
            """
            SELECT COUNT(*)
            FROM jvlink_race_keys
            WHERE jyo_cd BETWEEN '01' AND '10'
              AND kaiji BETWEEN '01' AND '12'
              AND nichiji BETWEEN '01' AND '12'
              AND race_num BETWEEN '01' AND '12'
            """
        ).fetchone()[0]
        by_spec = conn.execute(
            """
            SELECT record_spec2, COUNT(*) AS race_key_count
            FROM jvlink_race_keys
            WHERE jyo_cd BETWEEN '01' AND '10'
              AND kaiji BETWEEN '01' AND '12'
              AND nichiji BETWEEN '01' AND '12'
              AND race_num BETWEEN '01' AND '12'
            GROUP BY record_spec2
            ORDER BY race_key_count DESC, record_spec2 ASC
            """
        ).fetchall()
        recent = conn.execute(
            """
            SELECT source_relative_path, record_spec2, race_key_16, year, month_day, jyo_cd, kaiji, nichiji, race_num
            FROM jvlink_race_keys
            WHERE jyo_cd BETWEEN '01' AND '10'
              AND kaiji BETWEEN '01' AND '12'
              AND nichiji BETWEEN '01' AND '12'
              AND race_num BETWEEN '01' AND '12'
            ORDER BY id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

        return {
            "ok": True,
            "totalRaceKeys": int(total),
            "bySpec": [dict(row) for row in by_spec],
            "recentRaceKeys": [dict(row) for row in recent],
        }
    finally:
        conn.close()


def get_race_keys_in_date_range(start_date: str, end_date: str, limit: int = 20000):
    """Return distinct JV race_key_16 values whose YYYYMMDD falls within [start_date, end_date]."""
    init_jra_van_storage()

    def _to_yyyymmdd(value: str) -> str:
        text = str(value or "").strip()
        if re.fullmatch(r"\d{8}", text):
            return text
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            return text.replace("-", "")
        raise ValueError(f"Invalid date format: {value}")

    safe_limit = max(1, min(int(limit), 100000))
    start_yyyymmdd = _to_yyyymmdd(start_date)
    end_yyyymmdd = _to_yyyymmdd(end_date)
    if start_yyyymmdd > end_yyyymmdd:
        start_yyyymmdd, end_yyyymmdd = end_yyyymmdd, start_yyyymmdd

    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT race_key_16
            FROM jvlink_race_keys
            WHERE (year || month_day) BETWEEN ? AND ?
              AND jyo_cd BETWEEN '01' AND '10'
              AND kaiji BETWEEN '01' AND '12'
              AND nichiji BETWEEN '01' AND '12'
              AND race_num BETWEEN '01' AND '12'
            ORDER BY race_key_16 ASC
            LIMIT ?
            """,
            (start_yyyymmdd, end_yyyymmdd, safe_limit),
        ).fetchall()
        return [str(row["race_key_16"]) for row in rows if str(row["race_key_16"]).strip()]
    finally:
        conn.close()


def get_cache_dates_in_range(start_date: str, end_date: str, specs=None, limit_rows: int = 50000):
    """Return sorted YYYY-MM-DD dates covered by indexed cache file date ranges."""
    init_jra_van_storage()

    def _to_date(value: str) -> datetime.date:
        text = str(value or "").strip()
        if re.fullmatch(r"\d{8}", text):
            return datetime.datetime.strptime(text, "%Y%m%d").date()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            return datetime.datetime.strptime(text, "%Y-%m-%d").date()
        raise ValueError(f"Invalid date format: {value}")

    start_dt = _to_date(start_date)
    end_dt = _to_date(end_date)
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    spec_list = [str(s).upper() for s in (specs or ["RA", "SE", "BN", "JG", "TK"]) if str(s).strip()]
    if not spec_list:
        spec_list = ["RA", "SE", "BN", "JG", "TK"]

    placeholders = ",".join(["?"] * len(spec_list))
    safe_limit = max(1, min(int(limit_rows), 200000))

    conn = _connect()
    try:
        rows = conn.execute(
            f"""
            SELECT record_spec2, date_start, date_end
            FROM jvlink_cache_files
            WHERE record_spec2 IN ({placeholders})
              AND date_start <> ''
              AND date_end <> ''
              AND date_end >= ?
              AND date_start <= ?
            ORDER BY date_start ASC
            LIMIT ?
            """,
            (*spec_list, start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d"), safe_limit),
        ).fetchall()

        dates = set()
        for row in rows:
            ds = str(row["date_start"] or "")
            de = str(row["date_end"] or "")
            if not re.fullmatch(r"\d{8}", ds) or not re.fullmatch(r"\d{8}", de):
                continue
            try:
                range_start = datetime.datetime.strptime(ds, "%Y%m%d").date()
                range_end = datetime.datetime.strptime(de, "%Y%m%d").date()
            except Exception:
                continue
            if range_start > range_end:
                range_start, range_end = range_end, range_start

            cur = max(range_start, start_dt)
            cap = min(range_end, end_dt)
            while cur <= cap:
                dates.add(cur.isoformat())
                cur += datetime.timedelta(days=1)

        return sorted(dates)
    finally:
        conn.close()


def _normalize_stream_record_fields(rec: dict):
    file_name = str(rec.get("fileName") or "").strip()
    stem = Path(file_name).stem.upper()

    spec = str(rec.get("recordSpec") or "").strip()
    kubun = str(rec.get("dataKubun") or "").strip()

    if not re.fullmatch(r"[A-Z0-9]{2}", spec or ""):
        spec = stem[:2] if len(stem) >= 2 else ""
    if not re.fullmatch(r"[A-Z0-9]", kubun or ""):
        kubun = stem[2] if len(stem) >= 3 else ""

    race_key = str(rec.get("raceKey16") or "").strip()
    if not race_key:
        m = re.search(r"(?:19|20)\d{14}", str(rec.get("text") or ""))
        if m:
            race_key = m.group(0)

    return {
        "file_name": file_name,
        "record_spec": spec,
        "data_kubun": kubun,
        "race_key_16": race_key,
    }


def save_stream_sample(payload: dict) -> dict:
    init_jra_van_storage()

    records = payload.get("records") or []
    if not isinstance(records, list):
        records = []

    conn = _connect()
    inserted = 0
    try:
        for rec in records:
            if not isinstance(rec, dict):
                continue
            norm = _normalize_stream_record_fields(rec)
            conn.execute(
                """
                INSERT INTO jvlink_stream_records (
                    created_at, sid, data_spec, from_date,
                    record_index, ret_code, data_size, file_name,
                    record_spec, data_kubun, race_key_16, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _utc_now_iso(),
                    payload.get("sid") or "",
                    payload.get("dataSpec") or "",
                    payload.get("fromDate") or "",
                    int(rec.get("index") or 0),
                    int(rec.get("ret") or 0),
                    int(rec.get("size") or 0),
                    norm["file_name"],
                    norm["record_spec"],
                    norm["data_kubun"],
                    norm["race_key_16"],
                    json.dumps(rec, ensure_ascii=False),
                ),
            )
            inserted += 1

        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM jvlink_stream_records").fetchone()[0]
        with_race_key = conn.execute(
            "SELECT COUNT(*) FROM jvlink_stream_records WHERE race_key_16 <> ''"
        ).fetchone()[0]
        by_spec = conn.execute(
            """
            SELECT record_spec, COUNT(*) AS record_count
            FROM jvlink_stream_records
            GROUP BY record_spec
            ORDER BY record_count DESC, record_spec ASC
            LIMIT 20
            """
        ).fetchall()

        return {
            "ok": True,
            "inserted": int(inserted),
            "totalStreamRecords": int(total),
            "recordsWithRaceKey": int(with_race_key),
            "byRecordSpec": [dict(row) for row in by_spec],
        }
    finally:
        conn.close()


def get_stream_record_summary(limit: int = 100):
    init_jra_van_storage()

    safe_limit = max(1, min(int(limit), 5000))
    conn = _connect()
    try:
        total = conn.execute("SELECT COUNT(*) FROM jvlink_stream_records").fetchone()[0]
        with_race_key = conn.execute(
            "SELECT COUNT(*) FROM jvlink_stream_records WHERE race_key_16 <> ''"
        ).fetchone()[0]
        by_spec = conn.execute(
            """
            SELECT record_spec, data_kubun, COUNT(*) AS record_count
            FROM jvlink_stream_records
            GROUP BY record_spec, data_kubun
            ORDER BY record_count DESC, record_spec ASC, data_kubun ASC
            """
        ).fetchall()
        recent = conn.execute(
            """
            SELECT created_at, sid, data_spec, from_date,
                   record_index, ret_code, data_size, file_name,
                   record_spec, data_kubun, race_key_16
            FROM jvlink_stream_records
            ORDER BY id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

        return {
            "ok": True,
            "totalStreamRecords": int(total),
            "recordsWithRaceKey": int(with_race_key),
            "bySpec": [dict(row) for row in by_spec],
            "recentRecords": [dict(row) for row in recent],
        }
    finally:
        conn.close()


def save_probe_run(payload: dict) -> int:
    init_jra_van_storage()

    conn = _connect()
    try:
        cur = conn.execute(
            """
            INSERT INTO jvlink_probe_runs (
                created_at, sid, data_spec, from_date, max_read_calls, ok, version,
                init_code, set_save_path_code, set_service_key_code, open_code, close_code,
                read_count, download_count, last_file_timestamp, used_service_key, error, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _utc_now_iso(),
                payload.get("sid"),
                payload.get("dataSpec") or "",
                payload.get("fromDate") or "",
                int(payload.get("maxReadCalls") or 0),
                1 if payload.get("ok") else 0,
                payload.get("version") or "",
                payload.get("initCode"),
                payload.get("setSavePathCode"),
                payload.get("setServiceKeyCode"),
                payload.get("openCode"),
                payload.get("closeCode"),
                payload.get("readCount"),
                payload.get("downloadCount"),
                payload.get("lastFileTimestamp") or "",
                1 if payload.get("usedServiceKey") else 0,
                payload.get("error") or "",
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def list_probe_runs(limit: int = 20):
    init_jra_van_storage()

    safe_limit = max(1, min(int(limit), 200))
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT id, created_at, sid, data_spec, from_date, max_read_calls,
                   ok, version, open_code, read_count, download_count,
                   used_service_key, error
            FROM jvlink_probe_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()
