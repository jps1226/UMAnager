import json
import os
import sqlite3
import subprocess
from pathlib import Path

import config


ROOT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
TESTING_DIR = ROOT_DIR / "testing"
TESTING_JVLINK_SCRIPTS_DIR = TESTING_DIR / "jvlink_scripts"
TESTING_JVLINK_KEY_FILE = TESTING_DIR / "JV-Link code.txt"


def _resolve_script(script_name):
    testing_path = TESTING_JVLINK_SCRIPTS_DIR / script_name
    if testing_path.exists():
        return testing_path
    return SCRIPTS_DIR / script_name


def _load_service_key_from_testing_file():
    try:
        if not TESTING_JVLINK_KEY_FILE.exists():
            return ""
        lines = TESTING_JVLINK_KEY_FILE.read_text(encoding="utf-8", errors="ignore").splitlines()
        for line in lines:
            key = line.strip()
            if key:
                return key
    except Exception:
        return ""
    return ""


def _resolve_service_key(explicit_key=None):
    if explicit_key is not None:
        return explicit_key
    env_key = os.environ.get("JVLINK_SERVICE_KEY", "").strip()
    if env_key:
        return env_key
    return _load_service_key_from_testing_file()


PREFLIGHT_SCRIPT = _resolve_script("jvlink_preflight.ps1")
STATUS_SCRIPT = _resolve_script("jvlink_bridge_status.ps1")
HANDSHAKE_SCRIPT = _resolve_script("jvlink_bridge_handshake.ps1")
OPEN_PROBE_SCRIPT = _resolve_script("jvlink_bridge_open_probe.ps1")
OPEN_SETTINGS_SCRIPT = _resolve_script("jvlink_bridge_open_settings.ps1")
STREAM_SAMPLE_SCRIPT = _resolve_script("jvlink_bridge_stream_sample.ps1")
NATIVE_SCHEDULE_SCRIPT = SCRIPTS_DIR / "jvlink_bridge_native_schedule.ps1"

JVLINK_DATASPEC_PRESETS = [
    "TOKU",
    "RACE",
    "SNPN",
    "DIF",
    "NBL",
    "DNS",
    "LOP",
    "WOO",
    "DYS",
    "CHS",
    "NPN",
    "HOS",
    "NHO",
    "YU",
    "COM",
    "MING",
]


class BridgeError(RuntimeError):
    pass


def _run_process(command):
    completed = subprocess.run(
        command,
        capture_output=True,
        encoding="cp932",
        errors="replace",
        check=False,
    )
    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()
    if completed.returncode != 0:
        raise BridgeError(
            f"command failed ({completed.returncode}): {' '.join(command)} | stdout={stdout} | stderr={stderr}"
        )
    return stdout, stderr


def get_32bit_powershell_path():
    windir = os.environ.get("WINDIR", r"C:\\Windows")
    return str(Path(windir) / "SysWOW64" / "WindowsPowerShell" / "v1.0" / "powershell.exe")


def get_storage_layout():
    data_dir = Path(config.JRA_VAN_DATA_DIR)
    save_dir = Path(config.JVLINK_SAVE_DIR)
    data_dir.mkdir(parents=True, exist_ok=True)
    save_dir.mkdir(parents=True, exist_ok=True)
    return {
        "ok": True,
        "dataDir": str(data_dir),
        "dbFile": str(config.JRA_VAN_DB_FILE),
        "dbUrl": str(config.JRA_VAN_DB_URL),
        "jvlinkSaveDir": str(save_dir),
    }


def get_dataspec_presets():
    return {
        "ok": True,
        "presets": list(JVLINK_DATASPEC_PRESETS),
        "notes": [
            "Use values as provided by JVDataCheckTool when validating initial connectivity.",
            "from_date accepts YYYYMMDD or YYYYMMDDHHMMSS strings.",
        ],
    }


def _ensure_metadata_table():
    """Create jv_metadata table if it doesn't exist."""
    db_file = config.JRA_VAN_DB_FILE
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jv_metadata (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def get_last_file_timestamp(key="master_data_last_timestamp"):
    """Retrieve the last JVOpen lastFileTimestamp for a given function."""
    try:
        _ensure_metadata_table()
        db_file = config.JRA_VAN_DB_FILE
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM jv_metadata WHERE key = ?", (key,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def save_last_file_timestamp(timestamp, key="master_data_last_timestamp"):
    """Save the JVOpen lastFileTimestamp for continuous accumulation."""
    try:
        _ensure_metadata_table()
        db_file = config.JRA_VAN_DB_FILE
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO jv_metadata (key, value) VALUES (?, ?)",
            (key, timestamp),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Warning: failed to save timestamp: {e}")


def get_bridge_status():
    ps32 = get_32bit_powershell_path()
    if not Path(ps32).exists():
        return {
            "ok": False,
            "error": f"32-bit PowerShell not found at {ps32}",
            "runner": ps32,
        }
    if not STATUS_SCRIPT.exists():
        return {
            "ok": False,
            "error": f"status script missing: {STATUS_SCRIPT}",
            "runner": ps32,
        }

    cmd = [
        ps32,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(STATUS_SCRIPT),
    ]

    try:
        stdout, _ = _run_process(cmd)
    except BridgeError as exc:
        return {"ok": False, "error": str(exc), "runner": ps32}

    try:
        payload = json.loads(stdout.splitlines()[-1] if stdout else "{}")
    except Exception as exc:
        return {
            "ok": False,
            "error": f"failed to parse status JSON: {exc}; raw={stdout}",
            "runner": ps32,
        }

    payload["runner"] = ps32
    return payload


def get_preflight():
    if not PREFLIGHT_SCRIPT.exists():
        return {
            "ok": False,
            "error": f"preflight script missing: {PREFLIGHT_SCRIPT}",
            "lines": [],
        }

    cmd = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(PREFLIGHT_SCRIPT),
        "-WorkspaceRoot",
        str(ROOT_DIR),
    ]

    try:
        stdout, _ = _run_process(cmd)
    except BridgeError as exc:
        return {"ok": False, "error": str(exc), "lines": []}

    lines = [line for line in stdout.splitlines() if line.strip()]
    pass_count = sum(1 for line in lines if line.startswith("[PASS]"))
    fail_count = sum(1 for line in lines if line.startswith("[FAIL]"))

    return {
        "ok": fail_count == 0,
        "passCount": pass_count,
        "failCount": fail_count,
        "lines": lines,
    }


def run_handshake(
    service_key=None,
    sid=None,
    probe_data_spec=None,
    probe_from_date=None,
    data_option=1,
    skip_set_service_key=False,
):
    ps32 = get_32bit_powershell_path()
    if not Path(ps32).exists():
        return {
            "ok": False,
            "error": f"32-bit PowerShell not found at {ps32}",
            "runner": ps32,
        }
    if not HANDSHAKE_SCRIPT.exists():
        return {
            "ok": False,
            "error": f"handshake script missing: {HANDSHAKE_SCRIPT}",
            "runner": ps32,
        }

    layout = get_storage_layout()

    effective_sid = sid or os.environ.get("JVLINK_SID") or config.JVLINK_DEFAULT_SID
    effective_key = _resolve_service_key(service_key)

    cmd = [
        ps32,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(HANDSHAKE_SCRIPT),
        "-Sid",
        effective_sid,
        "-SavePath",
        layout["jvlinkSaveDir"],
        "-DataOption",
        str(max(1, min(int(data_option or 1), 3))),
    ]

    if skip_set_service_key:
        cmd.append("-SkipServiceKey")

    if effective_key:
        cmd.extend(["-ServiceKey", effective_key])

    if probe_data_spec and probe_from_date:
        cmd.extend(["-ProbeDataSpec", probe_data_spec, "-ProbeFromDate", probe_from_date])

    try:
        stdout, _ = _run_process(cmd)
    except BridgeError as exc:
        return {"ok": False, "error": str(exc), "runner": ps32}

    try:
        payload = json.loads(stdout.splitlines()[-1] if stdout else "{}")
    except Exception as exc:
        return {
            "ok": False,
            "error": f"failed to parse handshake JSON: {exc}; raw={stdout}",
            "runner": ps32,
        }

    # Never expose raw key presence beyond a boolean.
    payload["usedServiceKey"] = bool(effective_key)
    payload["runner"] = ps32
    payload["storage"] = layout
    return payload


def run_open_probe(
    data_spec,
    from_date,
    max_read_calls=3,
    service_key=None,
    sid=None,
    data_option=1,
    skip_set_service_key=False,
    max_status_wait_seconds=60,
):
    ps32 = get_32bit_powershell_path()
    if not Path(ps32).exists():
        return {
            "ok": False,
            "error": f"32-bit PowerShell not found at {ps32}",
            "runner": ps32,
        }
    if not OPEN_PROBE_SCRIPT.exists():
        return {
            "ok": False,
            "error": f"open-probe script missing: {OPEN_PROBE_SCRIPT}",
            "runner": ps32,
        }

    layout = get_storage_layout()
    effective_sid = sid or os.environ.get("JVLINK_SID") or config.JVLINK_DEFAULT_SID
    effective_key = _resolve_service_key(service_key)
    safe_max_reads = max(0, min(int(max_read_calls or 0), 20))
    safe_data_option = max(1, min(int(data_option or 1), 4))

    cmd = [
        ps32,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(OPEN_PROBE_SCRIPT),
        "-Sid",
        effective_sid,
        "-SavePath",
        layout["jvlinkSaveDir"],
        "-DataSpec",
        str(data_spec),
        "-FromDate",
        str(from_date),
        "-MaxReadCalls",
        str(safe_max_reads),
        "-DataOption",
        str(safe_data_option),
        "-MaxStatusWaitSeconds",
        str(max(1, min(int(max_status_wait_seconds or 60), 600))),
    ]

    if skip_set_service_key:
        cmd.append("-SkipServiceKey")

    if effective_key:
        cmd.extend(["-ServiceKey", effective_key])

    try:
        stdout, _ = _run_process(cmd)
    except BridgeError as exc:
        return {"ok": False, "error": str(exc), "runner": ps32}

    try:
        payload = json.loads(stdout.splitlines()[-1] if stdout else "{}")
    except Exception as exc:
        return {
            "ok": False,
            "error": f"failed to parse open-probe JSON: {exc}; raw={stdout}",
            "runner": ps32,
        }

    payload["usedServiceKey"] = bool(effective_key)
    payload["runner"] = ps32
    payload["storage"] = layout
    return payload


def run_open_settings(sid=None):
    ps32 = get_32bit_powershell_path()
    if not Path(ps32).exists():
        return {
            "ok": False,
            "error": f"32-bit PowerShell not found at {ps32}",
            "runner": ps32,
        }
    if not OPEN_SETTINGS_SCRIPT.exists():
        return {
            "ok": False,
            "error": f"open-settings script missing: {OPEN_SETTINGS_SCRIPT}",
            "runner": ps32,
        }

    effective_sid = sid or os.environ.get("JVLINK_SID") or "UNKNOWN"
    cmd = [
        ps32,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(OPEN_SETTINGS_SCRIPT),
        "-Sid",
        effective_sid,
    ]

    try:
        stdout, _ = _run_process(cmd)
    except BridgeError as exc:
        return {"ok": False, "error": str(exc), "runner": ps32}

    try:
        payload = json.loads(stdout.splitlines()[-1] if stdout else "{}")
    except Exception as exc:
        return {
            "ok": False,
            "error": f"failed to parse open-settings JSON: {exc}; raw={stdout}",
            "runner": ps32,
        }

    payload["runner"] = ps32
    return payload


def run_stream_sample(
    data_spec,
    from_date,
    max_records=100,
    service_key=None,
    sid=None,
    data_option=1,
    skip_set_service_key=False,
    max_status_wait_seconds=12,
):
    ps32 = get_32bit_powershell_path()
    if not Path(ps32).exists():
        return {
            "ok": False,
            "error": f"32-bit PowerShell not found at {ps32}",
            "runner": ps32,
        }
    if not STREAM_SAMPLE_SCRIPT.exists():
        return {
            "ok": False,
            "error": f"stream-sample script missing: {STREAM_SAMPLE_SCRIPT}",
            "runner": ps32,
        }

    layout = get_storage_layout()
    effective_sid = sid or os.environ.get("JVLINK_SID") or config.JVLINK_DEFAULT_SID
    effective_key = _resolve_service_key(service_key)
    safe_max_records = max(1, min(int(max_records or 100), 500))
    safe_data_option = max(1, min(int(data_option or 1), 4))

    cmd = [
        ps32,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(STREAM_SAMPLE_SCRIPT),
        "-Sid",
        effective_sid,
        "-SavePath",
        layout["jvlinkSaveDir"],
        "-DataSpec",
        str(data_spec),
        "-FromDate",
        str(from_date),
        "-MaxRecords",
        str(safe_max_records),
        "-DataOption",
        str(safe_data_option),
        "-MaxStatusWaitSeconds",
        str(max(1, min(int(max_status_wait_seconds or 12), 600))),
    ]

    if skip_set_service_key:
        cmd.append("-SkipServiceKey")

    if effective_key:
        cmd.extend(["-ServiceKey", effective_key])

    try:
        stdout, _ = _run_process(cmd)
    except BridgeError as exc:
        return {"ok": False, "error": str(exc), "runner": ps32}

    try:
        payload = json.loads(stdout.splitlines()[-1] if stdout else "{}")
    except Exception as exc:
        return {
            "ok": False,
            "error": f"failed to parse stream-sample JSON: {exc}; raw={stdout}",
            "runner": ps32,
        }

    payload["usedServiceKey"] = bool(effective_key)
    payload["runner"] = ps32
    payload["storage"] = layout
    return payload


def run_native_schedule(
    from_date,
    data_spec=None,
    max_records=20000,
    max_status_wait_seconds=180,
    service_key=None,
    sid=None,
    data_option=1,
    skip_set_service_key=True,
):
    ps32 = get_32bit_powershell_path()
    if not Path(ps32).exists():
        return {
            "ok": False,
            "error": f"32-bit PowerShell not found at {ps32}",
            "runner": ps32,
        }
    if not NATIVE_SCHEDULE_SCRIPT.exists():
        return {
            "ok": False,
            "error": f"native-schedule script missing: {NATIVE_SCHEDULE_SCRIPT}",
            "runner": ps32,
        }

    if not from_date:
        return {
            "ok": False,
            "error": "from_date is required",
            "runner": ps32,
        }

    layout = get_storage_layout()
    effective_sid = sid or os.environ.get("JVLINK_SID") or config.JVLINK_DEFAULT_SID
    effective_key = _resolve_service_key(service_key)
    effective_spec = str(data_spec or JVLINK_DATASPEC_PRESETS[0])
    safe_max_records = max(1, min(int(max_records or 20000), 200000))
    safe_data_option = max(1, min(int(data_option or 1), 4))

    cmd = [
        ps32,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(NATIVE_SCHEDULE_SCRIPT),
        "-Sid",
        effective_sid,
        "-SavePath",
        layout["jvlinkSaveDir"],
        "-DataSpec",
        effective_spec,
        "-FromDate",
        str(from_date),
        "-MaxRecords",
        str(safe_max_records),
        "-DataOption",
        str(safe_data_option),
        "-MaxStatusWaitSeconds",
        str(max(1, min(int(max_status_wait_seconds or 180), 900))),
    ]

    if skip_set_service_key:
        cmd.append("-SkipServiceKey")

    if effective_key:
        cmd.extend(["-ServiceKey", effective_key])

    try:
        stdout, _ = _run_process(cmd)
    except BridgeError as exc:
        return {"ok": False, "error": str(exc), "runner": ps32}

    try:
        payload = json.loads(stdout.splitlines()[-1] if stdout else "{}")
    except Exception as exc:
        return {
            "ok": False,
            "error": f"failed to parse native schedule JSON: {exc}; raw={stdout[:500]}",
            "runner": ps32,
        }

    payload["usedServiceKey"] = bool(effective_key)
    payload["runner"] = ps32
    payload["storage"] = layout
    return payload


def run_capability_scan(
    from_date,
    max_records_per_run=40,
    max_status_wait_seconds=30,
    dataspecs=None,
    data_options=None,
    sid=None,
    service_key=None,
    skip_set_service_key=True,
):
    if not from_date:
        return {
            "ok": False,
            "error": "from_date is required",
            "runs": [],
        }

    scan_specs = [str(s).strip() for s in (dataspecs or JVLINK_DATASPEC_PRESETS) if str(s).strip()]
    if not scan_specs:
        scan_specs = list(JVLINK_DATASPEC_PRESETS)

    raw_opts = data_options or [1, 2]
    scan_opts = []
    for opt in raw_opts:
        try:
            scan_opts.append(max(1, min(int(opt), 3)))
        except Exception:
            continue
    if not scan_opts:
        scan_opts = [1, 2]

    run_rows = []
    seen_specs = set()
    seen_prefixes = set()

    for spec in scan_specs:
        for opt in scan_opts:
            payload = run_stream_sample(
                data_spec=spec,
                from_date=str(from_date),
                max_records=max_records_per_run,
                service_key=service_key,
                sid=sid,
                data_option=opt,
                skip_set_service_key=skip_set_service_key,
                max_status_wait_seconds=max_status_wait_seconds,
            )

            records = payload.get("records") if isinstance(payload, dict) else []
            records = records if isinstance(records, list) else []

            observed_specs = sorted(
                {
                    str(r.get("recordSpec") or "").strip()
                    for r in records
                    if isinstance(r, dict) and str(r.get("recordSpec") or "").strip()
                }
            )

            observed_prefixes = sorted(
                {
                    str(r.get("fileName") or "").split(".")[0][:4].upper()
                    for r in records
                    if isinstance(r, dict) and str(r.get("fileName") or "").strip()
                }
            )

            seen_specs.update(observed_specs)
            seen_prefixes.update(observed_prefixes)

            run_rows.append(
                {
                    "dataSpec": spec,
                    "dataOption": opt,
                    "ok": bool(payload.get("ok")),
                    "openOk": bool(payload.get("openOk")),
                    "readOk": bool(payload.get("readOk")),
                    "openCode": payload.get("openCode"),
                    "readTransport": payload.get("readTransport") or "",
                    "error": payload.get("error") or "",
                    "warnings": payload.get("warnings") or [],
                    "downloadCount": payload.get("downloadCount"),
                    "statusCode": payload.get("statusCode"),
                    "recordCount": len(records),
                    "observedRecordSpecs": observed_specs,
                    "observedFilePrefixes": observed_prefixes,
                }
            )

    run_count = len(run_rows)
    runs_with_records = sum(1 for row in run_rows if int(row.get("recordCount") or 0) > 0)
    runs_open_ok = sum(1 for row in run_rows if bool(row.get("openOk")))

    return {
        "ok": True,
        "fromDate": str(from_date),
        "maxRecordsPerRun": int(max_records_per_run),
        "maxStatusWaitSeconds": int(max_status_wait_seconds),
        "scanDataSpecs": scan_specs,
        "scanDataOptions": scan_opts,
        "runCount": run_count,
        "runsWithRecords": runs_with_records,
        "runsOpenOk": runs_open_ok,
        "observedRecordSpecs": sorted(seen_specs),
        "observedFilePrefixes": sorted(seen_prefixes),
        "runs": run_rows,
        "notes": [
            "DataOption=3 is setup/update mode and may open JV-Link UI dialogs.",
            "When observedRecordSpecs remains narrow (e.g., HC only), this often indicates account/feed scope rather than bridge failure.",
        ],
    }


def load_jv_weekend_races(from_date, max_records=5000, max_status_wait_seconds=120):
    """
    Fetch upcoming weekend race cards, entries, and pedigree via two separate JVOpen calls.

    Call 1: TOKURACESNPN DataOption=2 (This Week / 非蓄積系)
        — gets this week's races with horse entries (RA+SE).
    Call 2: DIFN DataOption=1 (Normal / 蓄積系)
        — gets UM horse master records (pedigree) since from_date.

    Only DIFN (diff) returns UM records. RCVN/TCVN return RC coverage but no UM.
    BLDN returns HN/SK but no UM. DIFN is the only spec empirically returning pedigree.

    Mixing accumulating and non-accumulating specs causes JV-Link to revert to full historical
    cache (thousands of races without entries), so they must be separate calls.

    Returns: merged dict with races, entries, and horses (pedigree).
    """
    race_result = run_native_schedule(
        from_date=from_date,
        data_spec="TOKURACESNPN",
        max_records=max_records,
        max_status_wait_seconds=max_status_wait_seconds,
        data_option=2,
        skip_set_service_key=True,
    )

    if not isinstance(race_result, dict) or not race_result.get("ok"):
        return race_result

    pedigree_result = run_native_schedule(
        from_date=from_date,
        data_spec="DIFN",
        max_records=max_records,
        max_status_wait_seconds=max_status_wait_seconds,
        data_option=1,
        skip_set_service_key=True,
    )

    if isinstance(pedigree_result, dict) and pedigree_result.get("ok"):
        horses = pedigree_result.get("horses") or []
        race_result["horses"] = horses
        race_result["pedigreeRecordsRead"] = int(pedigree_result.get("recordsRead") or 0)
        race_result["pedigreeSpecCounts"] = pedigree_result.get("specCounts") or {}
    else:
        race_result["horses"] = []
        race_result["pedigreeError"] = (pedigree_result or {}).get("error") if isinstance(pedigree_result, dict) else "unknown"

    return race_result


def load_jv_master_data(from_date=None, is_initial=False, max_records=200000, max_status_wait_seconds=3600):
    """
    Build/update the master racehorse database with complete pedigree tree.

    ARCHITECTURE NOTE: This function is SEPARATE from load_jv_weekend_races().
    - Master data: Accumulating (蓄積系) - historical database of 100,000+ horses with pedigrees
    - Weekend races: Non-accumulating (非蓄積系) - this week's races with latest updates

    For first run (is_initial=True): Uses DataOption=4 (Setup/Silent) to bootstrap all historical data.
      Option=4 suppresses CD-ROM dialogs and downloads entire DIFN/RACE/BLDN file archives.
      Downloads decades of data: complete breeding family tree, all horse master records, historical races.

    For subsequent runs: Uses DataOption=1 (Normal) with lastFileTimestamp tracking for daily incremental updates.

    Args:
        from_date: If None, retrieves from saved lastFileTimestamp. If is_initial=True, ignored.
        is_initial: If True, uses DataOption=4 and ignores saved timestamp for full historical bootstrap.
        max_records: Max records per run (default 200000 for historical bootstrap).
        max_status_wait_seconds: Timeout for downloads (expect 30-60 min for initial Option=4).

    Returns: dict with complete UM records, BLDN pedigree data, metadata, and updated lastFileTimestamp.
    """
    if is_initial:
        # Initial bootstrap: Option 4 (Setup/Silent) downloads ALL historical data without dialogs
        effective_from_date = "20100101000000"
        effective_option = 4
    else:
        # Incremental updates: Option 1 (Normal) with timestamp tracking
        if not from_date:
            saved_timestamp = get_last_file_timestamp("master_data_last_timestamp")
            from_date = saved_timestamp or "20240101000000"
        effective_from_date = from_date
        effective_option = 1

    # Use complete concatenated spec for accumulating (蓄積系) data
    # Includes: TOKU (setup), RACE (historical races), DIFN (differential UM), BLDN (breeding tree),
    # SLOP (turf), WOOD (woodchip), YSCH (schedule), SNPN (new format snap), HOSN (new format horse market)
    result = run_native_schedule(
        from_date=effective_from_date,
        data_spec="TOKURACEDIFNBLDNSLOPWOODYSCHSNPN",
        max_records=max_records,
        max_status_wait_seconds=max_status_wait_seconds,
        data_option=effective_option,
        skip_set_service_key=True,
    )

    if result.get("ok") and result.get("lastFileTimestamp"):
        save_last_file_timestamp(result["lastFileTimestamp"], "master_data_last_timestamp")

    return result
