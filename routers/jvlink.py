from fastapi import APIRouter
from pydantic import BaseModel

from jra_van_storage import (
    get_cache_index_summary,
    get_cache_max_date,
    get_race_key_summary,
    get_stream_record_summary,
    ingest_race_keys_from_cache,
    index_cache_files,
    list_probe_runs,
    save_stream_sample,
    save_probe_run,
)
from jvlink_bridge import (
    JVLINK_DATASPEC_PRESETS,
    get_dataspec_presets,
    get_bridge_status,
    get_preflight,
    get_storage_layout,
    run_capability_scan,
    run_handshake,
    run_open_settings,
    run_open_probe,
    run_stream_sample,
    load_jv_weekend_races,
    load_jv_master_data,
)


router = APIRouter(tags=["jvlink"])


class JVLinkHandshakePayload(BaseModel):
    sid: str | None = None
    service_key: str | None = None
    probe_data_spec: str | None = None
    probe_from_date: str | None = None
    data_option: int = 1
    skip_set_service_key: bool = False


class JVLinkOpenProbePayload(BaseModel):
    data_spec: str
    from_date: str
    max_read_calls: int = 3
    data_option: int = 1
    max_status_wait_seconds: int = 60
    skip_set_service_key: bool = False
    sid: str | None = None
    service_key: str | None = None


class JVLinkOpenSettingsPayload(BaseModel):
    sid: str | None = None


class JVLinkCacheIndexPayload(BaseModel):
    max_files: int = 5000


class JVLinkRaceKeyIngestPayload(BaseModel):
    max_files: int = 500
    max_keys_per_file: int = 200


class JVLinkStreamSamplePayload(BaseModel):
    data_spec: str
    from_date: str
    max_records: int = 100
    data_option: int = 1
    max_status_wait_seconds: int = 12
    skip_set_service_key: bool = False
    sid: str | None = None
    service_key: str | None = None


class JVLinkCapabilityScanPayload(BaseModel):
    from_date: str
    max_records_per_run: int = 40
    max_status_wait_seconds: int = 30
    data_options: list[int] | None = None
    dataspecs: list[str] | None = None
    sid: str | None = None
    service_key: str | None = None
    skip_set_service_key: bool = True


@router.get("/api/jvlink/status")
def jvlink_status():
    status = get_bridge_status()
    return {"status": "ok" if status.get("ok") else "error", "bridge": status}


@router.get("/api/jvlink/preflight")
def jvlink_preflight():
    report = get_preflight()
    return {"status": "ok" if report.get("ok") else "warning", "report": report}


@router.get("/api/jvlink/storage-layout")
def jvlink_storage_layout():
    layout = get_storage_layout()
    return {"status": "ok", "layout": layout}


@router.get("/api/jvlink/dataspec-presets")
def jvlink_dataspec_presets():
    presets = get_dataspec_presets()
    return {"status": "ok", "dataspec": presets}


@router.post("/api/jvlink/handshake")
def jvlink_handshake(payload: JVLinkHandshakePayload):
    result = run_handshake(
        service_key=payload.service_key,
        sid=payload.sid,
        probe_data_spec=payload.probe_data_spec,
        probe_from_date=payload.probe_from_date,
        data_option=payload.data_option,
        skip_set_service_key=payload.skip_set_service_key,
    )
    return {"status": "ok" if result.get("ok") else "error", "handshake": result}


@router.post("/api/jvlink/probe-open")
def jvlink_probe_open(payload: JVLinkOpenProbePayload):
    result = run_open_probe(
        data_spec=payload.data_spec,
        from_date=payload.from_date,
        max_read_calls=payload.max_read_calls,
        data_option=payload.data_option,
        skip_set_service_key=payload.skip_set_service_key,
        max_status_wait_seconds=payload.max_status_wait_seconds,
        sid=payload.sid,
        service_key=payload.service_key,
    )
    run_id = save_probe_run(result)
    return {
        "status": "ok" if result.get("ok") else "error",
        "runId": run_id,
        "probe": result,
    }


@router.post("/api/jvlink/open-settings")
def jvlink_open_settings(payload: JVLinkOpenSettingsPayload):
    result = run_open_settings(sid=payload.sid)
    return {"status": "ok" if result.get("ok") else "error", "settings": result}


@router.get("/api/jvlink/probe-runs")
def jvlink_probe_runs(limit: int = 20):
    rows = list_probe_runs(limit=limit)
    return {"status": "ok", "count": len(rows), "runs": rows}


@router.post("/api/jvlink/stream-sample")
def jvlink_stream_sample(payload: JVLinkStreamSamplePayload):
    result = run_stream_sample(
        data_spec=payload.data_spec,
        from_date=payload.from_date,
        max_records=payload.max_records,
        data_option=payload.data_option,
        max_status_wait_seconds=payload.max_status_wait_seconds,
        skip_set_service_key=payload.skip_set_service_key,
        sid=payload.sid,
        service_key=payload.service_key,
    )
    save_result = save_stream_sample(result)
    return {
        "status": "ok" if result.get("ok") else "error",
        "stream": result,
        "saved": save_result,
    }


@router.get("/api/jvlink/stream-summary")
def jvlink_stream_summary(limit: int = 100):
    result = get_stream_record_summary(limit=limit)
    return {"status": "ok" if result.get("ok") else "error", "summary": result}


@router.post("/api/jvlink/capability-scan")
def jvlink_capability_scan(payload: JVLinkCapabilityScanPayload):
    target_specs = payload.dataspecs if payload.dataspecs else list(JVLINK_DATASPEC_PRESETS)
    result = run_capability_scan(
        from_date=payload.from_date,
        max_records_per_run=payload.max_records_per_run,
        max_status_wait_seconds=payload.max_status_wait_seconds,
        dataspecs=target_specs,
        data_options=payload.data_options,
        sid=payload.sid,
        service_key=payload.service_key,
        skip_set_service_key=payload.skip_set_service_key,
    )
    return {"status": "ok" if result.get("ok") else "error", "scan": result}


@router.post("/api/jvlink/cache-index")
def jvlink_cache_index(payload: JVLinkCacheIndexPayload):
    result = index_cache_files(max_files=payload.max_files)
    return {"status": "ok" if result.get("ok") else "error", "index": result}


@router.get("/api/jvlink/cache-summary")
def jvlink_cache_summary(limit: int = 50):
    result = get_cache_index_summary(limit=limit)
    return {"status": "ok" if result.get("ok") else "error", "summary": result}


@router.post("/api/jvlink/ingest-race-keys")
def jvlink_ingest_race_keys(payload: JVLinkRaceKeyIngestPayload):
    result = ingest_race_keys_from_cache(
        max_files=payload.max_files,
        max_keys_per_file=payload.max_keys_per_file,
    )
    return {"status": "ok" if result.get("ok") else "error", "ingest": result}


@router.get("/api/jvlink/race-key-summary")
def jvlink_race_key_summary(limit: int = 100):
    result = get_race_key_summary(limit=limit)
    return {"status": "ok" if result.get("ok") else "error", "summary": result}


class JVLinkRefreshUpcomingPayload(BaseModel):
    data_spec: str | None = None
    specs_for_date: list[str] | None = None
    data_option: int = 2
    max_status_wait_seconds: int = 180
    max_index_files: int = 5000
    sid: str | None = None
    service_key: str | None = None
    skip_set_service_key: bool = False


class JVLinkWeekendRacesPayload(BaseModel):
    from_date: str
    max_records: int = 5000
    max_status_wait_seconds: int = 120


class JVLinkMasterDataPayload(BaseModel):
    is_initial: bool = False
    max_records: int = 200000
    max_status_wait_seconds: int = 180


@router.post("/api/jvlink/refresh-upcoming")
def jvlink_refresh_upcoming(payload: JVLinkRefreshUpcomingPayload):
    """Download incremental JV cache files and re-index.

    Automatically determines FromDate from the latest date_end in the cache
    for selected schedule-relevant specs, then runs JVOpen to fetch any newer files from JRA-VAN,
    and finally re-indexes jvlink_cache_files so JV discovery sees new data.
    """
    date_specs = payload.specs_for_date or ["RA", "SE", "BN", "JG", "TK"]
    auto_from_date = get_cache_max_date(specs=date_specs)
    effective_data_spec = (
        payload.data_spec
        if payload.data_spec
        else "TOKU"
    )

    probe_result = run_open_probe(
        data_spec=effective_data_spec,
        from_date=auto_from_date,
        max_read_calls=0,
        data_option=max(1, min(int(payload.data_option or 2), 3)),
        skip_set_service_key=payload.skip_set_service_key,
        max_status_wait_seconds=payload.max_status_wait_seconds,
        sid=payload.sid,
        service_key=payload.service_key,
    )
    save_probe_run(probe_result)

    index_result = index_cache_files(max_files=payload.max_index_files)

    ok = probe_result.get("ok") and index_result.get("ok")
    return {
        "status": "ok" if ok else "error",
        "fromDate": auto_from_date,
        "dataSpec": effective_data_spec,
        "probe": probe_result,
        "index": index_result,
    }


@router.post("/api/jvlink/load-weekend-races")
def jvlink_load_weekend_races(payload: JVLinkWeekendRacesPayload):
    """Fetch upcoming weekend race cards and entries.

    Uses DataOption=2 (This Week) with concatenated weekend specs.
    Fast, recent-only race discovery.
    """
    result = load_jv_weekend_races(
        from_date=payload.from_date,
        max_records=payload.max_records,
        max_status_wait_seconds=payload.max_status_wait_seconds,
    )
    return {
        "status": "ok" if result.get("ok") else "error",
        "data": result,
    }


@router.post("/api/jvlink/load-master-data")
def jvlink_load_master_data(payload: JVLinkMasterDataPayload):
    """Build/update the master racehorse database with pedigree.

    is_initial=true: Full historical bootstrap (Option 4, downloads ALL historical data, may take 30-60 min)
    is_initial=false: Incremental update from last checkpoint (Option 1, silent)

    Automatically processes UM records into HORSE_CACHE.
    """
    from data_manager import HORSE_CACHE, update_horse_cache_pedigree, save_horse_dict, _decode_jv_hex
    import logging

    logger = logging.getLogger(__name__)

    result = load_jv_master_data(
        from_date=None,
        is_initial=payload.is_initial,
        max_records=payload.max_records,
        max_status_wait_seconds=payload.max_status_wait_seconds,
    )

    # Process UM records into HORSE_CACHE
    if result.get("ok"):
        horses = result.get("horses") or []
        processed = 0
        for h in horses:
            if not isinstance(h, dict):
                continue
            hid = str(h.get("KettoNum") or "").strip()
            if not hid:
                continue
            sj = _decode_jv_hex(str(h.get("Sire_JP") or "").strip())
            dj = _decode_jv_hex(str(h.get("Dam_JP") or "").strip())
            bj = _decode_jv_hex(str(h.get("BMS_JP") or "").strip())
            update_horse_cache_pedigree(
                hid,
                str(h.get("Sire_ID") or "").strip(),
                str(h.get("Dam_ID") or "").strip(),
                str(h.get("BMS_ID") or "").strip(),
                sj, dj, bj,
            )
            processed += 1
        if processed:
            save_horse_dict()
        logger.info(f"Master data loaded: {processed} horses processed, HORSE_CACHE size={len(HORSE_CACHE)}")
        result["horses_cached"] = processed

    return {
        "status": "ok" if result.get("ok") else "error",
        "data": result,
    }
