from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
import datetime
import os
from pathlib import Path
import shutil
import time
from typing import Optional
import zipfile
from pydantic import BaseModel

import config
from data_manager import clear_horse_runtime_cache
from storage import count_horse_cache_entries, clear_horse_cache_entries, dispose_storage_connections, init_storage_foundation

router = APIRouter(tags=["maintenance"])

CACHE_FILE = config.CACHE_FILE
HORSE_DICT_FILE = config.HORSE_DICT_FILE
DATA_DIR = config.DATA_DIR
BACKUP_DIR = Path(__file__).resolve().parent.parent / "backups"


class RestoreBackupPayload(BaseModel):
    backup_name: Optional[str] = None
    use_latest: bool = True
    create_safety_backup: bool = True


def _iter_data_files():
    if not DATA_DIR.exists() or not DATA_DIR.is_dir():
        return []
    return [p for p in DATA_DIR.rglob("*") if p.is_file()]


def _has_data_files() -> bool:
    if not DATA_DIR.exists() or not DATA_DIR.is_dir():
        return False
    return any(DATA_DIR.rglob("*")) and any(p.is_file() for p in DATA_DIR.rglob("*"))


def _create_backup_archive(prefix: str) -> str:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{prefix}_{stamp}.zip"
    backup_path = BACKUP_DIR / backup_name

    with zipfile.ZipFile(backup_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in DATA_DIR.rglob("*"):
            if not file_path.is_file():
                continue
            if file_path.parent == BACKUP_DIR:
                continue
            if file_path.name.lower() == "requirements.txt" and file_path.parent == DATA_DIR:
                continue

            arcname = file_path.relative_to(DATA_DIR.parent)
            zf.write(file_path, arcname=arcname)

    return backup_name


def _list_backups():
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backups = [p for p in BACKUP_DIR.glob("*.zip") if p.is_file()]
    backups.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return backups


def _clear_data_dir():
    if not DATA_DIR.exists():
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        return
    for child in DATA_DIR.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


@router.post("/api/cache/clear")
def clear_cache():
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
    return {"status": "success"}


@router.post("/api/dict/wipe")
def wipe_dict():
    runtime_cleared = clear_horse_runtime_cache()
    db_cleared = count_horse_cache_entries()
    clear_horse_cache_entries()
    legacy_file_deleted = False
    if os.path.exists(HORSE_DICT_FILE):
        os.remove(HORSE_DICT_FILE)
        legacy_file_deleted = True
    return {
        "status": "success",
        "message": (
            "Translation memory cleared. Existing loaded race cards may still show prior translated names "
            "until races are refreshed/re-scraped."
        ),
        "cleared": {
            "runtimeEntries": runtime_cleared,
            "dbEntries": db_cleared,
            "legacyFileDeleted": legacy_file_deleted,
        },
    }


@router.post("/api/data/backup")
def create_data_backup():
    backup_name = _create_backup_archive("umanager_data_backup")

    return {
        "status": "success",
        "filename": backup_name,
        "download_url": f"/api/data/backup/{backup_name}",
    }


@router.get("/api/data/backups")
def list_data_backups():
    backups = _list_backups()
    return {
        "status": "success",
        "backups": [p.name for p in backups],
    }


@router.get("/api/data/backup/{backup_name}")
def download_data_backup(backup_name: str):
    safe_name = Path(backup_name).name
    target = BACKUP_DIR / safe_name
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="Backup not found")
    return FileResponse(path=str(target), filename=safe_name, media_type="application/zip")


@router.post("/api/data/backup/restore")
def restore_data_backup(payload: RestoreBackupPayload):
    backups = _list_backups()
    if not backups:
        raise HTTPException(status_code=404, detail="No backup archives found")

    if payload.backup_name:
        safe_name = Path(payload.backup_name).name
        target = BACKUP_DIR / safe_name
        if not target.exists() or not target.is_file():
            raise HTTPException(status_code=404, detail="Backup not found")
    else:
        if not payload.use_latest:
            raise HTTPException(status_code=400, detail="backup_name is required when use_latest is false")
        target = backups[0]

    safety_backup_name = None
    if payload.create_safety_backup and _has_data_files():
        safety_backup_name = _create_backup_archive("umanager_safety_pre_restore")

    # Ensure SQLite files are not held open while data/ is replaced.
    dispose_storage_connections()
    clear_error = None
    for _ in range(3):
        try:
            _clear_data_dir()
            clear_error = None
            break
        except PermissionError as exc:
            clear_error = exc
            dispose_storage_connections()
            time.sleep(0.25)
    if clear_error is not None:
        raise HTTPException(
            status_code=409,
            detail=(
                "Could not restore because data files are locked by another process "
                "(likely another UMAnager/server instance). Close other instances and retry."
            ),
        )

    extracted_files = 0
    with zipfile.ZipFile(target, "r") as zf:
        members = [m for m in zf.namelist() if m.startswith("data/")]
        if not members:
            raise HTTPException(status_code=400, detail="Backup archive does not contain a data/ folder")

        for member in members:
            member_path = Path(member)
            if member_path.is_absolute() or ".." in member_path.parts:
                continue
            zf.extract(member, path=DATA_DIR.parent)
            if not member.endswith("/"):
                extracted_files += 1

    # Reinitialize DB engine/schema after restore so the app can continue safely.
    init_storage_foundation()

    return {
        "status": "success",
        "restored_from": target.name,
        "safety_backup": safety_backup_name,
        "restored_files": extracted_files,
    }
