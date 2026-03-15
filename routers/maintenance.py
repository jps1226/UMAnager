from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
import datetime
import os
from pathlib import Path
import zipfile

import config

router = APIRouter(tags=["maintenance"])

CACHE_FILE = config.CACHE_FILE
HORSE_DICT_FILE = config.HORSE_DICT_FILE
DATA_DIR = config.DATA_DIR
BACKUP_DIR = Path(__file__).resolve().parent.parent / "backups"


@router.post("/api/cache/clear")
def clear_cache():
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
    return {"status": "success"}


@router.post("/api/dict/wipe")
def wipe_dict():
    if os.path.exists(HORSE_DICT_FILE):
        os.remove(HORSE_DICT_FILE)
    return {"status": "success"}


@router.post("/api/data/backup")
def create_data_backup():
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"umanager_data_backup_{stamp}.zip"
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

    return {
        "status": "success",
        "filename": backup_name,
        "download_url": f"/api/data/backup/{backup_name}",
    }


@router.get("/api/data/backup/{backup_name}")
def download_data_backup(backup_name: str):
    safe_name = Path(backup_name).name
    target = BACKUP_DIR / safe_name
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="Backup not found")
    return FileResponse(path=str(target), filename=safe_name, media_type="application/zip")
