import asyncio
import threading
from typing import Literal

from fastapi import APIRouter
from pydantic import BaseModel

import config
import data_manager
from storage import get_active_data_engine

router = APIRouter(tags=["scrape"])

scrape_logs = []
scrape_logs_lock = threading.Lock()
scrape_job_lock = asyncio.Lock()


class ScrapeRequest(BaseModel):
    mode: Literal["new", "all"] = "new"
    source_mode: Literal["nk", "jv", "auto"] | None = None
    data_engine: Literal["nk", "jv"] | None = None


def log_progress(msg):
    with scrape_logs_lock:
        scrape_logs.append(msg)
        if len(scrape_logs) > config.MAX_CONSOLE_LOGS:
            scrape_logs.pop(0)


@router.post("/api/scrape")
async def run_scrape(payload: ScrapeRequest):
    global scrape_logs

    if scrape_job_lock.locked():
        return {"status": "busy", "message": "A scrape job is already running."}

    resolved_engine = payload.data_engine or get_active_data_engine()
    # Enforce zero crossover by default: engine determines the source mode unless explicitly requested.
    resolved_source_mode = payload.source_mode or ("jv" if resolved_engine == "jv" else "nk")

    with scrape_logs_lock:
        engine_label = "JRA-VAN" if resolved_engine == "jv" else "Netkeiba"
        scrape_logs = [f"Initializing {engine_label} Engine..."]

    async with scrape_job_lock:
        races = await asyncio.to_thread(
            data_manager.fetch_weekend_timeline,
            mode=payload.mode,
            progress_callback=log_progress,
            source_mode=resolved_source_mode,
            data_engine=resolved_engine,
        )

    with scrape_logs_lock:
        scrape_logs.append("Done! Refreshing schedule...")
    return {
        "status": "success",
        "mode": payload.mode,
        "source_mode": resolved_source_mode,
        "data_engine": resolved_engine,
        "cached_races": len(races or []),
    }


@router.get("/api/scrape/log")
def get_scrape_log():
    with scrape_logs_lock:
        return {"logs": list(scrape_logs)}