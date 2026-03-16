import asyncio
import threading
from typing import Literal

from fastapi import APIRouter
from pydantic import BaseModel

import config
import data_manager

router = APIRouter(tags=["scrape"])

scrape_logs = []
scrape_logs_lock = threading.Lock()
scrape_job_lock = asyncio.Lock()


class ScrapeRequest(BaseModel):
    mode: Literal["new", "all"] = "new"


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

    with scrape_logs_lock:
        scrape_logs = ["Initializing Netkeiba Scraper..."]

    async with scrape_job_lock:
        await asyncio.to_thread(
            data_manager.fetch_weekend_timeline,
            mode=payload.mode,
            progress_callback=log_progress,
        )

    with scrape_logs_lock:
        scrape_logs.append("Done! Refreshing schedule...")
    return {"status": "success"}


@router.get("/api/scrape/log")
def get_scrape_log():
    with scrape_logs_lock:
        return {"logs": list(scrape_logs)}