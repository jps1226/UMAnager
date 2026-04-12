# UMAnager

UMAnager is a local FastAPI dashboard for tracking Japanese horse racing cards, pedigree angles, manual marks, and lightweight race-day workflow data from Netkeiba.

It is built as a personal tool first: scrape upcoming cards, keep pedigree watchlists, mark runners quickly, import completed results for a day, and persist app state locally under `data/` with SQLite-backed storage.

## What It Does

- Scrapes upcoming race cards and caches them locally.
- Uses a wider upcoming window with fallback day-level discovery when monthly race lists are incomplete.
- Shows race days through a persistent calendar-first UI.
- Lets you move between loaded days quickly with both the sidebar calendar and header prev/next day controls.
- Supports manual horse marks that persist locally.
- Tracks horses through two simple local lists: Favorites and Watchlist.
- Translates and caches horse and pedigree names for easier reading.
- Imports completed results for a selected day back into cached race entries.
- Refreshes cached upcoming cards and can auto-refresh missing past-race history when enabled.
- Creates and restores backups of the local `data/` directory.
- Includes an in-app scrape console and a local server shutdown button.
- Provides a TV Mode page with embedded GreenChannel live playback plus synchronized marks panel.
- Supports split-resize, panel flip, and persistent TV layout preferences.

## Current UI/Workflow

- Sidebar calendar for loaded race days.
- Header day navigator for fast day-to-day movement.
- Search bar for horses across the currently loaded races.
- Auto-pick strategy slider and configurable display/settings toggles.
- Voting cheat sheet export for building bets outside the app.
- TV Mode tab/button that opens a side-by-side video + marks view in a new browser tab.
- TV Mode auto-collapses past races and keeps the current race focused while races are live.

## Storage Model

UMAnager now uses a local SQLite database as the primary persistence layer.

Primary runtime state under `data/`:

- `umanager.sqlite3`: SQLite database for config, lists, marks, OrePro history, horse cache, and race cache

Supporting storage behavior:

- Backups are written to `backups/` and snapshot the local `data/` directory.
- Legacy JSON/TXT/PKL files are no longer read automatically during normal app use.
- Recovery tooling is available in-app to export a legacy-format bundle or import old legacy files into SQLite explicitly when needed.

## Tech Stack

- Backend: Python, FastAPI, Uvicorn
- Frontend: vanilla HTML, CSS, JavaScript
- Data handling: pandas, BeautifulSoup4
- Scraping: requests, keibascraper
- Name conversion: pykakasi

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Start the app:

```bash
uvicorn server:app --reload --host 0.0.0.0 --port 8000
```

3. Open:

```text
http://127.0.0.1:8000
```

On Windows, you can also use `run.bat`, which opens the browser and writes server output to `server.log`.

For phone/tablet access on the same network, open:

```text
http://<your-lan-ip>:8000
```

Example:

```text
http://192.168.40.175:8000
```

TV Mode notes:

- The embedded live stream uses the same upstream playback chain as GreenChannel web (`/api/vij` -> Streaks playback API).
- Playback availability depends on upstream service conditions (including region/network requirements).

## Project Layout

- `server.py`: FastAPI app assembly, scrape job orchestration, root page, shutdown endpoint
- `tv.html`: TV Mode page (live stream embed + synchronized marks panel)
- `routers/maintenance.py`: cache, dictionary, and backup/restore endpoints
- `routers/lists_config.py`: Favorites/Watchlist and config endpoints
- `routers/races.py`: marks, race data, history refresh, day import, and day delete endpoints
- `data_manager.py`: scraping, translation, race discovery, and history/result fetch logic
- `static/`: browser-side app logic and styling
- `data/`: local runtime state

## Notes

- This tool is not a betting model and should not be treated like one.
- Netkeiba page structure changes can break parts of the scraper.
- Upcoming race discovery is only as good as the currently published Netkeiba data plus the day-list fallback.

## Contributing

If you want to improve scraper reliability, race/result parsing, UI workflow, or the auto-pick logic, contributions are welcome.