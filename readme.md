# UMAnager

UMAnager is a local FastAPI dashboard for tracking Japanese horse racing cards, pedigree angles, manual marks, and lightweight race-day workflow data from Netkeiba.

It is built as a personal tool first: scrape upcoming cards, keep pedigree watchlists, mark runners quickly, import completed results for a day, and keep everything in local files under `data/`.

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

## Current UI/Workflow

- Sidebar calendar for loaded race days.
- Header day navigator for fast day-to-day movement.
- Search bar for horses across the currently loaded races.
- Auto-pick strategy slider and configurable display/settings toggles.
- Voting cheat sheet export for building bets outside the app.

## Storage Model

UMAnager is local-file based. It does not use a database.

Important files under `data/`:

- `race_cache.pkl`: cached race cards and entry data
- `saved_marks.json`: saved per-horse marks
- `tracked_horses.txt`: Favorites list
- `watchlist_horses.txt`: Watchlist list
- `horse_names.json`: cached horse/pedigree translation data
- `config.json`: UI and behavior settings

Backups are written to `backups/`.

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
uvicorn server:app --reload --host 127.0.0.1 --port 8000
```

3. Open:

```text
http://127.0.0.1:8000
```

On Windows, you can also use `run.bat`, which opens the browser and writes server output to `server.log`.

## Project Layout

- `server.py`: FastAPI app assembly, scrape job orchestration, root page, shutdown endpoint
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