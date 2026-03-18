## UMAnager Storage Overhaul Reference

### Overview
The storage overhaul has been completed. UMAnager now uses SQLite (`data/umanager.sqlite3`) as the runtime source of truth for app configuration, horse lists, race cache, marks, horse translation cache, and OrePro history.

### What Changed (High-Level)
- Added a centralized SQLite storage foundation with SQLAlchemy engine/session management and schema bootstrap/version tracking.
- Migrated all major durable domains from file-based stores into relational tables.
- Kept existing API contracts stable during migration so frontend workflows remained consistent.
- Reworked maintenance tooling for DB-aware backup/restore and explicit legacy import/export support.
- Removed implicit runtime fallbacks to legacy JSON/TXT/PKL files; recovery from old files is now explicit via maintenance actions.

### Migration Summary by Domain
- App config: moved from `data/config.json` to `app_config` table.
- Favorites/watchlist lists: moved from `tracked_horses.txt` / `watchlist_horses.txt` to `tracked_horses` / `watchlist_horses` tables.
- Marks + race metadata: moved from `saved_marks.json` to `race_marks` + `race_metadata` tables.
- OrePro history: moved from `orepro_last_sync.json` / `orepro_results_history.json` to `orepro_daily_results` + `orepro_race_results` tables.
- Horse translation memory: moved from `horse_names.json` to `horses` table with in-process cache sync.
- Race cache: moved from `race_cache.pkl` to `races` + `race_entries` tables.

### Current App State
- Primary persistence: SQLite (`data/umanager.sqlite3`).
- Runtime behavior: DB-only reads for migrated domains.
- Legacy support: explicit import/export only (`/api/data/legacy/import`, `/api/data/legacy/export`).
- Maintenance tools validated:
	- Clear race cache
	- Reset translation memory
	- Backup/restore data folder
	- Legacy export/import bundle actions

### Validation Status
- Final regression smoke checks passed for:
	- App reachability and startup import
	- `/api/config`, `/api/lists`, `/api/marks`, `/api/races`
	- `/api/data/backups`
	- `/api/orepro/results/history`, `/api/orepro/results/last`
- In-app behavior checks passed across scrape persistence, marks persistence, backup/restore, and translation reset UX.

### Notes for Future Work
- Treat storage migration as complete; follow-ups should be separate feature/cleanup tasks.
- Keep `data/config.json` as local runtime/user-state artifact and avoid committing it unless intentionally changing defaults.
- If future schema changes are needed, add them via explicit migration steps in `storage.py` rather than reintroducing file fallback logic.

**Backup Testing Playbook (App-only)**
1. Open the app and navigate to **Maintenance**.
2. Click **Backup Data Folder** and confirm an alert appears: `Backup created automatically: backups/<filename>.zip`.
3. Make a visible data change in-app (for example, add/edit one mark, or adjust a config toggle and save).
4. Click **Restore Latest Backup**, confirm the prompt, and wait for the completion alert with `restored_from` and `restored_files`.
5. After auto-refresh, verify the data change from step 3 is rolled back to backup state.
6. Repeat restore once more; confirm it succeeds again (idempotent behavior, no HTTP 500).

**If Restore Still Fails (App-only signals)**
- If alert says `locked by another process`, close any duplicate UMAnager tabs/windows and stop extra server instances, then retry restore.
- If alert says `No backup archives found`, run **Backup Data Folder** first, then restore.
- If alert says restore completed but state looks unchanged, create a clearly visible change (single mark/config toggle) before retrying the test flow.
