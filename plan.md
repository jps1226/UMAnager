## Plan: Storage Overhaul

Replace the current mixed file-based persistence model with a local SQLite-backed storage layer, introduced incrementally so behavior stays stable while durable state moves off JSON, text, and pickle. Start with a DB foundation and repository layer, migrate low-risk domains first (config, lists, marks, OrePro), then refactor the higher-coupling horse cache and race cache onto relational tables. Defer voting analytics work until the storage layer is stable.

**Steps**
1. Phase 0: Inventory and DB foundation. Add a dedicated database module, connection/session management, schema bootstrap, and migration/version tracking. Decide on the access layer (recommended: SQLAlchemy with thin repository helpers, not raw ad hoc SQL) because multiple routers will need consistent transactional behavior.
2. Phase 0: Define the initial SQLite schema around current durable domains: `app_config`, `horses`, `tracked_horses`, `watchlist_horses`, `races`, `race_entries`, `race_marks`, `race_metadata`, `orepro_profiles`, `orepro_sessions`, `orepro_daily_results`, and `orepro_race_results`. Store flexible structures like formula weights and column layouts as JSON columns where that meaningfully reduces churn.
3. Phase 0: Add a migration bootstrap path that can create tables on startup and record a schema version. This blocks all later phases because it establishes the one supported persistence surface.
4. Phase 1: Introduce read/write repositories for config and horse lists. Migrate `data/config.json`, `data/tracked_horses.txt`, and `data/watchlist_horses.txt` to DB-backed access first because they are low-risk and easy to validate. Keep one-time import logic from the existing files and a fallback read path only during the migration window.
5. Phase 1: Replace current config and list access in `storage.py`, `routers/lists_config.py`, and any consumers in `routers/races.py` so all new writes go to SQLite. Remove direct append-style file writes and enforce transactional updates through the repository layer.
6. Phase 2: Migrate marks and race metadata. Replace the current `saved_marks.json` store with `race_marks` plus `race_metadata`, preserving the current API shape from `GET /api/marks` and `POST /api/marks` so the frontend does not need another behavior change during the storage rewrite. This can run in parallel with final cleanup of config/list imports once the DB foundation is in place.
7. Phase 2: Migrate OrePro durable state. Move `orepro_session.json`, `orepro_last_sync.json`, and `orepro_results_history.json` into `orepro_profiles`, `orepro_sessions`, `orepro_daily_results`, and `orepro_race_results`. Preserve current semantics for full-day replacement of partial syncs and the last-sync view so frontend behavior remains unchanged.
8. Phase 2: Update `routers/orepro.py` to read/write through the DB repositories while keeping the current API contract intact. This depends on the DB foundation and OrePro table definitions but not on race-cache migration.
9. Phase 3: Refactor the horse cache model. Replace the global in-memory `HORSE_CACHE` writeback pattern with DB-backed reads/writes to the `horses` table, plus a small in-process cache only if needed for performance. This is a prerequisite for clean race-cache migration because race entries depend on horse identity and pedigree fields.
10. Phase 4: Replace `race_cache.pkl` with relational race and entry storage. Move race metadata into `races` and row-level scraped data into `race_entries`, then update scrape/load codepaths so the app no longer depends on pickle serialization or Pandas-encoded cache structure for durable state. This is the highest-risk phase and should happen after the lower-risk domains are already stable.
11. Phase 4: Update `routers/races.py`, `data_manager.py`, `routers/scrape.py`, and maintenance/backup flows to treat SQLite as the source of truth. Replace backup/restore assumptions that currently operate on a folder of mixed files with a DB-aware snapshot strategy plus any remaining raw data artifacts that are intentionally kept outside the DB.
12. Phase 5: Cleanup and deprecation. Remove legacy JSON/text/pickle reads after the DB path has been validated, keep an import/export tool for recovery, and then revisit the voting-performance work on a fresh branch using DB-backed joins instead of custom file merges.

**Relevant files**
- `config.py` — add a canonical SQLite DB path and any storage-related constants.
- `storage.py` — evolve from file helper layer into the entry point for DB bootstrap, migration/version handling, and repository access helpers.
- `server.py` — initialize the DB on startup and ensure all routers run after schema/bootstrap completes.
- `data_manager.py` — replace direct JSON/pickle persistence and the global horse cache pattern with DB-backed storage.
- `routers/races.py` — migrate marks, race metadata, race reads, and cache-backed operations off file storage.
- `routers/orepro.py` — migrate session, last-sync, and historical OrePro result persistence to relational tables.
- `routers/lists_config.py` — replace text-list persistence and direct append writes with transactional DB operations.
- `routers/maintenance.py` — update backup/restore behavior to handle the SQLite file and any retained non-DB artifacts.
- `routers/scrape.py` — ensure scrape completion writes through the new repositories instead of pickle/file helpers.
- `static/script.js` — ideally unchanged for the first migration phases except where API payloads are intentionally preserved; avoid frontend churn during the storage move.
- `data/config.json` — one-time import source during migration, then deprecated.
- `data/saved_marks.json` — one-time import source during migration, then deprecated.
- `data/orepro_session.json` — one-time import source during migration, then deprecated.
- `data/orepro_last_sync.json` — one-time import source during migration, then deprecated.
- `data/orepro_results_history.json` — one-time import source during migration, then deprecated.
- `data/tracked_horses.txt` — one-time import source during migration, then deprecated.
- `data/watchlist_horses.txt` — one-time import source during migration, then deprecated.
- `data/horse_names.json` — migration source for horse identity and pedigree cache, then deprecated.
- `data/race_cache.pkl` — final migration target; remove only after relational race storage is verified.

**Verification**
1. Add repository-level tests for every migrated domain proving import from legacy files, DB writes, DB reads, and idempotent repeated startup behavior.
2. After each phase, run the affected API routes manually and confirm payload shapes are unchanged for the frontend, especially `/api/marks`, `/api/config`, list endpoints, `/api/orepro/results/last`, and `/api/orepro/results/history`.
3. Validate that app restart preserves data entirely from SQLite without needing the legacy files present.
4. Test backup and restore flows against the new DB file and confirm that restore returns the app to a known state without orphaning dependent records.
5. For the race-cache migration, validate scrape -> store -> load -> render round-trips on both upcoming and past races, including finish positions, odds, and mark sorting behavior.
6. Compare a known OrePro day before and after migration to confirm purchase, payout, profit, and race counts are identical.

**Decisions**
- Recommended DB: SQLite. It fits the current single-user local app model and avoids deployment overhead while giving transactional safety and query support.
- Recommended access layer: SQLAlchemy (or SQLModel if you want lighter models), not direct sqlite3 calls scattered through routers.
- Keep the current API contracts stable during migration whenever possible so the storage overhaul does not become a coupled frontend rewrite.
- Keep scratch artifacts, captured HTML, and reverse-engineering files out of the DB; only product state belongs there.
- Defer PR 2 voting analytics until after at least marks and OrePro history are DB-backed, otherwise the branch will keep accumulating custom merge logic on top of temporary storage.

**Further Considerations**
1. ORM choice: SQLAlchemy is the safer recommendation because the app already has multiple domains and will need explicit control over transactions. SQLModel is acceptable if you want less boilerplate, but it should still be used through repositories rather than directly from routers.
2. Migration style: prefer one-time import plus dual-read fallback only during a short transition window. Long-lived dual-write logic will create drift and make debugging harder.
3. Branching strategy: treat this as a new storage-foundation branch with phased PRs: DB foundation, low-risk state migration, marks/OrePro migration, horse-cache refactor, race-cache migration, then cleanup.

**Progress Log**
- 2026-03-16 — Step 1 complete: Added SQLite foundation constants, SQLAlchemy engine/session helpers, and startup initialization hook.
- 2026-03-16 — Step 2 complete: Added v1 schema definitions for config, lists, races, marks, and OrePro domains.
- 2026-03-16 — Step 3 complete: Added startup schema bootstrap (`create_all`) and idempotent schema version tracking via `schema_migrations`.
- 2026-03-16 — Steps 4 & 5 complete: Added DB-backed config and horse list repositories to `storage.py` (`load_horse_list`, `save_horse_list`, `add_horse_to_list`, `horse_ids_to_text`, `_parse_horse_lines_from_text`). Upgraded `load_app_config`/`save_app_config` to DB-backed upsert with one-time JSON import fallback. Updated `routers/lists_config.py` and `routers/races.py` to read/write through the new repositories; removed direct file access and file-path constants from both routers. Added `display_name` column to horse list tables (with idempotent `ALTER TABLE` migration on startup) and one-time backfill from legacy txt files so the `ID # Name` wire format is fully preserved.