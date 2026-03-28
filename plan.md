# Dual Engine Architecture Plan (NK vs JRA-VAN)

## Goal
Build and maintain two fully separate backend engines that can be switched from Settings while keeping the frontend race UI unchanged.

- `nk` engine: Netkeiba discovery and scrape pipeline
- `jv` engine: JRA-VAN discovery and data pipeline
- No backend crossover when one engine is selected

## Non-Negotiable Rules
1. Engine selection lives in app settings (`backend.dataEngine`).
2. Runtime race cache must be isolated by engine.
3. Selected engine must not call the other engine's discovery/scrape path.
4. Frontend rendering remains engine-agnostic.

## Implemented in this branch

### 1) Settings-level engine selection
- Added `backend.dataEngine` default in storage config defaults.
- Added Settings modal selector (`Netkeiba` / `JRA-VAN`).
- Persisted setting via existing `/api/config` path.
- Engine switch triggers UI refresh without changing frontend race components.

### 2) Hard race-cache isolation
- Added separate runtime tables:
  - `races_nk`, `race_entries_nk`
  - `races_jv`, `race_entries_jv`
- Added engine-aware storage APIs:
  - `load_race_cache(data_engine=...)`
  - `save_race_cache(..., data_engine=...)`
  - `clear_race_cache(data_engine=..., clear_all=...)`
- `/api/cache/clear` now clears both engine caches to avoid stale cross-engine artifacts.

### 3) Engine-aware flow wiring
- Scrape endpoint now resolves and passes `data_engine`.
- Races load/save wrappers now use active engine from settings.
- `fetch_weekend_timeline(..., data_engine=...)` persists to engine-specific cache tables.

### 4) Crossover hardening
- If engine is `jv`, source mode is forced to `jv`.
- JV scrape path no longer falls back to NK discovery in `jv` engine mode.
- JV engine path does not call NK entry/odds scraper; it stages JV snapshots only.

## Current Gaps (explicit)

### A) Full JV race payload decoding
Current JV engine snapshots are skeleton race objects unless full JV record decoding is available. This is intentional to preserve zero crossover.

Needed next:
1. Parse RA/SE records from JV cache/stream into full race metadata.
2. Parse race entries/horse IDs from JV payloads.
3. Populate `races_jv` / `race_entries_jv` with decoded JV data only.

### B) Horse cache isolation (optional but recommended)
Horse cache currently remains shared. For strict full-engine separation, split horse cache to:
- `horses_nk`
- `horses_jv`

## Verification Checklist
1. Set Settings -> Data Engine = `Netkeiba`, scrape, verify only `races_nk` changes.
2. Set Settings -> Data Engine = `JRA-VAN`, scrape, verify only `races_jv` changes.
3. Confirm race list endpoint output shape is unchanged across engines.
4. Confirm no NK fallback logs appear when engine is `JRA-VAN`.

## Operating Principle
Frontend stays stable; backend engine swap changes only the data source and isolated storage target.

## Session Closeout Notes (March 26, 2026)

### Completed in this session
- Added one-click JV cache refresh path:
  - backend endpoint `/api/jvlink/refresh-upcoming`
  - UI button `Refresh Cache` in the JVLink panel
  - auto `FromDate` based on latest cached RA/SE date
- Added source provenance fields on race snapshots:
  - `discovery_source`
  - `discovery_sources`
  - `scrape_source_mode`
  - `data_engine`
- Added engine-aware scrape logs (`Initializing Netkeiba Engine...` vs `Initializing JRA-VAN Engine...`).
- Added strict no-crossover behavior for JV engine mode in orchestration.

### Evidence captured during session
- Runtime race content fetch path in NK flow uses Netkeiba scraper and odds fetch.
- JV cache date coverage was confirmed stale relative to target weekend during debugging (`RA/SE` max date ended at `20260323` at that time).
- Because of strict separation requirements, JV mode now avoids NK scrape fallback and records JV-only snapshots until full JV decoding is finished.

### Explicit acceptance criteria for this plan
1. Engine toggle in Settings must be the sole selector for runtime race cache target.
2. NK and JV race caches must remain physically separate tables.
3. JV engine mode must not call NK race discovery or NK race content scraping.
4. Frontend race pages must continue to render without engine-specific UI divergence.
