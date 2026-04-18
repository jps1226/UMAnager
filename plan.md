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

### A) JV race payload completeness
JV engine now loads real race/entry data from native RA/SE reads (no NK crossover), but some fields are still minimal.

Needed next:
1. Improve race name decoding/normalization for better display quality.
2. Enrich JV entries with pedigree lineage fields (`Sire`, `Dam`, `BMS`, IDs) from JV-native records where available.
3. Add JV-native result/history parsing so post-race updates stay fully JV-backed.

## Implemented Since Initial Draft
1. Added native JV schedule reader script: `scripts/jvlink_bridge_native_schedule.ps1`.
2. Verified typed RA/SE extraction using JVRead/JVGets with correct VARIANT marshaling.
3. Integrated native JV loading into `data_manager.fetch_weekend_timeline()` for `data_engine=jv`.
4. Switched JV native loader to upcoming-focused stream combo:
  - `DataSpec = TOKU` with `DataOption = 2`
  - `DataSpec = RACE` with `DataOption = 1`
  - `DataSpec = SNPN` with `DataOption = 2`
5. Confirmed strict JV engine now assembles real races from RA/SE stream without NK fallback.

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
