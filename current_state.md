# Current State
_Last updated: 2026-05-16 (early morning, post-handoff prep)_

## Active Task
No active task — fully landed five threads (odds pipeline fix, UI nitpicks, tray app, pipe lifecycle bug, tray hardening). Saturday race day kicks off later today; primary next milestone is a live re-test of the odds fetch when JRA refreshes boards near post times.

## What Was Just Done
- **JVRTOpen("0B31") rc=-114 fixed** end-to-end. Oracle Q10: key for `"0B31"` must be 16-char `YYYYMMDDJJKKHHRR` (or 12-char short form), not an 8-char date — dataspec is per-race. Refactored `STREAM_ODDS` IPC payload to carry `race_ids[]`; Sidecar iterates per race. Verified: 36 races on 2026-05-16, Stored=36 Skipped=0, EntriesUpdated=493.
- **UI display polish (all three nitpicks):**
  - Stripped trailing `(JPN)` / `(USA)` / etc. country suffix from `horses.NameEn` — parser + one-time UPDATE on 210,745 rows.
  - `FinishPos = 0` → NULL — parser + one-time UPDATE on 2,333 rows. FIN column blank for upcoming races.
  - W/S column populated via grouped query over `race_entries WHERE FinishPos > 0`. Shows career `wins/starts` per horse.
- **Standalone WinForms tray app** built at `src/UMAnager.Tray` — colored status dot, right-click menu (Start / Stop / Restart / Open Dashboard / Open Logs / Exit). Added to `UMAnager.sln`.
- **Major pipe-lifecycle bug fixed** (surfaced by tray): `NexusPipeServer.ReceiveRecordsAsync` had `break;` on `STREAM_*_COMPLETE` → closed pipe → killed Sidecar. Replaced with `batch.Clear(); totalFlushed = 0;`. Pipe now persistent across unlimited streams. Verified with 2× consecutive odds fetches.
- **Tray hardening:** replaced fragile `AdoptFromPidFile` with `Rediscover()` using `Process.GetProcessesByName`. PID file demoted to advisory output. `RefreshStatus` rescans every tick; `StopAll` uses `KillAllByName` to nuke any matching process by name.

## In Progress / Incomplete
- Tray app is built and tested but **not currently running** — user closed it after the duplicate-launch incident. Re-launch from `src\UMAnager.Tray\bin\Release\net8.0-windows\UMAnager.Tray.exe` (it will Rediscover the running services and immediately show green).
- Saturday live re-test of odds fetch — should be re-fired periodically near post times to confirm odds move and FAV ordering tightens.

## Open Questions / Blockers
- (None blocking.) Bookkeeping: Oracle Q10 in `ORACLE_ANSWERS.md` should be flipped "Pending" → "Answered" with the per-race key format. Low priority.

## Key Context
- **Running processes** as of handoff: Sidecar PID 4648, Nexus PID 13064 (both started by PS launcher at 22:50). Tray app not running.
- **Pipe lifecycle:** the IPC pipe between Nexus and Sidecar is now **persistent for the life of the Sidecar process**. Never break out of `NexusPipeServer.ReceiveRecordsAsync` on stream completion — saved as memory file `pipe_lifecycle.md`. Sidecar has no reconnect logic; if pipe is closed externally, it exits.
- **`race_entries.UpdatedAt`** has `default now()` but isn't auto-touched on UPDATE → reflects insert time only. If you want "last odds refresh", set `entry.UpdatedAt = DateTime.UtcNow` in `OddsApplyService.cs:57` or add a separate column.
- **Auto-apply** fires automatically on `STREAM_ODDS_COMPLETE` in `NexusPipeServer.cs:181`. Manual `POST /api/jvlink/apply-odds` is therefore redundant after a fetch (returns 0/0, expected).
- **Tray app discovery:** uses `Process.GetProcessesByName("UMAnager.Sidecar" / "UMAnager.Nexus")` as the source of truth. Compatible with `launch-services.ps1` — both can be used interchangeably.
- **horses table** is ~212,534 rows (not ~1,400 as an older memory implied) — the UM stream populates the entire active horse population, not just runners.
- **breeding_horses table** has only `NameJa` — foreign breeders (Pure Prize, Uncle Mo, Giant's Causeway) appear as roman letters there because JRA-VAN ships them that way; JRA-bred breeders are kanji-only. User-noted future enhancement: optional netkeiba scraper to populate English names.

## Next Steps
1. **Saturday morning live re-test:** re-fire `POST /api/jvlink/fetch-current-odds` repeatedly near post times; confirm odds values drift and FAV ordering tightens. Spot-check one race in the UI.
2. Mark Oracle Q10 as answered in `ORACLE_ANSWERS.md` (bookkeeping).
3. Decide whether to add `LastOddsAppliedAt` column or set `UpdatedAt = DateTime.UtcNow` on apply.
4. (Optional defensive hardening) Wrap Sidecar's outer pipe loop in `while (!ct.IsCancellationRequested)` so it can reconnect on `EndOfStreamException` instead of exiting — defense in depth now that Nexus no longer tears the pipe down.
5. (Future, user-flagged) Manual netkeiba scraper to populate English names for `breeding_horses` ancestors with kanji-only NameJa.
6. (Future, Phase 5) Auto-refresh poll for live odds + SignalR pipeline for ticking UI.
