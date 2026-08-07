# UMAnager v2.0 — Current State

*Lean active-task snapshot. Updated 2026-07-31 (session 63) — Friday ET, ~10h before the live weekend opens.
All work committed, pushed, deployed and verified live.
Rolling history → [dev_log.md](dev_log.md) (recent only); full backlog → [TODO.md](TODO.md); debt register →
[TECH_DEBT.md](TECH_DEBT.md); blueprints/invariants/**North Star** → [CLAUDE.md](CLAUDE.md).*

---

## ✅ Clean stopping point — app healthy and ready for tonight

Two real bugs found and fixed today, both **verified live against real JRA-VAN**, both pushed:

```
3adb5f3 Fix wedged weekly master refresh: DIFN option=4 setup -> option=1 delta
368b156 Fix watchdog blaming wrong command, causing Sidecar restart loop
```

`main == origin/main`. Working tree clean (only the pre-existing untracked `ui redesign/` folder).

### The short version

The app was found **actively broken**: the Sidecar had been getting killed and relaunched **every 10 minutes**
since 06:07 that morning. Two separate causes, stacked:

1. **The watchdog was blaming the wrong command**, so its own 12h backoff never engaged and it re-queued the
   same hanging job forever. Fixed by tracking what the Sidecar is *actually executing* (a FIFO of in-flight
   commands) instead of *the last command sent*.
2. **Underneath it, the weekly horse-master refresh had genuinely stopped working** since 2026-07-23 — it was
   asking JRA-VAN for a full 1991-onward setup download *every week*, which stopped returning at all after
   JRA-VAN repacked their historical files in late July. Fixed by switching to delta mode with a saved cursor.

Both fixes are live and confirmed working. **0 Sidecar restarts** since deploy.

---

## Live / deploy state

**Nexus (PID 1100) + Sidecar (PID 10720) + Tray (PID 8336) — all in Session 1** (the normal desktop session),
all running today's code (DLLs rebuilt 08:27, verified fresh). Pipeline health: `parse` ✅, `streaming-watchdog`
✅, both at 0 consecutive failures. Phase **RACES_POPULATED**, 72 races loaded across 2026-08-01 + 2026-08-02
JST, all with post times. First post **09:40 JST Sat = ~20:40 ET Fri**; LIVE_OPERATIONS begins ~90 min before.

Master data is **current for the first time in over a week** (`last_um_refresh` = today; 8,386 records ingested).

**Frontend note:** Nexus serves static from SOURCE `wwwroot` (memory `nexus_serves_src_wwwroot`), so JS/CSS
changes are live on a cache-bust + **hard** refresh (Ctrl+Shift+R — a normal reload can serve a cached
`index.html`), no rebuild. Current: `script.js?v=20260718-1`.

---

## ⚠️ Operational trap hit today — read before any redeploy

**Services started over SSH land in Session 0 with an ELEVATED token, and then nothing in the desktop session
can stop them** — not Claude Code, not `taskkill /F`, and **not the Tray's "Stop services"**. `GetOwner`
returning blank/rc=2 is the tell; `Get-Process ... | Select SessionId` is the quick check. The Nexus DLL stays
locked and `launch-services.ps1` correctly aborts rather than shipping a stale binary.

**Recovery:** operator runs **one elevated** (Run as Administrator) `launch-services.ps1 -Action stop`, then a
normal non-elevated `-Action start` puts both back in Session 1. Memory `nexus_restart_session_pipe` has been
corrected — its old claim that "only the Tray can start the Sidecar" is **wrong**; a plain Session-1 launcher
start works fine. What actually matters is that Nexus and Sidecar share **one** session.

Also: `launch-services.ps1 -Action start` holds the shell's stdout open (children inherit the handle), so it
looks like it "hangs" from a tool context. It hasn't — verify with the API, not the exit code.

---

## Known gaps / carryovers

- **`GetTimestampAsync` UTC-vs-local skew (NEW, s63)** — timestamps are stored as UTC (`ToString("O")`) but
  parsed back with `DateTime.TryParse` as **Local**, then compared to `DateTime.UtcNow`. Every such comparison
  is off by the 5h ET offset (the "12h" UM backoff is really ~7h). Affects several timers. **Deliberately not
  fixed** — too wide a blast radius hours before a live night, and now moot for the job that exposed it.
  Good calm-day fix; check every `GetTimestampAsync` caller together.
- **`normalizeMarksPayload`'s field whitelist (s62)** — any NEW per-race field added to `raceMeta` must be added
  there too or it silently vanishes on the next reload (this is what ate `sideBets`). Still a live footgun.
- **◎ favorite-drift guard (H16)** — still one weekend short of `tuning_hypotheses.md`'s ≥3-weekend bar as of
  s61. Not touched since; don't assume it's cleared.
- **Iris/Hermes notification routing** — still deferred, see `TODO.md` §2. Conclusion from s62 stands: keep the
  tuning-hypotheses analysis on Claude regardless of any Iris hardware upgrade.
- **RB14 live-results gap** _(memory: live_results_rb14_gap)_ — still unverified. Query the Librarian first.
- `ui redesign/` folder — old mockups, untracked, still untouched.

---

## Reference: what changed in the code today

**`368b156` — watchdog attribution (Nexus only, no Sidecar/protocol change):**
`SidecarBridge` in-flight FIFO (`MarkCommandForwarded` / `MarkCommandCompleted` / `ClearInFlight` /
`IsInFlight`); `ActiveStreamCommand` is now the FIFO head. `NexusPipeServer` enqueues on forward, dequeues on
`STREAM_*_COMPLETE`, clears on every teardown path. `LiveOrchestrator` backs off if `STREAM_DIFN` is anywhere
in flight, not only at the head.

**`3adb5f3` — DIFN option=4 → option=1 (Sidecar + Nexus):**
`DifnStreamHandler` takes `fromTime`/`option`, returns the advanced `lastFileTimestamp`, and treats `rc=-1` as
success-with-no-data (preserving, never blanking, the cursor). Defaults stay option=4/1991 so **BLDN's one-shot
bloodline load is unchanged**. New `app_state.difn_file_cursor` persisted exactly like `toku_file_cursor`, seeded
from `AppStateService.DifnCursorBootstrap` (`20260720000000` — deliberately EARLIER than the last good run;
overlap re-UPSERTs harmlessly, a gap silently loses updates). `last_um_refresh` now stamps on
`record_count >= 0` (an empty delta week is a SUCCESS) with parsing still gated on `> 0`.

Full Oracle Q+A recorded at the end of `ORACLE_ANSWERS.md` (ANSWERED + IMPLEMENTED + VERIFIED LIVE 2026-07-31).


## OrePro submit investigation 窶・current state 2026-08-07

This is the active handoff state. The complete local OrePro investigation history is in git commits `806570f`, `57c64e0`, `d918a48`, `0ba9b4c`, `96ec105`, `ee3f56e`, `78f57f7`, and `b321920`.

### Current live result

- Nexus and Sidecar are currently running under the long-lived SSH supervisor; the API was verified HTTP 200 after recovery.
- The operator corrected the stored OrePro password, supplied a fresh Cookie header, and ran Apply Day Votes.
- Apply still fails at OrePro's authenticated write APIs. The latest redacted trace shows:
  - cart add: HTTP 200, `status:OK`
  - `api_post_bet_generator.html`: HTTP 200, `status:NG`, `reason:not login`
  - `api_post_mybet.html`: HTTP 200, `status:NG`, `reason:not login`
- The shutuba page loads and mark/cart add responses look superficially successful, but generator and final submit reject the session. No confirmed bet receipt was produced by the latest attempt.
- The redacted network trace remains in `logs/nexus.log` under `[OREPRO_TRACE]`; it deliberately excludes Cookie, Authorization, token, session, and password values.

### Code now in the repository

- Browser-matched Origin, Referer, XHR, Accept, callback, and UTF-8 form headers.
- Bet-generator request before final submit.
- Redacted OrePro request/response tracing.
- Cart-add confirmation no longer treats OrePro's `action=get` / `data:null` response as a failure when the add response is `status:OK`.
- Companion cookie-export/synchronization scaffolding is present, but the UMAnager VM cannot see the work PC's localhost CDP port; the work-PC Edge CDP endpoint was verified separately on port 9222.

### Do not claim resolved

The submit issue is not fixed. Do not run another full Apply Day Votes batch until the authenticated browser/server session boundary is resolved or a one-race live receipt is explicitly confirmed. Do not expose or commit cookies, credentials, passwords, or webhook secrets.

### Operational note

A one-shot SSH launch caused Nexus and Sidecar to die when the SSH session closed. The current working launch is under a long-lived SSH supervisor; a durable Windows service/task supervisor still needs to be established before calling service availability permanent.
