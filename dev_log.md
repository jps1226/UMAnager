## 2026-08-07 — Uptime Kuma weekend-card preflight reporting

**What/why:** Added Uptime Kuma reporting for the Friday weekend-card safeguard so a healthy Nexus port cannot mask missing JRA cards.

**Changes:** Created native Push monitor `UMAnager — Weekend Card Preflight` on watchdog (weekly interval). Added a secret app setting for its URL, Uptime Kuma HttpClient registration, and state-change reporting after Friday noon ET. The app reports `up` only when both expected weekend cards are present; missing cards report `down`. The Push URL is stored in app settings and excluded from Git.

**Result:** Tests passed 13/13; Release build passed. Direct UMAnager-to-Kuma labeled down/up probe returned HTTP 200 both ways, and Kuma recorded down then final up. Nexus remains RACES_POPULATED with healthy watchdog.

## 2026-08-07 — Weekend card safeguards deployed

**What/why:** After the JV-Link incident, added prevention for missing weekend cards, stale in-flight watchdog state, and ProtonVPN starting after a reboot.

**Changes:** Watchdog now uses an active in-flight command timestamp even if `IngestionStatus` is cleared prematurely; Friday-after-noon ET preflight checks the expected Saturday/Sunday JRA dates and sends a deduplicated alert only after Discord accepts it; added `tools/ops/Test-WeekendCardPreflight.ps1`; changed ProtonVPN Service and WireGuard to Manual start.

**Result:** Release build passed with 0 warnings/errors; automated tests passed 13/13; deployed Nexus + Sidecar in Session 1; forced live tick and preflight both passed with 72 upcoming Aug 8/9 races, phase `RACES_POPULATED`, healthy watchdog, and working JRA-VAN DNS.

## 2026-08-07 — JV-Link race-card recovery resolved

**Result update:** The normal cursor-based `TOKURACESNPN` refresh completed successfully after the repair. `JVOpen(..., option=2)` stored 3,040 records and advanced the cursor to `20260807112834`; Nexus staged 1,061 usable race-card records (1,979 intentionally skipped as unused types). `/api/races` now has 72 upcoming races on 2026-08-08 and 2026-08-09, and phase is `AWAITING_ODDS`.

## 2026-08-07 — JV-Link weekend race-card recovery

**What/why:** Investigated missing phase notifications and missing weekend/Sunday JRA cards. Nexus was stuck in `WAITING_FOR_RACES`; the UI’s Aug 5 card was stale local/NAR past-race data, not an upcoming JRA card.

**Changes:** Captured JV-Link program/service backups; rebooted UMAnager; installed the verified, JRA SYSTEM SERVICE-signed JV-Link update in place; fully closed ProtonVPN/its WireGuard service; confirmed JRA-VAN DNS resolution; restarted `JVLinkAgent`; relaunched Nexus and Sidecar together in desktop Session 1. Cleared a wedged option-4 `TOKURACESNPN` call by restarting only Nexus/Sidecar after it showed no connection or cache progress.

**Result:** Repair removed the immediate `JVOpen rc=-413` failure and `DIFN` successfully completed (3,700 received / 2,949 staged; cursor `20260806190143`). `TOKURACESNPN` race-card ingestion is still unresolved: `/api/races` has zero upcoming races, phase remains `WAITING_FOR_RACES`, and no Aug 9 JRA card is present. Normal cursor-based race-card refresh is the active recovery path; success is an actual Aug 9 entry in `upcoming_races_by_date`, not a newer past/local date.

**Lesson:** DIFN/master-data success does not prove race-card ingestion. Avoid repeat option-4 backfills during this incident; they can wedge `JVOpen` without a network/cache transfer. Keep ProtonVPN fully closed while isolating JRA-VAN connectivity.

# Developer Log

*This file is a permanent chronological journal of the project's development. Entries are added after every successful Verification Milestone or major Git commit.*

---

---
## Session 65 — 2026-08-01 — Discord notifications moved to embeds

**What/why:** Match UMAnager’s Discord notifications to the finance dashboard’s compact embed presentation.

**Changes:** Reworked `DiscordNotifier` to send colored Discord embeds with mobile-friendly titles, named fields, event-specific colors, and Discord field/title length limits. Covered status, odds, reminders, wins, recaps, errors, and test pings; delivery remains 2xx-gated.

**Result:** Nexus project compiled successfully in a separate verification output with 0 errors (one pre-existing nullable warning). Live services were not restarted during the active Saturday race window; deployment remains pending.

---
## Session 64 — 2026-08-01 — Discipline recap and bet reminders

**What/why:** Investigated the missing 2026-08-01 recap. Individual win pings handled frozen Discipline lines, but `DayRecapNotifier` only counted manual marks and skipped the recap. Added the requested Friday/Saturday Eastern reminders.

**Changes:** Recap now accepts locked frozen bet lines; added idempotent 4:00 PM and 6:30 PM Eastern reminder checks for cards with no locked bets; reminders use the existing Discord webhook and app-state ledger.

**Result:** Nexus project compiled successfully in a separate verification output with 0 errors (one pre-existing nullable warning). Commit `0776df1` pushed. Live services were not restarted during the active Saturday race window; deployment remains pending.

## Session 63 — 2026-07-31 (Friday ET, day before the live weekend) — killed a Sidecar restart loop, then fixed the JV-Link hang underneath it

**Focus:** Asked to "pull and pick up from there" — nothing to pull (already at `6d10fee`), but the app was
found actively broken: the Sidecar was being killed and relaunched **every 10 minutes**, and had been since
06:07 that morning. Traced it to a misattribution bug in the watchdog that landed in that morning's remote-agent
commits, fixed it, then went after the JV-Link hang the watchdog was papering over and fixed that too. Both
verified live against real JRA-VAN. 2 commits pushed (`368b156`, `3adb5f3`).

**1. The Sidecar kill/restart loop — watchdog was blaming the wrong command (`368b156`).** The streaming
watchdog tracked "last STREAM_* command *forwarded*" rather than "command currently *executing*". The pipe
forwarder drains the command queue back-to-back, so with two commands in flight the field held the LAST one
sent while the Sidecar was still working on the FIRST. A hung `STREAM_DIFN` was therefore logged as a hung
`STREAM_RTCARD` — so the "back off the weekly UM refresh for 12h" branch **never fired**,
`last_um_refresh_failed_at` was never written (confirmed absent in `app_state`), and the orchestrator
re-enqueued the same hanging pull every tick. 6+ observed cycles; any command queued behind the wedge died
with the killed Sidecar. **Fix:** the Sidecar's command loop is strictly sequential and sends exactly one
completion per command on every path (success, bad input, exception) — so Nexus can track this with a FIFO,
**no Sidecar or pipe-protocol change needed**. `SidecarBridge` now holds an in-flight queue whose HEAD is what's
really running; enqueue on forward, dequeue on completion, clear on teardown (including the normal non-throwing
session end, which previously leaked stale state into the next connection). Backoff now triggers if
`STREAM_DIFN` is anywhere in flight, not just at the head. **Verified live:** correct command + full in-flight
list logged, backoff written for the first time ever, "still in backoff … skipping this tick", 1 restart
instead of a loop, watchdog health back to 0 failures — and the phase immediately advanced
AWAITING_POSTS → AWAITING_ODDS → RACES_POPULATED, because card fetches had been getting killed mid-flight.

**2. Deploy was blocked by an elevation/session trap — worth remembering.** The morning's remote-agent session
started Nexus+Sidecar **over SSH**, landing them in **Session 0 with an elevated token**. From the desktop
session (where Claude Code AND the Tray both run, non-elevated) `Stop-Process` and `taskkill /F` both returned
**"Access is denied"** — a real Windows boundary, not a sandbox limit (identical failure with the sandbox
disabled). The Nexus DLL stayed locked and the launcher correctly **aborted rather than shipping a stale
binary**. The Tray's "Stop services" would have failed the same way. Operator ran one elevated
`launch-services.ps1 -Action stop`; everything then restarted into Session 1, restoring the normal
configuration. Memory `nexus_restart_session_pipe` updated — it had claimed only the Tray could start the
Sidecar, which this session **disproved** (a plain Session-1 launcher start works fine; what matters is that
both processes share one session).

**3. The real bug underneath: `JVOpen("DIFN", "19910101000000", 4)` had stopped returning entirely (`3adb5f3`).**
Last success 2026-07-23, no code change since. Hard block inside the COM call — ~0.7s CPU across 10 minutes,
nothing downloading, reproduced across 7+ fresh Sidecar processes. **Checked `ORACLE_ANSWERS.md` first** and
found a 2026-06-19 answer about a modal dialog that blocks identically — then **ruled it out with evidence**
rather than re-asking: that dialog blocks *every* connection attempt, yet `JVRTOpen("0B15"/"0B31")` succeeded on
the same STA thread seconds before each wedge; and the start-kit dialog auto-dismisser never logged a detection.
Wrote a scoped Oracle question leading with those exclusions. **Oracle answer:** we were misusing `option=4` —
it's for a one-shot initial build, it **ignores `fromtime` for master records**, and before returning `JVOpen`
synchronously cross-references the server manifest against the entire local cache, which scales *geometrically*
on a 35-year manifest. JRA-VAN's late-July repack of the historical setup files pushed it past the point where
it yields control back at all. There is no timeout and no pre-flight call. **Fix:** routine weekly refresh now
uses `option=1` (delta) with a persisted `difn_file_cursor` (mirrors the existing `toku_file_cursor` pattern);
BLDN deliberately stays on option=4 (genuinely run-once). **Verified live: rc=0 in under 4 seconds** — the same
call had not returned *at all* for 8 days — then 8,386 UM/KS/CH records streamed to completion in 32s, cursor
advanced to `20260730183442`, parsed, failure backoff cleared, 0 Sidecar restarts since.

**4. Two latent bugs caught during the delta migration (both would have failed quietly).** (a) On option=1 a
quiet week returns **zero records — a SUCCESS**, but `last_um_refresh` was stamped only on `record_count > 0`
(indistinguishable under option=4, where records always existed). Left as-is it would have marked the job "never
refreshed" and re-enqueued it every tick — a second, slower loop. Now gated on `>= 0`, with parsing still gated
on `> 0`. (b) `rc=-1` (no new data) would have **thrown**, and the blank `lastfiletimestamp` would have reset
the cursor to a full re-pull. Now treated as success-with-no-data, preserving the incoming cursor.

**Carried forward / deliberately NOT done:**
- **`GetTimestampAsync` UTC-vs-local skew** — timestamps are written as UTC (`"O"`) but read back via
  `DateTime.TryParse` as **Local**, then compared against `DateTime.UtcNow`. Every such comparison is off by the
  5h ET offset, so the "12h" UM backoff is really ~7h. Affects several timers; **not fixed** — too wide a blast
  radius hours before a live night, and now moot for this job. Worth a calm-day fix.
- **Why JV-Link wedges rather than erroring is JRA-VAN-side** — the Oracle attributes it to a manifest/perf
  cliff in the legacy 32-bit component. Nothing further to do on our side.
- `ui redesign/` still untracked, untouched.
---
## Session 62 — 2026-07-17/18 (Fri night → Sat, live weekend) — four real bugs found+fixed live, all committed

**Focus:** Started as a routine pre-flight check before the operator placed live bets, turned into finding and
fixing four separate, real, previously-mysterious bugs by digging into actual failures as they happened
rather than guessing — an OrePro session-cookie bug, a silently-dropping-side-bets bug, an empty TV sidebar,
and a geo-blocked livestream. Ended by wiring the livestream fix into the Tray app properly. Landed as 4 atomic
commits (`dd84a3a`, `91e9f24`, `a5832f5`, `a6c4f8e`), pushed to `origin/main` — see `current_state.md` for the
full writeup.

**1. OrePro "not login" failures on 2 of 36 races, TWICE (Friday and Saturday) — real root cause found.** Every
OrePro HTTP call seeded a brand-new session from the *static* cookie string in Settings, discarding any
Set-Cookie rotation OrePro sent back. A multi-step apply (marks-first POST, then a separate custom-bet POST)
could rotate the session between those two calls, so the second call used a stale cookie and failed —
looking exactly like a dead session even with a perfectly fine one. Fixed in `OreProVoteApplyService.cs` +
`OreProCustomBetService.cs`: persist the cookie after every call. **Found a second, self-inflicted bug in that
first fix**: cookies are seeded under both a wildcard (`.netkeiba.com`) and exact (`orepro.netkeiba.com`)
domain, so a rotation can leave the jar holding stale + fresh copies of the same cookie name — the naive
dedupe could grab either one depending on iteration order. Fixed to always prefer the non-wildcard (fresher)
copy. Added logging to `CheckCookieOnceAsync`'s previously 100%-silent failure branches — a "not logged in"
verdict used to leave zero trace of why. **Verified live both nights**: direct placement succeeded cleanly on
Hakodate R2/R7 after the fix, confirmed on OrePro's own site.

**2. Side bets kept silently not placing — traced to the ACTUAL cause, not a timing fluke.** First suspected
(wrongly) a stale browser tab; the real answer: `normalizeMarksPayload` in `script.js` rebuilds each race's
`raceMeta` from an explicit field whitelist on every marks reload (page load included), and `sideBets` was
never in that whitelist — so **every reload silently erased configured side bets from memory**, even though
the server kept them saved fine. This is what caused the exact-same-looking drop on Hakodate R2 on both
Friday and Saturday nights. Fixed by adding `sideBets` to the whitelist. Also added a defense-in-depth guard
(belt-and-suspenders, not the fix itself): `buildOreProCustomLinesForRace` now detects when a configured side
bet didn't make it into the built ticket and surfaces a clear warning, both in the single-race status line and
as a new "Side Bet Drops" section in Apply Day Votes' diagnostics — even on an otherwise-"ok" race, so this
class of silent loss can't hide behind a green summary again. Verified: confirmed the field now survives a
reload, and confirmed the guard fires/doesn't-fire correctly.

**3. JRA livestream (TV mode) was geo-blocked — real architecture fix, not a workaround.** `GchController.cs`
called `sp.gch.jp`/`streaks.jp` directly from inside Nexus, which is geo-gated to Japan IPs. Routing Nexus
itself through the VPN was already known-bad (the s61 incident — broke OrePro/Discord live). Extracted the
stream-fetch into a new standalone script, `tools/gch_stream_proxy.py` — the only thing that needs to sit
inside ProtonVPN's Japan split-tunnel, while Nexus stays completely out of it. Verified via real TCP
connection inspection (not just config-file reading, which turned out to be unreliable/confusing along the
way — the live connection list was the only trustworthy signal) that Nexus's connections never left the
normal LAN IP, both before and after. `GchController.cs` now just relays to the local proxy
(`127.0.0.1:5057`). **Verified end-to-end**: a real HLS manifest with a valid signed token came back through
the full chain (browser → Nexus → local proxy → JRA, Tokyo-routed).

**4. Also fixed while touching TV mode: empty sidebar + added live odds.** TV mode (`tv.html`) is a standalone
page with no access to `script.js`'s engine logic, and Discipline mode's ◎ pick is computed on-the-fly at
apply time rather than stored in the marks blob — so the sidebar had nothing to show for any Discipline-bet
race. Fixed: `getMarksForRace` now falls back to each race's frozen bet record (`raceMeta.betProfile.betLines`)
when the raw marks are empty — the real placed bet, not a guess. Verified live: all race cards populate
correctly. Also added live odds next to the Fav badge in the same sidebar (small, verified rendering, e.g.
"Fav 1 ×1.2").

**5. Wired the stream proxy into the Tray app as a real, independent feature — no more manual terminal
script.** New "Start stream proxy (TV mode)" / "Stop stream proxy" menu items in `TrayApp.cs`, deliberately
kept separate from "Start/Stop services" so a proxy hiccup can never touch the actual betting pipeline. Two
real bugs found while wiring this: (a) bare `"python"` as `Process.Start`'s FileName silently failed — no
process, no log, no visible error — fixed by resolving a real interpreter path instead of relying on PATH
search; (b) Python fully buffers stdout when it isn't a real terminal, so the log file stayed empty the whole
time the proxy was serving live requests — fixed with `-u` (unbuffered). Needed a new `System.Management`
package reference in `UMAnager.Tray.csproj` (WMI, so Stop can tell this specific python.exe apart from any
other python process on the machine). Verified live: Stop then Start produces a genuinely fresh process, the
log updates in real time, the port serves correctly.

**Operational lessons worth remembering:**
- **A normal browser refresh can serve a cached `index.html` and miss a bumped `script.js?v=` entirely** — a
  real hard refresh (Ctrl+Shift+R) was needed mid-session to actually pick up a fix; don't trust "I refreshed"
  at face value if a JS fix seems not to have landed.
- **Rebuilding the Tray app needs the same lock-avoidance dance as Nexus** — "Exit tray (leave services
  running)" first (confirmed multiple times this session: does NOT stop Nexus/Sidecar), rebuild, relaunch;
  it rediscovers already-running services by process name with zero disruption.
- **`normalizeMarksPayload`'s field whitelist is a proven footgun** — any new per-race field added to
  `raceMeta` in the future needs to be added there explicitly or it will silently vanish on the next reload,
  exactly like `sideBets` did for who knows how long before this session.
- Also discussed (no code changes): whether Iris/Hermes could generate the tuning-hypotheses day recap instead
  of Claude — concluded no, keep it on Claude regardless of any future Iris hardware upgrade, since that task
  needs judgment/faithfulness the local model has a demonstrated pattern of failing without heavy guardrails
  (per Hermes's own extensive testing log), and the honest-tuning discipline can't afford to get that wrong.
---
## Session 61 — 2026-07-16 (Thursday ET / calm day, sixty-first session) — closed s60's mid-task thread + a clean cleanup sweep

**Focus:** Picked up s60's unfinished 18-race Discord recovery and finished it (with a real twist), then
fixed the UI-scale regression it caused on the operator's daily 2K monitor, and closed out four carryover
cleanups — two of them real bugs traced to root cause. Everything committed, pushed, deployed, and verified.
**Ends at a genuinely clean stopping point**, unlike s60. 4 commits pushed (`1d8c25d`, `64b197e`, `19e79f4`,
`627e1ad`).

**1. The 18 missed Discord win-pings (¥251,000) — RECOVERED, but not the way s60 predicted.** Cleared the 18
IDs from `app_state.bet_win_notified_race_ids` (559→541, verified) and triggered `trigger-bet-win-recheck`.
**First attempt failed identically** — all 18 hit `SocketException 10049` again. **s60's VPN theory was WRONG:**
ProtonVPN only tunnels Python (operator confirmed; `ipconfig` shows Nexus outside the tunnel), and an OrePro
cookie-check succeeded *in the same seconds* the Discord posts failed. Real cause was **stale/hung outbound
connections in the long-running Nexus process** — the operator restarted via the Tray and it cleared instantly.
Retry: **10 of 18 delivered (HTTP 204); the other 8 got Discord 429 (rate-limited)** by firing back-to-back —
and were *still marked notified*, silently losing them again (see item 3 — this is what exposed the bug).
Cleared just those 8, paused for the limit window, retried → **all 8 delivered (204)**. **All 18 confirmed
delivered by checking Discord's actual 204 response per race, not just the presence of a `[BetWin]` log line.**
_Lesson: "it logged" ≠ "it delivered" — verify the transport's own success code._

**2. UI Scale regression on the daily 2K monitor — root cause was a one-signal formula.** Operator returned to
their main PC: 85% scale AND the J%/T% columns dropped, on a 2560px screen with plenty of room. s60's rebuilt
formula was **DPR-primary** (`100 − (dpr−1)×60`) — but the 1080px laptop and the 2560px 2K monitor are **both
at 125% OS scaling**, so they share an identical `dpr 1.25`. **DPR literally cannot tell them apart** and handed
both 85%, which also tripped the ≤85% compact-column rule. The two live data points have the same dpr and very
different widths, so **width is the only signal that separates them** — rebuilt WIDTH-primary (`≥1800px → 100%`,
`1080px → 85%`, linear between). Laptop preserved at 85%, 2K back to 100% with all columns. Operator: "looks
perfect." _Note the pattern: s60 swung from a width-based formula to DPR-only after a double-correction bug;
the right answer was width alone, no compounding — the original failure was how it was combined, not the signal._

**3. Discord silent-drop bug — found BY the recovery, fixed properly (`64b197e`, DEPLOYED).** `DiscordNotifier
.SendAsync` returned **void** and swallowed non-2xx responses (logged a warning, never told the caller), while
`BetWinNotifier` marked a race notified **before/regardless of** the send outcome. So a 429 — or any rejection —
lost the ping forever with a reassuring `[BetWin] Race X won` line in the log. **Fix:** `SendAsync` now returns
`bool` (true only on 2xx); `NotifyMarkHitsAsync` surfaces it; a WIN is only banked as notified once Discord
confirms, so a failed send stays eligible for the next tick. **Subtlety handled:** when no webhook is configured
at all, winners still mark (nothing to deliver) — otherwise they'd re-evaluate forever. Losers/no-runner races
mark immediately as before. The `[BetWin]` log now says "delivered" vs "NOT delivered."

**4. Marks-blob race condition — root cause CONFIRMED and fixed (`19e79f4`).** s60's carryover ("2 of 36 applied
races got an `appliedAt` but no frozen bet"). Traced it: `saveMarksToServer()` serializes the **ENTIRE** in-memory
blob and `POST /api/marks` is a bare **last-write-wins overwrite** (`SetStringAsync`, no merge/version check) —
and it's called **fire-and-forget from ~8 places with no in-flight guard**. Two overlapping POSTs race; whichever
the server processes LAST wins, so a staler snapshot (serialized moments before a bet froze at apply) silently
drops the `betProfile`. **Why the symptom looked like that:** `appliedAt` lives in a SEPARATE store (OrePro apply
status, written server-side per race) so it survives, while `betProfile` lives only in the clobbered blob. **Fix:**
single-flight + coalescing guard — only one POST in flight; concurrent requests collapse into exactly one follow-up
that re-serializes the LATEST state. **Verified in-browser:** burst of 5 → `maxConcurrent:1`, 2 posts; and a fresh
save after a burst still fires (**no deadlock** if a POST is slow). _Debug note: an early test read
`window._marksSaveInFlight` and got alarming "stuck guard" readings — top-level `let` does NOT attach to `window`
(function declarations do), so those reads were always `undefined`. The scare was the test, not the code._

**5. OrePro login-check false positive — fixed (`627e1ad`, DEPLOYED).** The no-race cookie-check treated
`shutuba.html?race_id=` links as proof of login, but **those race-card links are served to logged-OUT visitors
too** (the race list is public) — so a dead cookie could report **"logged in — bets can be placed."** Now trusts
only the login-gated markers (`ログアウト`, `/mydata/`); worst case is a false NEGATIVE ("open a race and re-test"),
the safe direction. Live-verified post-deploy: returns `loggedIn:true` through that exact path on a real session.

**6. Iris/Hermes integration — investigated, scoped, DEFERRED (documented, not built).** Operator asked about
routing Discord delivery through "Iris" (Nous Hermes agent on the hermes VM). Pulled `serverstuff`, read
`STATUS/hermes-vm.md`. **Technically straightforward** (LAN webhook `192.168.40.59:8644`, HMAC-signed,
agent-composed → Discord home channel; the arch-server docker-alert route is exact precedent). **Recommended
deferring, operator agreed**, for two grounded reasons: (a) **Iris's SOUL.md voice isn't developed yet** — the
hermes docs *themselves* defer this identical feature until it is, since a plain-voiced relay adds nothing over
the direct webhook; (b) **routing a money/win ping through a non-deterministic 4B agent is a reliability
downgrade** — her devlog has a logged case of posting a raw `CLAUDE HANDOFF` block to Discord instead of the alert.
**If ever built: HYBRID only** (direct-to-Discord stays primary; Iris gets a parallel best-effort copy for flavor).
**Networking prereq found:** this VM can't reach `.59` at all — diagnosed from the **servarr host** (clean vantage,
outside the VM's VPN): Hermes VM running, `:8644` listening and serving real HTTP, gateway active. **The block is
100% UMAnager-VM-side ProtonVPN routing** swallowing LAN traffic. Logged in `TODO.md` §2 + memory
`iris_hermes_notifications`. Did NOT touch VPN routing (fiddly; no reason to destabilize connectivity for a
deferred feature).

**7. Housekeeping + deploy.** Deleted the two stale `marks_blob_backup_*.json` files (verified as marks-blob
backups first). Deployed the two C# fixes properly: operator stopped services via Tray → confirmed no surviving
processes/port-5000 listener → **Release** rebuild (Tray launches from `bin/Release/net8.0`, NOT the Debug output
my first build produced) → verified the new DLL timestamp AND that the fix strings are actually in the binary →
operator started via Tray → **verified live**: Nexus pid 9144 from the Release path, `Sidecar INIT: JVLink-OK(rc=0)`,
cookie-check 200/`loggedIn:true`.

**Process note — a real safety catch worth remembering.** While debugging the marks-blob guard I nearly POSTed an
empty `{marks:{},raceMeta:{}}` probe to `/api/marks` to time the endpoint — **the auto-mode classifier blocked it,
correctly**: that's a last-write-wins overwrite and would have **wiped the operator's live picks** during what was
supposed to be a read-only investigation. Diagnosed via the network log instead (which immediately showed the one
POST returning 200 and disproved the "hung endpoint" theory). _Lesson: on a last-write-wins endpoint there is no
such thing as a harmless write probe — read the network log, don't poke the store._

**Decisions:** "It logged" never proves "it delivered" — gate bookkeeping on the transport's own success code, not
on code-didn't-throw (this exact class of bug has now bitten twice: s60's swallowed exceptions, s61's swallowed
429s); auto-scale must key off the signal that actually distinguishes the devices (width), and a formula calibrated
on ONE device must be re-checked on the others before shipping; don't route money-critical notifications through a
non-deterministic model; when a diagnosis implicates another machine, run it from a vantage point outside the
suspect's own network stack (the servarr host settled the Hermes question in one pass); **CLAUDE.md's "git is the
archive" handoff rationale is factually stale — see below.**

**⚠️ Process bug in CLAUDE.md itself — caught mid-handoff and FIXED.** CLAUDE.md's handoff section told every
session to **delete** dev_log entries past the 5 newest because "Git is the archive: every old entry is permanently
recoverable." **That premise was factually false:** commit `8adfa58` (2026-05-16) untracked ALL markdown
(`.gitignore`: `*.md`, only `!README.md` tracked), so `dev_log.md`/`current_state.md`/`TODO.md`/`CLAUDE.md` itself
are local-only with no git history since ~s30. **Every entry since s31 exists ONLY in the working file** — the rule
would have irreversibly destroyed it while assuring the session it was safe. (s60 had already refused to prune on
explicit operator instruction — the smell was there.) **Fixed at operator's go-ahead:** (1) rewrote CLAUDE.md's
handoff section — MOVE to `docs/archive/`, **never delete**, with the git-is-NOT-the-archive warning and a note on
why the old rule was wrong; (2) discovered the archive convention **already existed** (`docs/archive/` holds
`dev_log_sessions_01_35/36_40/41_43/44_45/51/52`) — the "just delete" rule had replaced a working process, so this
restored it rather than inventing one; clarified that `docs/archive/` is **write-on-handoff, ignore-on-read**;
(3) archived s55-s56 → `docs/archive/dev_log_sessions_55_56.md`, cutting from `dev_log.md` only AFTER byte-verifying
the copy (zero lines lost), leaving the 5 newest (s57-s61). **Deliberately NOT done:** re-tracking markdown in git —
the untrack was intentional ("specs/logs/Q&A reference are local-only") and `ORACLE_ANSWERS.md` carries JRA-VAN spec
excerpts that shouldn't land in a public repo. _Consequence: these docs live only on this VM — if that matters, the
answer is a backup, not git._ _Lesson: an instruction file can be confidently wrong; when a rule's stated rationale
is checkable, check it before doing the irreversible thing it authorizes._

**8. Same session, continued — built the actual tooling for the CLAUDE.md fix, plus a real backup, plus a
routing correction.** Operator pushed back on the plan ("second repo sounds redundant") and asked two sharper
questions instead: can we just do local backups, and does any of this even solve the "new sessions read too
much" problem (answer: no — that's the unrelated, already-working 5-entry-cap mechanism; tracking/backup status
doesn't touch it). Also flagged directly that the *manual* archive dance from item 7 above was too slow/token-
heavy — a real, actionable complaint, saved as feedback memory `handoff_archive_mechanics_too_heavy`.
- **`tools/handoff-archive.mjs` revived** (CLAUDE.md had referenced it as "removed — do by hand," which was
  itself part of the stale rule). Automates exactly the item-7 mechanics: keep newest 5, cut the rest into
  `docs/archive/dev_log_sessions_<lo>_<hi>.md` chronologically, append to a contiguous file if one fits
  (rename + old-file cleanup handled), verify the archive copy byte-intact BEFORE ever touching `dev_log.md`,
  abort with zero writes on anything malformed. **Tested against synthetic fixtures** (not the real file) for
  all three paths — new-file, append/rename, and abort-on-bad-input — before trusting it; one real bug caught
  in testing (an overly-strict verification check flagged the archive header's own intentionally-changing
  range line as "lost content" — false positive, fixed to exclude that one line specifically while still
  asserting the rewrite actually took effect). CLAUDE.md's Handoff section now points at the script instead of
  "do it by hand."
- **Investigated whether `UMAnager` could just go private** (would let `dev_log.md` etc. track normally, no
  second repo) — checked, and it's public. Then checked whether `dev_log.md` alone was safe to publish even in
  the current public repo — it isn't: 55+ hits for JRA-VAN spec terms and OrePro/cookie specifics, since it's
  the narrative describing fixes to those very systems. Public tracking of any of this local-only doc set is
  off the table without a repo-visibility change the operator hasn't decided on.
- **`tools/backup-docs.ps1` built + registered as a daily scheduled task** ("UMAnager2 Docs Backup", 4am,
  `C:\Users\UMAnager\DocsBackup\UMAnager2\<date>\`, 60-day retention). Copies every root `*.md` + `docs/archive/`
  — the same set `.gitignore` treats as local-only. Verified end-to-end: manual direct run, a fixed pruning bug
  (`[DateTime]::TryParseExact` 5-arg overload doesn't resolve cleanly in Windows PowerShell 5.1 — switched to
  `ParseExact` in a try/catch), a real Task-Scheduler-triggered run (not just direct PowerShell) confirmed
  `LastTaskResult: 0`, and pruning tested against a planted old-dated folder + a non-date decoy folder (old one
  removed, decoy and today's both left alone).
- **Real correction to the Iris/LAN-blocker diagnosis.** Originally attributed (this session, item 6) to
  ProtonVPN's split-tunnel. Wrong mechanism: `Get-NetRoute` shows a **Tailscale subnet route** for
  `192.168.40.0/24` via `100.100.100.100` at **metric 0**, beating the direct Ethernet route (metric 256) — so
  Tailscale, not ProtonVPN, is intercepting ALL LAN-peer traffic from this VM regardless of which app initiates
  it. Matches an existing memory (`lan_topology_and_vpn_hijack`) on this exact gotcha. Practical effect is the
  same as before (no LAN path to `.59`/servarr/arch-server right now, fix deferred off the live weekend) but
  the mechanism note in `TODO.md`/current_state.md needed correcting so the eventual fix targets the right
  setting (Tailscale route acceptance / metric, not the ProtonVPN app list).

**Left Off At (session 61 end, for real this time):** **CLEAN — no mid-task, nothing uncommitted, all deployed
and verified**, now genuinely including the process fix (script exists + tested, not just done once by hand) and
a working local backup (running daily, first real run already confirmed). `main == origin/main` (6 commits this
session total: `1d8c25d`, `64b197e`, `19e79f4`, `627e1ad`, `8e55534`). Next session is very likely the **live
weekend** (2026-07-18 JST card already loading; operator watches it Friday night ET). Top priority when it runs:
**validate the marks-blob fix at the real 36-race apply** — confirm every applied race gets BOTH `appliedAt` AND
a frozen `betProfile`. Second: re-run the H16 ◎-drift check (one clean weekend short of the ≥3 bar; don't build
the guard until it clears).
---
## Session 60 — 2026-07-11/12 (Sat–Sun ET / live weekend, sixtieth session) — LONGEST session yet, ends mid-task

**Focus:** An exceptionally dense live weekend. Fixed a real live-ops chain (JV-Link dialog → Discipline sunk-cost
blindness → Day Net accounting model → a silent SignalR data gap), shipped a Watchlist opt-in betting feature that
caused (and then fixed) a genuine live safety incident, re-verified the ◎ favorite-drift theory with real data,
did a full round of QOL/UI-scale work, found and fixed a dangerous display-vs-money mismatch bug live, and ended
mid-recovery from a VPN networking misconfiguration that silently ate 18 Discord win-pings. Committed + pushed one
batch (`40bedb6`); a second batch (sidebar/UI-scale/compact-columns/frozen-pick-display) is live-deployed but
**still uncommitted** — see current_state.md. **Session paused mid-task** (18-race Discord recovery approved but
not yet executed) for a context handoff — pick this up FIRST next session.

**1. JV-Link "セットアップ" dialog — auto-dismissed for good.** The dead `DialogHelper.cs` (previously only
*detected* the dialog and asked a human to click it) now runs a background watcher thread for the Sidecar's whole
life, auto-clicking "no CD" → OK the instant the dialog appears — including mid-run, not just at startup (found
live: it was blocking mid-poll, not just on restart, and had caused ~90 min of stalled ingestion + 3 watchdog
resets before being cleared). Deployed via a Sidecar-only restart (Nexus's pipe server loops and reconnects
automatically — confirmed, no need to restart both).

**2. SunkCostService — a 4th spot with the SAME "Discipline never writes globalMarks" blindness from s59.**
`GetPerRaceRecapAsync`'s "is this race placed" gate only checked for manual marks, so Discipline races (frozen
bet lines, no marks) were silently excluded from the sunk-cost tally entirely — invisible to `/api/sunk-cost` and
the Discord win-ping's "Day net" line. Fixed (also count a race "placed" if it has a frozen `betProfile.betLines`).
**Immediately exposed a second bug**: once Discipline races were counted, the OLD sunk-cost accounting model
(every placed bet's stake counts the INSTANT it's applied, hours before the race runs) made Day Net swing to
~-¥300,000+ — the full day's stake, most of it unsettled. **Reframed Day Net to SETTLED-basis** (starts at ¥0
each day, moves only as each race actually finishes) in both places: the backend (`SunkCostService`, day-scoped
calls only — the all-time Voting-tab tally intentionally keeps the original "counts immediately" model, that's
the whole point there) and the frontend home-tab quick-stat (`updateQuickStats`), plus a small hover panel
showing gained-vs-lost totals for the day (simplified down from an initial full per-race breakdown after the
operator said that was more detail than wanted).

**3. A live display bug traced back to the SignalR pipeline never sending payout data — the deepest fix of the
early session.** Operator reported a real Kokura win showing as "lost" on the race header + Day Net, staying
wrong for over a minute after Discord (correctly) pinged the win. Root cause, in two layers:
- `evaluateTemplateOutcome` (JS) was declaring a race `hasResults:true` the instant finish positions arrived,
  even if the payout table (`results_json`) hadn't — scoring against an empty payout object reads as a total
  loss. Fixed: now waits for real payout data (checks for a non-empty win/place/quinella/wide/trio array) before
  declaring a verdict, matching how the backend already worked (never wrong before, just late).
- **Deeper bug: `LiveBroadcastService.BroadcastResultsAsync`'s SignalR push never included the payout table at
  all** — only per-entry Finish/Odds/Fav. So a race could NEVER resolve via the live pipeline alone, only a full
  page reload, no matter how long you waited. Fixed: backend now includes `resultsJson` in the `ResultsUpdated`
  payload (results-only, not odds); frontend patches it into BOTH in-memory race representations
  (`globalRaceInfo[r_id]` — a separate shallow snapshot — AND the nested `race.info` from `globalRacesByDate`,
  since they're not the same object) and now actually calls `updateQuickStats()` from the live handler (it never
  had before — only the separate Voting-tab all-time figure got refreshed on a live push).

**4. Watchlist opt-in "paper bet" popup — built, then had to be safety-patched after a real live incident.**
Feature: at Apply Day Votes time, any Watchlist horse running that day (minus the engine's own ◎) gets offered a
¥1k place side bet, unchecked by default, riding the existing side-bet OrePro pipeline (no new bet "kind").
**The incident:** the operator clicked ✖ on this new popup expecting it to cancel the whole day's apply (matching
how ✖ behaves on the preceding day-preview screen) — but ✖ and "Skip" were coded identically, both just meaning
"no extras," with NO real abort path once you'd reached that screen. 36 races submitted anyway. No financial harm
(they were the exact races already confirmed on the prior screen, no extra side bets slipped through — verified
against live apply-state + the marks blob) but a genuine trust-breaking bug. **Fixed properly**, not just
patched: the popup is now the TRUE final gate whenever Watchlist candidates exist, with three honestly-labeled
outcomes (Cancel = abort everything, Skip = main bets only, Add selected = main + extras), and the PRECEDING
day-preview screen's button/copy now honestly says a further step follows instead of claiming to be "the last
stop" when it isn't. Each candidate row also shows a stat line (odds/fav/record/last3/jockey%) per a follow-up
ask ("I need to know if it's trash, not just that it's running").

**5. ◎ favorite-drift theory (H16) — re-verified with real data, not yet build-ready.** Operator noticed a
striking Saturday-night pattern (78% hit rate first half of the card vs 17% second half) and asked to dig in.
The blunt "early vs late in the card" theory did NOT replicate across the other 4 Discipline days on the books —
two of them reversed just as hard, one more dramatically. But testing the SHARPER, original hypothesis (does the
◎ pick itself drift off the closing favorite, regardless of race position?) held up well: 135 settled bets across
4 days, no-drift 60% hit vs drifted 43% hit, drift rate 26% closely matching an independent 28% read from weeks
ago. Logged as H16 in `tuning_hypotheses.md`; `TODO.md` §2 got a proper design sketch (3 shapes: notify-only,
re-point-with-confirmation, defer-submission — the last one reintroduces the exact "bets fire unattended" tension
from item 4, flagged explicitly). **~2.5-3 weekends of evidence — doesn't yet clear the file's own ≥3-weekend
bar.** Operator also asked about deliberately auto-submitting for a few days to get cleaner timing data;
recommended against it (doesn't sharpen the measurement — the drift test doesn't need controlled timing — and
reintroduces the same unattended-submission risk from item 4 for no real benefit) in favor of just accumulating
normal weekends.

**6. QOL round — collapsible sidebar, UI-scale rebuild, compact columns.** Operator on a 1080p/125%-scaled laptop
(2K monitor is the daily driver) reported the UI too big and asked for real auto-scaling instead of the old
manual slider ("horrible to use, takes ages to take effect"). Built in stages, with two real course-corrections
from live operator feedback:
- **Sidebar collapse** — a small circle toggle, then a full-height edge strip, then (after the strip drifted
  off-position under `zoom`) rebuilt to be `position:absolute` inside a `.sidebar-shell` wrapper anchored to
  `.sidebar`'s own box — no more separately-computed position to drift.
- **UI Scale auto-detection rebuilt TWICE.** First attempt (screen.width vs a 2560px/2K reference) reasoned that
  since screen.width already reflects OS scaling, one signal could catch both effects — WRONG in practice, it
  double-corrected (OS scaling already normalizes for density; deriving a second correction from the
  already-shrunk width stacks a correction on a correction). Computed 65% (the clamp floor) for the laptop when
  85% was actually right. **Recalibrated against real operator-reported numbers** (1080px screen width, dpr 1.25
  → 85% felt right): `100 - (dpr-1)*60`, DPR-primary — much closer to the ORIGINAL pre-session formula's
  philosophy, just re-tuned with real data instead of theory. Manual slider REMOVED from Settings entirely (a
  dev-only override field remains as an escape hatch). Made per-device (`localStorage`), not synced to the
  account — the actual root bug behind "it's too big on my laptop" was that a value tuned on the 2K monitor was
  silently overriding EVERY device on the account via backend-synced settings.
- **Compact-desktop column tier** — J%/T% drop below 85% effective scale (mirrors the existing mobile
  column-visibility mechanism as a third tier), bypassed when the sidebar's collapsed (frees enough room on its
  own). Also fixed a real CSS bug found along the way: a settled Discipline race's now-empty Shirushi cell was
  still reserving its full 192px column width (`table-layout:fixed` means the `<th>` governs the whole column
  regardless of cell content — both header and body needed the narrow-state class).

**7. CRITICAL — the Discipline grid was showing the WRONG horse. Found live by the operator, not by testing.**
"The prediction column shows a place 10k bet on #10 in Kokura R1, but OrePro says I voted #15. Despite showing
#10, who won, it says I lost — which is true, because I really placed it on 15." Confirmed against real data:
frozen bet was genuinely on PP15 (finished 7th), the grid was showing PP10 (the CURRENT live market favorite,
which won) as the ◎ pick. **The scoring was always correct** (used the real frozen bet) — only the DISPLAY was
wrong, because `engineMarkByHorse` (feeding the Shirushi badge) is a live re-recompute of the engine's CURRENT
ranking, recomputed fresh on every render, with no awareness that a real bet had already been frozen at a
different (now-drifted) horse. This is the direct, visible symptom of item 5's drift phenomenon — not a
coincidence. **Fixed:** when a race has a frozen single-horse Discipline bet, the grid now shows THAT horse,
never a live recompute; the 〇▲△ "leans" are suppressed too once a real bet is frozen (a live lean next to a
real bet is the same kind of misleading mix). Operator's own framing of the fix: "once we bet, it needs to stay
locked in, even if the odds change" — exactly right, and now what the code actually does.

**8. Two live "why did races fail" investigations — one closed, one is THE unfinished thread.**
- **Investigation 1 (closed):** 2 of 36 auto-applied races got an `appliedAt` timestamp but no frozen bet record
  at all — not a duplicate, just silently never written. Leading theory: the 36-race apply loop fires rapid
  async marks-blob saves (~2-4s apart) and two can race, a stale save clobbering a fresher one. Recovered live
  (operator just re-ran Apply for those 2, ~6 hours of runway before post) — the underlying race condition itself
  is UNFIXED, flagged as a carryover.
- **Investigation 2 (THE unfinished thread — pick up here next session):** operator reported no notifications
  since "the first two" last night. Traced to `DiscordNotifier.SendAsync` swallowing failures internally
  (logs a warning, never rethrows) — meaning a `[BetWin] Race X won...` log line does NOT prove Discord delivery
  succeeded, only that the C# code path didn't throw. Found the real signal (`[Discord] Webhook POST failed`)
  and its exact cause: `SocketException 10049 "address not valid in its context"` — a LOCAL networking failure,
  not Discord or OrePro's fault. **Confirmed live it wasn't Discord-specific**: OrePro's own cookie-check failed
  with the identical error, meaning bet SUBMISSION itself would have failed too if attempted during this window
  — a much bigger problem than missed notifications. Root cause, per the operator: changed the VM's VPN to a
  Japan server last night (for the TV-mode JRA livestream, which needs a JP IP) then had to set up split
  tunneling for Nexus + Python after that broke "claude." **The VPN tunnel itself was never unstable** (15+ hrs
  connected, live traffic, confirmed via screenshot) — it was specifically `UMAnager.Nexus.exe`'s app-match in
  ProtonVPN's split-tunnel "included apps" list. Removing Nexus from that list (keeping Python tunneled, VPN
  connection itself untouched) fixed connectivity immediately, confirmed via a fresh `cookie-check`. **Swept the
  full log for the damage: 18 distinct races, ¥251,000 in real unnotified wins**, spanning the whole card since
  the issue started (matches "nothing since the first two"). Recovery plan (same mechanism as s59's 6-race
  recovery) was fully scoped and operator-approved — **clear those 18 IDs from `app_state.bet_win_notified_race_ids`,
  call `trigger-bet-win-recheck` — but was NOT YET EXECUTED when the handoff was called.** Full race-ID list in
  current_state.md.

**Process note — a real safety-classifier lesson, twice in one session.** First attempt to run the DB recovery
query got blocked for hardcoding the live Postgres password as a plaintext literal in a bash command (a fair
catch — moved to a Python script that reads the password from `appsettings.json` at runtime, never printing it).
Second attempt got blocked for a DIFFERENT reason even after that was fixed: a bare "sure go for it" wasn't
specific enough consent for a direct production DB mutation — the classifier wanted the exact table/column/values
named. Both times, the fix was the same: stop, explain precisely what and why, let the operator give unambiguous
sign-off. Worth remembering the pattern for future DB-touching asks: credential handling and action-specificity
are two SEPARATE consent bars, not one.

**Decisions:** Discipline's globalMarks-blindness pattern (s59) recurs — keep checking for it in any new "does
this race have X" code, not just the 3 spots already found; sunk-cost/Day-Net accounting should be settled-basis
everywhere it represents "today," immediate-basis only for the intentional all-time tally; a locked/frozen bet's
DISPLAY must never re-derive from live state once real money is committed — this is now a general principle, not
just today's specific fix; auto-scale formulas should be calibrated against real operator numbers, not reasoned
from first principles (burned twice tonight — screen-width theory and the original DPR guess both needed
correction once real data came in); don't build the ◎-drift guard until H16 clears 3 full weekends by the same
method; VPN split-tunnel app-matching may not survive a Nexus rebuild — watch for this recurring after any future
C# change.

**Left Off At (session 60 end, mid-task):** **NOT a clean stopping point — resume immediately with the 18-race
Discord recovery**, fully scoped and approved, see current_state.md's ACTIVE/NEXT section for the exact race IDs
and mechanism. Nexus is currently OUTSIDE the VPN split-tunnel (the fixed, correct state — do not revert this
without understanding why). Commit `40bedb6` pushed; a second batch (sidebar/UI-scale/compact-columns/
frozen-pick-display, items 6-7 above) is live-deployed but uncommitted — confirm with operator before committing,
per usual. Phase LIVE_OPERATIONS, 2026-07-12 JST card in progress. Per explicit operator instruction this
session, dev_log.md was NOT pruned to 5 entries this time — all history kept.
---
## Session 59 — 2026-07-04/05 (Fri–Sun ET / live weekend, fifty-ninth session)

**Focus:** Picked up s58's #1 candidate — verify the s56 Discipline bet-flow overhaul live — and it paid off hard:
found and fixed one root-cause bug hitting three different features, caught 6 real Discord win-pings that had
been silently swallowed, fixed a live "price over" OrePro rejection mid-bet, and shipped a proper server-side
OrePro login to replace cookie copy-pasting. Committed + **pushed** (`e76726d`).

**The shared root cause — Discipline mode never writes `globalMarks`:** the engine picks the bet itself; nothing
gets manually marked ◎〇▲△. Three unrelated pieces of code still assumed a race with no `globalMarks` entry meant
"no bet," so they all went silently blind under Discipline (which has been the default mode since s56):
1. **Bets tab showed nothing.** Operator: "I don't think any of my bets are showing up." `buildRacecourseCheatHtml`
   only builds its race-card list from `globalMarks`. Fixed by synthesizing the same engine-◎〇▲ group the
   `hasMarks` fix (below) already used, for any Discipline race without a manual per-race override.
2. **Apply Day Votes did nothing.** Same blindness in the `hasMarks` eligibility filter — fixed by special-casing
   Discipline mode to check `collectDisciplineEngineRunners` instead of `globalMarks`.
3. **Discord win pings never fired for Discipline bets — the big one.** `BetWinNotifier.EvaluateAndNotifyAsync`
   bailed early whenever `TemplateBetEvaluator.BuildRunners` (mark-based) came back empty, without checking
   whether a FROZEN bet-line record existed to fall back on. Traced it, confirmed **6 real wins on 2026-07-04
   were silently marked "already handled" with no ping** (Hakodate R1/R2/R3, Fukushima R1/R3, Kokura R3). Fixed
   the bail condition, then added a permanent `POST /api/orchestrator/trigger-bet-win-recheck?date=` endpoint
   (mirrors the existing `trigger-recap`), cleared those 6 race-ids from `app_state.bet_win_notified_race_ids`,
   re-ran, and confirmed all 6 Discord pings actually delivered (204s in the log).

**Live "price over" bug, found mid-session while the operator was trying to bet:** a race's frozen bet record had
gotten a side-bet line baked in during an earlier buggy batch, and every retry kept appending a FRESH side-bet
line on top of the already-frozen one — two identical OrePro bet_ids, which OrePro rejects outright. Root-caused
to the day-apply loop's auto-lock: it used to lock/freeze **every** eligible race in a batch whenever *any* race
in that batch succeeded, so a race that itself failed could still get its (wrong) shape frozen. Fixed the lock
scope to only the races that individually succeeded, fixed `buildOreProCustomLinesForRace` to not re-append a
side line onto an already-frozen plan, and added a belt-and-suspenders dedup pass that collapses any two lines
resolving to the same OrePro bet_id right before the ticket is built (keeps the larger stake) — makes a duplicate
physically unsubmittable regardless of source. Along the way, discovered the diagnostics panel referenced by the
frontend (`orepro-session-status` / `orepro-sync-results`) had never actually existed in the page HTML — every
status message and failure detail had been silently generated and thrown away since it shipped. Added the
missing containers.

**OrePro session refresh — went from bookmarklet to real server-side login:** operator asked to make cookie
refresh easy. First attempt was a bookmarklet (drag to bookmarks bar, click while on OrePro, auto-captures
`document.cookie`) — built, tested end-to-end with Playwright, worked mechanically, but turned out to be the
wrong shape entirely: the operator refreshes OrePro from their **phone** via the public URL, not a desktop
browser on the VM. Scrapped it. Checked netkeiba's actual login form (no CAPTCHA, no JS hashing, 5 plain
fields) and built real server-side login instead: `POST /api/orepro/login` submits stored (or given)
credentials, harvests the `nkauth`/`netkeiba` cookies from the handshake, and `CheckCookieAsync` now
auto-relogins whenever it finds a dead session — so a stale cookie mostly self-heals without the operator
noticing. Settings got a login-id/password form + a 👁 Show/Hide toggle on the password (added after the first
live attempt failed with netkeiba's own "ID or password incorrect," to help catch autofill/typo mismatches).
Manual cookie copy-paste kept as a documented fallback.

**Found but NOT fixed — a login-check false positive:** empirically confirmed (fed it a garbage cookie) that
`CheckCookieAsync`'s "is this session logged in" probe can say `loggedIn:true` even for nonsense, because the
page it checks is publicly viewable either way. Doesn't affect actual bet placement (which clearly still needs
real auth — bets kept placing correctly all night), but the green checkmark itself isn't proof. Logged in
current_state as a known gap, not urgent given auto-relogin now covers most of the practical risk.

**UX fix — the ◎〇▲ ranking looked like 3 bets:** the display fix for bug #1 above showed the engine's full
top-3 ranking per race, which the operator (rushing out for July 4th plans) read as "3 horses bet per race,"
and worried they'd accidentally over-bet. Confirmed from actual staked totals (~¥335k across ~34 races = ~¥10k
singles, not ~¥1M) that only the ◎ was ever bet. Added an explicit "Betting the ◎ only — 〇 ▲ not bet" line plus
muted styling + a "not bet" tag on the non-bet rows, verified with a live Playwright screenshot.

**Process notes:** installed Playwright in this session (first use in this project) to actually drive the app
headlessly and verify fixes against live data instead of trusting code-reading alone — used it repeatedly to
confirm renders, test the bookmarklet mechanics before scrapping it, and screenshot the final ◎-only fix.
Restarted Nexus + Sidecar directly via PowerShell (not the Tray) after verifying the shell was running in the
same Windows session as the interactive desktop — confirmed safe (the documented pipe-breaking gotcha is
specific to a scheduled-task's different session, not this). One scary moment: a first attempt at a direct DB
edit briefly NULLed the entire marks blob due to a Postgres server-side file-permission quirk with `pg_read_file`;
caught it within seconds via a length check, restored byte-identical from a pre-edit snapshot, then redid the
edit with a client-side `\copy` instead.

**Decisions:** Discipline's globalMarks-blindness is a pattern to watch for in any future code that asks "does
this race have a bet" — check frozen bet lines / engine runners, not just marks; the bookmarklet idea was wrong
for a phone-first operator, server-side login is the right shape for OrePro auth; one bad Discipline night (36%
◎-place hit rate) is noise until confirmed across several weekends.

**Left Off At (session 59 end):** all pushed (`e76726d`, `main == origin/main`). Phase RACES_POPULATED, Nexus +
Sidecar up. OrePro login working (cookie has `nkauth`, `cookie-check` reports logged in). **NEXT = pick from
backlog:** (1) North Star §0 Step 1.5 (point-in-time jockey/trainer/sire in the backtest — still the standing
strategic thread, untouched this session), or (2) tighten the OrePro login-check false-positive, or (3) pull
several weekends of ◎-place hit rate together (operator asked, not started). Carryovers unchanged (JV-Link Setup
dialog, post-weekend fixture refresh, ◎-drift guard, codebase-navigation discussion still open).


## 2026-08-07 窶・OrePro submit investigation handoff

**What changed:** Compared a successful browser HAR with UMAnager traffic; added browser-equivalent callbacks/headers, the bet-generator call, redacted `[OREPRO_TRACE]` request/response logging, and safer cart-add confirmation. Added companion CDP cookie-export scaffolding. Corrected the stored OrePro password and supplied a fresh cookie for the latest attempt.

**Current result:** The latest Apply Day Votes run still fails. Cart add returns `status:OK`, but both `api_post_bet_generator.html` and `api_post_mybet.html` return `status:NG`, `reason:not login`. The failure remains an authenticated session-boundary problem. No receipt was confirmed.

**Live state:** Nexus and Sidecar were restored and API health verified HTTP 200 under a long-lived SSH supervisor. A one-shot SSH start had allowed child processes to die when the session closed; do not use that launch shape.

**Next:** Resolve the work-PC browser session to UMAnager bridge or correct the credential/session login so the generator API accepts the same authenticated state as the successful browser. Verify one race receipt before any full batch. Never log or commit cookies/credentials.
