# Oracle Answers — JRA-VAN Official Documentation

*This file stores answers from the Oracle (NotebookLM with JRA-VAN PDFs, BACKEND_API_SPEC.md, technical manuals). Each entry is a Q&A pair with exact technical details.*

---

## Phase 4D: DataSpec for Weekly Race Card Refresh + JVOpen rc=-1 Meaning

**Q:** Exact DataSpec string for JVOpen to retrieve upcoming weekend race plan data (RA + SE records) with opt=2. What does rc=-1 mean from JVOpen?

**A:**
- **DataSpec:** `"RACE"` (not "TOKURACESNPN" — that was an incorrect approximation in CLAUDE.md)
- **rc=-1 from JVOpen:** `該当データ無し` — "No applicable data." No new data matching the specified parameters exists on the server (or user chose to download latest software version). This is NOT a fatal error — treat as empty stream, return (0, 0) gracefully.

**✅ Fix required in TokuStreamHandler.cs:**
1. Change `"TOKURACESNPN"` → `"RACE"` on the JVOpen call
2. Handle `rc=-1` as no-data (log + return) instead of throwing an exception

---

## Complete Byte Offsets for UM/RA/SE Records (Phase 3: DIFN Record Parsing)

**Q:** Show exact byte offsets (1-indexed) for UM, RA, SE record types with all fields needed for horses, races, race_entries tables.

**A:**
```
RecordType | Field              | Start | Length | Notes
-----------+-------------------+-------+--------+-----------
UM         | HorseId           | 12    | 10     | KettoNum
UM         | NameJa            | 47    | 36     | Shift-JIS
UM         | NameEn            | 119   | 60     | English/Romanized
UM         | BirthYear         | 39    | 4      | Year (YYYY)
UM         | SireId            | 205   | 10     | Pedigree
UM         | DamId             | 251   | 10     | Pedigree
UM         | BmsId             | 389   | 10     | Brood Mare Sire
RA         | DataStatus        | 3     | 1      | データ区分
RA         | LastModified      | 4     | 8      | データ作成年月日 (YYYYMMDD)
RA         | RaceId            | 12    | 16     | YYYYMMDDPPNNNNNN
RA         | RaceDate          | 12    | 8      | YYYYMMDD
RA         | TrackCode         | 20    | 2      | 01-10 (10 JRA tracks)
RA         | RaceNumber        | 26    | 2      | Race number in day
RA         | NameJa            | 33    | 60     | Shift-JIS (Hondai)
RA         | Distance          | 698   | 4      | Meters (1000-3600)
RA         | Surface           | 706   | 2      | 1=turf, 2=dirt
SE         | DataStatus        | 3     | 1      | データ区分
SE         | LastModified      | 4     | 8      | データ作成年月日 (YYYYMMDD)
SE         | RaceId            | 12    | 16     | YYYYMMDDPPNNNNNN
SE         | HorseId           | 31    | 10     | KettoNum
SE         | PostPosition      | 29    | 2      | Gate number (1-18)
SE         | Bracket           | 28    | 1      | Internal bracket
SE         | Weight (Burden)   | 289   | 3      | Jockey weight (kg)
SE         | Weight (Horse)    | 325   | 3      | Horse weight (kg)
SE         | JockeyName        | 307   | 8      | Shift-JIS
SE         | Odds              | 360   | 4      | Win odds (÷10 = actual)
SE         | FavRank           | 364   | 2      | Favorite rank (1-18)
SE         | FinishPos         | 335   | 2      | Finishing position
```

**✅ VERIFIED:** Complete coverage of all database schema fields. All offsets are 1-indexed per JRA-VAN standard.

---

## JVGets IDL Definition & Parameters

**Q:** What is the exact IDL definition of JVGets, including parameter types and marshaling hints?

**A:** *(Pending query)*

---

## JVOpen Parameters & Behavior

**Q:** What do the return values of JVOpen mean? (rc=0, readcount=48, downloadcount=0)

**A:** *(Pending query)*

---

## JVGets Return Codes

**Q:** What are all possible return codes from JVGets and what do they mean?

**A:** *(Pending query)*

---

## DIFN DataSpec Availability

**Q:** Is DIFN available on all JRA-VAN installations, or are there systems still using DIFF?

**A:** *(Pending query)*

---

## IDispatch vs Vtable Marshaling for JV-Link

**Q:** Does JV-Link's COM interface officially support IDispatch, or is InterfaceIsDual required?

**A:** *(Pending query)*

---

## Optimal EF Core Batch Size for Large PostgreSQL Inserts (Phase 3D: 650K+ Records)

**Q:** What is the optimal batch size (records per SaveChangesAsync call) for inserting 650K+ records into PostgreSQL via EF Core? Should we batch 500 at a time, 5000, or larger? Any considerations for transaction locks or memory?

**A:** 
The provided sources do not contain specific recommendations for batch sizes (500 vs. 5000) or transaction lock/memory considerations for EF Core bulk inserts. However, the project's CLAUDE.md specifies critical performance requirements and architectural directives:

**Key Performance & Architecture Directives:**
1. **Performance Target:** Horse master sync of 50,000+ records must complete in **under 60 seconds**
2. **Memory Management:** Use `ReadOnlySpan<byte>` and `ArrayPool<byte>.Shared` to minimize GC pressure during parsing
3. **Architecture:** Named Pipes streaming (Sidecar → Nexus) is the designated pipeline, not traditional HTTP timeouts or background workers

**What This Means for Phase 3D:**
- The 650K+ record parsing must fit within a similar performance envelope (likely 10-15 minutes for full horse master)
- Memory pressure is the primary concern — batch sizes must be aggressive enough to complete quickly but small enough to avoid GC bloat
- The split-process architecture (Sidecar streaming → Nexus parsing → PostgreSQL insert) is already optimized for throughput

**Industry Standard (if external sources consulted):** EF Core typically uses 1,000–5,000 records per batch depending on row width; however, with streaming architecture, larger batches (5,000–10,000) are often preferred to reduce overhead.

---

## Long-Running HTTP Endpoint Pattern in ASP.NET Core

**Q:** For long-running database operations (5+ minutes), should we use HTTP endpoint timeout, background hosted service, or message queue pattern in ASP.NET Core? What is the recommended architecture?

**A:** 
The provided sources do not specify architectural patterns for 5+ minute operations. However, the project's existing design makes the answer clear:

**UMAnager v2.0 Already Implements the Optimal Pattern:**
- The Nexus (ASP.NET Core x64) uses a **command-driven background service** (not HTTP timeout)
- Commands are enqueued via HTTP (`POST /api/jvlink/load-master-data` returns 202 Accepted immediately)
- `NexusPipeServer` (background hosted service) processes the command asynchronously
- Long-running operations (DIFN stream, parsing) happen on the server's background thread, not on the HTTP request thread
- The HTTP endpoint returns control immediately while the service works in the background

**This is superior to:**
- **HTTP endpoint timeout:** Would block the request thread for 5+ minutes (unscalable)
- **Message queue pattern:** Not needed for single-worker architecture; Named Pipe communication already provides reliable async messaging

**Recommendation for Phase 3D:** Continue using the existing `IHostedService` pattern with command queuing. The `DifnRecordParsingService` is already registered as scoped; it should be awaited within `NexusPipeServer`'s `ReceiveRecordsAsync` loop to process batches sequentially.

---

---

## 2026-05-15 — Race-Card DataSpec

**Q1: DataSpec for full weekly race cards**
- DataSpec = **`RACE`** with `Option=2` (今週データ / This Week's Data).
- Bypasses differential updates; delivers the complete current-week set of RA + SE records.

**Q2: What DIFN actually contains for RA/SE**
- `DIFN` (and legacy `DIFF`) master differential stream does **NOT** contain RA or SE records for central JRA races.
- It only provides RA/SE diffs for **local/regional dirt tracks (地方競馬 / NAR)** and **overseas international races**.
- To get any RA/SE data for central JRA, you must use `RACE`.

**Q3: rc=-1 from JVOpen("RACE", from_time, 2)**
- Means **該当データ無し** (no applicable data): local store is up-to-date, no new data since `from_time` for the current week's cycle.
- Also returned when a mandatory JV-Link software update is published and the user opts to download it instead.

**Q4: Canonical sequence for upcoming weekend cards**
1. `JVInit`, then `JVOpen` with **Option=2** and concatenated DataSpec **`"TOKURACESNPN"`** (TOKU + RACE + SNPN — special entries, race cards, snapshot).
2. If `DownloadCount > 0`, poll `JVStatus()` until downloaded files == `DownloadCount`.
3. Read loop with `JVRead` / `JVGets`:
   - `rc > 0`: identify record type from first 2 bytes; decode Shift-JIS; parse RA/SE.
   - `rc = -1`: file boundary; continue.
   - `rc = 0`: EOF; break.
4. Always `JVClose()`.
- **Timing nuance:** Thursday call → SE has `DataStatus=1` (nominations, no posts). Friday/Saturday call → `DataStatus=2` (final declarations, full posts).

---

## 2026-05-15 — Round 2 (JVStatus + DataStatus + from_time)

**Q5: JVStatus polling & premature JVRead**
- `JVStatus()` returns an integer = count of files downloaded so far.
- Completion condition: `JVStatus() == DownloadCount` (DownloadCount from initial JVOpen).
- Calling `JVRead` / `JVGets` before download completes **throws / fails** — must wait.

**Q6: SE fields at DataStatus=1 + the 1→2 transition**
- At DataStatus=1 (Thursday nominations / 出走馬名表), only **Wakuban (bracket)** and **Umaban (post)** are guaranteed blank/initial. Other fields populated.
- DataStatus 1→2 emits a **new record** into the stream (no deletion of the old). Must upsert using **Horse ID** as the conflict key, NOT post position — else duplicates.

**Q7: from_time semantics**
- For Option=1 or 2 (Normal/Weekly): acts as a strict **cursor**. Must pass the exact `lastfiletimestamp` returned from the previous JVOpen for gap-free syncing.
- For Option=3 or 4 (Setup): acts as a **filter** (e.g., "19910101000000" = everything from 1991 onward).
- `"00000000000000"` = fetch from the absolute beginning of the available archive for that DataSpec.

---

## JVRTOpen DataSpec Codes and Timing for Pre-Race Odds/Fav

**Q8 — JVRTOpen "0B11" record types:**
- `"0B11"` = **Flash Horse Weight** (速報馬体重). Emits: **WH** (Horse Weight), **WE** (Weather/Track Condition), **AV** (Scratches), **JC** (Jockey Changes), **TC** (Start Time Changes), **CC** (Course Changes).
- **Does NOT emit SE records.** Odds and FavRank fields are absent from this stream entirely.
- Correct DataSpec for real-time SE updates is **`"0B15"`** — flips to rc=0 on **Thursday after the entry horse name list (出走馬名表) is announced**.

**Q9 — JVRTOpen timing (rc=-114 → rc=0):**
- **`"0B15"` (real-time SE / race card):** rc=0 from **Thursday after entry confirmation** (出走馬名表発表後).
- **`"0B31"` (win odds / O1):** rc=0 **after betting tickets go on sale**. For PAT early sale races: **Friday 19:00 JST**. Otherwise standard Saturday/Sunday betting window opening.
- **Pre-race FAV/odds come exclusively from O1 records via `"0B31"`, not from SE records.** kmy-keiba confirms: Odds and FavRank (人気) are parsed from `JV_O1_ODDS_TANFUKUWAKU.Single.Ninki` and mapped to `RaceHorse.Popular`. SE record odds fields (byte 360/364) are populated only in *result-stage* SE records (post-race), not pre-race.

> ⚠️ **2026-05-15 BUG CONFIRMED:** rc=-114 does NOT mean "no data yet" — it means **invalid `key` parameter**. We are currently passing `raceDateYyyyMmDd` (8 chars, e.g. "20260516") as the key. This is the wrong format. The fix requires knowing the correct `key` format for `JVRTOpen("0B31", ...)`. See pending Q10 below.

---

## 2026-05-15 — JVRTOpen "0B31" key parameter format

**Q10: What is the exact format and valid values of the `key` (`bstrKey`) parameter for `JVRTOpen` when called with dataspec `"0B31"` (速報オッズ 単複枠 / real-time win+place odds, O1 records)?**
- Is `key` an empty string `""`?
- Is `key` a race date `"YYYYMMDD"` (8 chars)?
- Is `key` a full 16-char race ID `"YYYYMMDDJJKKHHRR"`?
- Some other format?
- We currently pass the 8-char race date and receive rc=-114 (invalid key). What must we pass instead?

**A:** *(Pending query)*

