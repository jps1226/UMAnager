# Librarian Answers — kmy-keiba Reference Implementation

*This file stores answers from the Librarian (access to kmy-keiba codebase). Each entry shows exact code patterns and implementation decisions.*

---

## JVGets Call Pattern

**Q:** Show me the exact C# code where kmy-keiba calls JVGets. Include buffer declaration, the call itself, and immediate post-call handling.

**A:**
```csharp
public int Gets(ref byte[] buff, int size, out string filename)
{
  object obj = buff;
  var r = this.link.NVGets(ref obj, size, out filename);
  buff = (byte[])obj;
  return r;
}
```

**Key Pattern:**
1. Pre-allocate as `byte[]`
2. Convert to `object` for COM call
3. Pass as `ref object` (in-out parameter)
4. Cast back from `object` to `byte[]` immediately after
5. Return result code

**✅ VERIFIED WORKING:** This exact pattern successfully reads 224K+ DIFN records via IDispatch.

---

## Critical Fix: CodePages Encoding Provider Registration

**Q:** Why does `Encoding.GetEncoding(932)` throw `TypeInitializationException` on the first call?

**A:** The CodePages encoding provider must be registered in the main thread before any code uses `Encoding.GetEncoding(932)`. Add this at the top of `Run()` in Program.cs:

```csharp
System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
```

Without this, any static field initialization that calls `Encoding.GetEncoding(932)` will fail with `TypeInitializationException` before .NET can catch it.

---

## Parsing Order Strategy for UM/RA/SE Records (Phase 3: DIFN Record Parsing)

**Q:** Does kmy-keiba parse UM/RA/SE records sequentially or in parallel? What is the database save order?

**A:** 

**Stream Parsing Order (JVLinkReader.Load):**
kmy-keiba reads records in mixed order from the JV-Link stream (RA, SE, UM can appear in any sequence). Parsing is controlled by a while loop that dispatches by record type:

```csharp
// KmyKeiba.JVLink/Wrappers/JVLinkReader.cs (L325-L352)
switch (spec)
{
  case "RA":
    var a = new JVData_Struct.JV_RA_RACE(); a.SetDataB(ref d);
    ReadDic(Race.FromJV(a), data.Races, item.Key);
    break;
  case "SE":
    var a = new JVData_Struct.JV_SE_RACE_UMA(); a.SetDataB(ref d);
    ReadDic(RaceHorse.FromJV(a), data.RaceHorses, item.RaceKey + item.Name);
    break;
  case "UM":
    var a = new JVData_Struct.JV_UM_UMA(); a.SetDataB(ref d);
    ReadDic(Horse.FromJV(a), data.Horses, item.Code + item.CentralFlag);
    break;
}
```

**Database Save Order (SaveDataAsync):**
Despite in-stream order, kmy-keiba saves to database in a **SPECIFIC SEQUENCE** within a transaction:

```csharp
// KmyKeiba.Downloader/JVLinkLoader.cs (L703-L719)
logger.Info($"RaceHorsesの保存を開始 {data.RaceHorses.Count}");
await SaveDicAsync(data.RaceHorses, db.RaceHorses!, ...);  // SE (RaceHorses) FIRST
await db.CommitAsync();

logger.Info($"Racesの保存を開始 {data.Races.Count}");
await SaveDicAsync(data.Races, db.Races!, ...);            // RA (Races) SECOND

logger.Info($"Horsesの保存を開始 {data.Horses.Count}");
await SaveDicAsync(data.Horses, db.Horses!, ...);          // UM (Horses) THIRD
```

**Key Insight:** kmy-keiba saves **SE → RA → UM** (reverse dependency order) because it uses **composite keys without strict database foreign keys**. It relies on application-level key matching (e.g., `item.RaceKey + item.Name`).

**⚠️ FOR UMANAGER:** With PostgreSQL **foreign key constraints**, we must follow **dependency order: UM → RA → SE** to avoid FK violations:
- UM (horses) has no dependencies → Save first
- RA (races) references no UM/SE → Save second
- SE (race_entries) references both UM and RA → Save last

**Sequentiality:** Both parsing and saving are strictly sequential (enforced by lock in `onReaded` handler). No parallelization.

---

## IJVLink Interface Definition in kmy-keiba

**Q:** What is the exact IJVLink interface definition that kmy-keiba uses? (InterfaceType, DispIds, parameter types)

**A:** *(Pending query)*

---

## BSTR Marshaling Pattern for Record Buffers

**Q:** After JVGets returns a BSTR buffer, how does kmy-keiba convert it to raw bytes? Show exact code.

**A:** *(Pending query)*

---

## Post-JVOpen Dialog Handling

**Q:** How does kmy-keiba detect and handle the JRA-VAN setup dialog that appears after JVOpen?

**A:** *(Pending query)*

---

## JVRead vs JVGets

**Q:** Does kmy-keiba use JVRead or JVGets for reading DIFN records? Why that choice?

**A:** *(Pending query)*

---

## InterfaceIsDual vs InterfaceIsIDispatch

**Q:** Why does kmy-keiba use InterfaceIsDual instead of IDispatch? Are there known issues with IDispatch?

**A:** *(Pending query)*

---

## Phase 4: Weekly Race Card Refresh — Scheduling, Timestamp Persistence & Record Update Logic

**Q:** Show exact C# code for (1) the weekly race card refresh orchestrator, (2) last-processed timestamp persistence, (3) new vs. old record detection logic.

**A (from DownloadScheduler.cs + JVLinkLoader.cs):**

**Scheduling — time-elapsed polling, NOT day-of-week:**
```csharp
// KmyKeiba\Models\Connection\DownloadScheduler.cs — UpdateDiffAsync
// Race plans refreshed every 4 hours:
if (now - this._lastUpdatedPlanOfRace >= TimeSpan.FromHours(4) || this._isUpdateRtHeavyForce)
{
    await this.DownloadPlanOfRacesAsync();
    this._lastUpdatedPlanOfRace = now;
    await ConfigUtil.SetStringValueAsync(SettingKey.LastDownloadPlanOfRaceDate, now.ToString());
}
// Previous results: every 8 hours. Real-time news: every 5 minutes.
```

**Timestamp persistence — SQLite key-value store (ConfigUtil/SystemData table):**
```csharp
// On startup: load saved timestamps
if (DateTime.TryParse(ConfigUtil.GetStringValue(SettingKey.LastDownloadPlanOfRaceDate), out var dt))
    this._lastUpdatedPlanOfRace = dt;

// After successful download: persist immediately
await ConfigUtil.SetStringValueAsync(SettingKey.LastDownloadPlanOfRaceDate, now.ToString());
```

**Record new vs. old detection — compare DataStatus + LastModified from the JRA-VAN record:**
```csharp
// KmyKeiba.Downloader\JVLinkLoader.cs — SaveAsyncPrivate<E,D,I>
if (item.Data.DataStatus == item.Entity.DataStatus)
{
    if (item.Data.LastModified <= item.Entity.LastModified)
        item.Data.SetEntity(item.Entity);  // no-op: same status, not newer
}
else if (item.Data.DataStatus < item.Entity.DataStatus)
    item.Data.SetEntity(item.Entity);  // downgrade: skip, keep existing
// DataStatus upgrade (e.g., Preliminary → Final): update record

// Records with no DB match treated as new → AddRangeAsync
```

**JVOpen dataspec for race plans — bitflags combining multiple specs:**
```csharp
// KmyKeiba.Downloader\Downloader.cs — LoadAsync
var dataspec = JVLinkDataspec.Race | JVLinkDataspec.Blod | JVLinkDataspec.Diff | JVLinkDataspec.Slop;
loader.StartLoad(link, dataspec, option, startTime: start, endTime: end, ...);
```

**Key Insights for UMAnager Phase 4:**
- kmy-keiba polls on an elapsed-time threshold (4 hours), not a calendar schedule
- Timestamp state stored as plain string in a key-value config table
- "Is this record new?" is determined by JRA-VAN's own `DataStatus` + `LastModified` fields within each RA/SE record — NOT by our own `IsProcessed` flag
- Two separate timestamps: `LastDownloadPlanOfRaceDate` (future races) and `LastDownloadPreviousRaceDate` (past results)
- DataSpec for race plans is a bitflag combination, resolving to something equivalent to TOKURACESNPN

---

## Large-Scale Bulk Insert Pattern in kmy-keiba (650K+ Records)

**Q:** How does kmy-keiba handle inserting 650K+ records into the database? What batch size does it use (per SaveChangesAsync/transaction)? Any memory optimization techniques?

**A:**

**Multi-Layered Batching Strategy:**
kmy-keiba uses a three-level chunking strategy to handle 650K+ records efficiently:

1. **Reader Chunk (20,000 entities):** JVLinkReader accumulates 20,000 entities before signaling save
2. **Processing Chunk (10,000 entities):** SaveAsync splits each 20K wave into 10,000-entity sub-chunks
3. **Database Batch (1,000 records):** SaveChangesAsync is called every 1,000 records (JVLinkLoader.cs:~640)

**Critical Pattern (Per kmy-keiba Code):**
```csharp
if (changed == 1000)
{
  await db.SaveChangesAsync();
  saved += changed;
  changed = 0;
}
```

**Transaction & Context Handling:**
- One transaction per 20,000-entity chunk (CommitAsync opens a new transaction after each major data type)
- DbContext is **replaced** after every 20,000 entities to completely clear EF Core's change tracker
- This is the primary defense against memory bloat during massive inserts

**Memory Optimization Techniques:**
1. **Context Refresh:** New DbContext per 20K chunk (prevents change tracker memory accumulation)
2. **Backpressure Mechanism:** Reader thread blocked via `Task.Delay(50).Wait()` if saver is busy (prevents unbounded RAM growth)
3. **Process Isolation:** Data loading in separate executable (KmyKeiba.Downloader.exe) allows OS memory reclamation on completion
4. **Manual GC.Collect():** Called after heavy file loads to ensure immediate reclamation
5. **SQLite Retry Loop:** SaveChangesAsync wrapped in retry logic to handle concurrent write contention

**Key Insight for UMAnager:**
- **Do NOT exceed 5,000 records per SaveChangesAsync** (1,000 is kmy-keiba's safe default)
- **Replace DbContext after major data type completions** (UM batch done → new context → RA batch)
- **Implement backpressure:** If parsing speed > database speed, throttle the parser to avoid RAM explosion
- **Monitor change tracker size:** If inserts stall, explicitly call `db.ChangeTracker.Clear()` between batches

---

---

## Pending — 2026-05-15 (Race-Card DataSpec)

**Format Required:** Paste exact C# code. Include 5 lines of context above and below each cited line. No paraphrasing.

**Q1: DataSpec used to populate weekly race cards**
Find the location(s) in kmy-keiba that load weekly race cards (RA + SE records) into the database for an upcoming or recent date range. Paste the exact `JVOpen` (or wrapper) call, including the DataSpec string, the option value, and how `from_time` is computed.

**Q2: DataSpec used for historical race archive backfill**
Find the location in kmy-keiba where a user can backfill historical races for a multi-month or multi-year range. Paste the exact call and any surrounding logic that determines DataSpec and option.

**Q3: Handling of `DIFN`**
Does kmy-keiba use `DIFN` anywhere? If yes, paste every call site and describe what record types it expects from that stream. If no, say so explicitly.

**Q4: rc=-1 handling**
Paste kmy-keiba's exact code path for handling `rc=-1` (or any negative rc) returned from `JVOpen`. Is it treated as "no new data" or "error"?

**Q5: from_time format and freshness**
For the weekly race-card load (Q1), what `from_time` value does kmy-keiba pass on a typical Friday or Saturday call? Paste the code that constructs it.

---

## 2026-05-15 — Race-Card DataSpec

**Q1: Weekly race-card load**
- Real-time: `JVRTOpen` with dataspec `0B11` (`JVLinkDataspec.RB11`), called per target date.
- Setup/historical bundle: `JVOpen` with `RACE` (`JVLinkDataspec.Race`), typically combined with `DIFN`, `BLDN`, `SLOP`.

**Q2: Historical backfill**
- `Downloader.cs::LoadAsync` calls `JVOpen` with:
  - DataSpec = joined string `"RACE" + "DIFN" + "BLDN" + "SLOP"` (+ `"WOOD" + "HOSN" + "MING"` for Central).
  - Option = `Setup` (1) if from_time is older than ~11 months; otherwise `Normal` (2).
  - FromTime = `yyyyMMddHHmmss`.

**Q3: DIFN usage**
- Yes — included in the dataspec bitmask for both Setup and Normal downloads:
  `var dataspec = JVLinkDataspec.Race | JVLinkDataspec.Blod | JVLinkDataspec.Diff | JVLinkDataspec.Slop;`

**Q4: rc=-1 handling**
- `JVOpen` / `JVRTOpen`: rc=-1 → `JVLinkLoadResult.Exit` ("no more data"); returns `EmptyJVLinkReader`; non-error.
- `JVRead` (Gets): rc=-1 → `JVLinkReadResult.NewFile` (file boundary); continue loop.
- If `JVRead` returns 0 immediately after -1: end of all data; break.

**Q5: from_time construction**
- Real-time: `yyyyMMdd`. On Fri/Sat, `DownloadScheduler` triggers updates for today, today+1, +2, +3.
- Normal `JVOpen`: full `yyyyMMddHHmmss`.
- `RTLoadAsync` includes Saturday's races on Friday only if current time > 12:00 PM.

---

## Pending — 2026-05-15 (Round 2: JVStatus polling + DataStatus semantics)

**Context for Librarian:** We use kmy-keiba as a behavioral reference. We've confirmed that `Downloader.cs::LoadAsync` calls `JVOpen` with the `RACE|DIFN|BLDN|SLOP` bundle. Now we need to see the exact code that handles the post-`JVOpen` download synchronization and the parse-time handling of incomplete SE records.

**Format Required:** Paste exact C# code with 5 lines of context before/after each cited line. No paraphrasing.

**Q6: JVStatus polling loop**
Show me kmy-keiba's exact code that runs between a successful `JVOpen` (with `DownloadCount > 0`) and the first `JVRead`/`JVGets` call. Specifically:
- What is the condition variable / return value being polled?
- What is the sleep interval inside the loop?
- Is `JVStatus` called directly, or is there a different sync mechanism (event, callback, fixed delay)?
- Paste the full method that owns this loop, plus the call site.

**Q7: SE parsing when bracket/post is blank (DataStatus=1)**
Show me where kmy-keiba parses SE records and how it handles the case where `Wakuban`, `Umaban`, or other fields are blank (DataStatus=1, Thursday nominations). Does it:
- Store the record with NULL/0 values, or
- Skip the record entirely, or
- Defer until DataStatus=2 arrives?
Paste the exact branching logic.

**Q8: from_time for option=2 weekly fetch**
In `LoadAsync` (or the equivalent), when Option=Normal (2) is chosen for a weekly RACE bundle pull, what literal value gets assembled into `from_time`? Show the exact computation — e.g., is it "last successful download timestamp," "start of current week 00:00:00," "current time minus N days," etc. Paste the code that constructs the string.

**Q9: DownloadCount=0 vs ReadCount=0**
After `JVOpen` returns, kmy-keiba presumably checks `DownloadCount` and `ReadCount`. Show me the exact branching that distinguishes:
- "Nothing to download but data to read" (cache hit),
- "Nothing to download and nothing to read" (truly empty),
- "Files to download, then read" (normal path).
Paste the conditional block.

## 2026-05-15 — Round 2 (JVStatus + DataStatus + from_time)

**Q6: JVStatus polling loop (kmy-keiba)**
- `Downloader.cs`: status loop sleeps **1600ms** (`Task.Delay(1600).Wait()`) and calls `UpdateProcess()` which reads `loader.Process` for current Downloaded/DownloadSize.
- `JVLinkReader.cs`: read loop is tight `while(true)` over `link.Gets()`. Handles rc=**-3** ("Downloading") by continuing the loop (no delay) — effectively yields to the COM pump.

**Q7: SE parsing with blank bracket/post**
- `RaceHorse.FromJV` in `KmyKeiba.Data/Entities/RaceHorse.cs` uses `short.TryParse(uma.Wakuban.Trim(), out short wakuNum)` and same for Umaban.
- Blank/whitespace → `TryParse` fails → silently defaults to **0**. No special branching, no deferral.

**Q8: from_time construction for Option=Normal**
- Format: `yyyyMMddHHmmss` (e.g., `20260515000000`).
- For race data: `new DateTime(startYear, startMonth, 1)` — first of the target month.

**Q9: DownloadCount vs ReadCount branching**
- Progress reporting: if `Process == Downloading`, use DownloadCount; else use ReadCount.
- Termination (`JVLinkReader.cs`):
  ```
  if (prevResult == -1 || this.ReadedCount + 1 >= this.ReadCount)
  {
      break;
  }
  ```
- **ReadCount is the authoritative completion metric** for the read phase.

---

## JVRTOpen "0B11" Real-Time Race Card Flow + Pre-Race FAV Source

**Q10 — RTLoadAsync:** kmy-keiba iterates an array of dataspecs (RB12, RB15, RB30, **RB11**, RB14, …) per race. For `RB11`, `raceKey` is passed as `null` and `from` is constructed as `yyyyMMdd`. Calls `JVLinkObject.StartRead` which calls `JVRTOpen(dataspecs_joined, yyyyMMdd)`.

**Q11 — O1 record handling in read loop:** The `"0B11"` stream emits `O1` records (not SE). kmy-keiba dispatches `case "O1"` → `JV_O1_ODDS_TANFUKUWAKU.SetDataB` → `SingleAndDoubleWinOdds.FromJV`. Parses `data.Single.Odds` and `data.Single.Ninki` (FavRank) into `OddsData { Odds, Popular }`.

**Q12 — Pre-race 人気 field trace:**
1. Parsed: `O1.Ninki` → `OddsData.Popular` in `SingleAndDoubleWinOdds.FromJV`
2. Mapped: `horse.Popular = o.Popular` in `JVLinkLoader.SaveDataAsync`
3. Persisted: `RaceHorseData.Popular` (short) written via `SetEntity`
4. Displayed: `Data.Popular` in `RaceHorsePillar.xaml`

**Key conclusion:** Pre-race FAV comes from O1 records via `JVRTOpen("0B31")`, available from **Friday 19:00 JST** (PAT early sale). SE record odds fields are result-stage only (post-race). Our `fetch-current-odds` implementation is correct — rc=-114 is expected until Friday evening JST.
