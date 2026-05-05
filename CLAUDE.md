# UMAnager v2.0 — Project Guide

## Project Context

**UMAnager v2.0** is a complete ground-up implementation of a Japanese thoroughbred racing analysis and auto-betting platform, built natively in **C#** to work with JRA-VAN's JV-Link interface.

**The North Star:** Initialize JV-Link → download and continuously sync horse master data and pedigrees → fetch weekly race entries and live odds → display race cards with pedigree and performance → enable manual and auto-generated bet placement → pipe bets into OreProPlus for automation.

**What this is:** A Windows-native C# service that manages all JRA-VAN communication, paired with an ASP.NET Core web API and single-file HTML frontend accessible on the local network.

**What this is NOT:** A Python/PowerShell hybrid. A multi-user SaaS. A replacement for OreProPlus (feeds *into* it).

---

## Tech Stack

| Layer | Technology | Rationale |
|---|---|---|
| JRA-VAN Interface | C# with JV-Link COM | Native support; matches official SDK paradigm |
| Background Service | Windows Service (.NET Core) | Proper Windows citizen; handles long-running data pulls |
| Web Server | ASP.NET Core 7+ | Seamless C# integration; RESTful API |
| Database | PostgreSQL 14+ or SQL Server | Proper relational model; handles incremental updates cleanly |
| Frontend | Single-file HTML + vanilla JS | No build step; minimal dependencies; proven durable |
| Testing | xUnit | Industry standard for C#; parallel test execution |
| Time Management | Quartz.NET or Windows Task Scheduler | Reliable scheduling for data fetches |

**Why C#?** JRA-VAN's official SDK is built for C#, VB.NET, C++, and Delphi. C# has seamless COM interop, first-class async support for background tasks, and a mature ecosystem. Python's win32com works but is unconventional for this domain.

**Why PostgreSQL over SQLite?** SQLite is fine for embedded single-user apps, but JRA-VAN data is relational and schema-heavy. PostgreSQL handles concurrent reads, proper transactions, and schema migrations without friction.

---

## Architecture

### Three Decoupled Systems

```
┌──────────────────────────────────────┐
│    Web Layer (ASP.NET Core)          │
│  - RESTful API (/api/races, /api/horses, /api/bets)
│  - Serve index.html (frontend)       │
│  - READ-ONLY to database             │
│  - Accessible on local network       │
└──────────────────────────────────────┘
           ↓ (HTTP queries)
┌──────────────────────────────────────┐
│   Database (PostgreSQL/SQL Server)   │
│  - Single source of truth            │
│  - Horse master + pedigree archive   │
│  - Weekly race entries + odds        │
│  - Historical results                │
│  - User-saved bets                   │
└──────────────────────────────────────┘
           ↑ (WRITE-ONLY)
┌──────────────────────────────────────┐
│ Ingestion Service (Windows Service)  │
│  - JV-Link initialization & auth     │
│  - Data fetching (UM, DIFN, TOKURACESNPN)
│  - Parsing fixed-width records       │
│  - Timestamp-based incremental sync  │
│  - Real-time event listeners (FK, JVWatchEvent)
│  - Scheduled runs (Thu/Fri/Sat)      │
└──────────────────────────────────────┘
```

**Critical rule:** Only the ingestion service writes to the database. The web API is read-only. This eliminates race conditions and keeps concerns clean.

---

## The Golden Path (Thursday Evening to Saturday Night)

### Thursday Evening: Master Data & Weekly Setup
1. **Ingestion Service Startup**
   - Authenticate with JRA-VAN using service key
   - Load last-saved `LastFileTimestamp` from database config table
   - If timestamp is very old (> 2 weeks), perform a fresh bootstrap

2. **Bootstrap (If Needed): Full Horse Master**
   - `JVOpen("UM", Option=4, FromTime=baseline)`
   - Parse all UM records (horse ID, name, birth year, sire, dam, broodmare sire)
   - Decode Shift-JIS hex-encoded names to readable Japanese/romanized text
   - INSERT/UPDATE `horses` table
   - Expected result: 5,800+ horses with 3-generation pedigree complete

3. **Fetch This Week's Races**
   - `JVOpen("TOKURACESNPN", Option=2, FromTime=saved_timestamp)`
   - Parse RA (race), SE (entry), CK (horse pedigree) records
   - INSERT `races` table with metadata (track, date, distance, conditions, grade)
   - INSERT `race_entries` table with each horse in each race
   - UPDATE `horses` table with any new pedigree data from CK records
   - Save new `LastFileTimestamp`
   - Expected result: All weekend race cards visible in web UI with pedigree populated

### Friday: Analysis & Betting Decisions
1. **User opens web UI** → race cards load from `/api/races/list`
2. **Browse races** → for each race, `/api/races/{race_id}` returns:
   - Race metadata (distance, conditions, grade)
   - All entries with horse names, romanized names, sire/dam/bms (with horse names, not IDs)
   - Current odds (fetched live or cached from previous pull)
   - User's historical tracking if applicable
3. **User makes bets** → POST to `/api/bets/save` with selected horses and bet type
4. **Export bets** → GET `/api/bets/export` returns machine-readable format for OreProPlus

### Saturday Evening: Live Results
1. **Ingestion Service Listener Activated**
   - `JVWatchEvent` or polling-based fetch for FK (finish) records
   - As races complete, INSERT finish position, time, payoffs into `race_results`
2. **Web UI updates live** via WebSocket or polling
3. **TV Mode** displays race info + live results + user's bet slip side-by-side

---

## Database Schema

### Core Tables

**`horses`** — Master pedigree archive
```sql
CREATE TABLE horses (
    horse_id VARCHAR(10) PRIMARY KEY,
    horse_name_japanese NVARCHAR(MAX),
    horse_name_romaji VARCHAR(255),
    birth_year INT,
    sire_id VARCHAR(10) REFERENCES horses(horse_id),
    dam_id VARCHAR(10) REFERENCES horses(horse_id),
    broodmare_sire_id VARCHAR(10) REFERENCES horses(horse_id),
    last_updated TIMESTAMP,
    data_source VARCHAR(20) -- 'UM' or 'CK'
);
```

**`races`** — Race metadata
```sql
CREATE TABLE races (
    race_id VARCHAR(16) PRIMARY KEY,  -- YYYYMMDDPPNNNNNN
    race_date DATE,
    track_code VARCHAR(10),
    race_number INT,
    race_name_japanese NVARCHAR(MAX),
    distance INT,                     -- meters
    surface VARCHAR(10),              -- 'turf' or 'dirt'
    grade VARCHAR(10),                -- 'G1', 'G2', 'listed', 'open', etc.
    race_conditions NVARCHAR(MAX),    -- age/sex restrictions
    last_updated TIMESTAMP
);
```

**`race_entries`** — Who runs in each race
```sql
CREATE TABLE race_entries (
    id INT PRIMARY KEY IDENTITY,
    race_id VARCHAR(16) REFERENCES races(race_id),
    horse_id VARCHAR(10) REFERENCES horses(horse_id),
    post_position INT,
    frame_number INT,
    horse_weight INT,
    jockey_name NVARCHAR(MAX),
    trainer_name NVARCHAR(MAX),
    morning_line_odds DECIMAL(10, 2),
    latest_odds DECIMAL(10, 2),
    finish_position INT,
    finish_time_hundredths INT,       -- time * 100 (in 1/100 seconds)
    payoff_win DECIMAL(10, 2),
    payoff_place DECIMAL(10, 2),
    payoff_show DECIMAL(10, 2),
    updated_at TIMESTAMP
);
```

**`sync_state`** — Track JV-Link synchronization
```sql
CREATE TABLE sync_state (
    id INT PRIMARY KEY,
    last_timestamp_um BIGINT,         -- LastFileTimestamp from UM fetch
    last_timestamp_races BIGINT,      -- LastFileTimestamp from TOKURACESNPN
    last_sync_at TIMESTAMP,
    last_error VARCHAR(MAX),
    sync_count INT
);
```

**`bets_saved`** — User's saved betting slips
```sql
CREATE TABLE bets_saved (
    id INT PRIMARY KEY IDENTITY,
    race_id VARCHAR(16) REFERENCES races(race_id),
    bet_type VARCHAR(50),             -- 'win', 'exacta', 'trifecta', etc.
    horses_json NVARCHAR(MAX),        -- JSON array of horse IDs
    odds_json NVARCHAR(MAX),          -- JSON of odds at save time
    created_at TIMESTAMP,
    exported_at TIMESTAMP NULL
);
```

---

## Development Phases

### Phase 1: Foundation (Week 1)
- [ ] Create Visual Studio solution with 3 projects: `Ingestion.Service`, `WebApi`, `Common`
- [ ] Add JV-Link COM reference to Ingestion project
- [ ] Create PostgreSQL database and schema
- [ ] Implement JV-Link initialization and basic error handling (follow SDK sample)
- [ ] Write timestamp persistence (sync_state table)
- [ ] Setup logging infrastructure (Serilog to file)
- [ ] Create test data loader from SDK sample datasets

### Phase 2: Master Data Bootstrap (Week 2)
- [ ] Implement `JVOpen("DIFN", Option=4)` call (Note: shows dialog on FIRST EXECUTION ONLY; click "Download all"; takes several minutes)
- [ ] Use `JVGets(out byte[] buff)` to receive raw bytes (NOT `JVRead`, which corrupts Shift-JIS via COM)
- [ ] Parse UM fixed-width record format per JRA-VAN specification
- [ ] Decode Shift-JIS: `System.Text.Encoding.GetEncoding("shift_jis").GetString(buffer)`
- [ ] INSERT into horses table with full 3-gen pedigree
- [ ] Validation: Assert all horses have sire + dam + bms (no NULLs)
- [ ] Unit tests for Shift-JIS decoding
- [ ] Manual test: Download full UM data, verify 5,800+ horses
- [ ] Error handling: Catch -502 (Download Failure), implement exponential backoff retry

### Phase 3: Weekly Data Fetch (Week 3)
- [ ] Implement `JVOpen("TOKURACETCOVSNPN", Option=2)` with FromTime logic (TCOV bundles UM pedigree for new entries)
- [ ] Use `JVGets(out byte[] buff)` for safe Shift-JIS handling
- [ ] Parse RA (race) records → INSERT races table
- [ ] Parse SE (entry) records → INSERT race_entries table
- [ ] Parse UM (horse master) records from TCOV stream → UPDATE horses table with debuts + pedigree
- [ ] DO NOT parse CK (CK = placement statistics, NOT pedigree)
- [ ] Timestamp update logic after successful parse
- [ ] Error handling: Catch -502, implement retry with backoff
- [ ] Integration test: Full weekend cycle (Thu pull → verify Saturday races visible)

### Phase 4: Web API (Week 4)
- [ ] ASP.NET Core project scaffold
- [ ] `/api/races/list?date=YYYY-MM-DD` — all races for a date
- [ ] `/api/races/{race_id}` — full race card with pedigree
- [ ] `/api/horses/{horse_id}` — horse profile + sire/dam/bms names
- [ ] `/api/bets/save` — POST to save a bet slip
- [ ] `/api/bets/export` — GET export for OreProPlus
- [ ] Authentication (localhost check)

### Phase 5: Live Events & Results (Week 5)
- [ ] Implement `JVWatchEvent` listener for **JVEvtPay** (Payoff event, NOT FK — FK does not exist in JRA-VAN)
- [ ] When JVEvtPay fires, extract race key from event
- [ ] Call `JVRTOpen(race_key, "0B12")` (Fast Race Info spec)
- [ ] Parse "0B12" stream: RA (race meta), SE (finishes/times), HR (payoff data)
- [ ] Use `JVGets()` to receive raw bytes, decode Shift-JIS safely
- [ ] UPDATE race_entries with finish_position, finish_time_hundredths, payoff_win/place/show
- [ ] WebSocket endpoint for live UI updates
- [ ] TV Mode view (race info + live results + bet slip)

### Phase 6: Testing & Deployment (Week 6)
- [ ] End-to-end test: bootstrap → weekly fetch → live results
- [ ] Performance: Can API handle reasonable query load?
- [ ] Error handling: What happens if JV-Link is unavailable? Network timeout? Corrupted record?
- [ ] Windows Service installer
- [ ] Deployment checklist
- [ ] Documentation of any JRA-VAN quirks discovered during implementation

---

## Critical Implementation Notes

### Timestamp Management
**This is foundational. Get it right first.**
- Save `LastFileTimestamp` returned by every `JVRead` call
- Use it as `FromTime` in the next call to avoid gaps or duplicates
- Store in `sync_state` table, update after *successful* parse completion
- If a parse fails, do NOT update the timestamp (next run will retry)

### Shift-JIS Decoding (CORRECTED)
**Critical:** Do NOT use `JVRead` for string data; it corrupts CP932 bytes via COM UTF-16 marshalling.

**The Solution:** Use `JVGets(out byte[] buff, ...)` to receive raw bytes directly, then decode safely:
```csharp
Encoding.GetEncoding("shift_jis").GetString(buffer)
```

This completely bypasses COM string corruption. No hex decoder needed.

**Why this matters:** Horse names, sire/dam names are all CP932-encoded. Corrupted names render as gibberish and break pedigree matching.

### Real-Time Events (CORRECTED)
**For TV Mode live updates:**
- Register `JVWatchEvent` listener for **JVEvtPay** event (Payoff/Results notification)
- When JVEvtPay fires, extract race_key from event
- Call `JVRTOpen(race_key, "0B12")` to fetch Fast Race Info stream
- Parse RA (race), SE (finish order/times), HR (payoff amounts) from "0B12"
- **NOT FK:** There is no "FK" record type in JRA-VAN. Do not implement FK polling.
- SE records in "0B12" are ordered by finish position; HR records contain win/place/show payoffs

### Incremental Updates
**The database is not rebuilt each week; it's updated.**
- If a horse appears in this week's races but not in the master table, INSERT a pedigree stub
- If we fetch CK pedigree for a horse we already have, UPDATE (don't INSERT duplicate)
- Check `last_updated` timestamp to detect stale data

---

## Rules — Never Do

1. **Never hard-code JRA-VAN parameters.** Timestamps, service keys, track codes should all be config or database values.
2. **Never skip validation.** If you parsed a horse_id and it's not in the horses table, flag it (log + alert); don't silently ignore.
3. **Never use `JVRead` for string/byte data.** Always use `JVGets(out byte[] buff)` to avoid CP932 corruption via COM.
4. **Never parse CK records as pedigree.** CK = placement statistics, NOT pedigree. Pedigree comes from UM records bundled in TCOV/RCOV.
5. **Never assume record order.** Parse defensively; a horse's UM record might arrive after its SE record in a single fetch.
6. **Never lose the last timestamp.** If the sync_state table is corrupted, you'll have to re-bootstrap.
7. **Never deploy without testing against real JRA-VAN data.** Sample data is good; real data is essential.
8. **Never ignore -502 (Download Failure) errors.** Implement exponential backoff retry (30s, 60s, 120s) for transient network issues.
9. **Never ignore errors in parsing.** Log the raw record, the error, and the horse/race ID. This is your debugging lifeline.

---

## Rules — Always Do

1. **Log everything.** Every JVOpen, every JVGets, every INSERT/UPDATE. Include record counts, timestamps, errors, retry attempts. Logging is your only debugging tool when running as a Windows Service.
2. **Implement -502 retry logic.** When JVGets returns -502 (Download Failure), close connection, wait, and retry with exponential backoff (30s → 60s → 120s).
3. **Use `JVGets(out byte[] buff)` for all data.** Never use `JVRead` for string/byte content. Always decode received bytes as CP932 (shift_jis).
4. **Test early with real data.** By end of Phase 2, download an actual UM bootstrap from your JRA-VAN account and parse it. Confirm the horses table has 5,800+ rows with complete pedigrees.
5. **Use the JRA-VAN DataLab tool.** The official verification tool lets you manually inspect downloaded records before writing code. Use it.
6. **Document the JRA-VAN spec mapping.** For each record type (RA, SE, UM, CK, HR, KS, CH), document the byte offsets and formats you use. This is your specification.
7. **Version your database schema.** Create a `schema_version` table. Track which version of the JRA-VAN SDK was used for each data pull.
8. **Implement graceful degradation.** If JV-Link is unavailable, the service should log, retry, and eventually notify. The web API should serve cached data.
9. **Test the sync logic.** Manually break the timestamp, force a re-fetch, verify no duplicates and no missing data.
10. **Account for Option=4 dialog on first run.** The initial bootstrap will prompt the user to choose "Download all"; after that, runs are silent.

---

## Success Criteria

The project is **complete** when:

1. **Pedigree Completeness:** Every horse in an upcoming race has Sire, Dam, and Broodmare Sire (no NULLs). Coverage >= 99%.
2. **Weekly Automation:** From Thursday evening to Saturday, the system fetches and displays race data without user intervention.
3. **Live Results:** By Saturday evening, finish positions and payoffs appear in the UI as races complete.
4. **Bet Export:** User can save a betting slip and export it in OreProPlus-compatible format.
5. **Code Quality:**
   - Every JRA-VAN interaction traceable to official SDK docs or sample code
   - Comprehensive logging (diagnose any production issue without a debugger)
   - Proper error handling (timeouts, corrupted data, network failures)
   - No magic strings; all constants sourced from official documentation

---

## Questions Before You Start Coding

1. Have you reviewed the JRA-VAN SDK documentation for the data type you're about to implement?
2. Does the official sample program do what you're trying to do?
3. If not, have you found the specific byte-offset specification in the manual?
4. Have you tested the parsing logic against real downloaded data, not just the example?
5. Is the timestamp management logic correct? Will it detect duplicates? Gaps?

If you can't answer any of these with confidence, research before coding.

---

## Deployment

### Windows Service Setup
1. Build release build
2. Create Windows Service installer (use SC or NSSM)
3. Point service to .exe output
4. Configure to run under service account (or LocalSystem for testing)
5. Set to auto-start on boot
6. Verify logs are written (e.g., to `C:\Logs\UMAnager\`)

### Web API Setup
1. Publish ASP.NET Core app to folder (self-contained preferred)
2. Host via IIS (create app pool, add application) OR run Kestrel as a service
3. Ensure API is accessible at `http://localhost:5000` (or configure appropriately)
4. Configure CORS if accessing from different machine on network

### Database Setup
1. Create PostgreSQL database
2. Run schema migration scripts
3. Verify `sync_state` table is initialized
4. Backup database file before first production run

---

## Integration with OreProPlus

The `/api/bets/export` endpoint returns a JSON structure compatible with OreProPlus:
```json
{
  "races": [
    {
      "race_id": "202605040101",
      "bets": [
        { "type": "win", "horses": [1] },
        { "type": "exacta", "horses": [1, 2] }
      ]
    }
  ]
}
```

OreProPlus consumes this and automates clicking on the JRA website. This is output only; UMAnager does not control OreProPlus.

---

## Go Build It

You have the spec. You have the official SDK. You have a clear path to Phase 1.

Start with foundation. Get timestamps right. Understand Shift-JIS. Test early with real data.

The rest follows.
