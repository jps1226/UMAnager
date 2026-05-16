# UMAnager v2.0 — Project Guide (The Master Specification)

## Project Context

**UMAnager v2.0** is a ground-up, clean-room implementation of a Japanese thoroughbred racing analysis and betting platform. It is designed to overcome the 32-bit limitations of JRA-VAN's legacy COM interface while providing a high-performance, modern web-based "War Room" for racing operations.

**The Philosophy:** 
- **Clean Room**: No external library imports. `kmy-keiba` and `UMAnager2` are used strictly as behavioral and byte-offset references.
- **UI-First / API-First**: The UI and the `BACKEND_API_SPEC.md` contract drive the development.
- **Vertical Slices**: Each phase delivers a functional result you can see and interact with.

---

## Split Process Architecture

The system utilizes a **Split Process Architecture** to isolate the legacy 32-bit COM requirements from the 64-bit performance backend.

### 1. The Nexus (x64) — The Brain
- **Technology**: ASP.NET Core 8/9 (.NET x64)
- **Responsibility**: Implements HTTP API, high-performance parsing, PostgreSQL persistence, and SignalR.
- **Frontend Hosting**: Serves the provided `index.html`, `tv.html`, `style.css`, and `script.js` as static assets.

### 2. The Sidecar (x86) — The Bridge
- **Technology**: .NET 8/9 Console (x86)
- **Responsibility**: Hosts `JVDTLabLib` COM on an **STA Thread** and streams raw data.

### 3. IPC Protocol (Named Pipes)
The Nexus and Sidecar communicate via a Named Pipe using a binary message envelope:
- **[4 bytes]**: Payload Length (Little Endian)
- **[2 bytes]**: Message Type Code (e.g., `0x01`=Command, `0x02`=Raw Record, `0x03`=Status)
- **[N bytes]**: Raw JRA-VAN Data or JSON Command Payload

---

## The UI Goal (UMAnager2 Assets)

The project will carry over the existing frontend assets from `UMAnager2`:
- **Dashboard (`index.html`)**: Features a dual-timeline race calendar (Upcoming/Past), race card expansion, and advanced diagnostic tools.
- **TV Mode (`tv.html`)**: A split-panel view for live monitoring with GreenChannel video integration.
- **Engine (`script.js`)**: A ~6000 line vanilla JS engine that handles complex local sorting, horse highlighting, auto-pick scoring, and SignalR integration.
- **Styling (`style.css`)**: A comprehensive dark-mode CSS theme (~1600 lines) with specific intensity-based highlighting for pedigree tracking.

The Nexus must implement every endpoint in `BACKEND_API_SPEC.md` to satisfy this existing UI's requirements without modifying the JS/HTML files.

---

## Timezone — Critical Context

**JRA races run on Saturday and Sunday in Japan (JST, UTC+9).** The operator (you) is in America (Eastern Time, UTC-5 / UTC-4 DST), which puts the live betting windows on **Friday night and Saturday night local time**.

- "Saturday's races" = watched **Friday evening / overnight** in the US.
- "Sunday's races" = watched **Saturday evening / overnight** in the US.
- All `race_date` values in the DB are **JST calendar dates**. Never offset them to local time — the UI displays JST and the operator mentally maps it.
- When the user says "tonight's races," they mean the upcoming JST race day that begins in a few hours their local time.

Past sessions have confused this and assumed the user was watching mid-week. Don't.

---

## Application Phase State Machine

The system operates in one of three phases, driven by data state (not the calendar). The current phase is stored in `app_state` under key `app_phase` and surfaced in the UI header.

| Phase | JST Window | US Window | Trigger to enter | Behavior |
|:---|:---|:---|:---|:---|
| **WAITING_FOR_RACES** | Sun night → Thu | Sun → Thu | No race rows exist with `race_date >= today` | Poll TOKURACESNPN per `populate_poll_interval` (default 1h). Fires Discord webhook on first successful populate. |
| **RACES_POPULATED** | Thu → Fri evening | Thu → Fri afternoon | Race plan ingested; no race within `live_window_minutes` of post | Refresh odds per `odds_poll_interval_prelive` (default 1h, configurable). |
| **LIVE_OPERATIONS** | Fri night → Sun | Fri night → Sun | Any race within `live_window_minutes` (default 90) of post time, OR FK records actively streaming | Refresh odds per `odds_poll_interval_live` (default 5m, min 5m — JV-Link rate limit). SignalR active. Discord webhook on bet-card win. |

After Sunday's final race finishes and results settle, return to `WAITING_FOR_RACES`.

**Settings rows** (table `app_settings`, key/value):
- `populate_poll_interval` — how often to check for new race plans (default `01:00:00`)
- `odds_poll_interval_prelive` — Thu/Fri odds refresh (default `01:00:00`)
- `odds_poll_interval_live` — Fri night→Sun odds refresh (default `00:05:00`, **hard floor 5m**)
- `live_window_minutes` — minutes before post to enter LIVE phase (default `90`, mirrors kmy-keiba's RB41 gate)
- `discord_webhook_url` — nullable; webhook for phase-change and bet-win notifications

---

## The Golden Path (The Thursday-Sunday Lifecycle)

### Thursday Evening: Master Data & Weekly Setup
1. **Nexus** commands **Sidecar** to `JVOpen("UM", 4)`.
2. Sidecar streams raw UM records to Nexus.
3. Nexus parses and updates `horses` table (Horse ID, Names, Pedigree).
4. Nexus commands **Sidecar** to `JVOpen("TOKURACESNPN", 2)`.
5. Nexus parses `RA` (Race) and `SE` (Entry) records into the database.
6. **Result**: Weekend race cards are visible in the Web UI.

### Friday: Analysis & Strategy
1. User opens Web UI; races load from `/api/races`.
2. User applies marks (◎〇▲△X) saved via `POST /api/marks`.
3. Auto-pick engine (in `script.js`) calculates scores based on formula weights.

### Saturday/Sunday: Live Operations
1. Sidecar enters `JVWatchEvent` loop.
2. Real-time `FK` (Finish) and `O1-O6` (Odds) records stream to Nexus.
3. Nexus updates DB and broadcasts via SignalR.
4. User exports final bets to OreProPlus via `/api/orepro/votes/apply`.

---

## Database Schema (PostgreSQL)

### `horses` — Master Pedigree
```sql
CREATE TABLE horses (
    horse_id VARCHAR(10) PRIMARY KEY, -- JRA Horse ID
    name_ja TEXT NOT NULL,           -- Decoded Shift-JIS
    name_en TEXT,                    -- Romanized
    birth_year INT,
    sire_id VARCHAR(10) REFERENCES horses(horse_id),
    dam_id VARCHAR(10) REFERENCES horses(horse_id),
    bms_id VARCHAR(10) REFERENCES horses(horse_id),
    pedigree_json JSONB,             -- Cached recursive pedigree tree
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### `races` — Race Metadata
```sql
CREATE TABLE races (
    race_id VARCHAR(16) PRIMARY KEY, -- YYYYMMDDPPNNNNNN
    race_date DATE NOT NULL,
    track_code VARCHAR(10),
    race_number INT,
    name_ja TEXT,
    distance INT,
    surface VARCHAR(10),             -- 'turf' / 'dirt'
    sort_time TIMESTAMP,             -- Combined Date + Time
    results_json JSONB,              -- Payoffs and finish positions
    history_refreshed BOOLEAN DEFAULT FALSE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### `race_entries` — Competitors
```sql
CREATE TABLE race_entries (
    id SERIAL PRIMARY KEY,
    race_id VARCHAR(16) REFERENCES races(race_id),
    horse_id VARCHAR(10) REFERENCES horses(horse_id),
    post_position INT,
    bracket INT,
    weight INT,
    jockey_name TEXT,
    odds DECIMAL(10, 2),
    fav_rank INT,
    finish_pos INT,
    performance_json JSONB,          -- Finish time, margins, etc.
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Development Phases

### Phase 0: The Connection Doctor (Connectivity Proof)
- [ ] Create a minimal x86 console app.
- [ ] Implement `IJVLink` wrapper using **IDispatch late binding** (see Technical Directives).
- [ ] Call `JVInit` and verify connection.
- [ ] **Vertical Slice**: Console prints "JV-Link Connection Successful: Version X.XX".

### Phase 1: The Shell (UI & API Foundation)
- [ ] Initialize Nexus (x64) project and PostgreSQL.
- [ ] Host existing `index.html`, `style.css`, and `script.js` from `UMAnager2`.
- [ ] Implement all endpoints in `BACKEND_API_SPEC.md` with **Static Mock JSON**.
- [ ] **Vertical Slice**: Open the Dashboard and see "Dummy Horse" data populated in the existing UI tables.

### Phase 2: The Diagnostic Bridge (Sidecar & IPC)
- [ ] Implement the **Named Pipe** envelope protocol between Nexus and Sidecar.
- [ ] Bridge `/api/jvlink/status` to the Sidecar's `JVStatus()` call.
- [ ] **Vertical Slice**: The UI header "Green Light" turns on, showing the real JV-Link version.

### Phase 3: The Master Data Ingestor (UM Stream)
- [ ] Implement raw record streaming from Sidecar to Nexus via Pipe.
- [ ] Implement **Span-based parser** in Nexus for `UM` records.
- [ ] Populate `horses` table with real JRA data (Shift-JIS decoded).
- [ ] **Vertical Slice**: Dashboard's Search bar works with real Japanese horse names.

### Phase 4: The Race Day Engine (Weekly Automation)
- [ ] Implement `RA` (Race) and `SE` (Entry) record parsing in Nexus.
- [ ] Implement `LastFileTimestamp` state persistence in PostgreSQL.
- [ ] **Vertical Slice**: Calendar sidebar populates with real dates; racecards show real entries.

### Phase 5: The War Room (Live SignalR Pipeline)
- [ ] Sidecar: Implement `JVWatchEvent` listener and real-time push.
- [ ] Nexus: Implement **SignalR Hub** to broadcast `FK` (Finish) and `O1` (Odds) records.
- [ ] **Vertical Slice**: Dashboard updates live without a page refresh.

### Phase 6: The Conductor (Phase State Machine + Discord)
- [ ] Create `app_settings` table and seed defaults from the Settings table above.
- [ ] Implement a background `PhaseOrchestrator` service that:
  - Reads/writes `app_phase` in `app_state`.
  - Schedules ingest jobs per the current phase's interval setting.
  - Enforces the **5-minute hard floor** on live odds polling (matches kmy-keiba and JV-Link rate limits).
  - Applies kmy-keiba's gate: only pull timeline odds (RB41-equivalent) for races within `live_window_minutes` of post.
- [ ] Implement `IDiscordNotifier` with two events: `RacePlanPopulated` and `BetCardWon`.
- [ ] Settings UI panel (small, in existing dashboard) to edit intervals and webhook URL.
- [ ] **Vertical Slice**: UI header shows current phase; phase flips at the right moments; populate notification fires once per weekend in Discord.

### Phase 7: Recent Form (Last-3 Finish Strip)
- [ ] Add SQL view or computed column for each horse's last-3 finish positions, ordered most-recent-last (display format `4-1-0`; dashes for fewer-than-3 starts).
- [ ] Surface in race-card UI as a new column.
- [ ] Add **recency-weighted form score** to the auto-pick formula: `Σ wᵢ · f(finishᵢ)` where weights are `[0.5, 0.3, 0.2]` (most recent first) and `f(pos) = 1/pos` for top-5, else 0.
- [ ] **Vertical Slice**: Form column populated; auto-pick scores shift measurably when form weight is toggled in settings.

### Phase 8: Jockey & Trainer Masters (KS / CH Ingest)
- [ ] Implement Sidecar streaming for `KS` (jockey master) and `CH` (trainer master) DataSpecs.
- [ ] Implement Nexus span-parsers; persist Japanese names (Shift-JIS) keyed by JRA ID. **Do not translate.**
- [ ] Compute trailing-window win% and place% per jockey and per trainer (rolling 90-day, refreshed nightly).
- [ ] Add jockey/trainer strike-rate columns to race-card UI.
- [ ] Feed both rates into auto-pick scoring as additional weighted factors.
- [ ] **Vertical Slice**: Race cards show jockey/trainer Japanese names alongside Win%/Place% numbers.

### Phase 9: Pedigree Sire-Performance Score
- [ ] Build a `sire_performance` materialized view: for each `sire_id × surface × distance_bucket`, compute progeny starts / wins / win%.
- [ ] Distance buckets: Sprint (≤1400), Mile (1401–1800), Middle (1801–2200), Long (≥2201).
- [ ] For each runner, surface the sire's win% on the current race's bucket as `sire_fit_score`.
- [ ] Add to auto-pick formula as a low-weight tiebreaker (pedigree predicts less than form on a per-race basis).
- [ ] **Future / optional**: nicking flags (e.g. Sunday Silence 4x4) — defer until #1 is validated.
- [ ] **Vertical Slice**: Race cards show "Sire Fit: 18%" for each runner; sorting by it surfaces pedigree standouts.

---

## Technical Directives

### 1. COM Interop Strategy
The `IJVLink` interface **must** use `InterfaceIsIDispatch` to prevent Vtable access violations.
```csharp
[ComImport]
[Guid("2AB1774C-0C41-11D7-916F-0003479BEB3F")]
[InterfaceType(ComInterfaceType.InterfaceIsIDispatch)]
internal interface IJVLink {
    [DispId(4)] int JVInit(string sid);
    [DispId(7)] int JVOpen(string spec, string from, int opt, ref int rc, ref int dc, out string ts);
    // ... rest of interface
}
```

### 2. High-Performance Parsing (Nexus)
- **Zero-Allocation**: Use `ReadOnlySpan<byte>` for all field extractions.
- **Encoding**: Register `CodePagesEncodingProvider` and use `Encoding.GetEncoding(932)` (Shift-JIS).

### 3. Database Persistence
- **Migrations**: Use **EF Core Fluent API Migrations** for PostgreSQL.
- **JSONB**: Use for high-performance indexing of variable-depth results and pedigree.

---

## Critical Reference: Byte Offsets (1-Indexed)

| Record | Field | Start | Len | Notes |
|:---|:---|:---|:---|:---|
| **RA** | Race Name | 33 | 60 | Shift-JIS (Hondai) |
| **RA** | Start Time | 874 | 4 | HHMM format |
| **SE** | Horse ID | 31 | 10 | KettoNum |
| **SE** | Win Odds | 360 | 4 | Divide by 10 |
| **SE** | FavRank  | 364 | 2 | 1=favourite |
| **UM** | Horse Name | 119 | 60 | Romanized/English |
| **UM** | Sire ID | 205 | 10 | First pedigree slot |
| **UM** | Dam ID | 251 | 10 | Second pedigree slot |

---

## Rules — Always Do
1. **Log Raw Hex**: On any parsing error, log the raw hex string of the record.
2. **STA Threading**: All COM interactions in the Sidecar must occur on the same STA thread.
3. **Commit before Timestamp**: Never update `LastFileTimestamp` until the DB commit is successful.

## Rules — Never Do
1. **No Project Imports**: Do not import DLLs or projects from `kmy-keiba` or `UMAnager2`.
2. **No x64 COM Calls**: Never attempt to instantiate JV-Link in the Nexus process.
3. **No String Substrings**: Avoid `string.Substring()` during record parsing; use `Span<T>.Slice()`.

My Collaborative Ecosystem
When you (Claude) encounter a knowledge gap regarding JRA-VAN rules or legacy logic patterns, do not guess. Instead, ask me to query one of the following "Specialists" available in my environment:

The Oracle (NotebookLM): This is the source of truth for official documentation. It contains the JRA-VAN JV-Link PDFs, the BACKEND_API_SPEC.md, and technical manuals. Ask me to query the Oracle for byte offsets, record types, or official JRA-VAN rules.

The Librarian (Agentic AI in kmy-keiba): This agent has full access to a successful reference project. Ask me to query the Librarian to see how a specific logic problem was solved (e.g., "How does kmy-keiba handle race-day date ranges?") so we can "copy the homework" into our clean-room architecture.

---

## How to Query the Oracle & Librarian Efficiently

**Workflow:**
1. Claude (me) identifies knowledge gap and formulates targeted questions → appends to ORACLE_ANSWERS.md / LIBRARIAN_ANSWERS.md as "Pending query"
2. You run the queries using NotebookLM (Oracle) or kmy-keiba access (Librarian) and provide the answers
3. Claude appends the answers to the respective files under the "A:" section
4. Claude analyzes findings and implements architectural decisions

**Always check `ORACLE_ANSWERS.md` and `LIBRARIAN_ANSWERS.md` FIRST before asking a new question.**

### For Oracle (JRA-VAN Official Documentation)

**Good prompts:**
- "What is the exact IDL definition of [method]?" (request the literal IDL)
- "What are the possible return codes for [method] and their meanings?"
- "Show me the exact byte offsets for [record type]"
- "Is [feature] officially supported or deprecated?"

**Bad prompts:**
- "How does JVGets work?" (vague, will get verbose explanation)
- "Tell me about JV-Link" (too broad)

**Response format to request:**
- "Answer in 3 bullet points maximum"
- "Show me the exact IDL, nothing else"
- "Literal byte offsets only, no explanation"

### For Librarian (kmy-keiba Reference Implementation)

**Good prompts:**
- "Show me the exact C# code where kmy-keiba calls [method]" (request exact code)
- "Paste the entire method that handles [problem]"
- "What is the exact parameter type for [parameter]?"
- "Show me line-by-line how kmy-keiba converts [format] to [format]"

**Bad prompts:**
- "How does kmy-keiba handle marshaling?" (vague)
- "What's the pattern for COM interop?" (too broad)

**Response format to request:**
- "Code only, no explanation"
- "Show me the exact lines you reference"
- "Paste the surrounding context (5 lines before/after)"

### Golden Rule
If you're about to query and your question isn't specific enough to be answered in 3 bullet points or 5 lines of code, rephrase it.
