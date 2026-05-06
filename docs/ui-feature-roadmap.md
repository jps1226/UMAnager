# UMAnager UI Feature Roadmap

Derived from analysis of `oldindex.html` (V1.0 reference UI) combined with user clarifications.
Written: 2026-05-06

---

## Implementation Tiers

| Tier | Label | Focus |
|---|---|---|
| MVP | Phase 4 | View races, entries, horse pedigree, search |
| Phase 2 | Phase 4+ | Voting, auto-pick scoring, OrePro integration |
| Phase 3 | Phase 4+/5 | Live results, TV Mode, enrichment tools |

---

## MVP Features (Phase 4)

### F-01 — Race Calendar (Sidebar)

**What it does:** A custom calendar widget in the sidebar showing all months. Any day is clickable. Days that have races are highlighted, with a visual distinction between upcoming races (future dates) and historical races (past dates). Clicking a day sets it as the "active date" and loads that day's races.

**Backend requirements:**
- `GET /api/calendar?year=YYYY&month=MM` — returns an array of `{ date, raceCount, isUpcoming }` for each day in the month that has races
- Must query `races` table by `race_date` range; `isUpcoming` = `race_date >= TODAY`

**Database:**
- No schema changes needed — `races.race_date` is sufficient

**Frontend:**
- Build a pure JS calendar grid (no library needed)
- Color scheme: upcoming days = one accent color, historical = muted/secondary
- Non-race days are visible but clicking them clears the race list (shows "No races on this day")
- Active day shows race count in the sidebar nav display

---

### F-02 — Date-Based Race Display (Main Content)

**What it does:** When a date is selected, all races for that date load in the main scrollable area. Order: interleaved by race number across all tracks at that venue — e.g., Niigata R1, Hanshin R1, Tokyo R1, Niigata R2, Hanshin R2, etc. This requires actual race start times stored in the database.

**Backend requirements:**
- `GET /api/races?date=YYYY-MM-DD` — returns all races for that date, ordered by start time ascending, then track alphabetically as tiebreaker
- Enhance existing `/api/races` to accept a `date` query param instead of returning a static LIMIT 20

**Database — schema gap:**
- `races` table currently has `race_date` (DATE) but **no `race_start_time`** column
- RA records contain race time data — need to identify the correct byte offset in the JRA-VAN spec and add `race_start_time TIME` to the `races` table
- `RARecordParser.cs` will need to be updated to parse and store this field
- Until race time is available, fall back to ordering by `race_number ASC` within each track, then by `track_code` alphabetically

**Frontend:**
- Races render as collapsible cards
- Each card shows: race number, track name, distance (m), surface (turf/dirt), grade, conditions
- Default state: all races expanded
- "Toggle All Races" button collapses/expands all at once

---

### F-03 — Race Entry Table (Per Race Card)

**What it does:** Inside each race card, a table shows all horses entered, ordered by post position. Each row shows: frame number, post position, horse name (Japanese + romanized), jockey code, trainer code, morning line odds, and pedigree (sire / dam / broodmare sire with names, not IDs).

**Backend requirements:**
- Existing `GET /api/races/{race_id}` already returns entries with pedigree names
- May need to add jockey and trainer name lookup if we want human-readable names instead of codes; for now codes are acceptable

**Database:**
- No changes needed for MVP; jockey/trainer name tables are a future addition

**Frontend:**
- Configurable column visibility per F-15 (Settings: Race Table Columns)
- Columns for MVP: Frame #, Post Position, Horse (JP name + romanized), Jockey Code, Trainer Code, Morning Odds, Sire, Dam, Broodmare Sire

---

### F-04 — Horse Search (Autocomplete)

**What it does:** A search box in the main toolbar. As the user types, a dropdown shows matching horse names (Japanese or romanized). Clicking a suggestion navigates to that horse's profile or highlights all races that horse is entered in on the current date.

**Backend requirements:**
- `GET /api/horses/search?q=QUERY&limit=10` — searches `horse_name_japanese` and `horse_name_romaji` with a ILIKE/contains query; returns `{ horseId, japaneseeName, romajiName, birthYear }`
- With 212k+ horses this query MUST use a database index: `CREATE INDEX idx_horses_name_japanese ON horses USING gin(horse_name_japanese gin_trgm_ops)` (requires `pg_trgm` extension) — or a simple B-tree on the first N chars if pg_trgm is unavailable

**Database:**
- Add index on `horse_name_japanese` and `horse_name_romaji` for fast search
- `pg_trgm` extension enables fuzzy partial-match searching

**Frontend:**
- Debounce input (300ms) before firing API call
- Keyboard navigation (arrow keys, Enter to select)
- Clicking a result opens horse profile view or scrolls to that horse's entry in the current race list

---

### F-05 — Horse Profile View

**What it does:** A view (modal or dedicated panel) showing a single horse's full profile: Japanese name, romanized name, birth year, and 3-generation pedigree (sire, dam, broodmare sire — each with their own names and IDs). Future: race history.

**Backend requirements:**
- Existing `GET /api/horses/{horse_id}` already returns this data with nested pedigree objects

**Frontend:**
- Triggered from search results or clicking a horse name in any race entry table
- Shows pedigree in a tree format (horse → sire/dam, each sire/dam are clickable to navigate to their profile)

---

### F-06 — Active Date Navigation

**What it does:** Prev/Next arrow buttons in the sidebar jump one day backward or forward from the currently selected date. The display shows the active date label and the number of races on that day.

**Backend requirements:**
- Uses the same `GET /api/races?date=` endpoint as F-02
- No dedicated endpoint needed

**Frontend:**
- Prev/Next buttons skip over days with no races (optional UX enhancement — first version can just step day by day)

---

### F-07 — Dual Clock Display (JST + CT)

**What it does:** Two live clocks in the top sticky bar — one showing Japan Standard Time (UTC+9), one showing US Central Time. Both update every second via `setInterval`.

**Backend requirements:** None — pure client-side `Date` math

**Frontend:**
- Compute JST offset from user's local time using UTC
- CT offset: CST (UTC-6) or CDT (UTC-5) depending on daylight saving — detect automatically

---

### F-08 — Countdown to Next Race

**What it does:** A countdown timer showing time remaining until the next race start time on the active date. Displays the race name/number alongside the countdown.

**Backend requirements:**
- Requires `race_start_time` in the database (same dependency as F-02 ordering)
- Endpoint: `GET /api/races/next?date=YYYY-MM-DD` — returns the next upcoming race based on current JST time, or null if all races for the day have started

**Database:**
- Blocked on `race_start_time` column addition (see F-02)

**Frontend:**
- Shows "Next Race In HH:MM:SS — [Race Name/Number]"
- Hides if no future races on selected date

---

### F-09 — Console Status Box

**What it does:** A small text box in the top bar that shows status messages from background operations (data fetching, enrichment, sync). Not interactive — read only.

**Backend requirements:**
- `GET /api/status` — existing endpoint; poll it periodically (e.g., every 30s) to show sync state
- Future: Server-Sent Events or WebSocket for real-time messages

**Frontend:**
- Toggleable via Settings (F-15: "Show Console Log")
- Debug mode shows verbose messages

---

### F-10 — View Switcher (Main / Voting)

**What it does:** Two buttons in the sidebar to switch between "Main" view (race browsing) and "Voting" view (bet management workspace). The voting view is stubbed out in MVP.

**Backend requirements:** None for MVP; Voting view is Phase 2

**Frontend:**
- Active button visually indicated
- "Voting" view shows placeholder until Phase 2

---

## Server-Side User Data Features (Phase 4 — stored in database)

### F-11 — Favorites List

**What it does:** User can add a horse to a "Favorites" list by entering the horse's ID in a text field in the sidebar. The list persists on the server and is accessible from any device.

**Backend requirements:**
- `GET /api/user/lists/favorites` — returns all horses in the favorites list with basic horse info
- `POST /api/user/lists/favorites` with body `{ horseId }` — adds horse to favorites
- `DELETE /api/user/lists/favorites/{horseId}` — removes horse from favorites

**Database — new table:**
```sql
CREATE TABLE user_horse_lists (
    id SERIAL PRIMARY KEY,
    horse_id VARCHAR(10) REFERENCES horses(horse_id),
    list_type VARCHAR(20) NOT NULL,  -- 'favorites' or 'watchlist'
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(horse_id, list_type)
);
```

**Frontend:**
- Input: text field for horse ID (horse ID is same as JRA/NK, e.g., `2019104650`)
- On add: validate ID exists in database before saving, show error if not found
- Display: horse name (JP + romaji) with remove button

---

### F-12 — Watchlist

**What it does:** Same as Favorites but separate list type. User can track horses they want to monitor without marking them as full favorites.

**Backend requirements:**
- Same endpoints as F-11, replacing "favorites" with "watchlist" in the path

**Database:** Same `user_horse_lists` table, `list_type = 'watchlist'`

---

### F-13 — Weekend Watchlist (Sidebar Widget)

**What it does:** A sidebar panel that automatically shows horses from your Watchlist that are entered in any upcoming weekend races. Refreshes when race data updates.

**Backend requirements:**
- `GET /api/user/watchlist/upcoming` — joins `user_horse_lists` with `race_entries` and `races` WHERE `race_date >= current Saturday AND race_date <= current Sunday`

**Database:** No new tables — join of existing tables + user_horse_lists

---

## Phase 2 Features (Voting & Betting)

### F-14 — Voting Workspace (Main View)

**What it does:** The second main view. Shows all races for the active day with the ability to "mark" horses as your picks. Marks are saved to the database. Summary panel on the left shows picks grouped by racecourse.

**Backend requirements:**
- `GET /api/bets?date=YYYY-MM-DD` — all saved bets/marks for a day
- `POST /api/bets` with body `{ raceId, horseId, betType }` — save a mark
- `DELETE /api/bets/{id}` — remove a mark
- `PUT /api/bets/{id}` — update a mark

**Database:**
- `bets_saved` table per CLAUDE.md schema is sufficient

---

### F-15 — OrePro Companion Integration

**What it does:** UMAnager sends bet selections to a companion OrePro window (opened by the user). The workflow: user makes picks in UMAnager → clicks "Apply Votes" → UMAnager makes API calls into the open OrePro browser window to pre-fill bet forms → user confirms in OrePro.

**Backend requirements:**
- `POST /api/bets/export` — returns bet slip in OreProPlus-compatible JSON format (per CLAUDE.md schema)
- `GET /api/bets/export?date=YYYY-MM-DD` — export all bets for a day

**Frontend:**
- "Open OrePro" — opens `https://orepro.netkeiba.com/bet/race_list.html` in a new window
- "Apply Votes" — reads exported bet data and executes JS automation against the OrePro window
- "Sync Results" — reads results back from OrePro (Phase 3/5 dependency)
- Profile ID input for targeting correct OrePro account
- Session status display

---

### F-16 — Auto-Pick Strategy (Placeholder)

**What it does:** A risk slider from 0 (Chalk / safe favorites) to 100 (Chaos / high-value long shots) controls the auto-pick algorithm's weighting. "Auto Bet Day" applies auto-picks to all remaining races for the active day.

**Backend requirements (Phase 2 implementation):**
- `POST /api/autopick?date=YYYY-MM-DD&risk=50` — runs scoring algorithm, returns picks; old algorithm code to be ported from V1.0
- Formula weights (odds cap, win rate scale, freshness bonus/breakeven, pedigree scale) sent as parameters or read from user settings

**For now:** Render the slider and "Auto Bet Day" button as disabled UI placeholders with tooltip "Coming in Phase 2"

---

### F-17 — Settings Persistence (Server-Side)

**What it does:** User preferences (sidebar section visibility, display toggles, formula weights, race table column order) are saved to the server so settings are consistent across devices.

**Backend requirements:**
- `GET /api/user/settings` — returns settings JSON
- `PUT /api/user/settings` with JSON body — saves settings

**Database — new table:**
```sql
CREATE TABLE user_settings (
    id INT PRIMARY KEY DEFAULT 1,  -- single-row table (single user)
    settings_json JSONB NOT NULL DEFAULT '{}',
    updated_at TIMESTAMP DEFAULT NOW()
);
```

**Settings keys to persist:**
- `sidebarSections`: which sections are open/collapsed
- `displayOptions`: all checkbox toggles (float votes to top, auto-lock past, show console, debug mode, etc.)
- `formulaWeights`: oddsCap, formMultiplier, freshnessBonus, freshnessBreakeven, pedigreeMultiplier
- `raceTableColumns`: array of `{ key, visible, order }`
- `tvModeSplitPercent`, `tvModePanelsFlipped`

---

### F-18 — Race Table Column Configuration

**What it does:** In Settings, user can toggle which columns are visible in race entry tables and reorder them with up/down buttons.

**Backend requirements:** Stored via F-17 settings endpoint

**Default visible columns:**
Frame #, Post Position, Horse Name (JP), Horse Name (Romaji), Jockey, Trainer, Morning Odds, Sire, Dam, Broodmare Sire

**Hideable/reorderable:**
All of the above, plus future columns (finish position, win rate, past performance when Phase 5 data available)

---

## Phase 3 Features (Results & Live)

### F-19 — Live Results (Phase 5 Dependent)

**What it does:** After a race completes, finish positions and payoffs appear in the race entry table automatically.

**Backend:** JVEvtPay listener → JVRTOpen → parse SE/HR records → UPDATE `race_entries` with finish data (per CLAUDE.md Phase 5 spec)

**For now:** Render finish position and payoff columns as empty in race table with `--` placeholder values

---

### F-20 — Auto-Fetch Past Results Toggle

**What it does:** Setting that, when enabled, automatically triggers a results sync when the user opens a historical race date that has no result data yet.

**Backend:** Calls same Phase 5 results endpoint; stub for now

---

### F-21 — Auto-Lock Past Race Votes

**What it does:** When enabled, once a race's start time has passed, the user's marks for that race are locked (read-only). Prevents accidental edits after the race has started.

**Backend:** Mark `bets_saved.locked = TRUE` when `race_start_time < NOW()` — or check at read time

**Database:** Add `locked BOOLEAN DEFAULT FALSE` to `bets_saved`

---

### F-22 — TV Mode

**What it does:** Full-screen display at `/tv` route with two panels side by side: race card info on one side, live results + bet slip on the other. Designed for a TV browser. Configurable split percentage (20–80%) and flippable panel positions.

**Backend requirements:**
- Same race/entry data endpoints as MVP
- Results data from Phase 5

**Frontend:**
- Separate `/tv` route serving a full-screen layout
- Panel split controlled by CSS variable driven by `setting-tvModeSplitPercent`
- Toggle button to flip left/right panels

---

### F-23 — Live View Popout (Export Modal)

**What it does:** A modal that pops open the Voting Workspace in a dedicated window (for second-monitor or split-screen use).

**Backend requirements:** None — same data as voting view

**Frontend:** `window.open()` pointing to a dedicated `/voting` route that renders the voting workspace standalone

---

### F-24 — Netkeiba English Name Enrichment (Manual)

**What it does:** A manual-trigger button ("Fill Horse Names / Parents") that fetches English/official romanized names for horses from Netkeiba where our JRA-VAN romanized names are incomplete or missing.

**Backend requirements:**
- `POST /api/admin/enrich-horse-names` — triggers a background job that queries Netkeiba for horses missing romanized names
- Since JRA and NK use the same horse IDs, construct the NK URL from the horse ID

**Database:**
- Consider adding `horse_name_english VARCHAR(255)` to `horses` table as a separate field from `horse_name_romaji` (which comes from JRA-VAN)

---

### F-25 — Import Results For Day (Admin Tool)

**What it does:** Admin tool to manually import race results for a historical date (for backfill or manual correction).

**Backend requirements:**
- `POST /api/admin/import-results-day` with body `{ date: "YYYY-MM-DD" }` — triggers an ingestion service call for that date's result data

---

### F-26 — Delete Day Data (Admin Tool)

**What it does:** Admin tool to delete data for a selected date. Scope options: marks only, entries only, or all day data.

**Backend requirements:**
- `DELETE /api/admin/day-data?date=YYYY-MM-DD&scope=marks|entries|all`

---

## Feature Dependency Summary

```
F-01 Race Calendar           → GET /api/calendar
F-02 Date Race Display       → GET /api/races?date= + race_start_time column
F-03 Entry Table             → existing GET /api/races/{id}
F-04 Horse Search            → GET /api/horses/search + pg_trgm index
F-05 Horse Profile           → existing GET /api/horses/{id}
F-06 Date Navigation         → F-01 + F-02
F-07 Clocks                  → client-side only
F-08 Countdown               → race_start_time column
F-09 Console                 → existing GET /api/status
F-10 View Switcher           → client-side only
F-11 Favorites               → user_horse_lists table
F-12 Watchlist               → user_horse_lists table
F-13 Weekend Watchlist       → user_horse_lists + race_entries join
F-14 Voting Workspace        → bets_saved table
F-15 OrePro Integration      → F-14 + export endpoint
F-16 Auto-Pick (stub)        → F-14 (full: old algorithm port)
F-17 Settings Persistence    → user_settings table
F-18 Column Config           → F-17
F-19 Live Results            → Phase 5 (JVEvtPay)
F-20 Auto-Fetch Results      → F-19
F-21 Auto-Lock Past Votes    → bets_saved.locked column
F-22 TV Mode                 → /tv route + F-19
F-23 Live View Popout        → /voting route
F-24 NK Enrichment           → horses.horse_name_english column
F-25 Import Results (Admin)  → Phase 5 ingestion
F-26 Delete Day (Admin)      → DELETE endpoints
```

---

## New Database Changes Required (All Phases)

| Change | Needed For | Phase |
|---|---|---|
| `races.race_start_time TIME` | F-02, F-08 | 4 |
| `CREATE INDEX idx_horses_name_*` (pg_trgm) | F-04 | 4 |
| `user_horse_lists` table | F-11, F-12, F-13 | 4 |
| `user_settings` table | F-17, F-18 | 4 |
| `bets_saved` table | F-14, F-15 | 4+ |
| `bets_saved.locked BOOLEAN` | F-21 | 3 |
| `horses.horse_name_english` column | F-24 | 3 |
| `race_entries.finish_position INT` | F-19 | 5 |
| `race_entries.finish_time_hundredths INT` | F-19 | 5 |
| `race_entries.payoff_win DECIMAL` | F-19 | 5 |
| `race_entries.payoff_place DECIMAL` | F-19 | 5 |

---

## New API Endpoints Required (All Phases)

### Phase 4 MVP
| Method | Path | Feature |
|---|---|---|
| GET | `/api/calendar?year=&month=` | F-01 |
| GET | `/api/races?date=YYYY-MM-DD` | F-02 |
| GET | `/api/horses/search?q=&limit=` | F-04 |
| GET | `/api/races/next?date=` | F-08 |
| GET | `/api/user/lists/favorites` | F-11 |
| POST | `/api/user/lists/favorites` | F-11 |
| DELETE | `/api/user/lists/favorites/{horseId}` | F-11 |
| GET | `/api/user/lists/watchlist` | F-12 |
| POST | `/api/user/lists/watchlist` | F-12 |
| DELETE | `/api/user/lists/watchlist/{horseId}` | F-12 |
| GET | `/api/user/lists/watchlist/upcoming` | F-13 |
| GET | `/api/user/settings` | F-17 |
| PUT | `/api/user/settings` | F-17 |

### Phase 4+ (Voting)
| Method | Path | Feature |
|---|---|---|
| GET | `/api/bets?date=YYYY-MM-DD` | F-14 |
| POST | `/api/bets` | F-14 |
| DELETE | `/api/bets/{id}` | F-14 |
| GET | `/api/bets/export?date=YYYY-MM-DD` | F-15 |
| POST | `/api/autopick?date=&risk=` | F-16 |

### Phase 3+ (Admin / Results)
| Method | Path | Feature |
|---|---|---|
| POST | `/api/admin/enrich-horse-names` | F-24 |
| POST | `/api/admin/import-results-day` | F-25 |
| DELETE | `/api/admin/day-data` | F-26 |

---

## Implementation Order for Phase 4 (MVP)

1. Add `race_start_time` column to `races` table and update `RARecordParser` to extract it
2. Add `pg_trgm` extension and name search indexes
3. Create `user_horse_lists` and `user_settings` tables
4. Add all new API endpoints to `Program.cs`
5. Build new `index.html` with:
   - Layout: sidebar + main content shell
   - F-01: Race Calendar widget
   - F-02: Date-filtered race list
   - F-03: Race entry table with pedigree columns
   - F-04: Horse search with autocomplete
   - F-07: Dual clocks (JST + CT)
   - F-10: View switcher (Voting stubbed)
   - F-11/F-12: Favorites and Watchlist inputs
   - F-13: Weekend Watchlist panel
   - F-17/F-18: Settings modal with persistence
   - F-16: Risk slider (disabled placeholder)
6. Wire up all sidebar section collapse/expand with settings persistence
