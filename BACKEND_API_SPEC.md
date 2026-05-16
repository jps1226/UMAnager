# UMAnager — Clean Room API Specification

**Purpose:** This document fully specifies the HTTP API that the frontend (`index.html`, `tv.html`, `script.js`) expects. A new backend must implement every endpoint and JSON contract defined here. The existing HTML/CSS/JS must not be modified.

---

## 1. User Flow

### 1.1 Main Application (`/`)

1. **Page loads** → JS calls `GET /api/marks`, `GET /api/config`, then `GET /api/races` in sequence.
2. **Race Calendar (sidebar)** → Rendered from the dates returned in `/api/races`. Each cell with races is a clickable button. Dates bucket into `upcoming` (blue) and `past` (muted) timelines.
3. **User selects a date** → The main panel switches to that date's tab. Races are rendered as collapsible tables — past races earlier than "now" auto-collapse on first load.
4. **Race card expands** → A horse-entry table is shown. Each row has: Mark buttons (◎〇▲△X), BK bracket, PP post position, Horse name, W/S record, Sire, Dam, BMS, Odds, Favorite number, and Finish position.
5. **User clicks a mark button** → Mark is toggled in-memory and immediately saved via `POST /api/marks`. The table re-sorts voted horses to the top if "Float Votes To Top" is enabled.
6. **Fetch Upcoming Races** button → Calls `POST /api/scrape` then `POST /api/jvlink/load-master-data`.
7. **Fill Horse Names / Parents** button → Calls `POST /api/races/enrich-horse-info`.
8. **Check Pending Updates** button → Calls `GET /api/prefetch-check` (if enabled in settings), then optionally `POST /api/races/prefetch/apply`.
9. **User opens Settings modal** → Reads/writes `GET|POST /api/config` for all UI preferences, formula weights, sidebar section visibility, and column ordering.
10. **Pedigree Lists (sidebar)** → Horses added by ID via `POST /api/snipe`. Full list saved via `POST /api/lists`. Clicking a horse in the list jumps to its race card.
11. **Voting tab** → Switches to the Voting workspace. Shows marks organized by racecourse. Connects to OrePro via `/api/orepro/*` endpoints.

### 1.2 TV Mode (`/tv`)

1. **Page loads** → Calls `GET /api/config` to restore split-percent and panel-flip settings.
2. **Race list loads** → Calls `GET /api/races` and `GET /api/marks` in parallel.
3. **Date tabs** → Built from `upcoming_races_by_date` + `past_races_by_date` keys. Earliest upcoming date is pre-selected.
4. **Race list** → Shows only races where the user has set at least one mark (◎〇▲△). Past races auto-collapse; the in-progress race stays expanded and scrolls into view.
5. **Auto-refresh** → Calls `GET /api/races` + `GET /api/marks` every 60 seconds.
6. **GreenChannel panel** → Calls `GET /api/gch/live-playback-json` to initialize the embedded video stream.
7. **Layout persistence** → Split-percent and panel-flip are saved via `POST /api/config`.

---

## 2. API Contract

> **Base URL:** `http://localhost:{port}` (configurable; default 8000)
> **Content-Type for all POST bodies:** `application/json`
> All endpoints return `application/json`.
> On error, return HTTP 4xx/5xx with `{ "detail": "string" }` or `{ "message": "string" }`.

---

### 2.1 `GET /api/races`

Returns all race data in two timeline buckets. This is the primary data endpoint.

**Response:**
```json
{
  "upcoming_races_by_date": {
    "YYYY-MM-DD": [
      {
        "info": {
          "race_id": "string",
          "race_name": "string",
          "race_number": 1,
          "place": "string",
          "time": "15:35",
          "sort_time": "2026-05-10T15:35:00",
          "clean_date": "YYYY-MM-DD",
          "history_refreshed": false
        },
        "entries": [ /* see Section 3 */ ]
      }
    ]
  },
  "past_races_by_date": {
    "YYYY-MM-DD": [ /* same race structure */ ]
  },
  "top_picks": [
    ["TOKYO R5", "3/12", "8.5", "HORSE NAME", "⭐", "race_id_string"]
  ]
}
```

**Notes:**
- The JS also accepts the legacy key `races_by_date` (treated identically to `upcoming_races_by_date`).
- `race_id` is the stable unique identifier for a race used as the key in marks, estimates, and history calls. It must be consistent across all endpoints.
- `sort_time` is used for countdown calculations and auto-collapse. It is a wall-clock local timestamp (JST if the race is domestic). If the `time` field contains AM/PM or `clean_date` differs from the date part of `sort_time`, the JS treats it as CT-local rather than JST.
- `top_picks` is a flat array of 6-element tuples: `[raceLabel, record, odds, horseName, iconEmoji, race_id]`. Return an empty array `[]` if no auto-picks have been generated.
- `history_refreshed` should be `true` once finish-position data has been imported for a past race.

---

### 2.2 `GET /api/marks`

Returns all saved user bet marks.

**Response:**
```json
{
  "version": 2,
  "marks": {
    "{race_id}_{horse_id}": "◎"
  },
  "raceMeta": {
    "{race_id}": {
      "savedAt": "ISO-8601 or null",
      "updatedAt": "ISO-8601 or null",
      "markSource": "manual | auto | mixed | null",
      "strategySnapshot": {
        "riskSlider": 50,
        "riskLabel": "Balanced",
        "formulaWeights": {}
      },
      "manualAdjustments": 0,
      "lockStateAtSave": false,
      "activeSymbols": ["◎", "〇"]
    }
  }
}
```

**Notes:**
- Mark keys use the format `{race_id}_{horse_id}`. `horse_id` is the 10-character base ID with any `.x` decimal suffix stripped.
- Valid mark symbols: `◎` `〇` `▲` `△` `X`. `X` is an exclusion mark; the others are ranked prediction marks.
- The backend must accept the legacy flat format `{ "{race_id}_{horse_id}": "◎", ... }` for backwards compatibility (the JS normalizes it).

---

### 2.3 `POST /api/marks`

Saves (overwrites) all user bet marks. Called on every mark toggle.

**Request body:** Same shape as the `GET /api/marks` response.

**Response:** `{ "status": "ok" }`

---

### 2.4 `GET /api/config`

Returns application configuration. Called on page load and by TV Mode.

**Response:**
```json
{
  "ui": {
    "riskSlider": 50,
    "betSafetyIndicator": false,
    "voteSortingTop": true,
    "autoFetchPastResults": true,
    "prefetchRaceCheck": false,
    "debugConsole": false,
    "autoLockPastVotes": false,
    "showConsole": true,
    "highlightAutoBets": false,
    "highlightFallbackBridge": false,
    "tvModeSplitPercent": 50,
    "tvModePanelsFlipped": false,
    "raceTableColumns": [
      { "key": "Shirushi", "visible": true },
      { "key": "BK", "visible": true },
      { "key": "PP", "visible": true },
      { "key": "Horse", "visible": true },
      { "key": "Record", "visible": true },
      { "key": "Sire", "visible": true },
      { "key": "Dam", "visible": true },
      { "key": "BMS", "visible": true },
      { "key": "Odds", "visible": true },
      { "key": "Fav", "visible": true },
      { "key": "Finish", "visible": true }
    ],
    "formulaWeights": {
      "oddsCap": 100,
      "formMultiplier": 100,
      "freshnessBonus": 3,
      "freshnessBreakeven": 10,
      "pedigreeMultiplier": 30
    }
  },
  "sidebarTabs": {
    "raceDatabase": true,
    "pedigreeLists": true,
    "autoPickStrategy": true,
    "weekendWatchlist": true
  },
  "backend": {
    "dataEngine": "jv"
  }
}
```

---

### 2.5 `POST /api/config`

Saves the full configuration object. Called whenever any setting changes and on TV Mode layout drag.

**Request body:** Same shape as `GET /api/config` response.

**Response:** `{ "status": "ok" }`

---

### 2.6 `GET /api/lists`

Returns the user's pedigree watchlists as plain-text blobs.

**Response:**
```json
{
  "favorites": "1234567890 # Horse Name\n0987654321 # Another Horse\n",
  "watchlist": "1234567890 # Horse Name\n"
}
```

**Notes:**
- Each line is `{10-char-horse-id} # {horse-name}`. The JS parses these by splitting on `#` and taking the first segment as the ID.
- Empty lists return an empty string `""`.

---

### 2.7 `POST /api/lists`

Overwrites both pedigree lists.

**Request body:**
```json
{
  "favorites": "1234567890 # Horse Name\n",
  "watchlist": ""
}
```

**Response:** `{ "status": "ok" }`

---

### 2.8 `POST /api/snipe`

Adds a horse to a pedigree list. Can take either a direct ID or a Netkeiba URL.

**Request body (direct ID):**
```json
{ "id": "1234567890", "list_type": "favorites" }
```

**Request body (URL-based):**
```json
{ "url": "https://db.netkeiba.com/horse/...", "list_type": "watchlist" }
```

**Response:**
```json
{
  "status": "success",
  "message": "Added Horse Name to favorites.",
  "horse_id": "1234567890",
  "horse_name": "Horse Name"
}
```

On failure: `{ "status": "error", "message": "string" }`

---

### 2.9 `GET /api/prefetch-check`

Lightweight check for available race data updates. Called silently after page load if the `prefetchRaceCheck` setting is enabled.

**Response:**
```json
{
  "hasUpdates": true,
  "enabled": true,
  "updatesByDate": {
    "2026-05-10": ["newEntries", "postPositions"],
    "2026-05-11": ["finishPositions"]
  },
  "summary": {
    "newRaceDates": 1,
    "newEntryRaces": 3,
    "postPositionRaces": 2,
    "finishPositionRaces": 4,
    "checkedFutureRaces": 10,
    "checkedPastRaces": 5
  }
}
```

**Notes:**
- `updatesByDate` values are arrays of update-type strings. The JS displays these as labels on calendar cells and in the prefetch banner. Known type strings: `"newRaceDates"`, `"newEntries"`, `"postPositions"`, `"finishPositions"`.

---

### 2.10 `POST /api/races/upcoming/refresh`

Refreshes upcoming race entry data (e.g. post positions). Called by the "Check Pending Updates" button when updates are pending.

**Request body:** `{}`

**Response:**
```json
{ "status": "ok", "message": "Refreshed N races." }
```

---

### 2.11 `POST /api/races/prefetch/apply`

Applies all pending prefetch updates to the database.

**Request body:** `{}`

**Response:** `{ "status": "ok" }`

---

### 2.12 `POST /api/scrape`

Triggers a data fetch operation to pull upcoming race data.

**Request body:**
```json
{ "mode": "new" | "all" }
```

**Response:**
```json
{
  "status": "ok",
  "cached_races": 12,
  "data_engine": "jv"
}
```

**Notes:**
- `cached_races: 0` triggers a warning alert in the UI.
- If `data_engine` is `"jv"` and `cached_races` is 0, the alert message is different from the Netkeiba variant.

---

### 2.13 `GET /api/scrape/log`

Returns the current scraper log buffer. Polled every 500ms during an active scrape.

**Response:**
```json
{ "logs": ["[INFO] Starting scrape...", "[INFO] Found 5 races."] }
```

---

### 2.14 `POST /api/races/enrich-horse-info`

Fills in missing horse names, Sire, Dam, and BMS fields for horses that exist in race entries but lack pedigree data.

**Request body:** None required.

**Response:**
```json
{
  "updated_rows": 45,
  "updated_races": 8,
  "unique_horses": 30,
  "fetch_candidates": 32
}
```

---

### 2.15 `POST /api/races/day/import-results`

Imports finish-position results for all races on a specific date.

**Request body:**
```json
{ "date": "YYYY-MM-DD" }
```

**Response:** `{ "status": "ok", "message": "Imported results for N races." }`

---

### 2.16 `POST /api/races/{race_id}/refresh-history`

Forces a history/result refresh for a single past race. Called by the "Update History" button on individual race headers.

**Request body:** None.

**Response:** `{ "status": "ok" }`

---

### 2.17 `POST /api/day/delete`

Deletes race data for a specific date with configurable scope.

**Request body:**
```json
{
  "date": "YYYY-MM-DD",
  "scope": "marks" | "entries" | "all"
}
```

**Response:** `{ "status": "ok" }`

---

### 2.18 `POST /api/races/bet-estimate`

Calculates estimated ticket costs and potential payouts for one or more races, based on post positions.

**Request body:**
```json
{
  "races": [
    {
      "race_id": "string",
      "honmei_post": 3,
      "box_posts": [3, 7, 11, 14]
    }
  ]
}
```

**Response:**
```json
{
  "estimates": {
    "{race_id}": {
      "status": "ok | partial",
      "raceId": "string",
      "purchase": {
        "total": 3600
      },
      "win": {
        "net": 1200
      },
      "quinellaBox": {
        "tickets": 6,
        "resolvedTickets": 6,
        "missingTickets": 0,
        "minNet": -3600,
        "maxNet": 12400,
        "minPayout": 0,
        "maxPayout": 16000
      },
      "trioBox": {
        "tickets": 4,
        "resolvedTickets": 4,
        "missingTickets": 0,
        "minNet": -3600,
        "maxNet": 55000,
        "minPayout": 0,
        "maxPayout": 58600
      },
      "allHit": {
        "minNet": -3600,
        "maxNet": 68600
      },
      "message": "",
      "warnings": []
    }
  }
}
```

**Notes:**
- If a leg's odds are unavailable, set its numeric fields to `null` and add a descriptive entry to `warnings`.
- `status: "partial"` means some odds were resolved but others were not. The UI renders what it can.

---

### 2.19 `POST /api/cache/clear`

Clears cached race data.

**Request body:** None.

**Response:** `{ "status": "ok" }`

---

### 2.20 `POST /api/dict/wipe`

Resets the translation/romanization memory cache.

**Request body:** None.

**Response:**
```json
{
  "message": "Translation memory cleared.",
  "cleared": {
    "runtimeEntries": 120,
    "dbEntries": 450,
    "legacyFileDeleted": true
  }
}
```

---

### 2.21 `POST /api/data/backup`

Creates a timestamped backup of the data directory.

**Request body:** `{}`

**Response:** `{ "status": "ok", "path": "string", "message": "Backup created." }`

---

### 2.22 `POST /api/data/backup/restore`

Restores from the most recent backup.

**Request body:** `{}`

**Response:** `{ "status": "ok", "message": "Restored from backup at {path}." }`

---

### 2.23 `POST /api/data/legacy/export`

Exports a legacy recovery bundle (zip file or equivalent).

**Request body:** `{}`

**Response:** `{ "status": "ok", "path": "string" }`

---

### 2.24 `POST /api/data/legacy/import`

Imports data from a previously exported legacy bundle.

**Request body:**
```json
{ "overwrite_existing": false }
```

**Response:** `{ "status": "ok", "message": "string" }`

---

### 2.25 `POST /api/server/shutdown`

Initiates a graceful server shutdown. The JS closes the browser tab immediately after calling this.

**Request body:** None.

**Response:** `{ "status": "ok" }`

---

### 2.26 OrePro Endpoints

#### `POST /api/orepro/companion/window`

Opens or focuses the OrePro companion browser session.

**Request body:** `{ "action": "open" | "focus" }`

**Response:** `{ "status": "ok | error", "message": "string" }`

---

#### `POST /api/orepro/votes/apply`

Applies user marks to OrePro's bet cart for one or more races.

**Request body:**
```json
{
  "races": [
    {
      "race_id": "string",
      "marks": [
        { "symbol": "◎", "post": 3, "mark_code": "1" }
      ]
    }
  ],
  "dry_run": false,
  "force_refresh": true,
  "submit_after_apply": false,
  "go_next_race": false
}
```

**Notes on `mark_code`:** `"1"` = ◎, `"2"` = 〇, `"3"` = ▲, `"4"` = △. Multiple `△` marks (mark_code `"4"`) may appear for different posts.

**Response:**
```json
{
  "status": "ok | error",
  "message": "Applied votes for 5/5 race(s).",
  "results": [
    {
      "raceId": "string",
      "status": "ok | error",
      "message": "Marks applied.",
      "submitFlow": {
        "nextStatus": "ok"
      }
    }
  ]
}
```

---

#### `GET /api/orepro/results/last`

Returns the most recently synced OrePro bet results.

**Response:**
```json
{
  "status": "success",
  "kaisai_date": "20260510",
  "myRaceResults": [
    {
      "raceId": "string",
      "purchaseLabel": "¥3,600",
      "payoutLabel": "¥8,200",
      "profit": 4600,
      "profitLabel": "+¥4,600"
    }
  ],
  "historySummary": {}
}
```

---

#### `GET /api/orepro/results/history`

Returns historical OrePro sync summary data. Used to populate the lifetime bar.

**Response:** Same shape as `GET /api/orepro/results/last`, or `{}`

---

#### `POST /api/orepro/results/sync`

Fetches current OrePro bet results for a specific race day.

**Request body:**
```json
{
  "kaisai_date": "20260510",
  "kaisai_id": "2026051001",
  "yosoka_id": "20021241"
}
```

**Response:** Same shape as `GET /api/orepro/results/last`.

---

### 2.27 JV-Link / Data Engine Endpoints

These endpoints power the Advanced Tools panel. They accept the same base payload shape and return a diagnostic JSON object that is displayed verbatim in the test panel's `<pre>` element.

**Common request fields (from the test panel form):**
```json
{
  "data_spec": "TOKU",
  "from_date": "20250323000000",
  "sid": null,
  "max_records": 20,
  "data_option": 1,
  "skip_set_service_key": true
}
```

| Endpoint | Method | Description |
|---|---|---|
| `GET /api/jvlink/status` | GET | Returns bridge connectivity status |
| `GET /api/jvlink/storage-layout` | GET | Returns storage/file layout info |
| `POST /api/jvlink/open-settings` | POST | Opens the JV-Link settings dialog |
| `POST /api/jvlink/probe-open` | POST | Runs a connectivity probe |
| `POST /api/jvlink/stream-sample` | POST | Reads a sample of records from the stream |
| `POST /api/jvlink/stream-sample` (auto) | POST | Stream sample with automatic status polling |
| `POST /api/jvlink/refresh-upcoming` | POST | Refreshes the upcoming race cache from JV-Link |
| `GET /api/jvlink/stream-summary` | GET | Returns a record-type summary. Query param: `?limit=50` |
| `POST /api/jvlink/capability-scan` | POST | Scans available data specs and options |
| `POST /api/jvlink/load-weekend-races` | POST | Fetches upcoming weekend race cards (TOKU records) |
| `POST /api/jvlink/load-master-data` | POST | Loads horse master (UM) records |

**`POST /api/jvlink/load-master-data` request body:**
```json
{
  "is_initial": false,
  "max_records": 50000,
  "max_status_wait_seconds": 180
}
```

**`POST /api/jvlink/load-master-data` response:**
```json
{
  "status": "ok | error",
  "data": {
    "ok": true,
    "recordsRead": 1245
  }
}
```

All other JV-Link endpoints return a freeform JSON diagnostic object with at minimum `{ "status": "ok | error" }`.

---

### 2.28 TV Mode — GreenChannel

#### `GET /api/gch/live-playback-json`

Returns metadata for the GreenChannel live stream. Called once on TV Mode page load.

**Response:**
```json
{
  "playback": {
    /* Streaks Player SDK loadMedia payload — format defined by the Streaks Player SDK */
  }
}
```

If the stream is unavailable, return HTTP 404 or `{ "playback": null }`. The TV page handles both gracefully by showing the fallback "Open GreenChannel" button.

---

## 3. Data Dictionary — Race Entry Fields

Every element of the `entries` array in a race object must include the following fields. Fields marked *optional* may be absent or `null`.

| Field | Type | Required | Description |
|---|---|---|---|
| `Horse_ID` | `string` | ✅ | 10-character JRA horse identifier. May include a `.x` decimal suffix (e.g. `"1234567890.1"`); the JS strips anything after the first `.` for comparison. |
| `Horse` | `string` | ✅ | Display name of the horse (romanized). |
| `PP` | `integer` | ✅ | Post position (gate number). Used for OrePro mark submission and bet estimation. |
| `BK` | `integer` | ✅ | Bracket number (1–8). Controls the colored bracket badge color in TV Mode. |
| `Record` | `string` | ✅ | Win/start record in `"W/S"` format, e.g. `"3/12"`. Used by the auto-pick scoring engine. |
| `Sire` | `string` | ✅ | Sire (father) display name. Empty string `""` if unknown. |
| `Sire_ID` | `string` | ✅ | Sire's 10-character horse ID. Used for Favorites/Watchlist matching. |
| `Dam` | `string` | ✅ | Dam (mother) display name. |
| `Dam_ID` | `string` | ✅ | Dam's 10-character horse ID. |
| `BMS` | `string` | ✅ | Broodmare Sire (maternal grandfather) display name. |
| `BMS_ID` | `string` | ✅ | BMS's 10-character horse ID. |
| `Odds` | `string` | ✅ | Win odds as a string (e.g. `"8.5"`). Parsed as a float by the scoring engine. |
| `Fav` | `string` | ✅ | Favorite number (e.g. `"1"` for the market favorite). Empty string if not yet set. |
| `Finish` | `string` | optional | Finish position as a string (e.g. `"1"`, `"3"`). Empty string if the race has not been run. Used for hit-rate calculation. |
| `_fallbackSources` | `object` | optional | Key-value map where keys are field names (e.g. `"Sire"`, `"Odds"`) and values are the source name (e.g. `"nk"`) indicating the field came from a fallback data source. When `highlightFallbackBridge` is enabled in settings, the UI renders a visual indicator on these cells. |

---

## 4. Race `info` Object Fields

Every race object contains an `info` sub-object with the following fields:

| Field | Type | Required | Description |
|---|---|---|---|
| `race_id` | `string` | ✅ | Stable unique identifier. Used as the key for marks, meta, estimates, and history. No format constraint, but must be consistent across all API calls. |
| `race_name` | `string` | ✅ | Race name (may be in Japanese katakana; the JS romanizes it locally). |
| `race_number` | `integer` | ✅ | Race number within the day at that venue (1–12). |
| `place` | `string` | ✅ | Venue/racecourse name. Displayed in uppercase (e.g. `"Tokyo"` → `"TOKYO"`). |
| `time` | `string` | ✅ | Human-readable race time (e.g. `"15:35"`). Use `"TBA"` if not yet set. |
| `sort_time` | `string` | ✅ | Machine-sortable timestamp used for auto-collapse and countdown. Format: ISO-8601 (`"2026-05-10T15:35:00"`). For JST races, the JS will append `+09:00` if no timezone is embedded. |
| `clean_date` | `string` | ✅ | `YYYY-MM-DD` date string. Used to group races by day for the Voting view. |
| `history_refreshed` | `boolean` | optional | Set to `true` after finish-position data has been imported. Controls whether the "Update History" button appears on past race headers. |

---

## 5. UI States

### 5.1 Timeline States

Every race date is tagged as either `upcoming` or `past`. The JS derives this from which bucket in the `/api/races` response the date appears in. Past dates get muted calendar styling; upcoming dates get the primary color.

### 5.2 Mark Symbols

| Symbol | Name | Meaning |
|---|---|---|
| `◎` | Honmei | Primary pick (only one per race) |
| `〇` | Niban-te | Second pick (only one per race) |
| `▲` | Sanbante | Third pick (only one per race) |
| `△` | Tanaan | Box pick (multiple allowed) |
| `X` | Exclusion | Excluded horse (multiple allowed, does not participate in OrePro submission) |

When a `◎`/`〇`/`▲` symbol is applied to a horse that another horse already holds, the two horses swap symbols (the "steal" mechanic). `△` and `X` do not trigger a swap.

### 5.3 Race Lock State

Each race card has a lock toggle button. When locked:
- All mark buttons are disabled.
- Auto-pick, Clear Bets, and Smart Sort buttons are disabled.
- The `lockStateAtSave` field in `raceMeta` records this state at the time marks were last saved.

### 5.4 Risk Slider

A sidebar slider (0–100) controls the Auto-Pick scoring engine:

| Value | Label | Color |
|---|---|---|
| 0–20 | Ultra Safe | Cyan |
| 21–40 | Chalky | Green |
| 41–60 | Balanced | Orange |
| 61–85 | Value Hunter | Red |
| 86–100 | Maximum Chaos | Bright Red |

The current slider value is persisted in `appConfig.ui.riskSlider` via `POST /api/config`.

### 5.5 Auto-Pick Formula

The scoring engine weights are stored in `appConfig.ui.formulaWeights`:

| Weight | Default | Description |
|---|---|---|
| `oddsCap` | 100 | Maximum points from odds (`oddsCap ÷ odds`) |
| `formMultiplier` | 100 | Scale factor for win-rate score |
| `freshnessBonus` | 3 | Points per start below the breakeven count |
| `freshnessBreakeven` | 10 | Starts at which freshness is neutral |
| `pedigreeMultiplier` | 30 | Multiplier on pedigree score from tracked bloodlines |

### 5.6 Pedigree Highlighting

When a horse, its Sire, Dam, or BMS appears on the Favorites or Watchlist, the corresponding table row gets a colored highlight. The intensity scales with pedigree match weight:

| Condition | CSS Class |
|---|---|
| Weight ≤ 0.33 | `intensity-light` |
| Weight ≤ 0.50 | `intensity-medium` |
| Weight ≤ 0.66 | `intensity-strong` |
| Weight > 0.66 | `intensity-very-strong` |

Watchlist matches take visual priority over Favorites matches (`has-watch` overrides `has-fav` on the race header).

### 5.7 TV Mode

TV Mode (`/tv`) is a split-panel view with GreenChannel on one side and the marked-horses race list on the other. The split ratio is persisted in `appConfig.ui.tvModeSplitPercent` (20–80). The panel order (video left vs. right) is persisted in `appConfig.ui.tvModePanelsFlipped`. Both are written via `POST /api/config`.

### 5.8 Voting Workspace

Accessed via the "🗳️ Voting" tab. Shows all races with marks, grouped by racecourse. Each race card shows:
- An "Apply" button (calls `POST /api/orepro/votes/apply` for that single race)
- Bet estimate chips (from `/api/races/bet-estimate`) if OrePro results are not yet available
- OrePro PnL chips if results have been synced

### 5.9 Live View Popout / Export Modal

The "🪟 Live View Popout" button opens a modal showing the same racecourse-grouped mark summary as the Voting sidebar. This button is hidden when the active date is a `past` date.

### 5.10 Prefetch Status Banner

If `prefetchRaceCheck` is enabled, a status banner appears in the sidebar after load indicating whether updates are available. Calendar cells with pending updates show a colored pip. The banner and pip are driven entirely by the `GET /api/prefetch-check` response.

### 5.11 Column Visibility and Ordering

The race table columns are user-configurable (Settings modal → Race Table Columns). Visibility and order are stored in `appConfig.ui.raceTableColumns` as an ordered array of `{ key, visible }` objects. Valid column keys: `Shirushi`, `BK`, `PP`, `Horse`, `Record`, `Sire`, `Dam`, `BMS`, `Odds`, `Fav`, `Finish`.

### 5.12 Data Engine Setting

`appConfig.backend.dataEngine` is either `"nk"` (Netkeiba) or `"jv"` (JRA-VAN). This value is available to the scrape and refresh endpoints to determine which data source to use. Changing it triggers a full race list reload.

---

## 6. Static File Routes

The frontend requires two static routes:

| Route | File |
|---|---|
| `GET /static/style.css` | `static/style.css` |
| `GET /static/script.js` | `static/script.js` |

The HTML references these with cache-bust query params (e.g. `?v=20260410-8`) which the server must ignore.

| Route | File |
|---|---|
| `GET /` | `index.html` |
| `GET /tv` | `tv.html` |

---

*End of specification. All endpoints, JSON contracts, and UI states described above are derived exclusively from the existing frontend source code and represent the complete interface contract required for a new backend implementation.*
