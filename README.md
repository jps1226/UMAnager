# UMAnager v2.0

A real-time analysis and betting cockpit for **Japanese Central Racing (JRA)**, built on the [JRA-VAN](https://jra-van.jp/) DataLab feed.

It pulls the official race-day data (entries, odds, finishing positions, payouts, pedigree) from JV-Link, computes a layered "auto-pick" score per horse, and exports finalized bets to JRA's online betting interface. A web dashboard surfaces the day's card with sortable columns, live-updating odds and finishing positions, and per-race profit/loss tracking.

---

## Why it exists

JV-Link, the SDK JRA-VAN ships, is a **32-bit Windows COM component** with a synchronous, single-threaded API. A modern multi-process backend can't host it directly. UMAnager v1.0 worked around that with a Python wrapper, but ran into the usual 32-bit memory ceilings under heavy historical loads.

v2.0 is a clean-room rewrite in .NET with a **split-process architecture**: a lightweight 32-bit Sidecar hosts JV-Link on its required STA thread and streams raw records over a Named Pipe to a 64-bit Nexus that handles parsing, persistence, scoring, and the web UI. No COM ever runs in the main process.

---

## Architecture

```
┌──────────────────────┐  Named Pipe  ┌──────────────────────────┐
│ Sidecar (x86)        │ ◄──────────► │ Nexus (x64)              │
│ ─────────────────    │   4-byte len │ ─────────────────────    │
│ JV-Link COM (STA)    │   + 2-byte   │ ASP.NET Core 8           │
│ JVOpen / JVRead /    │   type code  │ Span<byte> parsers       │
│ JVWatchEvent loop    │   + payload  │ PostgreSQL (Npgsql/EF)   │
│ Raw byte streamer    │              │ SignalR live broadcast   │
└──────────────────────┘              │ Static-file web UI       │
                                      └────────────┬─────────────┘
                                                   │ HTTP + WebSocket
                                                   ▼
                                      ┌──────────────────────────┐
                                      │ Dashboard (vanilla JS)   │
                                      │ — sortable race cards    │
                                      │ — live odds / results    │
                                      │ — auto-pick scorer       │
                                      │ — per-race P/L           │
                                      └──────────────────────────┘
```

| Process | Bitness | Responsibilities |
|---|---|---|
| **Sidecar** (`src/UMAnager.Sidecar`) | x86 | Hosts `JVDTLabLib` COM on a dedicated STA thread; streams raw JV-Link records as they arrive. Late-bound `IDispatch` to avoid Vtable access violations. |
| **Nexus** (`src/UMAnager.Nexus`) | x64 | Parses raw records using zero-allocation `ReadOnlySpan<byte>`; decodes Shift-JIS via `Encoding.GetEncoding(932)`; persists to PostgreSQL; serves the HTTP API; broadcasts real-time updates over SignalR; serves the dashboard as static files. |
| **Tray** (`src/UMAnager.Tray`) | any CPU | WinForms tray icon — quick start/stop, status indicator. |

The Nexus ↔ Sidecar IPC envelope is a binary frame:
- 4 bytes: payload length (LE)
- 2 bytes: message type
- N bytes: raw JV-Link record bytes or JSON command

The pipe is **persistent** for the lifetime of the session; commands and record streams multiplex over it.

---

## What the dashboard does

### Race card

Each race expands into a sortable table with one row per horse:

| Col | Meaning |
|---|---|
| **PP** | Post position |
| **Horse** | Romanized name (or Japanese if no romaji on file) |
| **W/S** | Career wins / starts |
| **Form** | Last-3 finishes as a colored strip (`1-3-—`); colored win/place/show/out |
| **SF** | Sire-fit % — sire's win rate on this race's surface + distance bucket |
| **Sire / Dam / BMS** | Pedigree, links to NetKeiba DB |
| **Odds / Fav** | Current win odds + favorite rank |
| **Finish** | Result position (live during race) |

A **ⓘ** trigger next to each name opens a popover with the full auto-pick score breakdown — explains why one horse scores higher than another (odds contribution, form contribution, pedigree contribution, sliders applied, tiebreaker).

### Auto-pick scoring

The score blends three component groups, mixed by a single **Risk** slider:

- **Odds** weight = `(1 − risk)` — at low risk, follow the market
- **Form + Pedigree** weight = `risk` — at high risk, follow the model

The model contributes:

| Component | Source |
|---|---|
| Career win rate | `wins / starts × multiplier` |
| Freshness | `(breakeven − starts) × bonus` — rewards lightly-raced runners |
| Last-3 recency-weighted form | Server-computed score `[0.5·1/pos₁ + 0.3·1/pos₂ + 0.2·1/pos₃]` over the last 3 finishes (top-5 only) |
| Sire Fit | Sire's historical win-% on the same (surface, distance bucket) — materialized view across all `(sire_id × surface × bucket)` aggregated from ~700K entry rows |
| Pedigree match | Score from user's tracked-bloodline lists (favorites + watchlist) |
| Tiebreaker | Favorite rank penalty (1×10⁻⁴) — true favourite wins infinitesimal ties |

For **maiden** (未勝利), **debut** (新馬), and **unraced** (未出走) races — detected from JV-Link's authoritative JyokenCD field, not from inflated W/S aggregates — the engine automatically:
- Suppresses the career win-rate term (uninformative when no one has won)
- Boosts the Sire Fit weight (3× for maidens, 5× for debuts) since pedigree is the strongest available signal
- Suppresses Last-3 form on debuts (no prior runs to read)

### Race lifecycle phasing

A background orchestrator transitions the application through three phases driven by data state, not the calendar:

| Phase | Behavior |
|---|---|
| `WAITING_FOR_RACES` | No race rows for today or later. Poll JV-Link hourly for new race plans. |
| `RACES_POPULATED` | Race plan ingested. Refresh odds hourly. |
| `LIVE_OPERATIONS` | A race is within `live_window_minutes` (default 90) of post time. Refresh odds every 5 min (JV-Link's documented floor). SignalR broadcasts every finishing position and odds update. |

After the last race of the weekend finishes, the orchestrator returns to `WAITING_FOR_RACES`.

### Notifications

Discord webhooks fire on:
- First successful race-plan populate of a new weekend (`📅 Race Plan Loaded`)
- Per-race bet wins (`🏆 Mark Hit — ◎ / Q Box / T Box` with the winning horse name)
- End-of-day recap (`🏁 Day Recap` with hit count + estimated ¥ won)

### Bet export

Finalized marks export to **[OrePro](https://orepro.netkeiba.com/)** via a Chrome DevTools Protocol bridge — opens a controlled Chrome window pinned to OrePro, drives the form via JSON payloads, and reports back what landed.

---

## Storage

PostgreSQL 16. EF Core fluent-API migrations for the runtime tables, inline `ALTER TABLE` for incremental columns added during development.

| Table | Role |
|---|---|
| `horses` | Master pedigree per KettoNum (runner-keyed) |
| `breeding_horses` | Master pedigree per HansyokuNum (sire/dam-keyed; provides romaji names that don't appear in `horses`) |
| `races` | Race metadata + pre-computed `RaceClass` (debut / maiden / 1win / 2win / 3win / open / other) parsed from JyokenCD slot 5 at ingest time |
| `race_entries` | One row per horse per race; final results in `FinishPos`, performance JSONB |
| `raw_staging` | Append-only landing zone for every JV-Link record before parsing — keeps the raw bytes for reparse/backfill |
| `sire_performance` | Materialized view, `(sire_id × surface × distance_bucket) → win-%`, refreshed concurrently after each results-tick |
| `user_horse_lists` | Favorites + watchlist (HansyokuNum/KettoNum), name-resolved at GET time |
| `user_marks_blob` | Per-race ◎〇▲△X marks (JSON blob in `app_state`) |
| `app_state` | Generic key/value for orchestrator state, cursors, idempotency keys |
| `app_settings` | User-tunable poll intervals, weights, Discord webhook URL |

JSON-B is used aggressively for variable-shape data (results payouts, odds slots, performance breakdowns) so the schema doesn't have to evolve every time a new column matters.

---

## Tech stack

- **.NET 8** (Nexus x64, Sidecar x86, Tray any-CPU)
- **ASP.NET Core** — controllers, SignalR, response compression (gzip/brotli)
- **EF Core 8** + **Npgsql** — PostgreSQL provider
- **PostgreSQL 16** — JSON-B, materialized views with concurrent refresh
- **JV-Link COM** — late-bound `InterfaceIsIDispatch` to dodge marshaling bugs in the SDK
- **Vanilla JS + plain CSS** (no build step) — single-file dashboard, single-file TV mode

The frontend deliberately avoids a framework. It's a single `script.js` (~6K lines) and `style.css` (~1.7K lines) served as static files — first paint is a shimmer skeleton, real data lands as soon as `/api/races` (a single `GET`, ETag-cached) returns.

---

## Performance notes

- Race-card endpoint: cold ~2.7 s, warm cached ETag round-trip ~25 ms.
- Parsing: zero allocations on the hot path; `ReadOnlySpan<byte>.Slice` for every field; Shift-JIS decoded only where needed.
- Sire-performance MV is ~5K rows after aggregation; concurrent refresh keeps reads non-blocking under live updates.
- Live broadcasts debounce SignalR fan-out to 750 ms so a 12-horse race finishing inside 5 seconds doesn't flood the wire.

---

## Setup

### Prerequisites

- Windows 10 or 11
- .NET 8 SDK (the x86 runtime as well — required for the Sidecar)
- PostgreSQL 15 or newer
- A licensed **JRA-VAN DataLab** subscription
- The JV-Link SDK installed and registered (`regsvr32` on the COM type library)
- Drop these SDK files into the repo root (gitignored, not redistributable):
  - `JVData_Struct.cs`
  - `JVDTLab.IDL`

### Configure

```powershell
Copy-Item src\UMAnager.Nexus\appsettings.example.json   src\UMAnager.Nexus\appsettings.json
Copy-Item src\UMAnager.Sidecar\appsettings.example.json src\UMAnager.Sidecar\appsettings.json
```

Edit the new files to point at your local PostgreSQL instance and (optionally) your Discord webhook.

### Build

```powershell
dotnet build .\UMAnager.sln -c Release
```

### Run

```powershell
.\launch-services.ps1     # Starts Sidecar, Nexus, and the tray icon
```

Or individually:

```powershell
.\src\UMAnager.Sidecar\bin\Release\net8.0\UMAnager.Sidecar.exe
.\src\UMAnager.Nexus\bin\Release\net8.0\UMAnager.Nexus.exe
```

Dashboard: <http://localhost:5000>
TV mode (split-panel live view): <http://localhost:5000/tv.html>

---

## Licensing

The project's source code is under the repository's stated license. **JRA-VAN data and the JV-Link SDK are not redistributable** — you must obtain your own DataLab subscription to use this software for its intended purpose. Without JV-Link, the Sidecar will not connect and the Nexus will run in mock mode (HTTP API + UI but no real race data).

---

## Acknowledgements

This is a clean-room rewrite. The architecture is informed by:

- **kmy-keiba** — open-source JRA analysis project in C# whose record-handling patterns influenced the parsing pipeline (no code is imported)
- **JRA-VAN official documentation** — byte-offset references for every record type
