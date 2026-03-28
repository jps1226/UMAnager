# JRA-VAN Data Lab Migration Plan
*Replacing Netkeiba Scraping — Created 2026-03-21*

## Scope

- **In scope:** Replace Netkeiba scraping for race discovery, race metadata, entries, horse/pedigree enrichment, odds, and finalized results.
- **Out of scope:** OrePro integration and all other non-scraping Netkeiba workflows.

## Recommended Architecture

Evaluate both integration styles in a short spike, but bias toward **Option B: local Windows sidecar**. JV-Link is Windows-only, COM-based, synchronous, and Shift_JIS-heavy — all concerns better isolated behind a stable JSON boundary than embedded in the FastAPI app process.

| Option | Pros | Cons |
|--------|------|------|
| **A: Direct COM from Python** (`pywin32`/`comtypes`) | Fewer moving parts | COM lifetime tied to server process; threading hazards; harder error recovery |
| **B: Local Windows sidecar** (small exe or script over JV-Link, returns JSON) | Isolated COM lifetime; clean boundary; easier to test independently | Extra process to manage on startup |

## JRA-VAN Data to Current App Field Mapping

| App Field | Current Source | JRA-VAN Record |
|-----------|---------------|----------------|
| `race_id` (12-digit string) | Netkeiba ID scheme | Derived from `RACE_ID`: Year + MonthDay + JyoCD + Kaiji + Nichiji + RaceNum |
| `place` / `track` | keibascraper + `TRACK_TRANSLATIONS` | `JV_RA_RACE` venue code → same translation table |
| `race_name` | keibascraper entry | `JV_RA_RACE` race name fields |
| `race_number` | keibascraper entry | `RACE_ID.RaceNum` |
| `clean_date` / `sort_time` | keibascraper + Netkeiba shutuba HTML | `JV_RA_RACE` or scheduled race times from `JV_YS_SCHEDULE` |
| `BK` (frame number) | keibascraper entry | `JV_SE_RACE_UMA.WakuBan` |
| `PP` (horse number) | keibascraper entry | `JV_SE_RACE_UMA.UmaBan` |
| `Horse` (romanized name) | Netkeiba horse page + pykakasi | `JV_UM_UMA.Bamei` → romanize |
| `Horse_ID` | Netkeiba 10-char alphanumeric | `JV_SE_RACE_UMA.KettoNum` (pedigree registration number) |
| `Sire` / `Sire_ID` | Netkeiba pedigree HTML scrape | `JV_SK_SANKU` or `JV_UM_UMA` pedigree fields |
| `Dam` / `Dam_ID` | Netkeiba pedigree HTML scrape | `JV_SK_SANKU` or `JV_UM_UMA` pedigree fields |
| `BMS` / `BMS_ID` | Netkeiba pedigree HTML scrape | `JV_SK_SANKU` or `JV_UM_UMA` broodmare sire fields |
| `Record` (wins/starts) | Netkeiba horse page | `JV_SE_RACE_UMA` cumulative results or horse record struct |
| `Odds` (win) | Netkeiba odds API type=1 | `JV_O1_ODDS_TANFUKUWAKU.OddsTansyoInfo` |
| `Fav` (popularity rank) | Netkeiba odds API type=1 | `JV_O1_ODDS_TANFUKUWAKU.OddsTansyoInfo.Ninki` |
| Quinella odds (type=4) | Netkeiba odds API | `JV_O2_ODDS_UMAREN` or `JV_O3_ODDS_WIDE` |
| Trio odds (type=7) | Netkeiba odds API | `JV_O4_ODDS_UMATAN` / `JV_O5_ODDS_SANREN` |
| `finish_position` | Netkeiba result HTML scrape | `JV_SE_RACE_UMA.KakuteiJyuni` |
| Payouts (tansho/fukusho/etc.) | Not currently fetched | `JV_HR_PAY` |

## Reference Documents

### Top-Level (jra-van-docs/)
| File | Use |
|------|-----|
| `DataLab422.pdf` | Overview and developer guide |
| `JV-Data4901.pdf` | Data record specification |
| `JV-Link4901.pdf` | Windows API/interface specification |

### SDK Documentation (JVDTLABSDK4902/ドキュメント/)
| File | Use |
|------|-----|
| `JRA-VAN Data Lab.開発ガイド_4.2.2.pdf` | Core SDK development guide |
| `JRA-VAN Data Lab.開発ガイド(イベント C++).pdf` | Native COM/event-driven patterns |
| `JRA-VAN Data Lab.開発ガイド(イベント VB).pdf` | VB/COM lifecycle and event model |
| `JV-Data仕様書_4.9.0.1.pdf` + `.xlsx` | Detailed field specs and byte layouts |
| `JV-Linkインターフェース仕様書_4.9.0.1(Win).pdf` | Full Windows API contract |
| `蓄積系提供データ一覧.xls` | Comprehensive list of available accumulated data feeds |

### SDK Code
| File | Use |
|------|-----|
| `JV-Data構造体/C#版/JVData_Struct.cs` | Best parsing reference for Python port (Shift_JIS byte slicing) |
| `JV-Data構造体/VB2019版/JVData_Structure.vb` | Cross-check for encoding and field order |
| `JV-Data構造体/C++版/JVData_Structure.h` | Low-level struct byte offsets |
| `JV-Data構造体/Delphi7版/JVData_Structure.pas` | Alternate offset cross-check |
| `サンプルプログラム/sample1_VB2019/Form1.vb` | `JVInit` and service-key initialization |
| `サンプルプログラム/sample1_VB2019/Form2.vb` | `JVOpen` → `JVGets` loop → `JVClose` lifecycle |
| `サンプルプログラム/sample1_VC2019/jvlink.h` | Full COM method surface and properties |
| `DataLab.検証ツール/JVDataCheckToolVer2.6.0/` | Validation tool to confirm dataspec and record choices |

## JV-Link Lifecycle (from SDK samples)

```
JVInit(sid)                          // Initialize with application ID
JVSetServiceKey(servicekey)          // Authenticate with subscription key
JVSetSavePath(path)                  // Set download directory
JVSetSaveFlag(flag)                  // Enable/disable auto-save
JVOpen(dataspec, fromdate, option,   // Open data stream
       &readcount, &downloadcount, &timestamp)
loop:
    ret = JVGets(buf, bufsize, &filename)
    if ret > 0  → data record in buf (Shift_JIS, fixed-width)
    if ret == -1 → file boundary, continue
    if ret == 0  → EOF, break
    if ret < -1  → error
JVClose()
```

Key COM properties: `m_servicekey`, `m_savepath`, `m_saveflag`, `m_JVLinkVersion`, `m_TotalReadFilesize`, `m_CurrentReadFilesize`

## Implementation Phases

### Phase 1 — Baseline and Mapping
1. Capture the exact payload shape produced by the current scrape pipeline as the compatibility target.
2. Produce the full field-by-field mapping (table above) including any derived or transformed fields.
3. Read `蓄積系提供データ一覧.xls` to confirm which dataspecs contain the records needed.
4. Decide the ID normalization strategy: preserve Netkeiba-shaped strings by normalizing JRA-VAN composite keys, or introduce a new internal scheme.

### Phase 2 — Architecture Spike
1. Run both integration options against a real trial subscription:
   - **Option A:** `win32com.client.Dispatch("JVDTLabLib.JVLink")` inside Python
   - **Option B:** Thin sidecar (Python or PowerShell) wrapping JV-Link, exposing a local HTTP or named-pipe JSON API
2. Validate a minimal lifecycle for each: initialize → authenticate → open dataspec → read one RA/SE record → decode Shift_JIS correctly → close.
3. Choose the architecture based on stability under server restarts and background-thread execution.

### Phase 3 — Source Abstraction
Introduce a source-agnostic interface without changing `storage.py` or the frontend contract. Target functions in `data_manager.py`:

| Current function | Replace with interface method |
|-----------------|-------------------------------|
| `_get_month_race_ids()` + `_get_race_ids_from_daily_list()` | `fetch_race_ids(date_range)` |
| `keibascraper.load("entry", race_id)` | `fetch_race_card(race_id)` |
| `get_horse_data()` / `fetch_official_name_by_id()` | `fetch_horse_master(horse_id)` |
| `fetch_predictions()` | `fetch_odds(race_id)` |
| `fetch_race_history_by_id()` | `fetch_results(race_id)` |

Retain the Netkeiba adapter as a fallback. Ship each slice behind a `DATA_SOURCE` flag in `config.py`.

### Phase 4 — Vertical Slices
Implement in order, validating each before moving to the next:

1. **Race discovery + metadata** — replace `_get_month_race_ids`, `_get_race_ids_from_daily_list`, and the `keibascraper.load("entry")` call in `fetch_weekend_timeline`
2. **Entries + horse enrichment** — replace `get_horse_data`, `format_entry_data`, and pedigree scraping
3. **Odds** — replace `fetch_predictions` in `data_manager.py` and `_fetch_netkeiba_odds_map` in `routers/races.py`
4. **Finalized results** — replace `fetch_race_history_by_id` and `fetch_result_table_map_by_race_id`

### Phase 5 — Operational Hardening
- Formalize COM registration health check on startup.
- Store the service key via environment variable or a local config (do not commit it).
- Handle first-class failure modes: missing JV-Link install, failed COM registration, invalid credentials, empty feeds, malformed records.
- Add parser unit tests using captured byte buffers for: `RACE_ID`, `JV_RA_RACE`, `JV_SE_RACE_UMA`, `JV_HR_PAY`, `JV_O1_ODDS_TANFUKUWAKU`.

### Phase 6 — Documentation Cutover
- Update `readme.md` with JRA-VAN as primary source, Windows-only prerequisites, subscription requirements, fallback behavior, and operator setup steps.
- Keep OrePro documentation and flows unchanged.

## Verification Checklist

- [ ] Field mapping matrix complete for all race, entry, horse, odds, and result fields
- [ ] Minimal JV-Link lifecycle spike succeeds (both options tested)
- [ ] RACE_ID → app `race_id` normalization confirmed and tested
- [ ] Parser tests pass for all required record types using known byte buffers
- [ ] Known race day replayed through new source, SQLite output matches current schema
- [ ] UI loads timeline data without frontend contract changes
- [ ] Failure modes tested: missing install, bad credentials, empty feeds, malformed records
- [ ] Datasepcs validated with `JVDataCheckToolVer2.6.0` tool

## Open Decisions

1. **ID normalization:** Preserve Netkeiba-shaped 12-digit strings (reconstruct from RACE_ID components) or switch to JRA-VAN native keys with a migration for existing cached data.
2. **Odds centralization:** The current app calls the Netkeiba odds API from both `data_manager.py` and `routers/races.py`. Consider folding both into a single source interface during migration rather than preserving both call paths.
3. **Pedigree depth:** JRA-VAN can provide deeper pedigree data than the current sire/dam/BMS-only model. Decide scope before locking the horse enrichment interface.

## Files to Modify

| File | Change |
|------|--------|
| `config.py` | Add `DATA_SOURCE`, `JVLINK_SERVICE_KEY`, `JVLINK_SAVE_PATH`; remove Netkeiba URLs once cutover is complete |
| `data_manager.py` | Extract scrape logic into adapter pattern; wire up JRA-VAN adapter |
| `routers/scrape.py` | Update log message from "Netkeiba Scraper" to "JRA-VAN Sync" |
| `routers/races.py` | Route odds helper calls through source interface |
| `storage.py` | No changes expected in Phase 1–4; revisit for schema improvements later |
| `readme.md` | Add Windows prerequisites, subscription requirements, operator setup |
| *(new)* `data_sources/base.py` | Source interface definition |
| *(new)* `data_sources/netkeiba.py` | Existing logic extracted as Netkeiba adapter |
| *(new)* `data_sources/jra_van.py` | JRA-VAN adapter implementation |
| *(new)* `data_sources/jvlink_sidecar.py` *(or exe)* | Sidecar if Option B is chosen |
