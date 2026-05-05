# Critical Corrections to UMAnager JRA-VAN Integration

Based on rigorous external review against official JRA-VAN SDK and JV-Data specifications, the following **architectural corrections are mandatory before Phase 2 proceeds**.

---

## 1. Shift-JIS Handling: Switch from JVRead to JVGets (CRITICAL)

### Previous (WRONG):
```
JVRead → receives corrupt UTF-16 string → custom hex decoder
```

### Corrected (RIGHT):
```
JVGets → returns raw byte[] → System.Text.Encoding.GetEncoding("shift_jis").GetString(byte[])
```

**Why:** `JVRead` in C# COM interop corrupts CP932 byte streams during UTF-16 marshalling. `JVGets` bypasses this entirely by returning raw bytes directly, which C# can safely decode without loss.

**Action in Code:**
- Remove any hex-decoder logic
- Update `JVLinkClient.cs` to call `JVGets(out byte[] buff, ...)` instead of `JVRead(...)`
- Decode safely: `Encoding.GetEncoding("shift_jis").GetString(buffer)`
- This single fix unblocks Phase 2

---

## 2. Pedigree Updates: CK ≠ Pedigree; Use UM from TCOV/RCOV (CRITICAL)

### Previous (WRONG):
```
Phase 3: Parse CK (pedigree) records → UPDATE horses table
```

### Corrected (RIGHT):
```
CK = 出走別着度数 (Placement Statistics), NOT pedigree
New horse pedigrees come from UM records (競走馬マスタ)
For weekly races: fetch dataspec "TOKURACETCOVSNPN" or "TOKURACETUVCOVSNPN"
Parse UM records bundled in TCOV (特別登録馬情報補てん) or RCOV (レース情報補てん) streams
```

**Why:** 
- `CK` records contain historical placement data only
- Debuting horses in weekly races need their pedigree from UM
- Option=2 (weekly) does not support BLDN directly
- TCOV/RCOV supplemental streams bundle UM, KS (jockey), CH (trainer) for that week's entrants

**Action in Code:**
- Update `Worker.cs` Phase 3 dataspec from `"TOKURACESNPN"` to `"TOKURACETCOVSNPN"`
- Parse UM records from TCOV/RCOV (same UM format as Phase 2)
- Insert/update horses table with any new debuts + pedigree
- Leave CK parser for reference only; it's not pedigree

---

## 3. No "FK" Records Exist; Use JVRTOpen + "0B12" for Live Results (CRITICAL)

### Previous (WRONG):
```
Phase 5: JVWatchEvent → JVOpen("FK", Option=2) → FK records
```

### Corrected (RIGHT):
```
Phase 5 Workflow:
  1. Register JVWatchEvent listener for JVEvtPay (Payoff event)
  2. When event fires, extract race key from event
  3. Call JVRTOpen(race_key, "0B12") [Fast Race Info spec]
  4. Parse records: RA (race meta), SE (finishes/times), HR (払戻 - payoffs)
  5. Update race_entries with finish_position, finish_time, payoff_*
```

**Why:**
- There is no "FK" record type in JRA-VAN
- Real-time results come through JVEvtPay event → JVRTOpen workflow
- "0B12" spec returns race metadata + finish order + payoff amounts
- SE records in "0B12" stream have finish position, time; HR records have win/place/show payoffs

**Action in Code:**
- Remove "FK" from any documentation
- Implement `JVWatchEvent` listener in Worker for JVEvtPay
- Add `JVRTOpen` method to JVLinkClient
- Parse "0B12" spec records (RA, SE, HR) for live results
- Update race_entries: `finish_position`, `finish_time_hundredths`, `payoff_win/place/show`

---

## 4. Option=4 Dialog Behavior (Phase 2 Setup Note)

### Previous (Assumed):
Option=4 = completely silent, non-interactive

### Corrected:
```
Option=4 shows a dialog on FIRST EXECUTION ONLY:
  "Start Kit (CD/DVD)" OR "Download all"
  
Since JRA-VAN discontinued CD/DVDs in March 2022:
  → User must manually click "Download all"
  → Configuration saved to Windows registry
  → Subsequent Option=4 calls are silent (uses saved config)
```

**Impact:**
- Initial bootstrap will be interactive (user clicks dialog)
- Subsequent runs are silent
- Full "Download all" can take several minutes for 5,800+ horses + decades of history
- UI should account for multi-minute bootstrap on first run

**Action:**
- Document this behavior in deployment guide
- Plan for user interaction during initial setup
- After first successful bootstrap, subsequent runs are automated

---

## 5. Error Handling: JVGets Can Return -502 (Download Failure)

### Previous (Missing):
No mention of -502 handling

### Corrected:
```
JVGets/JVRead can return:
  -502 = Download Failure (network timeout, server congestion)
  
Required Behavior:
  1. Catch -502 return code
  2. Call JVClose()
  3. Wait exponential backoff (e.g., 30s, 60s, 120s)
  4. Retry JVOpen → JVGets
  5. Log all attempts for troubleshooting
```

**Action in Code:**
- Update JVLinkClient to detect -502 returns
- Implement retry loop with backoff in Worker.ExecuteAsync
- Log all retry attempts to Serilog

---

## 6. Bootstrap FromTime Format (Phase 2 Setup)

### Previous (Unclear):
`FromTime="00000000000000"` — works?

### Corrected:
```
Standard practice: Use valid historical date
  "19860101000000" (start of JRA-VAN era, 1986-01-01 00:00:00)
  OR current date for fresh setup
  
For master data (UM), JRA-VAN fetches all records regardless of FromTime.
For incremental sync, always use last known timestamp.
```

**Action:**
- Use `"19860101000000"` for initial bootstrap
- After bootstrap, always persist and reuse `LastFileTimestamp`

---

## Summary of Phase 2-5 Blockers Fixed

| Phase | Blocker | Fix |
|-------|---------|-----|
| 2 | Hex-encoded Shift-JIS myth | Switch to JVGets + native shift_jis decoder |
| 2 | Option=4 dialog assumption | Document user interaction required on first run |
| 3 | CK = pedigree (wrong) | Parse UM from TCOV/RCOV instead |
| 3 | Wrong dataspec | Use `"TOKURACETCOVSNPN"` or `"TOKURACETUVCOVSNPN"` |
| 5 | FK records don't exist | Implement JVEvtPay → JVRTOpen("0B12") workflow |
| All | No -502 retry logic | Add exponential backoff retry for download failures |

---

## Code Changes Required

### 1. JVLinkClient.cs
- Add `JVGets(out byte[] buff, ...)` call
- Remove hex-decoder logic
- Add -502 retry detection
- Add JVRTOpen method for Phase 5

### 2. Worker.cs
- Update Phase 3 dataspec to `"TOKURACETCOVSNPN"`
- Implement -502 retry loop with backoff
- Phase 5: Add JVWatchEvent listener for JVEvtPay
- Phase 5: Add JVRTOpen call + "0B12" parsing

### 3. Documentation
- Remove "FK" record references
- Document Option=4 dialog behavior
- Add retry strategy for -502
- Clarify TCOV/RCOV for pedigree updates

---

## Next Steps

1. **Immediate**: Update JVLinkClient to use JVGets
2. **Phase 2**: Test with real JRA-VAN data, validate horse count + pedigree completeness
3. **Phase 3**: Implement TCOV/RCOV parsing with UM record extraction
4. **Phase 5**: Implement JVEvtPay listener + JVRTOpen + "0B12" parsing
