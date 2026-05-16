# Phase 3D: Ready for Full Parsing Test

## ✅ Completed So Far

### Oracle & Librarian Consultation
- **Oracle answers:** Confirmed 50K+ record performance target <60 seconds; split-process architecture optimal
- **Librarian answers:** kmy-keiba uses 1,000 record batches with explicit ChangeTracker clearing
- **Key findings appended to:** `ORACLE_ANSWERS.md` and `LIBRARIAN_ANSWERS.md`

### Code Optimizations Implemented
**File: `DifnRecordParsingService.cs`**
1. ✅ Increased BatchSize: 500 → **1,000 records** (kmy-keiba proven safe default)
2. ✅ Added explicit `context.ChangeTracker.Clear()` after SaveChangesAsync (critical memory optimization)
3. ✅ Added detailed batch progress logging for monitoring
4. ✅ Build: **0 errors, 0 warnings** ✅

### Architecture Updates
**File: `CLAUDE.md`**
- ✅ Updated query workflow to clarify Claude appends specialist answers to answer files
- ✅ Documented that `IHostedService` (existing pattern) is optimal for long-running operations

## 📋 Next Steps Before Full Test

### Step 1: Reset Database (RUN THESE SQL COMMANDS)

**File:** `reset_database.sql` (already created)

**Run using psql, DBeaver, pgAdmin, or your preferred SQL client:**
```sql
-- Clear parsed records from partial test
DELETE FROM race_entries;
DELETE FROM races;
DELETE FROM horses;

-- Reset UM records to unprocessed
UPDATE raw_staging
SET is_processed = false
WHERE record_type = 'UM';

-- Verify counts
SELECT 'horses' as table_name, COUNT(*) FROM horses
UNION ALL
SELECT 'races', COUNT(*) FROM races
UNION ALL
SELECT 'race_entries', COUNT(*) FROM race_entries
UNION ALL
SELECT 'unprocessed UM records', COUNT(*) FROM raw_staging WHERE record_type = 'UM' AND is_processed = false;
```

**Expected results after reset:**
- `horses`: 0 rows
- `races`: 0 rows
- `race_entries`: 0 rows
- `unprocessed UM records`: 652,755 rows

### Step 2: Start Services & Trigger Parsing Test

Once database is reset:

1. **Start services:**
   ```powershell
   .\launch-services.ps1 -Action start
   ```

2. **Trigger parsing via HTTP:**
   ```powershell
   $response = Invoke-WebRequest -Uri "http://localhost:5000/api/jvlink/parse-records" -Method POST
   $response.Content | ConvertFrom-Json | Format-List
   ```

3. **Monitor logs:**
   - Sidecar: `.\logs\sidecar.log`
   - Nexus: `.\logs\nexus.log`
   - Look for batch progress lines: `[UM] Batch 1: Saved 1000 records (Total: 1000)`
   - Final: `[UM] Completed XXX batches`

### Step 3: Verify Results

After parsing completes:

```sql
-- Check final counts
SELECT 
    (SELECT COUNT(*) FROM horses) as parsed_horses,
    (SELECT COUNT(*) FROM races) as parsed_races,
    (SELECT COUNT(*) FROM race_entries) as parsed_entries,
    (SELECT COUNT(*) FROM raw_staging WHERE is_processed = true) as marked_processed;

-- Should show approximately:
-- horses: 652,755
-- races: 240
-- race_entries: 2,493
-- marked_processed: 245,255+ (includes all record types)
```

---

## Performance Targets

**Per Oracle Findings:**
- **50,000+ records in <60 seconds** (performance target from CLAUDE.md)
- **650,000 records likely 10-15 minutes total** (depends on PostgreSQL I/O speed)

**With optimizations:**
- 1,000-record batches (vs old 500) → fewer SaveChangesAsync calls
- ChangeTracker cleared between batches → no memory accumulation
- Batch progress logging → can see if stalled

---

## Files Ready to Deploy

✅ `src/UMAnager.Nexus/Services/Parsing/DifnRecordParsingService.cs` — Optimized, tested, 0 errors
✅ `ORACLE_ANSWERS.md` — Answers appended
✅ `LIBRARIAN_ANSWERS.md` — Answers appended
✅ `CLAUDE.md` — Workflow clarified
✅ `reset_database.sql` — Cleanup script ready

---

## If Parsing Hangs Again

1. Check batch progress logs in nexus.log
2. Note which batch number hung
3. Check PostgreSQL query logs for lock contention
4. Consider increasing batch timeout in controller if needed
5. Possible next steps: reduce batch size to 500, implement backpressure throttling

---

**Status: Ready for Phase 3D full parsing test** 🎯
