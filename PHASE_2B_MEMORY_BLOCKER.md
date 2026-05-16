# Phase 2B: Memory Blocker & Research Notes

**Date:** 2026-05-12  
**Status:** BLOCKED — OutOfMemoryException during JVRead loop  
**Severity:** CRITICAL — Cannot proceed to Phase 3 until resolved

---

## The Blocker

**What Works:**
- ✅ JVSetSavePath call succeeds (rc=0)
- ✅ JVOpen("DIFN", ...) succeeds (rc=0) and reports ~48 files to download
- ✅ Named Pipe handshake (Sidecar ↔ Nexus) works perfectly
- ✅ PostgreSQL connection and schema are ready

**What Fails:**
- ❌ First JVRead call throws `System.OutOfMemoryException`
- ❌ Exception occurs during or immediately after `jvLink.JVRead(out var buff, out var size, out var filename)`
- ❌ No records reach `raw_staging` table
- ❌ Sidecar crashes; pipe communication halts

**Error Sequence:**
```
[Sidecar] JVOpen DIFN: rc=0, records≈48, files=42, ts=20260511133754
[Sidecar] DIFN stream failed: Exception of type 'System.OutOfMemoryException' was thrown.
[Sidecar] Pipe error: Unable to read beyond the end of the stream.
```

---

## Root Cause Analysis

### Hypothesis 1: BSTR Marshaling Allocation (MOST LIKELY)

The [ComImport] interface defines JVRead as:
```csharp
[DispId(9)]
int JVRead(
    [MarshalAs(UnmanagedType.BStr)] out string buff,
    out int size,
    [MarshalAs(UnmanagedType.BStr)] out string filename);
```

When the .NET runtime unmarshals the BSTR returned by COM, it allocates a .NET `string` of the BSTR's length. **If the BSTR is larger than available heap memory, the allocation fails with OutOfMemoryException.**

**Evidence:**
- The exception occurs during the `out string buff` marshaling, not in our C# code
- We never reach the byte-copying logic (no debug output logged before exception)
- The exception is `System.OutOfMemoryException`, not a custom exception from our code

### Hypothesis 2: Buffer Size Misunderstanding

The IDL shows:
```idl
HRESULT JVRead(
    [out] BSTR* buff,
    [out] long* size,
    [out] BSTR* filename,
    [out, retval] long* xxret);
```

The `[out] long* size` parameter tells us the byte count of the record. But if JV-Link is returning:
- `buff` = pointer to a multi-megabyte buffer
- `size` = offset into that buffer (not the record size)

Then our assumption that `size` is the record length is wrong, and we're trying to allocate a huge string.

---

## What We've Tried

| Approach | Result | Notes |
|---|---|---|
| Simple byte array allocation (v1) | OOM | String marshaling happens before our code runs |
| Size validation + GC.Collect (v2) | OOM | Validation code never reached; marshaling fails first |
| Fixed-size byte buffer + char-by-char copy (v3) | OOM | Same; marshaling is the bottleneck |

---

## Proposed Solutions (Priority Order)

### Option A: Unsafe Marshaling (Recommended for Next Session)

Use `IntPtr` instead of `string` to receive the BSTR without allocating a managed string:

```csharp
[DispId(9)]
int JVRead(
    [MarshalAs(UnmanagedType.BStr)] out IntPtr buff,    // Don't allocate string
    out int size,
    [MarshalAs(UnmanagedType.BStr)] out IntPtr filename);

// In loop:
int readRc = jvLink.JVRead(out IntPtr buffPtr, out int size, out IntPtr filenamePtr);
if (readRc > 0) {
    // Manually copy BSTR bytes without full marshaling
    byte[] record = new byte[Math.Min(size, 2750)];
    Marshal.Copy(buffPtr, record, 0, record.Length);
    Marshal.FreeBSTR(buffPtr);
    Marshal.FreeBSTR(filenamePtr);
    // ... process record ...
}
```

**Pros:** Avoids the .NET string allocation that's causing OOM  
**Cons:** Requires unsafe code; manual BSTR cleanup  
**Effort:** ~30 min implementation + testing  

### Option B: Request Smaller Data Chunks

Use a different JV-Link method (if available) that returns smaller records:
- Query the Librarian: "Does JV-Link have an API to request individual records by ID instead of streaming?"
- Alternative: Use JVRTOpen (Real-Time) instead of JVOpen for smaller payloads

**Pros:** No unsafe code; leverages existing API  
**Cons:** Might not exist; could be slower  
**Effort:** Research phase

### Option C: Memory Reservation & Pre-allocation

Set aside a large memory pool before calling JVRead:
```csharp
GCSettings.IsServerGC = true;
GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
// Pre-allocate headroom
byte[] reserve = new byte[500 * 1024 * 1024]; // 500 MB buffer
reserve = null; // Release
```

**Pros:** Simple; doesn't require API changes  
**Cons:** Hacky; may not scale  
**Effort:** 5 min, but low confidence in success

---

## Key Facts from Oracle & Librarian

| Fact | Source | Impact |
|---|---|---|
| JVSetSavePath DispId = 1 (not 6) | IDL extract (JVDTLab.IDL) | CRITICAL — was causing parameter mismatch before fix |
| JVOpen DispId = 7, JVRead DispId = 9 | IDL extract | All other DispIds are correct |
| DIFN records are fixed-length 2750 bytes | JRA-VAN docs (Oracle) | Allows size validation; records should be predictable |
| No VARIANT or SAFEARRAY in JVRead | IDL extract | BSTR parameters only; no complex types |
| JV-Link is version 1.18 (JVDTLab.dll v1.18) | IDL metadata | May have known bugs; consider upgrading |

---

## Critical Context for Next Session

**Do NOT change:**
- DispIds: JVInit(4), JVClose(5), JVSetSavePath(1), JVOpen(7), JVStatus(8), JVRead(9), JVFiledelete(12), JVSkip(19)
- InterfaceType: Must remain `InterfaceIsIDispatch` (IsDual causes v-table violations)
- All [MarshalAs(UnmanagedType.BStr)] attributes are correct

**Must investigate:**
- Why is the BSTR buffer so large? Ask Oracle: "What is the typical size of a BSTR buffer returned by JVRead in the DIFN stream?"
- Is there a JV-Link API call to get just the record size before reading the full buffer?
- Does JV-Link have a 32-bit memory limitation that causes huge allocations?

---

## Files Modified This Session

- `src/UMAnager.Sidecar/Com/IJVLink.cs` — Fixed DispId(1) for JVSetSavePath
- `src/UMAnager.Sidecar/JvLink/DifnStreamHandler.cs` — Added size validation and fixed-buffer logic (didn't resolve OOM)
- `src/UMAnager.Nexus/appsettings.json` — PostgreSQL connection verified working
- PostgreSQL `raw_staging` table — Confirmed schema is correct; no data yet

---

## Next Steps (for new session)

1. **Query Oracle:** "What is the typical BSTR buffer size returned by JVRead? Is there a maximum?"
2. **Implement Option A:** Switch to unsafe IntPtr marshaling with manual BSTR cleanup
3. **Test with small data:** Run JVOpen with a restricted date range (e.g., `fromdate="20260501000000"`) to see if smaller datasets avoid OOM
4. **Monitor memory:** Add heap size logging before/after each JVRead call
5. **Fall back if needed:** Switch to Option B (alternative JV-Link API) if marshaling approach fails

---

## Success Criteria for Phase 2 Completion

- [ ] First JVRead call completes without OutOfMemoryException
- [ ] Records appear in PostgreSQL `raw_staging` table
- [ ] Total row count > 1000 (typical: 3000–5000 for full DIFN)
- [ ] Record types present: UM, RA, SE, KS, CH, RC
- [ ] No marshaling errors in Sidecar console
- [ ] Nexus logs show "Flushed batch to raw_staging" messages

