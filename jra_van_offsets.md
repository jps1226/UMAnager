# JRA-VAN Data Lab API Backend Reference

This cheat sheet provides the technical mapping required to fulfill the `BACKEND_API_SPEC.md` contract using the JRA-VAN JV-Link COM object. All byte offsets and COM signatures are derived from the official JV-Data Specifications and the JVDTLab.IDL type library.

---

## 0. Data Spec Categories & JVOpen vs JVRTOpen

**Critical Distinction:**

JV-Link has TWO different spec categories, each with its own method:

### Accumulation Data (蓄積系データ) → Use `JVOpen`
- `"RACE"` → RA + SE records
- `"DIFF"` / `"DIFN"` → UM records  
- `"YSCH"` → Race schedules
- Must use `JVOpen(dataspec, fromTime, option, ref readcount, ref downloadcount, out lasttime)`

**JVOpen Option Modes:**
- **`option=1`** → Normal/Incremental (requires prior setup; small payloads)
- **`option=2`** → This Week's Data (downloads if missing; respects weekly schedule)
- **`option=3`** → Setup Mode (triggers UI dialog for first-time setup)
- **`option=4`** → Dialog-less Setup (suppresses UI after first execution)

### Real-Time Data (速報系データ) → Use `JVRTOpen` ⭐ Phase 0 Path
- `"0B15"` → Real-time Race Info Runner List (速報レース情報 出走馬名表)
- **Blocks until complete** — no polling needed
- **Key format:** `"YYYYMMDD"` for race-day-level requests
- Call: `int returnCode = jvLink.JVRTOpen("0B15", "20260509");` // Saturday
- On success (returnCode == 0): immediately call `JVRead()` / `JVGets()`

---

## 3. JVRead Method (Exact Official Signature)

**Return Value:** 
- `> 0` = byte count read
- `-1` = file boundary (continue looping)
- `0` = EOF

**C# Signature:**
```csharp
[DispId(9)]
int JVRead(
    [MarshalAs(UnmanagedType.BStr)] ref string buff,    // Pre-allocate with '\0'
    int size,                                             // 150000 or 60000 typical
    [MarshalAs(UnmanagedType.BStr)] out string filename);
```

**Key Details:**
- `buff`: Must be **pre-allocated** string (e.g., `new string('\0', 150000)`) before calling
- `size`: Standard 32-bit `int` (not `long`). Set to buffer size.
- `filename`: Receives the name of current file being read
- Call pattern: `int bytesRead = jvLink.JVRead(ref buff, buff.Length, out filename);`

---

## 1. COM Interop Critical Issue: Vtable vs. IDispatch

**IMPORTANT DISCOVERY:**
When using `[InterfaceType(ComInterfaceType.InterfaceIsDual)]`, the CLR maps C# interface methods to the COM vtable in **exact sequential declaration order**. Skipping methods (e.g., not declaring `JVSetUIProperties`, `JVStatus`) causes subsequent methods to jump to wrong memory addresses, resulting in `0xC0000005` access violations.

**SOLUTION:** Use `[InterfaceType(ComInterfaceType.InterfaceIsIDispatch)]` for late binding via `IDispatch::Invoke`. This routes calls using `[DispId(x)]` attributes instead of vtable offsets. Method declaration order becomes irrelevant.

---

## 2. COM Interop Signatures (from JVDTLab.IDL)

**GUIDs (from type library):**
- **Coclass CLSID:** `{2AB1774D-0C41-11D7-916F-0003479BEB3F}` (JVLink class)
- **Interface IID:** `{2AB1774C-0C41-11D7-916F-0003479BEB3F}` (IJVLink interface — dual, oleautomation)

**C# [ComImport] Interface:**

```csharp
[ComImport]
[Guid("2AB1774D-0C41-11D7-916F-0003479BEB3F")]
[ProgId("JVDTLab.JVLink.1")]
[ClassInterface(ClassInterfaceType.None)]
internal class JvLink { }

[ComImport]
[Guid("2AB1774C-0C41-11D7-916F-0003479BEB3F")]
[InterfaceType(ComInterfaceType.InterfaceIsDual)]
internal interface IJvLink
{
    [DispId(4)]
    int JVInit([MarshalAs(UnmanagedType.BStr)] string sid);

    [DispId(7)]
    int JVOpen(
        [MarshalAs(UnmanagedType.BStr)] string dataspec,
        [MarshalAs(UnmanagedType.BStr)] string fromdate,
        int option,
        [In, Out] ref int readcount,
        [In, Out] ref int downloadcount,
        [MarshalAs(UnmanagedType.BStr)] out string lastfiletimestamp);

    [DispId(8)]
    int JVStatus();

    // JVRead: Simpler than JVGets — returns BSTR directly instead of buffer management
    [DispId(9)]
    int JVRead(
        [MarshalAs(UnmanagedType.BStr)] out string buff,
        out int size,
        [MarshalAs(UnmanagedType.BStr)] out string filename);

    // JVGets: Takes VARIANT * (not IntPtr). Use JVRead for Phase 0.
    [DispId(0x16)]
    int JVGets(
        [In, Out] ref object buff,  // VARIANT in IDL
        int size,
        [MarshalAs(UnmanagedType.BStr)] out string filename);

    [DispId(5)]
    int JVClose();
}
```

**Key Differences from Earlier Assumptions:**
- JVOpen parameters are `[in, out] ref int` (not plain `ref int`).
- **JVRead exists** and is simpler: returns BSTR buffers directly.
- **JVGets takes `VARIANT *`**, not `IntPtr`. For Phase 0, use **JVRead** instead.

---

## 2. RA Record (Race Details) Offsets

The RA record provides the core race metadata required by the `/api/races` contract.

| Field Name | 1-Indexed Start Byte | Length | Record Type |
|:---|:---|:---|:---|
| Race Year (開催年) | 12 | 4 | RA |
| Race Month/Day (開催月日) | 16 | 4 | RA |
| Venue Code / JyoCD (競馬場コード) | 20 | 2 | RA |
| Race Number (レース番号) | 26 | 2 | RA |
| Race Name / Hondai (競走名本題) | 33 | 60 | RA |
| Start Time (発走時刻) | 874 | 4 | RA |

---

## 3. SE Record (Race Entry) Offsets

The SE record maps to the basic entry list (post positions and odds).

| Field Name | 1-Indexed Start Byte | Length | Record Type |
|:---|:---|:---|:---|
| Bracket Number (枠番) | 28 | 1 | SE |
| Post Position (馬番) | 29 | 2 | SE |
| Horse ID / KettoNum (血統登録番号) | 31 | 10 | SE |
| Finish Position (確定着順) | 335 | 2 | SE |
| Win Odds (単勝オッズ) | 449 | 5 | SE |
| Favorite Rank (単勝人気順) | 454 | 2 | SE |

---

## 4. UM Record (Horse Master) Offsets

The UM record is necessary for joining romanized names, pedigree info, and win/start statistics.

| Field Name | 1-Indexed Start Byte | Length | Record Type |
|:---|:---|:---|:---|
| Horse ID (血統登録番号) | 12 | 10 | UM |
| Romanized Name (馬名欧字) | 119 | 60 | UM |

### Pedigree Array (3代血統情報)

The pedigree data is stored as a sequential array of 14 generations/slots.

- **Array Start Byte:** 205
- **Slot Length:** 46 bytes per slot (10 bytes ID + 36 bytes Name)
- **Array Indices:**
  - **Sire (父):** Index 0
  - **Dam (母):** Index 1
  - **BMS / Broodmare Sire (母父):** Index 4

### Win/Start Record Arrays (着回数)

To calculate the "W/S" string, sum the cumulative finish positions. Each array is 18 bytes long (comprising six 3-byte integers representing 1st, 2nd, 3rd, 4th, 5th, and unplaced finishes).

- **Turf Finishes (芝着回数):** Start Byte 1107, Length 18
- **Dirt Finishes (ダート着回数):** Start Byte 1125, Length 18
- **Jump Finishes (障害着回数):** Start Byte 1143, Length 18

---

## 5. Code Tables Reference

To satisfy the `place` string requirement (e.g., "TOKYO"), map the 2-digit Venue Code (`JyoCD`) from the RA record using Official Code Table 2001.

| Track Code (JyoCD) | Romanized Translation |
|:---|:---|
| 01 | SAPPORO |
| 02 | HAKODATE |
| 03 | FUKUSHIMA |
| 04 | NIIGATA |
| 05 | TOKYO |
| 06 | NAKAYAMA |
| 07 | CHUKYO |
| 08 | KYOTO |
| 09 | HANSHIN |
| 10 | KOKURA |
