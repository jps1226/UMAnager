using System.Runtime.InteropServices;

namespace UMAnager.Sidecar.Com;

/// <summary>
/// Surgical IJVLink interface using IDispatch (late-binding, DispId-based).
/// Order-independent and safe—avoids vtable fragility of InterfaceIsDual.
/// Only includes methods Phase 2 needs: JVSetSavePath, JVInit, JVOpen, JVStatus, JVRead, JVClose, JVSkip, JVFiledelete, and JVGets (fallback).
/// </summary>
[ComImport]
[Guid("2AB1774C-0C41-11D7-916F-0003479BEB3F")]
[InterfaceType(ComInterfaceType.InterfaceIsIDispatch)]
internal interface IJVLink
{
    [DispId(1)]
    int JVSetSavePath(string savepath);

    // ParentHWnd property (propput) — must be set before JVOpen to prevent dialog hangs
    [DispId(2)]
    int ParentHWnd { set; }

    [DispId(4)]
    int JVInit(string sid);

    [DispId(7)]
    int JVOpen(
        string ds,
        string fd,
        int opt,
        ref int rc,
        ref int dc,
        out string ts);

    [DispId(8)]
    int JVStatus();

    [DispId(9)]
    int JVRead(
        [MarshalAs(UnmanagedType.BStr)] out string buff,
        out int size,
        [MarshalAs(UnmanagedType.BStr)] out string filename);

    [DispId(5)]
    int JVClose();

    [DispId(19)]
    void JVSkip();

    [DispId(12)]
    int JVFiledelete(string filename);

    // JVGets: ref object buffer for raw byte data (kmy-keiba pattern).
    // Caller pre-allocates byte[] and passes as object; COM returns data in-place.
    [DispId(22)]
    int JVGets(
        ref object buff,
        int size,
        [MarshalAs(UnmanagedType.BStr)] out string filename);

    // JVRTOpen: real-time (速報系) data fetch. Blocks until data is ready — no JVStatus polling needed.
    // dataspec: e.g. "0B31" (odds 単複枠 — per-race unit). key for "0B31" MUST be a 16-char
    // race ID "YYYYMMDDJJKKHHRR" (or 12-char "YYYYMMDDJJRR"). 8-char date is rejected with rc=-114.
    [DispId(10)]
    int JVRTOpen(
        [MarshalAs(UnmanagedType.BStr)] string dataspec,
        [MarshalAs(UnmanagedType.BStr)] string key);
}
