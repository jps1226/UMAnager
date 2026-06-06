// ============================================================
// FILE: RtResultsStreamHandler.cs
// LAYER: Sidecar JV-Link stream handler
// PURPOSE: JVRTOpen("0B12", yyyyMMdd) → streams RA+SE+HR result records for every venue on that
//          date back over the pipe. 0B12's provision unit is per-day (one call per date).
// KEY DEPENDENCIES: IJVLink, SidecarPipeClient.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Text;
using UMAnager.Sidecar.Com;
using UMAnager.Sidecar.Pipes;

namespace UMAnager.Sidecar.JvLink;

/// <summary>
/// Real-time results stream via <c>JVRTOpen("0B12", yyyyMMdd)</c>.
/// Unlike <c>0B31</c> (odds, per-race), <c>0B12</c>'s provision unit is per-day:
/// a single call returns RA + SE + HR records for every venue on that date.
/// Oracle Q14/Q16 (2026-05-16).
/// </summary>
internal static class RtResultsStreamHandler
{
    public static async Task<(int Stored, int Skipped)> StreamAsync(
        IJVLink jvLink, SidecarPipeClient pipe, string yyyyMmDd, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(yyyyMmDd) || yyyyMmDd.Length != 8)
        {
            Console.WriteLine($"[Sidecar] STREAM_RESULTS: malformed date '{yyyyMmDd}' (must be 8 chars).");
            return (0, 1);
        }

        Console.WriteLine($"[Sidecar] JVRTOpen(0B12, {yyyyMmDd})...");
        int rc;
        try { rc = jvLink.JVRTOpen("0B12", yyyyMmDd); }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[Sidecar] JVRTOpen(0B12, {yyyyMmDd}) threw: {ex.Message}");
            return (0, 1);
        }

        if (rc != 0)
        {
            // rc=-1: no applicable data (race day has no results yet). Common pre-race; skip quietly.
            if (rc == -1) Console.WriteLine($"[Sidecar]   rc=-1 (no results yet for {yyyyMmDd}).");
            else          Console.WriteLine($"[Sidecar]   JVRTOpen(0B12, {yyyyMmDd}) returned rc={rc}.");
            return (0, 1);
        }

        int stored = 0;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                byte[] buffBytes = new byte[2750];
                object buffObj = buffBytes;

                int readRc = jvLink.JVGets(ref buffObj, 2750, out _);
                buffBytes = (byte[])buffObj;

                if (readRc > 0)
                {
                    if (buffBytes.Length >= 2)
                    {
                        var recType = Encoding.ASCII.GetString(buffBytes[..2]);
                        await pipe.SendRawRecordAsync(buffBytes, ct);
                        stored++;

                        if (stored % 50 == 0)
                            Console.WriteLine($"[Sidecar]   RESULTS progress: {stored} records (last type: {recType})");
                    }
                }
                else if (readRc == 0) break;
                else if (readRc == -1) continue;
                else
                {
                    Console.WriteLine($"[Sidecar]   RESULTS unexpected rc={readRc}, stopping.");
                    break;
                }
            }
        }
        finally
        {
            try { jvLink.JVClose(); } catch { }
        }

        Console.WriteLine($"[Sidecar] STREAM_RESULTS finished for {yyyyMmDd}. Stored={stored}.");
        return (stored, 0);
    }
}
