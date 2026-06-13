// ============================================================
// FILE: RtRaceCardStreamHandler.cs
// LAYER: Sidecar JV-Link stream handler
// PURPOSE: JVRTOpen("0B15", yyyyMMdd) → streams the real-time race card (RA+SE) for every venue on
//          that date back over the pipe. 0B15 (速報レース情報〔出走馬名表〜〕) is the officially-
//          sanctioned stream for the post-draw (AWAITING_POSTS) window: it pushes the finalized SE
//          records (DataStatus=2, post positions drawn) the moment they publish, with none of the
//          option=2 "this week" batch-sync lag. Oracle 2026-06-04. Provision unit is per-day.
// KEY DEPENDENCIES: IJVLink, SidecarPipeClient.
// LAST DOCUMENTED: 2026-06-04
// ============================================================
using System.Text;
using UMAnager.Sidecar.Com;
using UMAnager.Sidecar.Pipes;

namespace UMAnager.Sidecar.JvLink;

/// <summary>
/// Real-time race-card stream via <c>JVRTOpen("0B15", yyyyMMdd)</c>. Like <c>0B12</c> (results),
/// 0B15's provision unit is per-day: a single call returns RA + SE records for every venue on that
/// date. Used during AWAITING_POSTS to grab the finalized card the instant posts are drawn, instead
/// of waiting on the lagging option=2 "this week" stream. Oracle Q (2026-06-04).
/// </summary>
internal static class RtRaceCardStreamHandler
{
    public static async Task<(int Stored, int Skipped)> StreamAsync(
        IJVLink jvLink, SidecarPipeClient pipe, string yyyyMmDd, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(yyyyMmDd) || yyyyMmDd.Length != 8)
        {
            Console.WriteLine($"[Sidecar] STREAM_RTCARD: malformed date '{yyyyMmDd}' (must be 8 chars).");
            return (0, 1);
        }

        Console.WriteLine($"[Sidecar] JVRTOpen(0B15, {yyyyMmDd})...");
        int rc;
        try { rc = jvLink.JVRTOpen("0B15", yyyyMmDd); }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[Sidecar] JVRTOpen(0B15, {yyyyMmDd}) threw: {ex.Message}");
            return (0, 1);
        }

        if (rc == JvReturnCodes.Maintenance)
        {
            Console.WriteLine($"[Sidecar]   JRA-VAN server under maintenance (rc={rc}). Backing off.");
            return (JvReturnCodes.MaintenanceStored, 0);
        }
        if (rc != 0)
        {
            // rc=-1: no applicable data (race card not yet announced for this date). Common before
            // Thursday's entry-name announcement; skip quietly so the orchestrator just retries.
            if (rc == -1) Console.WriteLine($"[Sidecar]   rc=-1 (no race card yet for {yyyyMmDd}).");
            else          Console.WriteLine($"[Sidecar]   JVRTOpen(0B15, {yyyyMmDd}) returned rc={rc}.");
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
                            Console.WriteLine($"[Sidecar]   RTCARD progress: {stored} records (last type: {recType})");
                    }
                }
                else if (readRc == 0) break;
                else if (readRc == -1) continue;
                else
                {
                    Console.WriteLine($"[Sidecar]   RTCARD unexpected rc={readRc}, stopping.");
                    break;
                }
            }
        }
        finally
        {
            try { jvLink.JVClose(); } catch { }
        }

        Console.WriteLine($"[Sidecar] STREAM_RTCARD finished for {yyyyMmDd}. Stored={stored}.");
        return (stored, 0);
    }
}
