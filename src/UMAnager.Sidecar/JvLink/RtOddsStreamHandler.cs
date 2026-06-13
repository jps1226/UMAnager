// ============================================================
// FILE: RtOddsStreamHandler.cs
// LAYER: Sidecar JV-Link stream handler
// PURPOSE: JVRTOpen("0B31", raceId) per race → streams real-time O1 odds records back over the
//          pipe. 0B31's provision unit is per-race, so the handler iterates the supplied race-id list.
// KEY DEPENDENCIES: IJVLink, SidecarPipeClient.
// CAUTION: Key MUST be a 16- or 12-char race id; an 8-char date is rejected with rc=-114.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Text;
using UMAnager.Sidecar.Com;
using UMAnager.Sidecar.Pipes;

namespace UMAnager.Sidecar.JvLink;

internal static class RtOddsStreamHandler
{
    // JVRTOpen blocks until data is ready — no JVStatus polling required.
    // DataSpec "0B31" = 速報オッズ 単複枠 (real-time win/place/bracket odds, contains O1 records).
    // Provision unit is per-race: the key MUST be a 16-char race ID "YYYYMMDDJJKKHHRR"
    // (or 12-char short form). 8-char date keys are rejected with rc=-114.
    public static async Task<(int Stored, int Skipped)> StreamAsync(
        IJVLink jvLink, SidecarPipeClient pipe, IReadOnlyList<string> raceIds, CancellationToken ct)
    {
        int stored = 0, skipped = 0;

        for (int i = 0; i < raceIds.Count; i++)
        {
            if (ct.IsCancellationRequested) break;

            var raceId = raceIds[i];
            if (string.IsNullOrEmpty(raceId) || (raceId.Length != 16 && raceId.Length != 12))
            {
                Console.WriteLine($"[Sidecar] Skipping malformed race_id '{raceId}' (len={raceId?.Length}).");
                skipped++;
                continue;
            }

            Console.WriteLine($"[Sidecar] [{i + 1}/{raceIds.Count}] JVRTOpen(0B31, {raceId})...");
            int rc;
            try { rc = jvLink.JVRTOpen("0B31", raceId); }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[Sidecar] JVRTOpen({raceId}) threw: {ex.Message}");
                skipped++;
                continue;
            }

            if (rc == JvReturnCodes.Maintenance)
            {
                // Maintenance affects the whole server, not just this race — no point looping the
                // rest. Report the maintenance sentinel so the Nexus backs off (any odds already
                // streamed this batch were sent over the pipe and remain valid).
                Console.WriteLine($"[Sidecar]   JRA-VAN server under maintenance (rc={rc}). Aborting odds batch.");
                return (JvReturnCodes.MaintenanceStored, skipped);
            }
            if (rc != 0)
            {
                // rc=-1: no applicable data (race not yet open / no odds published). Common; skip quietly.
                if (rc == -1)
                    Console.WriteLine($"[Sidecar]   rc=-1 (no odds yet for {raceId}).");
                else
                    Console.WriteLine($"[Sidecar]   JVRTOpen returned rc={rc} for {raceId}.");
                skipped++;
                continue;
            }

            int raceStored = 0;
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    byte[] buffBytes = new byte[2750];
                    object buffObj = buffBytes;

                    int readRc = jvLink.JVGets(ref buffObj, 2750, out var filename);
                    buffBytes = (byte[])buffObj;

                    if (readRc > 0)
                    {
                        if (buffBytes.Length >= 2)
                        {
                            var recType = Encoding.ASCII.GetString(buffBytes[..2]);
                            await pipe.SendRawRecordAsync(buffBytes, ct);
                            stored++;
                            raceStored++;

                            if (stored % 20 == 0)
                                Console.WriteLine($"[Sidecar]   ODDS progress: {stored} records (last type: {recType})");
                        }
                    }
                    else if (readRc == 0)
                    {
                        break;
                    }
                    else if (readRc == -1)
                    {
                        continue;
                    }
                    else
                    {
                        Console.WriteLine($"[Sidecar]   ODDS unexpected rc={readRc} for {raceId}, stopping race.");
                        break;
                    }
                }
            }
            finally
            {
                try { jvLink.JVClose(); } catch { }
            }

            Console.WriteLine($"[Sidecar]   {raceId}: {raceStored} record(s).");
        }

        Console.WriteLine($"[Sidecar] STREAM_ODDS finished. Total stored={stored}, skipped races={skipped}.");
        return (stored, skipped);
    }
}
