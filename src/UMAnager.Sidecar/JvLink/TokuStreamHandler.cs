// ============================================================
// FILE: TokuStreamHandler.cs
// LAYER: Sidecar JV-Link stream handler
// PURPOSE: JVOpen("TOKURACESNPN", fromTime, option) → streams RA+SE race-card records back over
//          the pipe. option=2 = cursor-based (fromTime = prior lastfiletimestamp); option=4 =
//          dialog-less setup (fromTime = date filter, one-shot historical backfill). Returns the
//          advanced lastFileTimestamp for the Nexus to persist as the cursor.
// KEY DEPENDENCIES: IJVLink, SidecarPipeClient.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Text;
using UMAnager.Sidecar.Com;
using UMAnager.Sidecar.Pipes;

namespace UMAnager.Sidecar.JvLink;

internal static class TokuStreamHandler
{
    public static Task<(int Stored, int Skipped, string LastFileTimestamp)> StreamAsync(
        IJVLink jvLink, SidecarPipeClient pipe, string fromTime, CancellationToken ct)
        => StreamAsync(jvLink, pipe, fromTime, option: 2, ct);

    public static async Task<(int Stored, int Skipped, string LastFileTimestamp)> StreamAsync(
        IJVLink jvLink, SidecarPipeClient pipe, string fromTime, int option, CancellationToken ct)
    {
        int readcount = 0, downloadcount = 0;

        // Ensure save path exists.
        string savePath = @"C:\JRA-VAN\";
        if (!System.IO.Directory.Exists(savePath))
            System.IO.Directory.CreateDirectory(savePath);

        int rcSetPath = jvLink.JVSetSavePath(savePath);
        if (rcSetPath < 0)
            throw new InvalidOperationException($"JVSetSavePath failed: rc={rcSetPath}");

        try { jvLink.ParentHWnd = 0; } catch { /* non-fatal */ }

        // TOKURACESNPN = canonical concat of TOKU + RACE + SNPN.
        //   Delivers special entries, full race cards (RA+SE), and snapshot data.
        // option=2 (Normal): cursor-based fetch — fromTime MUST be the lastfiletimestamp
        //   returned from the previous JVOpen, OR "00000000000000" for the first run.
        // option=4 (Dialog-less Setup): filter mode — fromTime is a date (yyyyMMddHHmmss);
        //   delivers everything from that date onward. One-shot historical backfill.
        const string dataSpec = "TOKURACESNPN";
        Console.WriteLine($"[Sidecar] Calling JVOpen({dataSpec}, {fromTime}, option={option})...");
        int rc = jvLink.JVOpen(dataSpec, fromTime, option,
                                ref readcount, ref downloadcount, out var lastts);
        if (rc == -1)
        {
            Console.WriteLine("[Sidecar] No new race-card data available (rc=-1).");
            return (0, 0, fromTime); // no advance — preserve caller's cursor
        }
        if (rc < 0)
            throw new InvalidOperationException($"JVOpen({dataSpec}) failed: rc={rc}");

        Console.WriteLine($"[Sidecar] JVOpen RACE: rc={rc}, records≈{readcount}, files={downloadcount}, ts={lastts}");

        if (downloadcount > 0)
        {
            Console.WriteLine($"[Sidecar] Downloading {downloadcount} TOKU files...");
            while (true)
            {
                int status = jvLink.JVStatus();
                if (status >= downloadcount) break;
                await Task.Delay(80, ct);
            }
            Console.WriteLine("[Sidecar] TOKU download complete.");
        }

        int stored = 0, skipped = 0;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                byte[] buffBytes = new byte[2750];
                object buffObj = buffBytes;

                int readRc = jvLink.JVGets(ref buffObj, 2750, out var filename);
                buffBytes = (byte[])buffObj; // Cast back after COM call

                if (readRc > 0)
                {
                    if (buffBytes.Length >= 2)
                    {
                        var recType = Encoding.ASCII.GetString(buffBytes[..2]);
                        await pipe.SendRawRecordAsync(buffBytes, ct);
                        stored++;

                        if (stored % 100 == 0)
                            Console.WriteLine($"[Sidecar] TOKU progress: {stored} records streamed (last type: {recType})");
                    }
                    Array.Resize(ref buffBytes, 0); // COM marshaler memory hint — after use
                }
                else if (readRc == 0)
                {
                    Console.WriteLine($"[Sidecar] TOKU EOF. Stored={stored}");
                    break;
                }
                else if (readRc == -1)
                {
                    continue; // File boundary — advance immediately
                }
                else if (readRc == -3)
                {
                    await Task.Delay(500, ct); // Background download in progress
                }
                else if (readRc is -402 or -403)
                {
                    jvLink.JVFiledelete(filename);
                    throw new InvalidOperationException($"Corrupted TOKU file: {filename} (rc={readRc})");
                }
                else
                {
                    throw new InvalidOperationException($"JVGets unexpected rc={readRc}");
                }
            }
        }
        finally
        {
            try { jvLink.JVClose(); } catch { /* non-fatal */ }
        }

        return (stored, skipped, lastts ?? fromTime);
    }
}
