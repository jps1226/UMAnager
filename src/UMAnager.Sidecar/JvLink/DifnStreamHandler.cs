// ============================================================
// FILE: DifnStreamHandler.cs
// LAYER: Sidecar JV-Link stream handler
// PURPOSE: JVOpen(dataSpec, option) for master-data streams — DIFN (horse/jockey/trainer
//          masters, option=1 cursor-based) and BLDN (breeding-horse HN records, option=4
//          one-shot setup). Sets the save path + ParentHWnd=0, downloads files, then
//          JVGets-loops raw records back over the pipe. Returns the advanced
//          lastFileTimestamp for the Nexus to persist as the cursor.
// KEY DEPENDENCIES: IJVLink, SidecarPipeClient.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Runtime.InteropServices;
using System.Text;
using UMAnager.Sidecar.Com;
using UMAnager.Sidecar.Pipes;

namespace UMAnager.Sidecar.JvLink;

internal static class DifnStreamHandler
{
    private static readonly Encoding ShiftJis = Encoding.GetEncoding(932);

    // BR/BN records now stored (removed from skip list per Oracle validation).
    // Previously skipped but should be included in raw_staging for complete master data.
    private static readonly HashSet<string> SkipTypes = [];

    // fromTime/option default to the one-shot SETUP form (option=4, everything from 1991) because
    // that is what BLDN's one-time bloodline load still wants. DIFN's routine weekly refresh MUST
    // pass option=1 with the previous run's lastFileTimestamp — see the JVOpen comment below.
    public static async Task<(int Stored, int Skipped, string LastFileTimestamp)> StreamAsync(
        IJVLink jvLink, SidecarPipeClient pipe, CancellationToken ct,
        string dataSpec = "DIFN",
        string fromTime = "19910101000000",
        int option = 4)
    {
        int readcount = 0, downloadcount = 0;

        // Ensure the JV-Link save path exists and is set.
        // If m_savepath is "UNKNOWN", JVOpen will fail with -211.
        string savePath = @"C:\JRA-VAN\";
        if (!System.IO.Directory.Exists(savePath))
            System.IO.Directory.CreateDirectory(savePath);

        Console.WriteLine($"[Sidecar] Calling JVSetSavePath({savePath})...");
        int rcSetPath = jvLink.JVSetSavePath(savePath);
        Console.WriteLine($"[Sidecar] JVSetSavePath returned: rc={rcSetPath}");
        if (rcSetPath < 0)
        {
            Console.Error.WriteLine($"[Sidecar] JVSetSavePath failed: rc={rcSetPath}");
            throw new InvalidOperationException($"JVSetSavePath({savePath}) failed: rc={rcSetPath}");
        }
        Console.WriteLine($"[Sidecar] Save path set to: {savePath}");

        // CRITICAL: Set ParentHWnd to 0 for headless console mode.
        // If JRA-VAN tries to show a News dialog and ParentHWnd is not set, JVRead will hang indefinitely.
        // (Per kmy-keiba CheckJraVanNews() and Librarian guidance)
        Console.WriteLine("[Sidecar] Setting ParentHWnd = 0 (headless mode)...");
        try
        {
            jvLink.ParentHWnd = 0;
            Console.WriteLine("[Sidecar] ParentHWnd set to 0.");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[Sidecar] Warning: ParentHWnd set failed: {ex.Message}");
            // Continue anyway; ParentHWnd failure shouldn't block the stream
        }

        // DIFN = post-Aug 2023 Differential/Accumulation DataSpec (supports 10-byte breeding IDs).
        // BLDN = Bloodline (HN/SK/BT records) — same machinery, different DataSpec.
        //
        // option=1 (通常データ / normal delta) — what the ROUTINE weekly DIFN refresh must use.
        //   fromTime acts as a strict CURSOR: pass the lastFileTimestamp returned by the previous
        //   JVOpen for gap-free syncing.
        // option=4 (dialog-less SETUP) — one-shot initial build only (BLDN's bloodline load).
        //
        // 2026-07-31 (Oracle): the weekly refresh used to call option=4 with fromTime=19910101000000
        // — a FULL 35-year setup, every week. option=4 ignores fromTime for master records and forces
        // a full recalculation. Before returning, JVOpen synchronously fetches the server file
        // manifest and cross-references it against every file in the local C:\JRA-VAN\data cache; on a
        // 35-year manifest that evaluation scales geometrically. After JRA-VAN's late-July server-side
        // repack of the historical setup files it stopped yielding control back to managed code at all
        // — JVOpen never returned, permanently wedging the Sidecar's single STA thread (there is no
        // timeout property and no pre-flight call to abort it). Last success 2026-07-23.
        Console.WriteLine($"[Sidecar] Calling JVOpen({dataSpec}, {fromTime}, option={option})...");
        int rc = jvLink.JVOpen(dataSpec, fromTime, option,
                                ref readcount, ref downloadcount, out var lastts);
        Console.WriteLine($"[Sidecar] JVOpen returned: rc={rc}");
        if (rc == -1)
        {
            // No new data since the cursor — the normal outcome for a quiet week on option=1.
            // Not an error: return without advancing so the caller keeps its existing cursor.
            Console.WriteLine($"[Sidecar] No new {dataSpec} master data since {fromTime} (rc=-1).");
            return (0, 0, fromTime);
        }
        if (rc == JvReturnCodes.Maintenance)
        {
            Console.WriteLine($"[Sidecar] JRA-VAN server under maintenance (rc={rc}). Backing off.");
            return (JvReturnCodes.MaintenanceStored, 0, fromTime); // no advance — preserve cursor
        }
        if (rc < 0)
            throw new InvalidOperationException($"JVOpen({dataSpec}) failed: rc={rc}");

        Console.WriteLine($"[Sidecar] JVOpen {dataSpec}: rc={rc}, records≈{readcount}, files={downloadcount}, ts={lastts}");

        // If files are being downloaded (downloadcount > 0), poll JVStatus() until download completes.
        // Per Oracle: downloadcount=0 means files are cached locally, so no polling needed.
        if (downloadcount > 0)
        {
            Console.WriteLine($"[Sidecar] Downloading {downloadcount} files. Polling JVStatus() until complete...");
            int poll = 0;
            while (true)
            {
                int status = jvLink.JVStatus();
                if (status >= downloadcount)
                {
                    Console.WriteLine($"[Sidecar] Download complete (status={status})");
                    break;
                }
                await Task.Delay(80, ct); // kmy-keiba polls every 80ms
                poll++;
                if (poll % 50 == 0) // Log every ~4 seconds
                    Console.WriteLine($"[Sidecar] Download progress: {status}/{downloadcount}...");
            }
        }

        // JV-Link setup dialog may appear after JVOpen.
        // (User handles dialog interaction manually when it appears.)

        int stored = 0, skipped = 0;
        var readStartTime = DateTime.UtcNow; // Watchdog timer for hang detection
        const int ReadTimeoutSeconds = 120; // Restart if no progress in 2 minutes
        try
        {
            Console.WriteLine($"[Sidecar] ════════════════════════════════════════════════════════");
            Console.WriteLine($"[Sidecar] ENTERING READ LOOP");
            Console.WriteLine($"[Sidecar] ════════════════════════════════════════════════════════");
            Console.Out.Flush();

            while (!ct.IsCancellationRequested)
            {
                // Watchdog timer: if no progress in ReadTimeoutSeconds, abort to avoid infinite hang
                if ((DateTime.UtcNow - readStartTime).TotalSeconds > ReadTimeoutSeconds)
                {
                    Console.Error.WriteLine($"[Sidecar] Read timeout ({ReadTimeoutSeconds}s) exceeded. Aborting stream.");
                    throw new TimeoutException($"JVGets/JVRead hung for {ReadTimeoutSeconds} seconds without progress.");
                }

                int readRc;
                string filename;
                int attemptNum = stored + skipped + 1;

                Console.WriteLine($"[Sidecar] ─────────────────────────────────────────────────────");
                Console.WriteLine($"[Sidecar] ATTEMPT #{attemptNum}");
                Console.Out.Flush();

                try
                {
                    // Pre-allocate buffer as byte[], convert to object for COM call (kmy-keiba pattern).
                    Console.WriteLine($"[Sidecar] [CHECKPOINT A] About to allocate byte[2750]");
                    Console.Out.Flush();

                    byte[] buffBytes = new byte[2750];
                    Console.WriteLine($"[Sidecar] [CHECKPOINT B] Allocated buffBytes. Length={buffBytes.Length}");
                    Console.Out.Flush();

                    object buffObj = buffBytes;
                    Console.WriteLine($"[Sidecar] [CHECKPOINT C] Converted to object. Type={buffObj?.GetType().Name ?? "null"}");
                    Console.Out.Flush();

                    Console.WriteLine($"[Sidecar] >>> CALLING JVGets <<<");
                    Console.Out.Flush();

                    readRc = jvLink.JVGets(ref buffObj, 2750, out filename);

                    Console.WriteLine($"[Sidecar] <<< JVGets RETURNED: rc={readRc} >>>");
                    Console.Out.Flush();
                    Console.WriteLine($"[Sidecar] [CHECKPOINT D] After JVGets call. buffObj type={buffObj?.GetType().Name ?? "null"}");
                    Console.Out.Flush();

                    readStartTime = DateTime.UtcNow; // Reset watchdog on successful read

                    // Cast object back to byte[] after COM call.
                    Console.WriteLine($"[Sidecar] [CHECKPOINT E] Attempting cast back to byte[]");
                    Console.Out.Flush();
                    buffBytes = (byte[])buffObj;
                    Console.WriteLine($"[Sidecar] [CHECKPOINT F] Cast successful. buffBytes.Length={buffBytes.Length}");
                    Console.Out.Flush();

                    Console.WriteLine($"[Sidecar] Processing rc={readRc}...");
                    Console.Out.Flush();

                    if (readRc > 0)
                    {
                        Console.WriteLine($"[Sidecar] rc > 0: Record data received, length={readRc}");
                        Console.WriteLine($"[Sidecar] buffBytes.Length={buffBytes.Length}");

                        // rawBytes is already the actual byte data from COM.
                        byte[] rawBytes = buffBytes;
                        Console.WriteLine($"[Sidecar] ✓ rawBytes.Length={rawBytes.Length}");

                        // Clamp to max known record length to prevent malformed data.
                        if (rawBytes.Length > 2750)
                        {
                            Console.WriteLine($"[Sidecar] ⚠️  Clamping oversized record from {rawBytes.Length} to 2750");
                            Array.Resize(ref rawBytes, 2750);
                        }

                        if (rawBytes.Length >= 2)
                        {
                            var recType = Encoding.ASCII.GetString(rawBytes[..2]);
                            Console.WriteLine($"[Sidecar] Record type: {recType}");

                            if (SkipTypes.Contains(recType))
                            {
                                Console.WriteLine($"[Sidecar] >>> JVSkip() for {recType} file");
                                Console.Out.Flush();
                                jvLink.JVSkip();
                                skipped++;
                                Console.WriteLine($"[Sidecar] ✓ Skipped. Total skipped={skipped}");
                            }
                            else
                            {
                                Console.WriteLine($"[Sidecar] Sending raw record to pipe ({rawBytes.Length} bytes)...");
                                Console.Out.Flush();
                                await pipe.SendRawRecordAsync(rawBytes, ct);
                                stored++;
                                Console.WriteLine($"[Sidecar] ✓ Sent. Total stored={stored}");
                                if (stored % 100 == 0)
                                    Console.WriteLine($"[Sidecar] PROGRESS: {stored} records streamed");
                            }
                        }
                        else
                        {
                            Console.Error.WriteLine($"[Sidecar] ⚠️  Record too short ({rawBytes.Length} bytes). Skipping.");
                        }

                        // Memory management hack: hint to COM marshaler to free the buffer.
                        // Reduces memory pressure on 224K+ iterations (kmy-keiba pattern).
                        Array.Resize(ref buffBytes, 0);
                    }
                    else if (readRc == 0)
                    {
                        Console.WriteLine($"[Sidecar] ════════════════════════════════════════════════════════");
                        Console.WriteLine($"[Sidecar] rc=0: END OF FILE. Stream complete.");
                        Console.WriteLine($"[Sidecar] Total stored: {stored}");
                        Console.WriteLine($"[Sidecar] Total skipped: {skipped}");
                        Console.WriteLine($"[Sidecar] ════════════════════════════════════════════════════════");
                        Console.Out.Flush();
                        break; // EOF — all files consumed
                    }
                    else if (readRc == -1)
                    {
                        Console.WriteLine($"[Sidecar] rc=-1: File boundary. Advancing to next file...");
                        Console.Out.Flush();
                        // File boundary — advance to the next physical file immediately.
                        // Oracle-confirmed: -1 is NOT "downloading"; no delay needed.
                        continue;
                    }
                    else if (readRc == -3)
                    {
                        Console.WriteLine($"[Sidecar] rc=-3: Download in progress. Waiting 500ms...");
                        Console.Out.Flush();
                        // Background download still in progress — brief wait and retry.
                        await Task.Delay(500, ct);
                    }
                    else if (readRc is -402 or -403)
                    {
                        Console.Error.WriteLine($"[Sidecar] ❌ Corrupted file '{filename}': rc={readRc}. Deleting.");
                        Console.Out.Flush();
                        jvLink.JVFiledelete(filename);
                        throw new InvalidOperationException($"Corrupted DIFN file: {filename} (rc={readRc})");
                    }
                    else
                    {
                        Console.Error.WriteLine($"[Sidecar] ❌ Unknown JVGets error: rc={readRc}");
                        throw new InvalidOperationException($"JVGets error: rc={readRc}");
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[Sidecar] ❌ JVGets threw exception:");
                    Console.Error.WriteLine($"[Sidecar] {ex.GetType().Name}: {ex.Message}");
                    Console.Error.WriteLine($"[Sidecar] Stack trace: {ex.StackTrace}");
                    Console.Error.Flush();
                    throw;
                }
            }
        }
        finally
        {
            Console.WriteLine($"[Sidecar] ════════════════════════════════════════════════════════");
            Console.WriteLine($"[Sidecar] CLOSING JV-LINK");
            Console.WriteLine($"[Sidecar] Calling JVClose()...");
            Console.Out.Flush();

            try
            {
                jvLink.JVClose();
                Console.WriteLine($"[Sidecar] ✓ JVClose() completed");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[Sidecar] ⚠️  JVClose() threw: {ex.GetType().Name}: {ex.Message}");
            }

            Console.WriteLine($"[Sidecar] ════════════════════════════════════════════════════════");
            Console.Out.Flush();
        }

        Console.WriteLine($"[Sidecar] ════════════════════════════════════════════════════════");
        Console.WriteLine($"[Sidecar] STREAM COMPLETE");
        Console.WriteLine($"[Sidecar] Final: Stored={stored}, Skipped={skipped}, lastFileTs={lastts}");
        Console.WriteLine($"[Sidecar] ════════════════════════════════════════════════════════");
        Console.Out.Flush();

        // lastts is the cursor for the NEXT option=1 call. Guard against a blank/null out-param
        // (seen on some rc paths) by falling back to the cursor we came in with — never advance to
        // an empty string, which would reset the cursor and re-pull from scratch.
        var advanced = string.IsNullOrWhiteSpace(lastts) ? fromTime : lastts;
        return (stored, skipped, advanced);
    }
}
