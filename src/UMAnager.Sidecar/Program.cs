using System.Text.Json;
using Microsoft.Extensions.Configuration;
using UMAnager.Sidecar.Com;
using UMAnager.Sidecar.JvLink;
using UMAnager.Sidecar.Pipes;

// All COM interactions must occur on an STA thread.
// The default console thread is MTA, so we park on a dedicated STA thread.
var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

int exitCode = 0;
var staThread = new Thread(() => exitCode = Run(cts.Token));
staThread.SetApartmentState(ApartmentState.STA);
staThread.Start();
staThread.Join();
return exitCode;

static int Run(CancellationToken ct)
{
    // Register CodePages encoding provider for Shift-JIS (CP932) support
    System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

    // ── Configuration ────────────────────────────────────────────────────────
    var config = new ConfigurationBuilder()
        .SetBasePath(AppContext.BaseDirectory)
        .AddJsonFile("appsettings.json", optional: false)
        .Build();

    // "UNKNOWN" is the correct dev-phase value per JRA-VAN spec.
    // Replace with "AppName/Version" after official registration.
    var sid = config["JvLink:SoftwareId"] ?? "UNKNOWN";
    if (string.IsNullOrWhiteSpace(sid)) sid = "UNKNOWN";

    // ── JV-Link COM instantiation ────────────────────────────────────────────
    IJVLink? jvLink = null;
    int initResult = -1;
    string jvVersion = "Unknown";

    try
    {
        var progId = Type.GetTypeFromProgID("JVDTLab.JVLink");
        if (progId is null)
        {
            Console.Error.WriteLine("[Sidecar] ERROR: JVDTLab.JVLink ProgID not found. Is JV-Link installed?");
            return 2;
        }

        var instance = Activator.CreateInstance(progId)
            ?? throw new InvalidOperationException("Activator returned null for JVDTLab.JVLink.");
        jvLink = (IJVLink)instance;

        initResult = jvLink.JVInit(sid);
        Console.WriteLine($"[Sidecar] JVInit returned: {initResult}");

        // JVInit returns 0 on success; positive values are informational status codes.
        // Normalize: treat any non-negative return as success for the handshake.
        jvVersion = initResult >= 0 ? $"JVLink-OK(rc={initResult})" : $"JVLink-ERR(rc={initResult})";
        initResult = initResult >= 0 ? 0 : initResult;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Sidecar] COM error: {ex.Message}");
        jvVersion = "COM-Error";
        initResult = -99;
    }

    // ── Named Pipe handshake ─────────────────────────────────────────────────
    var pipeClient = new SidecarPipeClient();
    try
    {
        Console.WriteLine("[Sidecar] Waiting for Nexus connection...");
        pipeClient.ConnectAsync(ct).GetAwaiter().GetResult();

        var commandJson = pipeClient.WaitForCommandAsync(ct).GetAwaiter().GetResult();
        Console.WriteLine($"[Sidecar] Received command: {commandJson}");

        pipeClient.SendStatusAsync(jvVersion, initResult, ct).GetAwaiter().GetResult();
        Console.WriteLine("[Sidecar] Status sent to Nexus.");

        // Active command loop — waits for STREAM_DIFN and future commands.
        while (!ct.IsCancellationRequested)
        {
            var nextCommand = pipeClient.WaitForNextCommandAsync(ct).GetAwaiter().GetResult();
            Console.WriteLine($"[Sidecar] Received command: {nextCommand}");

            using var doc = JsonDocument.Parse(nextCommand);
            var cmd = doc.RootElement.TryGetProperty("command", out var c) ? c.GetString() : null;

            if (cmd == "STREAM_DIFN")
            {
                try
                {
                    var (stored, skippedFiles) = DifnStreamHandler.StreamAsync(jvLink!, pipeClient, ct)
                        .GetAwaiter().GetResult();
                    pipeClient.SendStreamCompleteAsync(stored, skippedFiles, ct).GetAwaiter().GetResult();
                    Console.WriteLine($"[Sidecar] STREAM_DIFN complete. Stored={stored}, SkippedFiles={skippedFiles}");
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[Sidecar] DIFN stream failed: {ex.Message}");
                    pipeClient.SendStreamCompleteAsync(-1, 0, ct).GetAwaiter().GetResult();
                }
            }
            else if (cmd == "STREAM_BLDN")
            {
                // Bloodline DataSpec — carries HN (breeding-horse master) records that contain
                // romaji names for HansyokuNum-keyed ancestors. Reuses the DIFN streaming
                // machinery; only the JVOpen DataSpec changes.
                try
                {
                    var (stored, skippedFiles) = DifnStreamHandler.StreamAsync(jvLink!, pipeClient, ct, "BLDN")
                        .GetAwaiter().GetResult();
                    pipeClient.SendStreamCompleteAsync(stored, skippedFiles, ct).GetAwaiter().GetResult();
                    Console.WriteLine($"[Sidecar] STREAM_BLDN complete. Stored={stored}, SkippedFiles={skippedFiles}");
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[Sidecar] BLDN stream failed: {ex.Message}");
                    pipeClient.SendStreamCompleteAsync(-1, 0, ct).GetAwaiter().GetResult();
                }
            }
            else if (cmd == "STREAM_ODDS")
            {
                // race_ids: array of 16-char (or 12-char) JV-Link race IDs to fetch via JVRTOpen("0B31", id).
                // DataSpec "0B31"'s provision unit is per-race; an 8-char date key gives rc=-114.
                var raceIds = new List<string>();
                if (doc.RootElement.TryGetProperty("race_ids", out var ridArr) && ridArr.ValueKind == JsonValueKind.Array)
                {
                    foreach (var el in ridArr.EnumerateArray())
                    {
                        var s = el.GetString();
                        if (!string.IsNullOrEmpty(s)) raceIds.Add(s);
                    }
                }

                if (raceIds.Count == 0)
                {
                    Console.WriteLine("[Sidecar] STREAM_ODDS received with no race_ids; nothing to fetch.");
                    pipeClient.SendOddsCompleteAsync(0, 0, ct).GetAwaiter().GetResult();
                    continue;
                }

                try
                {
                    Console.WriteLine($"[Sidecar] STREAM_ODDS starting for {raceIds.Count} race(s).");
                    var (stored, skipped) = RtOddsStreamHandler.StreamAsync(jvLink!, pipeClient, raceIds, ct)
                        .GetAwaiter().GetResult();
                    pipeClient.SendOddsCompleteAsync(stored, skipped, ct).GetAwaiter().GetResult();
                    Console.WriteLine($"[Sidecar] STREAM_ODDS complete. Stored={stored}, Skipped={skipped}");
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[Sidecar] ODDS stream failed: {ex.Message}");
                    pipeClient.SendOddsCompleteAsync(-1, 0, ct).GetAwaiter().GetResult();
                }
            }
            else if (cmd == "STREAM_RESULTS")
            {
                // race_date: 8-char YYYYMMDD (JST). 0B12 returns all venues for that date in one call.
                var raceDate = doc.RootElement.TryGetProperty("race_date", out var rd) ? rd.GetString() ?? "" : "";
                if (string.IsNullOrEmpty(raceDate) || raceDate.Length != 8)
                {
                    Console.WriteLine($"[Sidecar] STREAM_RESULTS received with bad race_date '{raceDate}'.");
                    pipeClient.SendResultsCompleteAsync(-1, 1, raceDate, ct).GetAwaiter().GetResult();
                    continue;
                }

                try
                {
                    var (stored, skipped) = RtResultsStreamHandler.StreamAsync(jvLink!, pipeClient, raceDate, ct)
                        .GetAwaiter().GetResult();
                    pipeClient.SendResultsCompleteAsync(stored, skipped, raceDate, ct).GetAwaiter().GetResult();
                    Console.WriteLine($"[Sidecar] STREAM_RESULTS complete. Stored={stored}, Skipped={skipped}");
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[Sidecar] RESULTS stream failed: {ex.Message}");
                    pipeClient.SendResultsCompleteAsync(-1, 0, raceDate, ct).GetAwaiter().GetResult();
                }
            }
            else if (cmd == "STREAM_TOKU")
            {
                // from_time is the JV-Link cursor (yyyyMMddHHmmss). For option=2 it MUST be
                // the lastfiletimestamp returned by the previous JVOpen. For option=4 (setup)
                // it's a date filter. First-ever option=2 call uses "00000000000000".
                var fromTime = doc.RootElement.TryGetProperty("from_time", out var ft)
                    ? ft.GetString() ?? ""
                    : "";
                if (string.IsNullOrEmpty(fromTime))
                    fromTime = "00000000000000";

                int option = doc.RootElement.TryGetProperty("option", out var op) ? op.GetInt32() : 2;

                try
                {
                    var (stored, skippedFiles, lastFileTs) = TokuStreamHandler.StreamAsync(jvLink!, pipeClient, fromTime, option, ct)
                        .GetAwaiter().GetResult();
                    pipeClient.SendTokuCompleteAsync(stored, skippedFiles, lastFileTs, ct).GetAwaiter().GetResult();
                    Console.WriteLine($"[Sidecar] STREAM_TOKU complete. Stored={stored}, SkippedFiles={skippedFiles}, lastFileTs={lastFileTs}");
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[Sidecar] TOKU stream failed: {ex.Message}");
                    pipeClient.SendTokuCompleteAsync(-1, 0, "", ct).GetAwaiter().GetResult();
                }
            }
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("[Sidecar] Shutdown requested.");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Sidecar] Pipe error: {ex.Message}");
    }
    finally
    {
        jvLink?.JVClose();
        pipeClient.Dispose();
    }

    return 0;
}
