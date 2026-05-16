using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;
using UMAnager.Nexus.Services;
using UMAnager.Nexus.Services.Parsing;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/jvlink")]
public sealed class JvLinkController : ControllerBase
{
    private readonly SidecarBridge _bridge;
    private readonly DifnRecordParsingService _parsingService;
    private readonly BreedingHorseBackfillService _breedingBackfill;
    private readonly RaceCardRefreshService _refreshService;
    private readonly AppStateService _appState;
    private readonly OddsApplyService _oddsApply;
    private readonly IDbContextFactory<AppDbContext> _dbFactory;

    public JvLinkController(
        SidecarBridge bridge,
        DifnRecordParsingService parsingService,
        BreedingHorseBackfillService breedingBackfill,
        RaceCardRefreshService refreshService,
        AppStateService appState,
        OddsApplyService oddsApply,
        IDbContextFactory<AppDbContext> dbFactory)
    {
        _bridge           = bridge;
        _parsingService   = parsingService;
        _breedingBackfill = breedingBackfill;
        _refreshService   = refreshService;
        _appState         = appState;
        _oddsApply        = oddsApply;
        _dbFactory        = dbFactory;
    }

    [HttpGet("status")]
    public IActionResult GetStatus() => Ok(new
    {
        connected              = _bridge.IsConnected,
        jvlink_version         = _bridge.JvLinkVersion,
        init_result            = _bridge.InitResult,
        message                = _bridge.IsConnected
            ? "JV-Link connection active"
            : "Waiting for Sidecar...",
        ingestion_status       = _bridge.IngestionStatus,
        staged_record_count    = _bridge.StagedRecordCount,
    });

    // ── Phase 2+ stubs ───────────────────────────────────────────────────────

    [HttpGet("storage-layout")]
    public IActionResult GetStorageLayout() => Ok(new { status = "not_implemented" });

    [HttpPost("open-settings")]
    public IActionResult OpenSettings() => Ok(new { status = "not_implemented" });

    [HttpPost("probe-open")]
    public IActionResult ProbeOpen() => Ok(new { status = "not_implemented" });

    [HttpPost("stream-sample")]
    public IActionResult StreamSample() => Ok(new { status = "not_implemented" });

    [HttpPost("refresh-upcoming")]
    public IActionResult RefreshUpcoming() => Ok(new { status = "not_implemented" });

    [HttpPost("refresh-race-cards")]
    public async Task<IActionResult> RefreshRaceCards(CancellationToken ct)
    {
        var result = await _refreshService.TriggerNowAsync(ct);
        var last   = await _appState.GetTimestampAsync(AppStateService.Keys.LastRacePlanDownload);
        return Accepted(new { status = result, last_refresh = last });
    }

    [HttpGet("stream-summary")]
    public IActionResult StreamSummary() => Ok(new { status = "not_implemented" });

    [HttpPost("capability-scan")]
    public IActionResult CapabilityScan() => Ok(new { status = "not_implemented" });

    [HttpPost("load-weekend-races")]
    public IActionResult LoadWeekendRaces() => Ok(new { status = "not_implemented" });

    [HttpPost("load-master-data")]
    public async Task<IActionResult> LoadMasterData()
    {
        if (_bridge.IngestionStatus == "Streaming")
            return Conflict(new { error = "Ingestion already in progress." });

        _bridge.StagedRecordCount = 0;  // Reset counter for new stream
        _bridge.IngestionStatus = "Streaming";
        await _bridge.CommandQueue.Writer.WriteAsync("{\"command\":\"STREAM_DIFN\"}");
        return Accepted(new { status = "DIFN stream command enqueued." });
    }

    [HttpPost("parse-records")]
    public async Task<IActionResult> ParseRecords(CancellationToken ct)
    {
        try
        {
            var stats = await _parsingService.ParseAllRecordsAsync(ct);

            if (stats.ErrorMessage != null)
            {
                return StatusCode(500, new
                {
                    error = stats.ErrorMessage,
                    stats = new
                    {
                        parsed_um = stats.ParsedUm,
                        parsed_ra = stats.ParsedRa,
                        parsed_se = stats.ParsedSe,
                        failed_count = stats.FailedCount,
                        duration_ms = stats.DurationMs
                    }
                });
            }

            return Ok(new
            {
                status = "Parsing complete",
                stats = new
                {
                    parsed_um = stats.ParsedUm,
                    parsed_ra = stats.ParsedRa,
                    parsed_se = stats.ParsedSe,
                    failed_count = stats.FailedCount,
                    duration_ms = stats.DurationMs
                }
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }

    [HttpPost("backfill-breeding-horses")]
    public async Task<IActionResult> BackfillBreedingHorses(CancellationToken ct)
    {
        try
        {
            var (scanned, upserted) = await _breedingBackfill.BackfillAsync(ct);
            return Ok(new { status = "Backfill complete", scanned, upserted });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }

    // Fetch live odds for a race day via JVRTOpen("0B31", raceId), one call per race.
    // "0B31"'s provision unit is per-race (Oracle Q10), so the Sidecar must iterate the day's
    // race IDs and call JVRTOpen for each. race_date defaults to today (JST) if omitted.
    [HttpPost("fetch-current-odds")]
    public async Task<IActionResult> FetchCurrentOdds([FromBody] FetchOddsRequest? req, CancellationToken ct)
    {
        if (!_bridge.IsConnected)
            return StatusCode(503, new { error = "Sidecar not connected." });

        var raceDate = req?.RaceDate ?? TimeZoneInfo
            .ConvertTimeFromUtc(DateTime.UtcNow,
                TimeZoneInfo.FindSystemTimeZoneById("Tokyo Standard Time"))
            .ToString("yyyyMMdd");

        if (raceDate.Length != 8 || !int.TryParse(raceDate, out _))
            return BadRequest(new { error = "race_date must be YYYYMMDD" });

        var raceDateDt = DateTime.SpecifyKind(
            DateTime.ParseExact(raceDate, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture),
            DateTimeKind.Utc);

        List<string> raceIds;
        await using (var db = await _dbFactory.CreateDbContextAsync(ct))
        {
            raceIds = await db.Races
                .Where(r => r.RaceDate == raceDateDt)
                .Select(r => r.RaceId)
                .OrderBy(id => id)
                .ToListAsync(ct);
        }

        if (raceIds.Count == 0)
            return Ok(new { status = "No races scheduled for date.", race_date = raceDate, race_count = 0 });

        var payload = JsonSerializer.Serialize(new
        {
            command = "STREAM_ODDS",
            race_ids = raceIds,
        });

        await _bridge.CommandQueue.Writer.WriteAsync(payload, ct);

        return Accepted(new { status = "Odds stream enqueued.", race_date = raceDate, race_count = raceIds.Count });
    }

    public sealed record FetchOddsRequest([property: JsonPropertyName("race_date")] string? RaceDate);

    // Apply all unprocessed O1 records from raw_staging to race_entries immediately.
    // Useful to process the 3,624 historical O1 records already in staging.
    [HttpPost("apply-odds")]
    public async Task<IActionResult> ApplyOdds(CancellationToken ct)
    {
        try
        {
            var (processed, updated) = await _oddsApply.ApplyAllPendingAsync(ct);
            return Ok(new { status = "Done", records_processed = processed, entries_updated = updated });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }

    // One-shot historical race-card backfill via JVOpen("TOKURACESNPN", from_time, 4).
    // Option=4 (Dialog-less Setup) treats from_time as a date filter — delivers every
    // weekend's RA+SE from that date onward. Heavy pull; do not invoke routinely.
    public sealed record HistoricalBackfillRequest(string FromTime);

    [HttpPost("backfill-historical-races")]
    public async Task<IActionResult> BackfillHistoricalRaces([FromBody] HistoricalBackfillRequest req, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(req.FromTime) || req.FromTime.Length != 14)
            return BadRequest(new { error = "from_time must be 14 chars (yyyyMMddHHmmss)" });
        if (!_bridge.IsConnected)
            return StatusCode(503, new { error = "Sidecar not connected." });
        if (_bridge.IngestionStatus == "Streaming")
            return Conflict(new { error = "Stream already in progress." });

        _bridge.IngestionStatus = "Streaming";
        await _bridge.CommandQueue.Writer.WriteAsync(
            $"{{\"command\":\"STREAM_TOKU\",\"from_time\":\"{req.FromTime}\",\"option\":4}}", ct);

        return Accepted(new { status = "Historical backfill enqueued.", from_time = req.FromTime, option = 4 });
    }

    [HttpGet("sidecar-log")]
    public IActionResult GetSidecarLog([FromQuery] int lines = 30)
    {
        lines = Math.Clamp(lines, 1, 200);
        var logPath = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "logs", "sidecar.log"));
        if (!System.IO.File.Exists(logPath))
            return Ok(new { log_path = logPath, lines = Array.Empty<string>() });

        string[] all;
        try
        {
            using var fs = new FileStream(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var sr = new StreamReader(fs);
            all = sr.ReadToEnd().Split('\n', StringSplitOptions.RemoveEmptyEntries);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }

        var tail = all.Length <= lines ? all : all[^lines..];
        return Ok(new { log_path = logPath, lines = tail });
    }
}
