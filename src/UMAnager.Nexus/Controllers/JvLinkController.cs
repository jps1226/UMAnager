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
    private readonly HnNameBackfillService _hnNameBackfill;
    private readonly RaceCardRefreshService _refreshService;
    private readonly AppStateService _appState;
    private readonly OddsApplyService _oddsApply;
    private readonly OddsFetchService _oddsFetch;
    private readonly ResultsFetchService _resultsFetch;
    private readonly IDbContextFactory<AppDbContext> _dbFactory;

    public JvLinkController(
        SidecarBridge bridge,
        DifnRecordParsingService parsingService,
        BreedingHorseBackfillService breedingBackfill,
        HnNameBackfillService hnNameBackfill,
        RaceCardRefreshService refreshService,
        AppStateService appState,
        OddsApplyService oddsApply,
        OddsFetchService oddsFetch,
        ResultsFetchService resultsFetch,
        IDbContextFactory<AppDbContext> dbFactory)
    {
        _bridge           = bridge;
        _parsingService   = parsingService;
        _breedingBackfill = breedingBackfill;
        _hnNameBackfill   = hnNameBackfill;
        _refreshService   = refreshService;
        _appState         = appState;
        _oddsApply        = oddsApply;
        _oddsFetch        = oddsFetch;
        _resultsFetch     = resultsFetch;
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
                        parsed_hr = stats.ParsedHr,
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

    // Phase 15: bloodline ingest. Asks the Sidecar to JVOpen("BLDN", 4) and stream HN
    // (breeding-horse master) records. These carry the romaji name we need to populate
    // breeding_horses.NameEn — the only path netkeiba/JBIS can't provide.
    [HttpPost("load-bloodline")]
    public async Task<IActionResult> LoadBloodline()
    {
        if (_bridge.IngestionStatus == "Streaming")
            return Conflict(new { error = "Ingestion already in progress." });

        _bridge.StagedRecordCount = 0;
        _bridge.IngestionStatus = "Streaming";
        await _bridge.CommandQueue.Writer.WriteAsync("{\"command\":\"STREAM_BLDN\"}");
        return Accepted(new { status = "BLDN stream command enqueued." });
    }

    // Phase 15: parse HN records from raw_staging and UPDATE breeding_horses.NameEn.
    // Run this after load-bloodline finishes streaming.
    [HttpPost("backfill-hn-names")]
    public async Task<IActionResult> BackfillHnNames(CancellationToken ct)
    {
        try
        {
            var (scanned, updated) = await _hnNameBackfill.BackfillAsync(ct);
            return Ok(new { status = "HN name backfill complete", scanned, updated });
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
        var raceDate = req?.RaceDate ?? TimeZoneInfo
            .ConvertTimeFromUtc(DateTime.UtcNow,
                TimeZoneInfo.FindSystemTimeZoneById("Tokyo Standard Time"))
            .ToString("yyyyMMdd");

        if (raceDate.Length != 8 || !int.TryParse(raceDate, out _))
            return BadRequest(new { error = "race_date must be YYYYMMDD" });

        var (result, count, _) = await _oddsFetch.EnqueueForDateAsync(raceDate, ct);
        return result switch
        {
            OddsFetchService.EnqueueResult.SidecarDisconnected
                => StatusCode(503, new { error = "Sidecar not connected." }),
            OddsFetchService.EnqueueResult.NoRaces
                => Ok(new { status = "No races scheduled for date.", race_date = raceDate, race_count = 0 }),
            _   => Accepted(new { status = "Odds stream enqueued.", race_date = raceDate, race_count = count }),
        };
    }

    public sealed record FetchOddsRequest([property: JsonPropertyName("race_date")] string? RaceDate);

    // Fetch post-race results (RA + SE + HR) via JVRTOpen("0B12", yyyyMMdd).
    // 0B12 is per-day — one call per date returns every venue's results.
    [HttpPost("fetch-results")]
    public async Task<IActionResult> FetchResults([FromBody] FetchOddsRequest? req, CancellationToken ct)
    {
        var raceDate = req?.RaceDate ?? TimeZoneInfo
            .ConvertTimeFromUtc(DateTime.UtcNow,
                TimeZoneInfo.FindSystemTimeZoneById("Tokyo Standard Time"))
            .ToString("yyyyMMdd");

        if (raceDate.Length != 8 || !int.TryParse(raceDate, out _))
            return BadRequest(new { error = "race_date must be YYYYMMDD" });

        var (result, _) = await _resultsFetch.EnqueueForDateAsync(raceDate, ct);
        return result switch
        {
            ResultsFetchService.EnqueueResult.SidecarDisconnected
                => StatusCode(503, new { error = "Sidecar not connected." }),
            _   => Accepted(new { status = "Results stream enqueued.", race_date = raceDate }),
        };
    }

    // Apply all unprocessed O1 records from raw_staging to race_entries immediately.
    // Useful to process the 3,624 historical O1 records already in staging.
    [HttpPost("apply-odds")]
    public async Task<IActionResult> ApplyOdds(CancellationToken ct)
    {
        try
        {
            var result = await _oddsApply.ApplyAllPendingAsync(ct);
            return Ok(new { status = "Done", records_processed = result.RecordsProcessed, entries_updated = result.EntriesUpdated });
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
