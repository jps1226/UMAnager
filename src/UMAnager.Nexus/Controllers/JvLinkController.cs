// ============================================================
// FILE: JvLinkController.cs
// LAYER: API (api/jvlink) — operator/dev control surface for ingest
// PURPOSE: Status of the Sidecar bridge plus the manual triggers: load master data /
//          bloodline, ingest jockeys/trainers, run the one-shot backfills, fetch current
//          odds / results, apply staged odds, historical race backfill, tail sidecar log.
// KEY DEPENDENCIES: SidecarBridge + most Parsing/backfill services + OddsFetchService /
//          ResultsFetchService / OddsApplyService / RaceCardRefreshService, AppStateService.
// CAUTION: Many actions enqueue commands onto SidecarBridge.CommandQueue and return Accepted;
//          they guard on IngestionStatus == "Streaming" to avoid overlapping pulls.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
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
    private readonly SurfaceBackfillService _surfaceBackfill;
    private readonly RaceClassBackfillService _raceClassBackfill;
    private readonly JockeyTrainerIngestService _jtIngest;
    private readonly SeCodeBackfillService _seBackfill;
    private readonly JockeyTrainerStatsService _jtStats;
    private readonly SirePerformanceService _sirePerf;
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
        SurfaceBackfillService surfaceBackfill,
        RaceClassBackfillService raceClassBackfill,
        JockeyTrainerIngestService jtIngest,
        SeCodeBackfillService seBackfill,
        JockeyTrainerStatsService jtStats,
        SirePerformanceService sirePerf,
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
        _surfaceBackfill  = surfaceBackfill;
        _raceClassBackfill = raceClassBackfill;
        _jtIngest         = jtIngest;
        _seBackfill       = seBackfill;
        _jtStats          = jtStats;
        _sirePerf         = sirePerf;
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

    // Phase 8: process staged KS/CH records into jockeys / trainers, then re-parse staged
    // SE records to fill the new JockeyCode / TrainerCode columns on race_entries. Idempotent.
    [HttpPost("ingest-jockeys-trainers")]
    public async Task<IActionResult> IngestJockeysTrainers(CancellationToken ct)
    {
        try
        {
            var (jScanned, jUpserted) = await _jtIngest.IngestJockeysAsync(ct);
            var (tScanned, tUpserted) = await _jtIngest.IngestTrainersAsync(ct);
            var (sScanned, sUpdated)  = await _seBackfill.BackfillAsync(ct);
            await _jtStats.RefreshAsync(ct);
            return Ok(new {
                status = "Phase 8 ingest complete",
                jockeys  = new { scanned = jScanned, upserted = jUpserted },
                trainers = new { scanned = tScanned, upserted = tUpserted },
                se_codes = new { scanned = sScanned, updated  = sUpdated  }
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }

    // Phase 8: force a refresh of jockey/trainer rolling stats. Normally fires off the
    // results-tick path; this endpoint is for manual recompute after a backfill.
    [HttpPost("refresh-jockey-trainer-stats")]
    public async Task<IActionResult> RefreshJockeyTrainerStats(CancellationToken ct)
    {
        try
        {
            await _jtStats.RefreshAsync(ct);
            return Ok(new { status = "jockey/trainer stats refreshed" });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }

    // Phase 9: force a refresh of the sire_performance materialized view. Cheap
    // (CONCURRENTLY, sub-second). Mostly useful after a manual backfill; the normal
    // results-tick path already refreshes after each batch of finishes.
    [HttpPost("refresh-sire-performance")]
    public async Task<IActionResult> RefreshSirePerformance(CancellationToken ct)
    {
        try
        {
            await _sirePerf.EnsureSchemaAsync(ct);
            await _sirePerf.RefreshAsync(ct);
            return Ok(new { status = "sire_performance refreshed" });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }

    // Oracle Q20: backfill races.RaceClass from JyokenCD slot 5 in raw_staging RA
    // records. Run once after the column is added; subsequent ingests populate it
    // automatically via the fixed RaRecordParser.
    [HttpPost("backfill-race-class")]
    public async Task<IActionResult> BackfillRaceClass(CancellationToken ct)
    {
        try
        {
            var (scanned, updated) = await _raceClassBackfill.BackfillAsync(ct);
            return Ok(new { status = "race class backfill complete", scanned, updated });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }

    // Phase 9 dependency: backfill races.Surface from raw_staging RA records. The
    // original RaRecordParser had a bug where it compared the 2-digit TrackCD field
    // to "1"/"2" and never matched, leaving Surface NULL on all 6,631 historical
    // races. This re-parses RA records with the fixed code and UPDATEs Surface.
    [HttpPost("backfill-surface")]
    public async Task<IActionResult> BackfillSurface(CancellationToken ct)
    {
        try
        {
            var (scanned, updated) = await _surfaceBackfill.BackfillAsync(ct);
            // Chain: surface fix invalidates the MV; refresh it now so the next
            // /api/races call returns non-null Sire_Fit values.
            await _sirePerf.RefreshAsync(ct);
            return Ok(new { status = "surface backfill complete", scanned, updated });
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
