// ============================================================
// FILE: OddsFetchService.cs
// LAYER: Service
// PURPOSE: Builds + enqueues STREAM_ODDS commands (race-id lists) to the Sidecar. One code path
//          for the manual endpoint and the orchestrator — EnqueueForDate / EnqueueForLiveWindow
//          / EnqueueForUpcomingDates.
// KEY DEPENDENCIES: SidecarBridge, AppDbContext, SettingsService.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Builds and enqueues STREAM_ODDS commands to the Sidecar. Extracted from JvLinkController so
/// both the manual endpoint and the LiveOrchestrator background loop go through one code path.
/// </summary>
public sealed class OddsFetchService
{
    private static readonly TimeZoneInfo JstZone = TimeZoneInfo.FindSystemTimeZoneById("Tokyo Standard Time");

    private readonly SidecarBridge _bridge;
    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly SettingsService _settings;
    private readonly ILogger<OddsFetchService> _logger;

    public OddsFetchService(
        SidecarBridge bridge,
        IDbContextFactory<AppDbContext> dbFactory,
        SettingsService settings,
        ILogger<OddsFetchService> logger)
    {
        _bridge = bridge;
        _dbFactory = dbFactory;
        _settings = settings;
        _logger = logger;
    }

    public enum EnqueueResult { Enqueued, NoRaces, SidecarDisconnected }

    public async Task<(EnqueueResult Result, int RaceCount, string RaceDate)>
        EnqueueForDateAsync(string yyyyMmDd, CancellationToken ct)
    {
        if (!_bridge.IsConnected) return (EnqueueResult.SidecarDisconnected, 0, yyyyMmDd);

        var raceDateDt = DateTime.SpecifyKind(
            DateTime.ParseExact(yyyyMmDd, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture),
            DateTimeKind.Utc);

        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        var raceIds = await db.Races
            .Where(r => r.RaceDate == raceDateDt)
            .Select(r => r.RaceId)
            .OrderBy(id => id)
            .ToListAsync(ct);

        return await EnqueueAsync(raceIds, yyyyMmDd, ct);
    }

    /// <summary>
    /// Enqueues an odds fetch for races whose SortTime falls within ±live_window_minutes of JST-now.
    /// Used by the orchestrator's LIVE-phase tick.
    /// </summary>
    public async Task<(EnqueueResult Result, int RaceCount, string RaceDate)>
        EnqueueForLiveWindowAsync(CancellationToken ct)
    {
        if (!_bridge.IsConnected) return (EnqueueResult.SidecarDisconnected, 0, "");

        var windowMin = await _settings.GetIntAsync(
            SettingsService.Keys.LiveWindowMinutes,
            SettingsService.Defaults.LiveWindowMinutes);

        // SortTime is stored Kind=Utc with JST wall-clock values. Match the Kind for Npgsql.
        var jstNowUnspec = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, JstZone);
        var jstNow       = DateTime.SpecifyKind(jstNowUnspec, DateTimeKind.Utc);
        var winStart = jstNow.AddMinutes(-windowMin);
        var winEnd   = jstNow.AddMinutes(windowMin);

        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        var raceIds = await db.Races
            .Where(r => r.SortTime != null && r.SortTime >= winStart && r.SortTime <= winEnd)
            .Select(r => r.RaceId)
            .OrderBy(id => id)
            .ToListAsync(ct);

        return await EnqueueAsync(raceIds, jstNow.ToString("yyyyMMdd"), ct);
    }

    /// <summary>
    /// Enqueues an odds fetch for every upcoming JST race date. Used by the RACES_POPULATED
    /// phase tick so prelive odds are refreshed on the configured interval.
    /// </summary>
    public async Task<int> EnqueueForUpcomingDatesAsync(CancellationToken ct)
    {
        if (!_bridge.IsConnected) return 0;

        var jstNow    = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, JstZone);
        var jstToday  = DateTime.SpecifyKind(jstNow.Date, DateTimeKind.Utc);

        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        var upcomingDates = await db.Races
            .Where(r => r.RaceDate >= jstToday)
            .Select(r => r.RaceDate)
            .Distinct()
            .OrderBy(d => d)
            .ToListAsync(ct);

        var enqueued = 0;
        foreach (var date in upcomingDates)
        {
            var (result, _, _) = await EnqueueForDateAsync(date.ToString("yyyyMMdd"), ct);
            if (result == EnqueueResult.Enqueued) enqueued++;
        }
        return enqueued;
    }

    private async Task<(EnqueueResult, int, string)> EnqueueAsync(List<string> raceIds, string label, CancellationToken ct)
    {
        if (raceIds.Count == 0)
        {
            _logger.LogDebug("[OddsFetch] No races to fetch for {Label}.", label);
            return (EnqueueResult.NoRaces, 0, label);
        }

        var payload = JsonSerializer.Serialize(new
        {
            command = "STREAM_ODDS",
            race_ids = raceIds,
        });

        await _bridge.CommandQueue.Writer.WriteAsync(payload, ct);
        _logger.LogInformation("[OddsFetch] Enqueued STREAM_ODDS for {Label} ({Count} races).", label, raceIds.Count);
        return (EnqueueResult.Enqueued, raceIds.Count, label);
    }
}
