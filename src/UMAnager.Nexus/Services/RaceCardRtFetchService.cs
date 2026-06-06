// ============================================================
// FILE: RaceCardRtFetchService.cs
// LAYER: Service
// PURPOSE: Enqueues STREAM_RTCARD commands (JVRTOpen "0B15", per-day) to the Sidecar — the real-time
//          race-card stream that delivers finalized posts (DataStatus=2) the instant they're drawn.
//          Used by the orchestrator's AWAITING_POSTS tick instead of the option=2 card refresh,
//          whose "this week" stream lags hours behind on the 1→2 transition (Oracle 2026-06-04).
// KEY DEPENDENCIES: SidecarBridge, AppDbContext.
// LAST DOCUMENTED: 2026-06-04
// ============================================================
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Enqueues STREAM_RTCARD commands to the Sidecar (<c>JVRTOpen("0B15", yyyyMMdd)</c>). 0B15's
/// provision unit is per-day, so this enqueues one command per upcoming JST race date. The Sidecar
/// streams RA+SE; the Nexus parse path promotes DataStatus 1→2 via the existing UPSERT guard.
/// </summary>
public sealed class RaceCardRtFetchService
{
    private static readonly TimeZoneInfo JstZone = TimeZoneInfo.FindSystemTimeZoneById("Tokyo Standard Time");

    private readonly SidecarBridge _bridge;
    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly ILogger<RaceCardRtFetchService> _logger;

    public RaceCardRtFetchService(
        SidecarBridge bridge,
        IDbContextFactory<AppDbContext> dbFactory,
        ILogger<RaceCardRtFetchService> logger)
    {
        _bridge = bridge;
        _dbFactory = dbFactory;
        _logger = logger;
    }

    /// <summary>
    /// Enqueues a 0B15 race-card fetch for every upcoming JST race date that still has at least one
    /// entry without a drawn post (PostPosition is null or 0). Returns the number of dates enqueued.
    /// Dates whose cards aren't announced yet simply yield rc=-1 in the Sidecar (harmless retry).
    /// </summary>
    public async Task<int> EnqueueForPendingPostDatesAsync(CancellationToken ct)
    {
        if (!_bridge.IsConnected) return 0;

        var jstNow   = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, JstZone);
        var jstToday = DateTime.SpecifyKind(jstNow.Date, DateTimeKind.Utc);

        await using var db = await _dbFactory.CreateDbContextAsync(ct);

        // Upcoming race dates with any entry still missing a real post position. Once a date is fully
        // drawn it drops out here (and the phase advances past AWAITING_POSTS anyway).
        var pendingDates = await db.Races
            .Where(r => r.RaceDate >= jstToday
                     && db.RaceEntries.Any(e => e.RaceId == r.RaceId
                                             && (e.PostPosition == null || e.PostPosition == 0)))
            .Select(r => r.RaceDate)
            .Distinct()
            .OrderBy(d => d)
            .ToListAsync(ct);

        var enqueued = 0;
        foreach (var date in pendingDates)
        {
            var payload = JsonSerializer.Serialize(new
            {
                command   = "STREAM_RTCARD",
                race_date = date.ToString("yyyyMMdd"),
            });
            await _bridge.CommandQueue.Writer.WriteAsync(payload, ct);
            enqueued++;
        }

        if (enqueued > 0)
            _logger.LogInformation("[RaceCardRtFetch] Enqueued STREAM_RTCARD (0B15) for {Count} pending-post date(s).", enqueued);
        else
            _logger.LogDebug("[RaceCardRtFetch] No pending-post dates to fetch.");

        return enqueued;
    }
}
