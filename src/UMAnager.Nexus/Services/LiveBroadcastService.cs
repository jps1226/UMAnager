// ============================================================
// FILE: LiveBroadcastService.cs
// LAYER: Realtime service
// PURPOSE: Re-reads touched races' entries and pushes them to all SignalR clients as
//          "OddsUpdated" / "ResultsUpdated" (odds, prevOdds, fav, finish per horse).
// KEY DEPENDENCIES: AppDbContext, IHubContext<LiveHub>.
// CALLED BY: NexusPipeServer after odds/results apply.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;
using UMAnager.Nexus.Hubs;

namespace UMAnager.Nexus.Services;

public sealed class LiveBroadcastService
{
    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly IHubContext<LiveHub> _hub;
    private readonly ILogger<LiveBroadcastService> _logger;

    public LiveBroadcastService(
        IDbContextFactory<AppDbContext> dbFactory,
        IHubContext<LiveHub> hub,
        ILogger<LiveBroadcastService> logger)
    {
        _dbFactory = dbFactory;
        _hub       = hub;
        _logger    = logger;
    }

    public Task BroadcastOddsAsync(IEnumerable<string> raceIds, CancellationToken ct = default)
        => BroadcastAsync("OddsUpdated", raceIds, ct);

    public Task BroadcastResultsAsync(IEnumerable<string> raceIds, CancellationToken ct = default)
        => BroadcastAsync("ResultsUpdated", raceIds, ct);

    private async Task BroadcastAsync(string method, IEnumerable<string> raceIds, CancellationToken ct)
    {
        var ids = raceIds.Where(s => !string.IsNullOrWhiteSpace(s)).Distinct().ToList();
        if (ids.Count == 0) return;

        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        var rows = await db.RaceEntries.AsNoTracking()
            .Where(e => ids.Contains(e.RaceId))
            .Select(e => new
            {
                e.RaceId,
                horseId    = e.HorseId,
                pp         = e.PostPosition,
                odds       = e.Odds,
                prevOdds   = e.PrevOdds,
                fav        = e.FavRank,
                finish     = e.FinishPos
            })
            .ToListAsync(ct);

        var byRace = rows.GroupBy(r => r.RaceId);
        foreach (var grp in byRace)
        {
            var entries = grp.Select(e => new
            {
                horseId = e.horseId ?? "",
                pp      = e.pp ?? 0,
                odds    = e.odds?.ToString("F1") ?? "",
                prevOdds = e.prevOdds?.ToString("F1") ?? "",
                fav     = e.fav?.ToString() ?? "",
                finish  = e.finish?.ToString() ?? ""
            }).ToList();

            await _hub.Clients.All.SendAsync(method, new { raceId = grp.Key, entries }, ct);
        }

        _logger.LogInformation("[LiveBroadcast] {Method} sent for {Count} race(s).", method, ids.Count);
    }
}
