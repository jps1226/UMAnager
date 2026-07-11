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

        // ResultsUpdated also needs the payout table (Races.ResultsJson). Without it the client can
        // never actually SCORE a placed bet from a live push — only Finish positions patch via the
        // entries above — so a win/loss verdict would stay stuck until a full page reload, no matter
        // how long you wait. s60 fix: found live when a real win's badge + Day Net never updated,
        // even a minute after the Discord ping (which reads the DB directly and was already correct).
        Dictionary<string, string?> resultsJsonByRace = new();
        if (method == "ResultsUpdated")
        {
            resultsJsonByRace = await db.Races.AsNoTracking()
                .Where(r => ids.Contains(r.RaceId))
                .Select(r => new { r.RaceId, r.ResultsJson })
                .ToDictionaryAsync(r => r.RaceId, r => r.ResultsJson, ct);
        }

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

            object payload = method == "ResultsUpdated"
                ? new { raceId = grp.Key, entries, resultsJson = resultsJsonByRace.GetValueOrDefault(grp.Key) }
                : new { raceId = grp.Key, entries };

            await _hub.Clients.All.SendAsync(method, payload, ct);
        }

        _logger.LogInformation("[LiveBroadcast] {Method} sent for {Count} race(s).", method, ids.Count);
    }
}
