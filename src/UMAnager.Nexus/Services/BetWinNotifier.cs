using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services;

/// <summary>
/// After a results-fetch tick lands fresh finish positions, evaluate each touched race
/// against the user's marks blob and fire a Discord webhook for any newly-won bets.
/// Mirrors the frontend's <c>evaluateRaceRecap</c> logic so dashboard pills and Discord
/// pings always agree:
///   ◎ Win   — ◎ Honmei pick finished 1st
///   Q Box   — any two marked horses (◎〇▲△) took 1st + 2nd
///   T Box   — any three marked horses took 1st + 2nd + 3rd
/// Races we've already notified are tracked in <c>app_state.bet_win_notified_race_ids</c>
/// so a race only ever pings once, even though the orchestrator re-broadcasts results on
/// every 5-minute tick until phase leaves LIVE.
/// </summary>
public sealed class BetWinNotifier
{
    private const string MarksStateKey         = "user_marks_blob";
    private const string NotifiedRacesStateKey = "bet_win_notified_race_ids";

    private static readonly IReadOnlyDictionary<string, string> TrackNames = new Dictionary<string, string>
    {
        ["01"] = "Sapporo", ["02"] = "Hakodate", ["03"] = "Fukushima", ["04"] = "Niigata",
        ["05"] = "Tokyo",   ["06"] = "Nakayama", ["07"] = "Chukyo",    ["08"] = "Kyoto",
        ["09"] = "Hanshin", ["10"] = "Kokura",
    };

    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly AppStateService _state;
    private readonly IDiscordNotifier _discord;
    private readonly ILogger<BetWinNotifier> _logger;

    public BetWinNotifier(
        IDbContextFactory<AppDbContext> dbFactory,
        AppStateService state,
        IDiscordNotifier discord,
        ILogger<BetWinNotifier> logger)
    {
        _dbFactory = dbFactory;
        _state     = state;
        _discord   = discord;
        _logger    = logger;
    }

    public async Task EvaluateAndNotifyAsync(IEnumerable<string> raceIds, CancellationToken ct = default)
    {
        var ids = raceIds.Where(s => !string.IsNullOrWhiteSpace(s)).Distinct().ToList();
        if (ids.Count == 0) return;

        var marks = await LoadMarksAsync();
        if (marks.Count == 0) return; // no bets placed → nothing to win

        var notified = await LoadNotifiedSetAsync();
        var freshCandidates = ids.Where(id => !notified.Contains(id)).ToList();
        if (freshCandidates.Count == 0) return;

        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        var entries = await db.RaceEntries.AsNoTracking()
            .Where(e => freshCandidates.Contains(e.RaceId) && e.FinishPos != null && e.FinishPos > 0)
            .Select(e => new { e.RaceId, e.HorseId, Finish = e.FinishPos!.Value })
            .ToListAsync(ct);

        var races = await db.Races.AsNoTracking()
            .Where(r => freshCandidates.Contains(r.RaceId))
            .Select(r => new { r.RaceId, r.TrackCode, r.RaceNumber })
            .ToListAsync(ct);
        var raceMeta = races.ToDictionary(r => r.RaceId, r => r);

        // Resolve horse names for every top-3 finisher across all candidate races. One round-trip;
        // we'll filter to actual hits per-race below. NameEn preferred, fallback to NameJa.
        var topFinisherIds = entries.Select(e => e.HorseId!).Where(s => !string.IsNullOrEmpty(s)).Distinct().ToList();
        var horseNames = await db.Horses.AsNoTracking()
            .Where(h => topFinisherIds.Contains(h.HorseId))
            .Select(h => new { h.HorseId, h.NameEn, h.NameJa })
            .ToListAsync(ct);
        var nameByHorse = horseNames.ToDictionary(
            h => h.HorseId,
            h => !string.IsNullOrEmpty(h.NameEn) ? h.NameEn! : (h.NameJa ?? h.HorseId));

        var anyNotified = false;
        foreach (var raceId in freshCandidates)
        {
            var top3 = entries
                .Where(e => e.RaceId == raceId && e.Finish >= 1 && e.Finish <= 3)
                .GroupBy(e => e.Finish)
                .ToDictionary(g => g.Key, g => g.First().HorseId);

            // Wait for a complete top-3 before deciding. Partial data → leave un-notified,
            // re-check next tick.
            if (!(top3.ContainsKey(1) && top3.ContainsKey(2) && top3.ContainsKey(3))) continue;

            var raceMarks = marks
                .Where(kv => kv.Key.StartsWith(raceId + "_"))
                .ToDictionary(kv => kv.Key.Substring(raceId.Length + 1), kv => kv.Value);

            var pickedSet = new HashSet<string>(raceMarks.Keys);
            raceMarks.TryGetValue(top3[1], out var firstMark);

            var honmei   = firstMark == "◎";
            var quinella = pickedSet.Contains(top3[1]) && pickedSet.Contains(top3[2]);
            var trio     = quinella && pickedSet.Contains(top3[3]);

            var pills = new List<string>();
            if (honmei)   pills.Add("◎ Win");
            if (quinella) pills.Add("Q Box");
            if (trio)     pills.Add("T Box");

            notified.Add(raceId);

            if (pills.Count == 0) continue; // top-3 complete but no hit — mark notified, no ping

            // Build per-hit horse detail: each marked horse that landed in top 3, with mark + name + position.
            // For Q Box show top-2; for T Box top-3; for ◎ Win alone, just the winner.
            var maxPos = trio ? 3 : (quinella ? 2 : 1);
            var hits = new List<MarkHit>();
            for (int pos = 1; pos <= maxPos; pos++)
            {
                var horseId = top3[pos];
                if (!raceMarks.TryGetValue(horseId, out var mark) || string.IsNullOrEmpty(mark)) continue;
                var name = nameByHorse.TryGetValue(horseId, out var n) ? n : horseId;
                hits.Add(new MarkHit(mark, name, pos));
            }

            var label = BuildRaceLabel(raceMeta.TryGetValue(raceId, out var rm) ? rm.TrackCode : null,
                                       raceMeta.TryGetValue(raceId, out var rm2) ? rm2.RaceNumber : null);
            try
            {
                await _discord.NotifyMarkHitsAsync(label, pills, hits, ct);
                _logger.LogInformation("[BetWin] Race {RaceId} hits: {Pills} | {Hits}",
                    raceId, string.Join(",", pills),
                    string.Join(", ", hits.Select(h => $"{h.Mark} {h.HorseName} ({h.Finish})")));
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[BetWin] Discord notify failed for race {RaceId}", raceId);
            }
            anyNotified = true;
        }

        if (anyNotified || notified.Count > 0)
            await SaveNotifiedSetAsync(notified);
    }

    private static string BuildRaceLabel(string? trackCode, int? raceNumber)
    {
        var name = trackCode != null && TrackNames.TryGetValue(trackCode, out var n) ? n : (trackCode ?? "?");
        return $"{name} R{raceNumber?.ToString() ?? "?"}";
    }

    private async Task<Dictionary<string, string>> LoadMarksAsync()
    {
        var raw = await _state.GetStringAsync(MarksStateKey);
        if (string.IsNullOrWhiteSpace(raw)) return new();
        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (!doc.RootElement.TryGetProperty("marks", out var marksEl)) return new();
            var dict = new Dictionary<string, string>();
            foreach (var prop in marksEl.EnumerateObject())
            {
                var v = prop.Value.GetString();
                if (string.IsNullOrEmpty(v) || v == "X") continue;
                dict[prop.Name] = v;
            }
            return dict;
        }
        catch (JsonException)
        {
            return new();
        }
    }

    private async Task<HashSet<string>> LoadNotifiedSetAsync()
    {
        var raw = await _state.GetStringAsync(NotifiedRacesStateKey);
        if (string.IsNullOrWhiteSpace(raw)) return new();
        try
        {
            var arr = JsonSerializer.Deserialize<string[]>(raw);
            return arr is null ? new() : new HashSet<string>(arr);
        }
        catch (JsonException)
        {
            return new();
        }
    }

    private Task SaveNotifiedSetAsync(HashSet<string> set)
        => _state.SetStringAsync(NotifiedRacesStateKey, JsonSerializer.Serialize(set.ToArray()));
}
