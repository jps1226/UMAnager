// ============================================================
// FILE: ClaudeRecapWriter.cs
// LAYER: Service (scoped)
// PURPOSE: Writes recap_data.json to the project root after a day settles (Phase 22). A Claude
//          Desktop routine polls for processed=false, generates a narrative, posts to Discord,
//          and marks processed=true. Defines the recap DTO graph.
// KEY DEPENDENCIES: AppDbContext.
// CAUTION: Locates project root by walking up from AppContext.BaseDirectory looking for CLAUDE.md.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Phase 22: writes recap_data.json to the project root after a race day settles.
/// A Claude Code routine polls for processed=false, generates a narrative + betting
/// suggestions, posts to Discord, then marks the file processed=true.
/// </summary>
public sealed class ClaudeRecapWriter
{
    private static readonly IReadOnlyDictionary<string, string> TrackNames = new Dictionary<string, string>
    {
        ["01"] = "Sapporo", ["02"] = "Hakodate", ["03"] = "Fukushima", ["04"] = "Niigata",
        ["05"] = "Tokyo",   ["06"] = "Nakayama", ["07"] = "Chukyo",    ["08"] = "Kyoto",
        ["09"] = "Hanshin", ["10"] = "Kokura",
    };

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly ILogger<ClaudeRecapWriter> _logger;

    public ClaudeRecapWriter(IDbContextFactory<AppDbContext> dbFactory, ILogger<ClaudeRecapWriter> logger)
    {
        _dbFactory = dbFactory;
        _logger = logger;
    }

    /// <summary>
    /// Builds and writes recap_data.json. Called by DayRecapNotifier after its own
    /// Discord send succeeds. Non-fatal — exceptions are caught and logged.
    /// </summary>
    public async Task WriteAsync(
        string dateKey,
        IReadOnlyList<string> dayRaceIds,
        Dictionary<string, string> marks,
        DayRecap recap,
        CancellationToken ct = default)
    {
        try
        {
            await using var db = await _dbFactory.CreateDbContextAsync(ct);

            // Load races
            var races = await db.Races.AsNoTracking()
                .Where(r => dayRaceIds.Contains(r.RaceId))
                .OrderBy(r => r.RaceNumber)
                .ToListAsync(ct);

            // Load all entries for the day with their finish positions
            var entries = await db.RaceEntries.AsNoTracking()
                .Where(e => dayRaceIds.Contains(e.RaceId))
                .ToListAsync(ct);

            // Build horse name lookup (NameEn preferred, NameJa fallback)
            var horseIds = entries.Select(e => e.HorseId).Distinct().ToList();
            var horses = await db.Horses.AsNoTracking()
                .Where(h => horseIds.Contains(h.HorseId))
                .Select(h => new { h.HorseId, h.NameEn, h.NameJa })
                .ToListAsync(ct);
            var horseNames = horses.ToDictionary(
                h => h.HorseId,
                h => !string.IsNullOrWhiteSpace(h.NameEn) ? h.NameEn : h.NameJa ?? h.HorseId);

            var entriesByRace = entries.GroupBy(e => e.RaceId)
                .ToDictionary(g => g.Key, g => g.OrderBy(e => e.PostPosition ?? 99).ToList());

            var racePayloads = new List<RecapRacePayload>();

            foreach (var race in races)
            {
                if (!entriesByRace.TryGetValue(race.RaceId, out var raceEntries) || raceEntries.Count == 0)
                    continue;

                var userMarked = marks.Keys.Any(k => k.StartsWith(race.RaceId + "_"));
                var finishers  = raceEntries.Where(e => e.FinishPos is > 0).OrderBy(e => e.FinishPos).ToList();
                var winner     = finishers.FirstOrDefault(e => e.FinishPos == 1);
                var second     = finishers.FirstOrDefault(e => e.FinishPos == 2);
                var third      = finishers.FirstOrDefault(e => e.FinishPos == 3);

                // Upset: winner's fav rank was 4+ (long shot won)
                var upset = winner?.FavRank >= 4;
                // Chalk fail: favourite (fav_rank=1) finished outside top 3
                var favHorse  = raceEntries.FirstOrDefault(e => e.FavRank == 1);
                var chalkFail = favHorse?.FinishPos is > 3;

                // Parse payouts from ResultsJson
                int winPayout = 0, qPayout = 0, tPayout = 0;
                if (!string.IsNullOrEmpty(race.ResultsJson))
                {
                    try
                    {
                        using var doc = JsonDocument.Parse(race.ResultsJson);
                        var root = doc.RootElement;
                        if (winner?.PostPosition.HasValue == true)
                            winPayout = FindPayout(root, "win", [winner.PostPosition.Value]);
                        if (winner?.PostPosition.HasValue == true && second?.PostPosition.HasValue == true)
                        {
                            var qCombo = new[] { winner.PostPosition.Value, second.PostPosition.Value };
                            Array.Sort(qCombo);
                            qPayout = FindPayout(root, "quinella", qCombo);
                        }
                        if (winner?.PostPosition.HasValue == true && second?.PostPosition.HasValue == true && third?.PostPosition.HasValue == true)
                        {
                            var tCombo = new[] { winner.PostPosition.Value, second.PostPosition.Value, third.PostPosition.Value };
                            Array.Sort(tCombo);
                            tPayout = FindPayout(root, "trio", tCombo);
                        }
                    }
                    catch (JsonException) { }
                }

                // Per-user result
                var pickedSet = marks
                    .Where(kv => kv.Key.StartsWith(race.RaceId + "_"))
                    .ToDictionary(kv => kv.Key[(race.RaceId.Length + 1)..], kv => kv.Value);

                var honmeiHit   = winner != null && pickedSet.TryGetValue(winner.HorseId, out var wm) && wm == "◎";
                var quinellaHit = winner != null && second != null
                                  && pickedSet.ContainsKey(winner.HorseId) && pickedSet.ContainsKey(second.HorseId);
                var trioHit     = quinellaHit && third != null && pickedSet.ContainsKey(third.HorseId);

                // Build entry list (all runners, with user mark and finish)
                var entryPayloads = raceEntries.Select(e => new RecapEntryPayload(
                    PostPosition: e.PostPosition,
                    HorseName:    horseNames.GetValueOrDefault(e.HorseId, e.HorseId),
                    Odds:         e.Odds.HasValue ? (double)e.Odds.Value : null,
                    FavRank:      e.FavRank,
                    UserMark:     pickedSet.GetValueOrDefault(e.HorseId),
                    FinishPos:    e.FinishPos
                )).ToList();

                racePayloads.Add(new RecapRacePayload(
                    RaceId:     race.RaceId,
                    Track:      TrackNames.GetValueOrDefault(race.TrackCode ?? "", race.TrackCode ?? "?"),
                    RaceNumber: race.RaceNumber,
                    RaceName:   !string.IsNullOrWhiteSpace(race.NameJa) ? race.NameJa : race.RaceClass,
                    Distance:   race.Distance,
                    Surface:    race.Surface,
                    UserMarked: userMarked,
                    Entries:    entryPayloads,
                    Result: new RecapRaceResult(
                        WinnerPost:    winner?.PostPosition,
                        WinnerName:    winner != null ? horseNames.GetValueOrDefault(winner.HorseId, winner.HorseId) : null,
                        WinnerOdds:    winner?.Odds.HasValue == true ? (double)winner.Odds!.Value : null,
                        WinnerFavRank: winner?.FavRank,
                        SecondPost:    second?.PostPosition,
                        SecondName:    second != null ? horseNames.GetValueOrDefault(second.HorseId, second.HorseId) : null,
                        ThirdPost:     third?.PostPosition,
                        ThirdName:     third != null ? horseNames.GetValueOrDefault(third.HorseId, third.HorseId) : null,
                        WinPayout:     winPayout > 0 ? winPayout : null,
                        QuinellaPayout: qPayout > 0 ? qPayout : null,
                        TrioPayout:    tPayout > 0 ? tPayout : null,
                        Upset:         upset,
                        ChalkFail:     chalkFail
                    ),
                    UserResult: userMarked ? new RecapUserResult(
                        HonmeiHit:   honmeiHit,
                        QBoxHit:     quinellaHit,
                        TBoxHit:     trioHit,
                        WonYen:      recap.WinningLines.Any(l => l.StartsWith(TrackNames.GetValueOrDefault(race.TrackCode ?? "", "?")))
                                     ? (int?)null // approximate — exact calc is in DayRecapNotifier
                                     : null
                    ) : null
                ));
            }

            // Fetch Discord webhook URL so the cron routine doesn't need DB access.
            var webhookRow = await db.AppSettings.AsNoTracking()
                .FirstOrDefaultAsync(s => s.Key == SettingsService.Keys.DiscordWebhookUrl, ct);
            var webhookUrl = webhookRow?.Value;

            var payload = new ClaudeRecapPayload(
                Date:              dateKey,
                GeneratedAtUtc:    DateTime.UtcNow,
                Processed:         false,
                DiscordWebhookUrl: webhookUrl,
                Summary: new RecapSummary(
                    RacesTotal:     recap.RacesTotal,
                    RacesMarked:    recap.RacesMarked,
                    RacesWon:       recap.RacesWon,
                    TotalStakedYen: recap.TotalStakedYen,
                    TotalWonYen:    recap.TotalWonYen,
                    NetYen:         recap.NetYen,
                    WinningLines:   recap.WinningLines
                ),
                Races: racePayloads
            );

            var path = ResolveRecapPath();
            await File.WriteAllTextAsync(path, JsonSerializer.Serialize(payload, JsonOpts), ct);
            _logger.LogInformation("[ClaudeRecap] Wrote {Path} ({Races} races, {Marked} marked)", path, racePayloads.Count, recap.RacesMarked);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[ClaudeRecap] Failed to write recap_data.json");
        }
    }

    /// <summary>
    /// Find the project root and return the recap_data.json path there. The Claude routine
    /// reads from <c>C:\…\UMAnager2\recap_data.json</c>, but the Nexus process's
    /// <c>Environment.CurrentDirectory</c> is the EXE output dir (<c>bin/Release/net8.0</c>)
    /// because <c>AppContext.BaseDirectory</c> is pinned there. Walking up from BaseDirectory
    /// looking for CLAUDE.md as a marker reliably locates the project root regardless of
    /// where the EXE is launched from. Falls back to CWD if no marker is found (preserves
    /// the prior behavior for dev environments without the marker).
    /// </summary>
    private static string ResolveRecapPath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "CLAUDE.md")))
                return Path.Combine(dir.FullName, "recap_data.json");
            dir = dir.Parent;
        }
        return Path.GetFullPath("recap_data.json");
    }

    private static int FindPayout(JsonElement root, string betType, int[] combo)
    {
        if (!root.TryGetProperty(betType, out var arr) || arr.ValueKind != JsonValueKind.Array) return 0;
        foreach (var slot in arr.EnumerateArray())
        {
            if (!slot.TryGetProperty("combo", out var slotCombo) || slotCombo.ValueKind != JsonValueKind.Array) continue;
            if (slotCombo.GetArrayLength() != combo.Length) continue;
            var slotArr = slotCombo.EnumerateArray().Select(e => e.GetInt32()).ToArray();
            Array.Sort(slotArr);
            if (!slotArr.SequenceEqual(combo)) continue;
            return slot.TryGetProperty("payout", out var p) ? p.GetInt32() : 0;
        }
        return 0;
    }
}

// ── DTOs ─────────────────────────────────────────────────────────────────────

public sealed record ClaudeRecapPayload(
    string Date,
    DateTime GeneratedAtUtc,
    bool Processed,
    string? DiscordWebhookUrl,
    RecapSummary Summary,
    List<RecapRacePayload> Races);

public sealed record RecapSummary(
    int RacesTotal,
    int RacesMarked,
    int RacesWon,
    int TotalStakedYen,
    int TotalWonYen,
    int NetYen,
    List<string> WinningLines);

public sealed record RecapRacePayload(
    string RaceId,
    string Track,
    int? RaceNumber,
    string? RaceName,
    int? Distance,
    string? Surface,
    bool UserMarked,
    List<RecapEntryPayload> Entries,
    RecapRaceResult Result,
    RecapUserResult? UserResult);

public sealed record RecapEntryPayload(
    int? PostPosition,
    string HorseName,
    double? Odds,
    int? FavRank,
    string? UserMark,
    int? FinishPos);

public sealed record RecapRaceResult(
    int? WinnerPost,
    string? WinnerName,
    double? WinnerOdds,
    int? WinnerFavRank,
    int? SecondPost,
    string? SecondName,
    int? ThirdPost,
    string? ThirdName,
    int? WinPayout,
    int? QuinellaPayout,
    int? TrioPayout,
    bool Upset,
    bool? ChalkFail);

public sealed record RecapUserResult(
    bool HonmeiHit,
    bool QBoxHit,
    bool TBoxHit,
    int? WonYen);
