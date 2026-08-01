// ============================================================
// FILE: DayRecapNotifier.cs
// LAYER: Service (scoped)
// PURPOSE: Once a JST race-day is ≥80% finished with all ResultsJson present, fires ONE Discord
//          recap (placed/won/staked/net + winning lines) and hands off to ClaudeRecapWriter.
//          Idempotent via app_state.day_recap_sent_dates. Defines the DayRecap record.
// KEY DEPENDENCIES: AppDbContext, AppStateService, SettingsService, IDiscordNotifier,
//          ClaudeRecapWriter, TemplateBetEvaluator.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Phase 16: once all races for a JST race-day are fully scored (every entry has
/// FinishPos), fires a single Discord recap covering the day's marked-bet hits and
/// estimated ¥ won. Idempotent via app_state.day_recap_sent_dates.
///
/// Triggered piggyback off NexusPipeServer's results-tick path — same place
/// BetWinNotifier runs — so the recap arrives within seconds of the last race
/// of the day settling.
/// </summary>
public sealed class DayRecapNotifier
{
    private const string MarksStateKey       = "user_marks_blob";
    private const string SentDatesStateKey   = "day_recap_sent_dates";

    private static readonly IReadOnlyDictionary<string, string> TrackNames = new Dictionary<string, string>
    {
        ["01"] = "Sapporo", ["02"] = "Hakodate", ["03"] = "Fukushima", ["04"] = "Niigata",
        ["05"] = "Tokyo",   ["06"] = "Nakayama", ["07"] = "Chukyo",    ["08"] = "Kyoto",
        ["09"] = "Hanshin", ["10"] = "Kokura",
    };

    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly AppStateService _state;
    private readonly SettingsService _settings;
    private readonly IDiscordNotifier _discord;
    private readonly ClaudeRecapWriter _claudeRecap;
    private readonly ILogger<DayRecapNotifier> _logger;

    public DayRecapNotifier(
        IDbContextFactory<AppDbContext> dbFactory,
        AppStateService state,
        SettingsService settings,
        IDiscordNotifier discord,
        ClaudeRecapWriter claudeRecap,
        ILogger<DayRecapNotifier> logger)
    {
        _dbFactory = dbFactory;
        _state = state;
        _settings = settings;
        _discord = discord;
        _claudeRecap = claudeRecap;
        _logger = logger;
    }

    /// <summary>
    /// Inspect the JST race-dates touched by the given raceIds. For any date whose
    /// races are all finalized AND that we haven't recapped yet, build and send the
    /// recap message.
    /// </summary>
    public async Task EvaluateAndNotifyAsync(IEnumerable<string> raceIds, CancellationToken ct = default)
    {
        var ids = raceIds.Where(s => !string.IsNullOrWhiteSpace(s)).Distinct().ToList();
        if (ids.Count == 0) return;

        await using var db = await _dbFactory.CreateDbContextAsync(ct);

        // What dates were touched by this tick?
        var touchedDates = await db.Races.AsNoTracking()
            .Where(r => ids.Contains(r.RaceId))
            .Select(r => r.RaceDate)
            .Distinct()
            .ToListAsync(ct);
        if (touchedDates.Count == 0) return;

        var sentDates = await LoadSentDatesAsync();
        var dateKeys = touchedDates
            .Select(d => d.ToString("yyyy-MM-dd"))
            .Where(k => !sentDates.Contains(k))
            .ToList();
        if (dateKeys.Count == 0) return;

        var (marks, locked, frozenLines) = await LoadMarksAndLocksAsync();

        foreach (var dateKey in dateKeys)
        {
            var date = DateTime.SpecifyKind(DateTime.ParseExact(dateKey, "yyyy-MM-dd", null), DateTimeKind.Utc);

            // All races for the date: must all have ResultsJson AND every entry must
            // have a non-null FinishPos. If any race is still in flight, defer recap.
            var dayRaces = await db.Races.AsNoTracking()
                .Where(r => r.RaceDate == date)
                .Select(r => new { r.RaceId, r.TrackCode, r.RaceNumber, r.ResultsJson })
                .ToListAsync(ct);
            if (dayRaces.Count == 0) continue;

            var dayRaceIds = dayRaces.Select(r => r.RaceId).ToList();
            var totalEntries = await db.RaceEntries.AsNoTracking()
                .Where(e => dayRaceIds.Contains(e.RaceId))
                .CountAsync(ct);
            var finishedEntries = await db.RaceEntries.AsNoTracking()
                .Where(e => dayRaceIds.Contains(e.RaceId) && e.FinishPos != null && e.FinishPos > 0)
                .CountAsync(ct);
            // Allow a small tolerance for cancelled / scratched horses (they get FinishPos=0).
            // If finished entries < 80% of total OR any race has no ResultsJson, the day isn't done.
            if (totalEntries == 0) continue;
            var pctFinished = (double)finishedEntries / totalEntries;
            if (pctFinished < 0.80) continue;
            if (dayRaces.Any(r => string.IsNullOrEmpty(r.ResultsJson))) continue;

            // Load EVERY entry of the day (the evaluator needs marked horses' post positions,
            // which aren't necessarily top-3 finishers).
            var allEntries = await db.RaceEntries.AsNoTracking()
                .Where(e => dayRaceIds.Contains(e.RaceId))
                .Select(e => new { e.RaceId, e.HorseId, e.PostPosition, e.FinishPos })
                .ToListAsync(ct);
            var entriesByRace = allEntries.GroupBy(e => e.RaceId).ToDictionary(g => g.Key, g => g.ToList());

            // Build the recap from the ACTUAL placed (locked) templates. A race counts only
            // if it's locked AND carries marks; won is priced per the template (place/wide/trio).
            int placed = 0, racesWon = 0, totalWon = 0, totalStaked = 0;
            var winningLines = new List<string>();
            foreach (var r in dayRaces)
            {
                if (!locked.Contains(r.RaceId)) continue;
                var raceEntries = entriesByRace.GetValueOrDefault(r.RaceId) ?? new();
                var ppByHorse = raceEntries.GroupBy(e => e.HorseId).ToDictionary(g => g.Key, g => g.First().PostPosition);
                var runners = TemplateBetEvaluator.BuildRunners(r.RaceId, marks, ppByHorse);
                var hasFrozenLines = frozenLines.TryGetValue(r.RaceId, out var frozen) && frozen.Count > 0;
                // Discipline mode has no manual marks; its applied bet is represented by
                // the frozen bet lines. Keep the same fallback used by BetWinNotifier.
                if (runners.Count == 0 && !hasFrozenLines) continue;
                placed++;

                int? pp1 = null, pp2 = null, pp3 = null;
                foreach (var e in raceEntries)
                {
                    if (e.FinishPos == 1) pp1 = e.PostPosition;
                    else if (e.FinishPos == 2) pp2 = e.PostPosition;
                    else if (e.FinishPos == 3) pp3 = e.PostPosition;
                }

                JsonElement? payouts = null; JsonDocument? pdoc = null;
                try { pdoc = JsonDocument.Parse(r.ResultsJson ?? "{}"); payouts = pdoc.RootElement; }
                catch (JsonException) { }
                // Applied races carry frozen lines → score them verbatim; else default template.
                var outcome = hasFrozenLines
                    ? TemplateBetEvaluator.EvaluateLines(frozen!, runners.Count, pp1, pp2, pp3, payouts)
                    : TemplateBetEvaluator.Evaluate(runners, pp1, pp2, pp3, payouts);
                pdoc?.Dispose();

                totalStaked += outcome.Staked;
                totalWon    += outcome.Won;
                if (outcome.AnyHit)
                {
                    racesWon++;
                    var trackName = (r.TrackCode != null && TrackNames.TryGetValue(r.TrackCode, out var tn)) ? tn : (r.TrackCode ?? "?");
                    var detail = string.Join(" · ", outcome.HitLabels
                        .GroupBy(x => x).Select(g => g.Count() > 1 ? $"{g.Key}×{g.Count()}" : g.Key));
                    winningLines.Add($"{trackName} R{r.RaceNumber} — {detail} · ¥{outcome.Won:N0}");
                }
            }

            DayRecap? recap = placed == 0
                ? null
                : new DayRecap(dateKey, dayRaces.Count, placed, racesWon, totalStaked, totalWon, totalWon - totalStaked, winningLines);

            // Mark the day as recap-evaluated even if there were no marks, so we don't
            // keep computing this every tick for the rest of the week.
            sentDates.Add(dateKey);

            if (recap == null)
            {
                _logger.LogInformation("[DayRecap] {Date} settled with no marked races — skipping ping", dateKey);
                continue;
            }

            try
            {
                await _discord.NotifyDayRecapAsync(recap, ct);
                _logger.LogInformation("[DayRecap] Sent for {Date}: {RacesMarked} marked, ¥{Won:N0} won",
                    dateKey, recap.RacesMarked, recap.TotalWonYen);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[DayRecap] Discord send failed for {Date}", dateKey);
            }

            // Phase 22: write recap_data.json for the Claude routine to pick up.
            await _claudeRecap.WriteAsync(dateKey, dayRaceIds, marks, recap, ct);
        }

        await SaveSentDatesAsync(sentDates);
    }


    /// <summary>Load the marks dict (non-X) and the set of locked (placed) race-ids.</summary>
    private async Task<(Dictionary<string, string> Marks, HashSet<string> Locked, Dictionary<string, List<TemplateBetEvaluator.BetLine>> FrozenLines)> LoadMarksAndLocksAsync()
    {
        var marks = new Dictionary<string, string>();
        var locked = new HashSet<string>();
        var frozen = new Dictionary<string, List<TemplateBetEvaluator.BetLine>>();
        var raw = await _state.GetStringAsync(MarksStateKey);
        if (string.IsNullOrWhiteSpace(raw)) return (marks, locked, frozen);
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            if (root.TryGetProperty("marks", out var marksEl) && marksEl.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in marksEl.EnumerateObject())
                {
                    var v = prop.Value.GetString();
                    if (string.IsNullOrEmpty(v) || v == "X") continue;
                    marks[prop.Name] = v;
                }
            }
            if (root.TryGetProperty("raceMeta", out var metaEl) && metaEl.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in metaEl.EnumerateObject())
                {
                    if (prop.Value.ValueKind != JsonValueKind.Object) continue;
                    if (prop.Value.TryGetProperty("lockStateAtSave", out var lk) && lk.ValueKind == JsonValueKind.True)
                        locked.Add(prop.Name);
                    if (prop.Value.TryGetProperty("betProfile", out var bpEl) && bpEl.ValueKind == JsonValueKind.Object
                        && bpEl.TryGetProperty("betLines", out var blEl) && blEl.ValueKind == JsonValueKind.Array)
                    {
                        var lines = TemplateBetEvaluator.ParseFrozenLines(blEl);
                        if (lines.Count > 0) frozen[prop.Name] = lines;
                    }
                }
            }
        }
        catch (JsonException) { }
        return (marks, locked, frozen);
    }

    private async Task<HashSet<string>> LoadSentDatesAsync()
    {
        var raw = await _state.GetStringAsync(SentDatesStateKey);
        if (string.IsNullOrWhiteSpace(raw)) return new();
        try
        {
            var arr = JsonSerializer.Deserialize<string[]>(raw);
            return arr is null ? new() : new HashSet<string>(arr);
        }
        catch (JsonException) { return new(); }
    }

    private Task SaveSentDatesAsync(HashSet<string> set)
        => _state.SetStringAsync(SentDatesStateKey, JsonSerializer.Serialize(set.ToArray()));
}

public sealed record DayRecap(
    string DateKey,
    int RacesTotal,
    int RacesMarked,   // = placed (locked + marked) races
    int RacesWon,      // races where the placed template hit
    int TotalStakedYen,
    int TotalWonYen,
    int NetYen,
    List<string> WinningLines);
