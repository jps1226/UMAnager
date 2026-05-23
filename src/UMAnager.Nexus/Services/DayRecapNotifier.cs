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

        var marks = await LoadMarksAsync();
        var stakes = await _settings.GetBetStakesAsync();

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

            // Build the recap.
            var topByRace = await db.RaceEntries.AsNoTracking()
                .Where(e => dayRaceIds.Contains(e.RaceId) && e.FinishPos != null && e.FinishPos >= 1 && e.FinishPos <= 3)
                .Select(e => new { e.RaceId, e.HorseId, e.PostPosition, Finish = e.FinishPos!.Value })
                .ToListAsync(ct);

            var recap = ComputeRecap(date, dateKey, dayRaces.Cast<object>().ToList(), topByRace.Cast<object>().ToList(), marks, stakes);

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

    private DayRecap? ComputeRecap(DateTime date, string dateKey, List<object> dayRaces,
                                   List<object> topByRace, Dictionary<string, string> marks,
                                   (int Win, int Quinella, int Trio) stakes)
    {
        // dayRaces: anon { RaceId, TrackCode, RaceNumber, ResultsJson }
        // topByRace: anon { RaceId, HorseId, PostPosition, Finish }
        // marks: full marks dict keyed "{raceId}_{horseId}" → symbol (◎/〇/▲/△/X)
        var raceTops = topByRace
            .Select(o =>
            {
                var t = o.GetType();
                return new
                {
                    RaceId       = (string)t.GetProperty("RaceId")!.GetValue(o)!,
                    HorseId      = (string)(t.GetProperty("HorseId")!.GetValue(o) ?? ""),
                    PostPosition = (int?)t.GetProperty("PostPosition")!.GetValue(o),
                    Finish       = (int)t.GetProperty("Finish")!.GetValue(o)!,
                };
            })
            .GroupBy(x => x.RaceId)
            .ToDictionary(g => g.Key, g => g.GroupBy(x => x.Finish).ToDictionary(f => f.Key, f => f.First()));

        int honmei = 0, qBox = 0, tBox = 0, totalWon = 0, marked = 0;
        var winningLines = new List<string>();

        foreach (var rObj in dayRaces)
        {
            var t = rObj.GetType();
            var raceId      = (string)t.GetProperty("RaceId")!.GetValue(rObj)!;
            var trackCode   = (string?)t.GetProperty("TrackCode")!.GetValue(rObj);
            var raceNumber  = (int?)t.GetProperty("RaceNumber")!.GetValue(rObj);
            var resultsJson = (string?)t.GetProperty("ResultsJson")!.GetValue(rObj);

            var raceMarks = marks
                .Where(kv => kv.Key.StartsWith(raceId + "_"))
                .ToDictionary(kv => kv.Key.Substring(raceId.Length + 1), kv => kv.Value);
            if (raceMarks.Count == 0) continue; // unmarked race — skip from totals
            marked++;

            if (!raceTops.TryGetValue(raceId, out var topMap) || !topMap.ContainsKey(1) || !topMap.ContainsKey(2) || !topMap.ContainsKey(3))
                continue; // incomplete results — shouldn't happen given the gate above, but be safe

            var first  = topMap[1];
            var second = topMap[2];
            var third  = topMap[3];

            var pickedSet = new HashSet<string>(raceMarks.Keys);
            var honmeiHit   = raceMarks.TryGetValue(first.HorseId, out var firstMark) && firstMark == "◎";
            var quinellaHit = pickedSet.Contains(first.HorseId) && pickedSet.Contains(second.HorseId);
            var trioHit     = quinellaHit && pickedSet.Contains(third.HorseId);

            if (!honmeiHit && !quinellaHit && !trioHit) continue;

            // Parse payouts for this race.
            int winPayout = 0, qPayout = 0, tPayout = 0;
            try
            {
                using var doc = JsonDocument.Parse(resultsJson ?? "{}");
                var root = doc.RootElement;
                if (honmeiHit && first.PostPosition.HasValue)
                {
                    winPayout = FindPayout(root, "win", new[] { first.PostPosition.Value });
                }
                if (quinellaHit && first.PostPosition.HasValue && second.PostPosition.HasValue)
                {
                    var combo = new[] { first.PostPosition.Value, second.PostPosition.Value };
                    Array.Sort(combo);
                    qPayout = FindPayout(root, "quinella", combo);
                }
                if (trioHit && first.PostPosition.HasValue && second.PostPosition.HasValue && third.PostPosition.HasValue)
                {
                    var combo = new[] { first.PostPosition.Value, second.PostPosition.Value, third.PostPosition.Value };
                    Array.Sort(combo);
                    tPayout = FindPayout(root, "trio", combo);
                }
            }
            catch (JsonException) { /* payouts stay 0 */ }

            // Scale per-¥100 payouts by each leg's own configured stake.
            var winYen = (int)Math.Round(winPayout * (stakes.Win      / 100.0));
            var qYen   = (int)Math.Round(qPayout   * (stakes.Quinella / 100.0));
            var tYen   = (int)Math.Round(tPayout   * (stakes.Trio     / 100.0));
            var raceWon = winYen + qYen + tYen;

            if (honmeiHit)   honmei++;
            if (quinellaHit) qBox++;
            if (trioHit)     tBox++;
            totalWon += raceWon;

            var trackName = (trackCode != null && TrackNames.TryGetValue(trackCode, out var n)) ? n : (trackCode ?? "?");
            var hitTags = new List<string>();
            if (honmeiHit)   hitTags.Add($"◎ Win ¥{winYen:N0}");
            if (quinellaHit) hitTags.Add($"Q ¥{qYen:N0}");
            if (trioHit)     hitTags.Add($"T ¥{tYen:N0}");
            winningLines.Add($"{trackName} R{raceNumber} — {string.Join(" · ", hitTags)}");
        }

        if (marked == 0) return null;
        return new DayRecap(dateKey, dayRaces.Count, marked, honmei, qBox, tBox, totalWon, winningLines);
    }

    /// <summary>Find the payout for a specific combination in a typed payouts array.</summary>
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
        catch (JsonException) { return new(); }
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
    int RacesMarked,
    int HonmeiHits,
    int QBoxHits,
    int TBoxHits,
    int TotalWonYen,
    List<string> WinningLines);
