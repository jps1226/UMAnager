// ============================================================
// FILE: BetWinNotifier.cs
// LAYER: Service (scoped)
// PURPOSE: After results land, prices each touched + LOCKED (placed) race's marks via
//          TemplateBetEvaluator and fires a Discord win ping (with this-race + running-net line).
//          Idempotent per race via app_state.bet_win_notified_race_ids.
// KEY DEPENDENCIES: AppDbContext, AppStateService, IDiscordNotifier, SunkCostService,
//          SettingsService, TemplateBetEvaluator.
// CAUTION: Waits for a complete top-3 before deciding; only locked races are eligible.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
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
    private readonly SunkCostService _sunk;
    private readonly SettingsService _settings;
    private readonly ILogger<BetWinNotifier> _logger;

    public BetWinNotifier(
        IDbContextFactory<AppDbContext> dbFactory,
        AppStateService state,
        IDiscordNotifier discord,
        SunkCostService sunk,
        SettingsService settings,
        ILogger<BetWinNotifier> logger)
    {
        _dbFactory = dbFactory;
        _state     = state;
        _discord   = discord;
        _sunk      = sunk;
        _settings  = settings;
        _logger    = logger;
    }

    public async Task EvaluateAndNotifyAsync(IEnumerable<string> raceIds, CancellationToken ct = default)
    {
        var ids = raceIds.Where(s => !string.IsNullOrWhiteSpace(s)).Distinct().ToList();
        if (ids.Count == 0) return;

        var (marks, locked, frozenLines) = await LoadMarksAndLocksAsync();
        if (marks.Count == 0) return; // no bets placed → nothing to win

        var notified = await LoadNotifiedSetAsync();
        // Only LOCKED (placed) races are eligible — never ping a win on a card you didn't bet.
        var freshCandidates = ids.Where(id => !notified.Contains(id) && locked.Contains(id)).ToList();
        if (freshCandidates.Count == 0) return;

        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        // Need EVERY entry of the candidates (the evaluator prices the marked horses' post
        // positions, which aren't necessarily top-3 finishers).
        var entries = await db.RaceEntries.AsNoTracking()
            .Where(e => freshCandidates.Contains(e.RaceId))
            .Select(e => new { e.RaceId, e.HorseId, e.PostPosition, e.FinishPos })
            .ToListAsync(ct);
        var entriesByRace = entries.GroupBy(e => e.RaceId).ToDictionary(g => g.Key, g => g.ToList());

        var races = await db.Races.AsNoTracking()
            .Where(r => freshCandidates.Contains(r.RaceId))
            .Select(r => new { r.RaceId, r.TrackCode, r.RaceNumber, r.RaceDate, r.ResultsJson })
            .ToListAsync(ct);
        var raceMeta = races.ToDictionary(r => r.RaceId, r => r);

        // Resolve names for every marked horse across candidates (for the hit-detail line). Also
        // resolve horses referenced only by a FROZEN bet line (Discipline mode races carry no
        // manual marks at all) so their names are available for the fallback hit-detail below.
        var frozenHorseIds = freshCandidates
            .Where(id => frozenLines.ContainsKey(id))
            .SelectMany(id =>
            {
                var ppToHorse = entriesByRace.GetValueOrDefault(id)?
                    .Where(e => e.PostPosition.HasValue)
                    .GroupBy(e => e.PostPosition!.Value).ToDictionary(g => g.Key, g => g.First().HorseId)
                    ?? new Dictionary<int, string>();
                return frozenLines[id].SelectMany(l => l.AxisPp.HasValue ? l.Pps.Append(l.AxisPp.Value) : l.Pps)
                    .Select(pp => ppToHorse.GetValueOrDefault(pp)).Where(h => h != null)!;
            });
        var markedHorseIds = freshCandidates
            .SelectMany(id => marks.Keys.Where(k => k.StartsWith(id + "_")).Select(k => k.Substring(id.Length + 1)))
            .Concat(frozenHorseIds!)
            .Distinct().ToList();
        var horseNames = await db.Horses.AsNoTracking()
            .Where(h => markedHorseIds.Contains(h.HorseId))
            .Select(h => new { h.HorseId, h.NameEn, h.NameJa })
            .ToListAsync(ct);
        var nameByHorse = horseNames.ToDictionary(
            h => h.HorseId,
            h => !string.IsNullOrEmpty(h.NameEn) ? h.NameEn! : (h.NameJa ?? h.HorseId));

        // Whether a Discord webhook is even configured. When it ISN'T, win-pings are intentionally
        // disabled, so we still mark winners "notified" (nothing to deliver → don't re-evaluate them
        // forever). When it IS configured, we only mark a WIN notified once Discord actually accepts
        // the message — a failed send (e.g. 429 rate-limit, or a transient network drop) leaves the
        // race un-notified so the next tick retries it, instead of silently losing the ping (s60).
        var webhookConfigured = !string.IsNullOrWhiteSpace(
            await _settings.GetStringAsync(SettingsService.Keys.DiscordWebhookUrl));

        var anyNotified = false;
        SunkCostSummary? running = null; // cumulative tally snapshot — computed once, lazily
        foreach (var raceId in freshCandidates)
        {
            if (!raceMeta.TryGetValue(raceId, out var rm)) continue;
            var raceEntries = entriesByRace.GetValueOrDefault(raceId) ?? new();

            int? pp1 = null, pp2 = null, pp3 = null;
            foreach (var e in raceEntries)
            {
                if (e.FinishPos == 1) pp1 = e.PostPosition;
                else if (e.FinishPos == 2) pp2 = e.PostPosition;
                else if (e.FinishPos == 3) pp3 = e.PostPosition;
            }
            // Wait for a complete top-3 before deciding. Partial → leave un-notified, re-check next tick.
            if (pp1 is null || pp2 is null || pp3 is null) continue;

            var ppByHorse = raceEntries.GroupBy(e => e.HorseId).ToDictionary(g => g.Key, g => g.First().PostPosition);
            var runners = TemplateBetEvaluator.BuildRunners(raceId, marks, ppByHorse);
            // Discipline mode never writes manual ◎〇▲△ marks (the engine picks the bet, not a
            // click) — so `runners` is empty for every Discipline race even when it was bet and
            // locked. Only bail here if there's ALSO no frozen bet-line record to fall back on,
            // or a real Discipline win never pings (the same globalMarks-only gap already fixed on
            // the frontend's Bets-tab race list — this was the backend half of it).
            var hasFrozenLines = frozenLines.TryGetValue(raceId, out var fl) && fl.Count > 0;
            if (runners.Count == 0 && !hasFrozenLines) { notified.Add(raceId); continue; }

            JsonElement? payouts = null; JsonDocument? pdoc = null;
            try { pdoc = JsonDocument.Parse(rm.ResultsJson ?? "{}"); payouts = pdoc.RootElement; }
            catch (JsonException) { }
            // Applied races carry frozen lines → score them verbatim; else default template.
            var outcome = hasFrozenLines
                ? TemplateBetEvaluator.EvaluateLines(fl!, runners.Count, pp1, pp2, pp3, payouts)
                : TemplateBetEvaluator.Evaluate(runners, pp1, pp2, pp3, payouts);
            pdoc?.Dispose();

            // Loser: placed + settled but didn't hit → mark notified (nothing to deliver) and move on.
            // Winners are marked notified LATER, only once Discord confirms delivery (see below).
            if (!outcome.AnyHit) { notified.Add(raceId); continue; }

            // Pills = the template lines that hit (e.g. "3連複", "ワイド×2").
            var pills = outcome.HitLabels.GroupBy(x => x)
                .Select(g => g.Count() > 1 ? $"{g.Key}×{g.Count()}" : g.Key).ToList();

            // Detail: which marked horses landed in the top 3.
            var finishByHorse = raceEntries.Where(e => e.FinishPos is >= 1 and <= 3)
                .GroupBy(e => e.HorseId).ToDictionary(g => g.Key, g => g.First().FinishPos!.Value);
            var raceMarks = marks.Where(kv => kv.Key.StartsWith(raceId + "_"))
                .ToDictionary(kv => kv.Key.Substring(raceId.Length + 1), kv => kv.Value);
            var hits = new List<MarkHit>();
            foreach (var kv in raceMarks)
            {
                if (!finishByHorse.TryGetValue(kv.Key, out var fin)) continue;
                var name = nameByHorse.TryGetValue(kv.Key, out var n) ? n : kv.Key;
                hits.Add(new MarkHit(kv.Value, name, fin));
            }
            // No manual marks (Discipline mode) → fall back to the horses the FROZEN bet lines
            // actually referenced, so the ping still names which horse hit instead of an empty list.
            if (hits.Count == 0 && hasFrozenLines)
            {
                var ppToHorse = raceEntries.Where(e => e.PostPosition.HasValue)
                    .GroupBy(e => e.PostPosition!.Value).ToDictionary(g => g.Key, g => g.First().HorseId);
                var betPps = fl!.SelectMany(l => l.AxisPp.HasValue ? l.Pps.Append(l.AxisPp.Value) : l.Pps).Distinct();
                foreach (var pp in betPps)
                {
                    if (!ppToHorse.TryGetValue(pp, out var horseId)) continue;
                    if (!finishByHorse.TryGetValue(horseId, out var fin)) continue;
                    var name = nameByHorse.TryGetValue(horseId, out var n) ? n : horseId;
                    hits.Add(new MarkHit("◎", name, fin));
                }
            }

            var label = BuildRaceLabel(rm.TrackCode, rm.RaceNumber);
            var raceNet = outcome.Won - outcome.Staked;
            var raceSign = raceNet >= 0 ? "+" : "−";
            // Scope the running tally to THIS race's JST day — the ping shows the day's running
            // net, not the lifetime total. (Batch is one race day, so compute once.)
            running ??= await _sunk.GetSummaryAsync(ct, rm.RaceDate);
            var runSign = running.NetYen >= 0 ? "+" : "−";
            var runningLine =
                $"This race: {raceSign}¥{Math.Abs(raceNet):N0} (won ¥{outcome.Won:N0} − staked ¥{outcome.Staked:N0}) · "
              + $"Day net {runSign}¥{Math.Abs(running.NetYen):N0}";

            bool delivered;
            try
            {
                delivered = await _discord.NotifyMarkHitsAsync(label, pills, hits, runningLine, ct);
            }
            catch (Exception ex)
            {
                // SendAsync already swallows exceptions internally, so this is belt-and-suspenders.
                _logger.LogWarning(ex, "[BetWin] Discord notify threw for race {RaceId}", raceId);
                delivered = false;
            }

            if (delivered)
                _logger.LogInformation("[BetWin] Race {RaceId} won ¥{Won:N0} ({Pills}) — delivered to Discord",
                    raceId, outcome.Won, string.Join(",", pills));
            else
                _logger.LogWarning("[BetWin] Race {RaceId} won ¥{Won:N0} ({Pills}) — NOT delivered to Discord; " +
                    "leaving un-notified so the next tick retries", raceId, outcome.Won, string.Join(",", pills));

            // Only bank the race as "notified" once it's genuinely delivered — UNLESS no webhook is
            // configured at all, in which case there's nothing to retry and we mark it to avoid an
            // endless re-evaluation loop. This is the actual fix for the s60 silent-drop: a 429 (or
            // any non-2xx) now leaves the race eligible for the next recheck tick instead of being
            // marked done with the ping lost.
            if (!webhookConfigured || delivered)
                notified.Add(raceId);

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
                    // Frozen-at-apply bet lines → score these verbatim (any custom composition).
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
