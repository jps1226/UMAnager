// ============================================================
// FILE: SunkCostService.cs
// LAYER: Service
// PURPOSE: Sunk-cost ledger — every PLACED (locked + marked) bet is "lost" until a result
//          credits it back. All figures DERIVED on read from the marks blob + race results
//          (no event log). Imported OrePro-history races use actual ¥; live races template-price.
//          Also imports real marks from OrePro exports and backfills frozen bet profiles.
// KEY DEPENDENCIES: AppDbContext, AppStateService, SettingsService, TemplateBetEvaluator.
// CAUTION: STAKED is exact (ladder); WON reuses the per-leg payout approximation (pending bet rework).
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Sunk-cost ledger. The model: assume every PLACED bet is lost until proven won, so the
/// running total of money wagered "rises" as you place bets and is only credited back when
/// a bet actually wins.
///
/// "Placed" = a race you've LOCKED (lock = "this is a committed bet" — whether you marked it
/// yourself and froze it, applied it through OrePro, which auto-locks, or bet it manually and
/// locked it here). Stake per race comes from the editable mark-count template-cost ladder
/// (<see cref="SettingsService.Keys.BetTemplateCosts"/>) — what OrePro actually fires.
///
/// Everything is DERIVED on read from the persisted marks blob + race results — no event log,
/// no separate table. It therefore survives restarts and is identical across devices. A
/// <c>sunk_cost_reset_at</c> JST date lets the operator zero the tally going forward.
///
/// Caveat (pending the bet rework): STAKED is exact (the ladder), but WON reuses the per-leg
/// win/quinella/trio payout approximation, since the ladder templates also bet 複勝/ワイド that
/// the payout lookup doesn't yet price.
/// </summary>
public sealed class SunkCostService
{
    public const string MarksStateKey   = "user_marks_blob";
    public const string ResetAtStateKey = "sunk_cost_reset_at";

    private static readonly TimeZoneInfo JstZone = TimeZoneInfo.FindSystemTimeZoneById("Tokyo Standard Time");

    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly AppStateService _state;
    private readonly SettingsService _settings;
    private readonly ILogger<SunkCostService> _logger;

    public SunkCostService(
        IDbContextFactory<AppDbContext> dbFactory,
        AppStateService state,
        SettingsService settings,
        ILogger<SunkCostService> logger)
    {
        _dbFactory = dbFactory;
        _state     = state;
        _settings  = settings;
        _logger    = logger;
    }

    public static DateTime JstToday()
    {
        var jst = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, JstZone).Date;
        return DateTime.SpecifyKind(jst, DateTimeKind.Utc); // match RaceDate convention
    }

    /// <summary>Compute the current sunk-cost summary across all placed (locked + marked)
    /// races. Imported OrePro-history races carry ACTUAL ¥ (betProfile.actualStaked/Won) and
    /// use it verbatim; live races template-derive. One unified, deduped tally.
    /// When <paramref name="jstRaceDate"/> is supplied, the tally is scoped to that single JST
    /// race day (used by the per-race win ping so it shows the DAY's running net, not lifetime).</summary>
    public async Task<SunkCostSummary> GetSummaryAsync(CancellationToken ct = default, DateTime? jstRaceDate = null)
    {
        // The tally is just the aggregate of the per-race recap (single source of truth, so the
        // header net, the per-race badges, and the TV ticker can never disagree). The sunk-cost
        // RESET cutoff applies to the tally only.
        var resetAt = await GetResetCutoffAsync();
        var perRace = await GetPerRaceRecapAsync(jstRaceDate, applyResetCutoff: true, ct);
        if (perRace.Count == 0)
            return SunkCostSummary.Empty(resetAt);

        long totalStaked = 0, totalWon = 0;
        int settled = 0, pending = 0, hits = 0;
        foreach (var r in perRace)
        {
            totalStaked += r.Staked;
            if (r.HasResults) { settled++; totalWon += r.Won; if (r.Won > 0) hits++; }
            else pending++;
        }

        return new SunkCostSummary(
            PlacedRaces:    perRace.Count,
            SettledRaces:   settled,
            PendingRaces:   pending,
            HitRaces:       hits,
            TotalStakedYen: totalStaked,
            TotalWonYen:    totalWon,
            SunkCostYen:    totalStaked - totalWon,
            NetYen:         totalWon - totalStaked,
            ResetAt:        resetAt?.ToString("yyyy-MM-dd"));
    }

    /// <summary>Per-race recap for DISPLAY (the TV ticker + dashboard win badges): each placed
    /// (locked + marked) race's staked/won/net + the ticket labels that actually hit, scored via
    /// the SAME authoritative evaluator the tally uses — so a "win" reflects the bet you actually
    /// placed, never just your marks. By default the sunk-cost RESET cutoff is NOT applied (a reset
    /// is a tally concept; result badges should still show); only <see cref="GetSummaryAsync"/>
    /// passes <paramref name="applyResetCutoff"/>=true.</summary>
    public async Task<List<RaceRecap>> GetPerRaceRecapAsync(
        DateTime? jstRaceDate = null, bool applyResetCutoff = false, CancellationToken ct = default)
    {
        var (marks, lockedRaces, profiles) = await LoadMarksAndLocksAsync();

        // Placed races = locked AND carry at least one (non-X) mark.
        var markCountByRace = new Dictionary<string, int>();
        foreach (var raceId in lockedRaces)
        {
            var n = marks.Keys.Count(k => k.StartsWith(raceId + "_"));
            if (n > 0) markCountByRace[raceId] = n;
        }

        var result = new List<RaceRecap>();
        if (markCountByRace.Count == 0) return result;

        var placedIds = markCountByRace.Keys.ToList();

        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        var races = await db.Races.AsNoTracking()
            .Where(r => placedIds.Contains(r.RaceId))
            .Select(r => new { r.RaceId, r.RaceDate, r.ResultsJson })
            .ToListAsync(ct);

        // Tally-only: count races on/after the reset JST date.
        if (applyResetCutoff && await GetResetCutoffAsync() is DateTime cutoff)
            races = races.Where(r => r.RaceDate >= cutoff).ToList();

        // Optional single-day scope (TV shows the day-of-action; per-race win ping → "Day net").
        if (jstRaceDate is DateTime day)
            races = races.Where(r => r.RaceDate == day).ToList();

        if (races.Count == 0) return result;

        var countedIds = races.Select(r => r.RaceId).ToHashSet();

        // Load EVERY entry of the placed races (not just top-3) — the evaluator needs the
        // post positions of all marked horses, which aren't necessarily top finishers.
        var entries = await db.RaceEntries.AsNoTracking()
            .Where(e => countedIds.Contains(e.RaceId))
            .Select(e => new { e.RaceId, e.HorseId, e.PostPosition, e.FinishPos })
            .ToListAsync(ct);
        var entriesByRace = entries.GroupBy(e => e.RaceId).ToDictionary(g => g.Key, g => g.ToList());

        foreach (var race in races)
        {
            entriesByRace.TryGetValue(race.RaceId, out var raceEntries);
            raceEntries ??= new();

            var ppByHorse = raceEntries
                .GroupBy(e => e.HorseId)
                .ToDictionary(g => g.Key, g => g.First().PostPosition);
            var runners = TemplateBetEvaluator.BuildRunners(race.RaceId, marks, ppByHorse);

            int? pp1 = null, pp2 = null, pp3 = null;
            foreach (var e in raceEntries)
            {
                if (e.FinishPos == 1) pp1 = e.PostPosition;
                else if (e.FinishPos == 2) pp2 = e.PostPosition;
                else if (e.FinishPos == 3) pp3 = e.PostPosition;
            }

            JsonElement? payouts = null;
            JsonDocument? doc = null;
            try { doc = JsonDocument.Parse(race.ResultsJson ?? "{}"); payouts = doc.RootElement; }
            catch (JsonException) { }

            profiles.TryGetValue(race.RaceId, out var prof);
            var (staked, won, hasResults, labels, imported) = ScoreRace(runners, prof, pp1, pp2, pp3, payouts);
            doc?.Dispose();

            result.Add(new RaceRecap(
                RaceId:     race.RaceId,
                MarkCount:  markCountByRace[race.RaceId],
                HasResults: hasResults,
                Staked:     staked,
                Won:        won,
                Net:        won - staked,
                HitLabels:  labels,
                Imported:   imported));
        }
        return result;
    }

    /// <summary>Score ONE race's placed bet → (staked, won, hasResults, hit-labels, imported).
    /// Imported OrePro-history races use the ACTUAL ¥ verbatim; applied races score their FROZEN
    /// line list (matches the UI exactly for any custom composition); a locked-but-unapplied race
    /// falls back to the default template. The single seam shared by the tally and the per-race
    /// recap so they can never diverge.</summary>
    private static (int Staked, int Won, bool HasResults, List<string> Labels, bool Imported) ScoreRace(
        IReadOnlyList<TemplateBetEvaluator.MarkedRunner> runners,
        RaceBetProfile? prof, int? pp1, int? pp2, int? pp3, JsonElement? payouts)
    {
        // Imported OrePro-history race: ACTUAL ¥ staked/returned is truth (no per-ticket labels).
        if (prof?.ActualStaked is int aStaked)
        {
            var aWon = prof.ActualWon ?? 0;
            return (aStaked, aWon, true, new List<string>(), true);
        }

        TemplateBetEvaluator.BetOutcome outcome;
        if (prof?.BetLines is { Count: > 0 } frozenLines)
            outcome = TemplateBetEvaluator.EvaluateLines(frozenLines, runners.Count, pp1, pp2, pp3, payouts);
        else
        {
            var oreProStake = prof?.Stake > 0 ? prof.Stake : TemplateBetEvaluator.DefaultStakeYen;
            outcome = TemplateBetEvaluator.Evaluate(runners, pp1, pp2, pp3, payouts, TemplateBetEvaluator.BetStructures.Default, oreProStake);
        }
        return (outcome.Staked, outcome.Won, outcome.HasResults, outcome.HitLabels, false);
    }

    /// <summary>Reset the tally so only races on/after the given JST date (default: today) count.</summary>
    public async Task ResetAsync(DateTime? jstDate = null, CancellationToken ct = default)
    {
        var d = (jstDate ?? JstToday()).Date;
        await _state.SetStringAsync(ResetAtStateKey, d.ToString("yyyy-MM-dd"));
        _logger.LogInformation("[SunkCost] Tally reset — counting races from {Date} forward.", d.ToString("yyyy-MM-dd"));
    }

    private async Task<DateTime?> GetResetCutoffAsync()
    {
        var raw = await _state.GetStringAsync(ResetAtStateKey);
        if (string.IsNullOrWhiteSpace(raw)) return null;
        if (DateTime.TryParse(raw, out var d))
            return DateTime.SpecifyKind(d.Date, DateTimeKind.Utc);
        return null;
    }

    /// <summary>Load the marks dict (non-X), the set of locked race-ids, and any frozen
    /// per-race bet profiles (raceMeta.betProfile) from the marks blob.</summary>
    private async Task<(Dictionary<string, string> Marks, HashSet<string> Locked, Dictionary<string, RaceBetProfile> Profiles)> LoadMarksAndLocksAsync()
    {
        var marks = new Dictionary<string, string>();
        var locked = new HashSet<string>();
        var profiles = new Dictionary<string, RaceBetProfile>();
        var raw = await _state.GetStringAsync(MarksStateKey);
        if (string.IsNullOrWhiteSpace(raw)) return (marks, locked, profiles);
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
                    if (prop.Value.TryGetProperty("lockStateAtSave", out var lockEl) && lockEl.ValueKind == JsonValueKind.True)
                        locked.Add(prop.Name);
                    if (prop.Value.TryGetProperty("betProfile", out var bp) && bp.ValueKind == JsonValueKind.Object)
                    {
                        int stake = bp.TryGetProperty("stake", out var s) && s.ValueKind == JsonValueKind.Number && s.TryGetInt32(out var sv) ? sv : 0;
                        int? aStaked = bp.TryGetProperty("actualStaked", out var ase) && ase.ValueKind == JsonValueKind.Number && ase.TryGetInt32(out var asv) ? asv : null;
                        int? aWon = bp.TryGetProperty("actualWon", out var awe) && awe.ValueKind == JsonValueKind.Number && awe.TryGetInt32(out var awv) ? awv : null;
                        // Frozen-at-apply line list — the authoritative record of what was placed.
                        List<TemplateBetEvaluator.BetLine>? betLines = null;
                        if (bp.TryGetProperty("betLines", out var blEl) && blEl.ValueKind == JsonValueKind.Array)
                        {
                            var parsed = TemplateBetEvaluator.ParseFrozenLines(blEl);
                            if (parsed.Count > 0) betLines = parsed;
                        }
                        if (betLines is not null || aStaked.HasValue || stake > 0)
                            profiles[prop.Name] = new RaceBetProfile(stake, aStaked, aWon, betLines);
                    }
                }
            }
        }
        catch (JsonException) { /* return whatever parsed */ }
        return (marks, locked, profiles);
    }

    // JP track name → JRA track code (2-digit). Local/NAR tracks are unmapped (skipped).
    private static readonly IReadOnlyDictionary<string, string> TrackNameToCode = new Dictionary<string, string>
    {
        ["札幌"]="01",["函館"]="02",["福島"]="03",["新潟"]="04",["東京"]="05",
        ["中山"]="06",["中京"]="07",["京都"]="08",["阪神"]="09",["小倉"]="10",
    };

    public sealed record ImportRecord(string Track, int RaceNum, int Purchase, int Payout, List<(string Sym, string Horse)> Marks);
    public sealed record ImportResult(int RacesMatched, int MarksWritten, List<string> Unresolved);

    /// <summary>Import REAL marks from a parsed OrePro 俺プロフ export: resolve each race by
    /// (track, race#, the set of marked horse names) → our race_id, map each marked horse →
    /// horse_id+post, and WRITE the marks into the marks blob (+ lock the race as placed +
    /// stamp the actual ¥ staked/returned). Idempotent per race. So those past races show your
    /// real marks in the program and price at their actual ¥.</summary>
    public async Task<ImportResult> ImportMarksFromRecordsAsync(IReadOnlyList<ImportRecord> records, CancellationToken ct = default)
    {
        var unresolved = new List<string>();
        var raw = await _state.GetStringAsync(MarksStateKey);
        JsonObject root;
        try { root = (!string.IsNullOrWhiteSpace(raw) && JsonNode.Parse(raw) is JsonObject o) ? o : new JsonObject(); }
        catch (JsonException) { root = new JsonObject(); }
        if (root["marks"] is not JsonObject marksObj) { marksObj = new JsonObject(); root["marks"] = marksObj; }
        if (root["raceMeta"] is not JsonObject metaObj) { metaObj = new JsonObject(); root["raceMeta"] = metaObj; }

        // Normalize the circled-O variant (◯ U+25EF) to the app's 〇 (U+3007); others pass through.
        static string NormSym(string s) => s == "◯" ? "〇" : s;

        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        int racesMatched = 0, marksWritten = 0;

        foreach (var rec in records)
        {
            if (!TrackNameToCode.TryGetValue(rec.Track, out var code))
            { unresolved.Add($"{rec.Track} {rec.RaceNum}R (non-JRA / unknown track)"); continue; }

            var names = rec.Marks.Select(m => m.Horse).Distinct().ToList();
            var rows = await (from e in db.RaceEntries.AsNoTracking()
                              join h in db.Horses on e.HorseId equals h.HorseId
                              join r in db.Races on e.RaceId equals r.RaceId
                              where r.TrackCode == code && r.RaceNumber == rec.RaceNum && names.Contains(h.NameJa)
                              select new { e.RaceId, e.HorseId, h.NameJa }).ToListAsync(ct);
            if (rows.Count == 0) { unresolved.Add($"{rec.Track} {rec.RaceNum}R (no matching race/horses)"); continue; }

            // The race that contains the most of this record's marked horses is the match.
            var best = rows.GroupBy(x => x.RaceId).OrderByDescending(g => g.Count()).First();
            var raceId = best.Key;
            var horseByName = best.GroupBy(x => x.NameJa).ToDictionary(g => g.Key, g => g.First().HorseId);

            int wroteHere = 0;
            foreach (var (sym, horse) in rec.Marks)
            {
                if (!horseByName.TryGetValue(horse, out var hid)) { unresolved.Add($"{rec.Track} {rec.RaceNum}R: horse '{horse}' not in matched race"); continue; }
                marksObj[$"{raceId}_{hid}"] = NormSym(sym);
                wroteHere++; marksWritten++;
            }
            if (wroteHere == 0) continue;
            racesMatched++;

            // Lock (placed) + stamp actual ¥ so this race prices at its real OrePro outcome.
            metaObj[raceId] = new JsonObject
            {
                ["lockStateAtSave"] = true,
                ["markSource"] = "orepro-import",
                ["betProfile"] = new JsonObject
                {
                    ["mode"] = "orepro_default",
                    ["stake"] = rec.Purchase,
                    ["actualStaked"] = rec.Purchase,
                    ["actualWon"] = rec.Payout,
                    ["source"] = "orepro_import",
                },
            };
        }

        await _state.SetStringAsync(MarksStateKey, root.ToJsonString());
        _logger.LogInformation("[SunkCost] Imported marks: {Races} races, {Marks} marks, {Unresolved} unresolved.",
            racesMatched, marksWritten, unresolved.Count);
        return new ImportResult(racesMatched, marksWritten, unresolved);
    }

    /// <summary>Retroactively stamp a frozen bet profile on every LOCKED race in the marks
    /// blob, so the all-time tally prices them as actually bet (immune to later global
    /// Bet-Mode changes). Re-runnable. Returns the number of races stamped.</summary>
    public async Task<int> BackfillProfilesForLockedAsync(string mode, int stake, CancellationToken ct = default)
    {
        var raw = await _state.GetStringAsync(MarksStateKey);
        if (string.IsNullOrWhiteSpace(raw)) return 0;
        JsonNode? root;
        try { root = JsonNode.Parse(raw); } catch (JsonException) { return 0; }
        if (root is not JsonObject obj || obj["raceMeta"] is not JsonObject meta) return 0;

        int count = 0;
        foreach (var kv in meta)
        {
            if (kv.Value is not JsonObject rm) continue;
            bool isLocked = rm["lockStateAtSave"] is JsonValue jv && jv.TryGetValue<bool>(out var b) && b;
            if (!isLocked) continue;
            rm["betProfile"] = new JsonObject { ["mode"] = mode, ["stake"] = stake };
            count++;
        }
        if (count > 0)
        {
            await _state.SetStringAsync(MarksStateKey, obj.ToJsonString());
            _logger.LogInformation("[SunkCost] Backfilled {Count} locked races as {Mode} @ ¥{Stake}.", count, mode, stake);
        }
        return count;
    }
}

public sealed record RaceBetProfile(int Stake, int? ActualStaked, int? ActualWon, List<TemplateBetEvaluator.BetLine>? BetLines);

/// <summary>One placed race's display recap (TV ticker + dashboard win badges). HitLabels are the
/// Japanese ticket labels that actually paid (単勝/馬連/ワイド/3連複/3連複ながし); empty for imported
/// history (only the net ¥ is known). Net = Won − Staked.</summary>
public sealed record RaceRecap(
    string RaceId, int MarkCount, bool HasResults,
    int Staked, int Won, int Net, IReadOnlyList<string> HitLabels, bool Imported);

public sealed record SunkCostSummary(
    int PlacedRaces,
    int SettledRaces,
    int PendingRaces,
    int HitRaces,
    long TotalStakedYen,
    long TotalWonYen,
    long SunkCostYen,
    long NetYen,
    string? ResetAt)
{
    public static SunkCostSummary Empty(DateTime? resetAt) =>
        new(0, 0, 0, 0, 0, 0, 0, 0, resetAt?.ToString("yyyy-MM-dd"));
}
