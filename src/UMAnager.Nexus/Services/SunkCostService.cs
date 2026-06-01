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

    public const string ImportedStateKey = "imported_bets";

    /// <summary>Compute the current sunk-cost summary.</summary>
    public async Task<SunkCostSummary> GetSummaryAsync(CancellationToken ct = default)
    {
        // IMPORT IS TRUTH: if real OrePro history has been imported, that ledger IS the
        // all-time tally — actual ¥ staked/returned per race, exact and dedup-by-race-id.
        // The marks-derived estimate below is only the fallback before anything is imported.
        var imported = await LoadImportedAsync();
        if (imported.Count > 0)
        {
            var resetImp = await GetResetCutoffAsync();
            long stk = imported.Sum(b => (long)b.Purchase);
            long wn  = imported.Sum(b => (long)b.Payout);
            int  hit = imported.Count(b => b.Payout > 0);
            return new SunkCostSummary(
                PlacedRaces: imported.Count, SettledRaces: imported.Count, PendingRaces: 0, HitRaces: hit,
                TotalStakedYen: stk, TotalWonYen: wn, SunkCostYen: stk - wn, NetYen: wn - stk,
                ResetAt: resetImp?.ToString("yyyy-MM-dd"));
        }

        var (marks, lockedRaces, profiles) = await LoadMarksAndLocksAsync();

        // Placed races = locked AND carry at least one (non-X) mark.
        var markCountByRace = new Dictionary<string, int>();
        foreach (var raceId in lockedRaces)
        {
            var n = marks.Keys.Count(k => k.StartsWith(raceId + "_"));
            if (n > 0) markCountByRace[raceId] = n;
        }

        var resetAt = await GetResetCutoffAsync();

        if (markCountByRace.Count == 0)
            return SunkCostSummary.Empty(resetAt);

        var placedIds = markCountByRace.Keys.ToList();

        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        var races = await db.Races.AsNoTracking()
            .Where(r => placedIds.Contains(r.RaceId))
            .Select(r => new { r.RaceId, r.RaceDate, r.ResultsJson })
            .ToListAsync(ct);

        // Apply the reset cutoff: only count races on/after the reset JST date.
        if (resetAt is DateTime cutoff)
            races = races.Where(r => r.RaceDate >= cutoff).ToList();

        if (races.Count == 0)
            return SunkCostSummary.Empty(resetAt);

        var countedIds = races.Select(r => r.RaceId).ToHashSet();

        // Load EVERY entry of the placed races (not just top-3) — the evaluator needs the
        // post positions of all marked horses, which aren't necessarily top finishers.
        var entries = await db.RaceEntries.AsNoTracking()
            .Where(e => countedIds.Contains(e.RaceId))
            .Select(e => new { e.RaceId, e.HorseId, e.PostPosition, e.FinishPos })
            .ToListAsync(ct);
        var entriesByRace = entries.GroupBy(e => e.RaceId).ToDictionary(g => g.Key, g => g.ToList());

        var ladder = await _settings.GetBetTemplateCostsAsync();

        long totalStaked = 0, totalWon = 0;
        int settled = 0, pending = 0, hits = 0;

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

            // Price by this race's FROZEN bet record if it has one (set at placement /
            // backfill), else the current ladder. Keeps history stable across setting changes.
            profiles.TryGetValue(race.RaceId, out var prof);
            var betMode = prof?.Mode ?? "custom";
            var oreProStake = prof?.Stake ?? 0;
            var outcome = TemplateBetEvaluator.Evaluate(runners, pp1, pp2, pp3, payouts, ladder, betMode, oreProStake);
            doc?.Dispose();

            totalStaked += outcome.Staked;
            if (outcome.HasResults) { settled++; totalWon += outcome.Won; if (outcome.AnyHit) hits++; }
            else pending++;
        }

        return new SunkCostSummary(
            PlacedRaces:    races.Count,
            SettledRaces:   settled,
            PendingRaces:   pending,
            HitRaces:       hits,
            TotalStakedYen: totalStaked,
            TotalWonYen:    totalWon,
            SunkCostYen:    totalStaked - totalWon,
            NetYen:         totalWon - totalStaked,
            ResetAt:        resetAt?.ToString("yyyy-MM-dd"));
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
                        var mode = bp.TryGetProperty("mode", out var m) && m.ValueKind == JsonValueKind.String ? m.GetString() : null;
                        int stake = bp.TryGetProperty("stake", out var s) && s.ValueKind == JsonValueKind.Number && s.TryGetInt32(out var sv) ? sv : 0;
                        if (mode == "orepro_default" || mode == "custom") profiles[prop.Name] = new RaceBetProfile(mode, stake);
                    }
                }
            }
        }
        catch (JsonException) { /* return whatever parsed */ }
        return (marks, locked, profiles);
    }

    /// <summary>Load the imported OrePro-history ledger (keyed by netkeiba race id).</summary>
    private async Task<List<ImportedBet>> LoadImportedAsync()
    {
        var list = new List<ImportedBet>();
        var raw = await _state.GetStringAsync(ImportedStateKey);
        if (string.IsNullOrWhiteSpace(raw)) return list;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (doc.RootElement.ValueKind == JsonValueKind.Object)
                foreach (var p in doc.RootElement.EnumerateObject())
                {
                    var o = p.Value;
                    int purchase = o.TryGetProperty("purchase", out var pu) && pu.ValueKind == JsonValueKind.Number && pu.TryGetInt32(out var pv) ? pv : 0;
                    int payout   = o.TryGetProperty("payout",   out var pa) && pa.ValueKind == JsonValueKind.Number && pa.TryGetInt32(out var av) ? av : 0;
                    list.Add(new ImportedBet(p.Name, purchase, payout));
                }
        }
        catch (JsonException) { }
        return list;
    }

    /// <summary>Merge a batch of imported OrePro-history records into the ledger, deduped by
    /// race id (re-uploading the same export overwrites, never double-counts). Returns the
    /// total ledger size after merge.</summary>
    public async Task<int> MergeImportedBetsAsync(JsonElement records, CancellationToken ct = default)
    {
        var raw = await _state.GetStringAsync(ImportedStateKey);
        JsonObject ledger;
        try { ledger = (!string.IsNullOrWhiteSpace(raw) && JsonNode.Parse(raw) is JsonObject o) ? o : new JsonObject(); }
        catch (JsonException) { ledger = new JsonObject(); }

        if (records.ValueKind == JsonValueKind.Array)
        {
            foreach (var rec in records.EnumerateArray())
            {
                if (rec.ValueKind != JsonValueKind.Object) continue;
                if (!rec.TryGetProperty("raceId", out var ridEl) || ridEl.ValueKind != JsonValueKind.String) continue;
                var rid = ridEl.GetString();
                if (string.IsNullOrWhiteSpace(rid)) continue;
                ledger[rid] = JsonNode.Parse(rec.GetRawText());
            }
        }
        await _state.SetStringAsync(ImportedStateKey, ledger.ToJsonString());
        _logger.LogInformation("[SunkCost] Imported ledger now holds {Count} races.", ledger.Count);
        return ledger.Count;
    }

    /// <summary>Clear the imported OrePro-history ledger.</summary>
    public async Task ClearImportedBetsAsync(CancellationToken ct = default)
        => await _state.SetStringAsync(ImportedStateKey, "{}");

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

public sealed record RaceBetProfile(string Mode, int Stake);
public sealed record ImportedBet(string RaceId, int Purchase, int Payout);

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
