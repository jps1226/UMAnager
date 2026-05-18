using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/races")]
public sealed class RacesController : ControllerBase
{
    // JRA central-track codes → romaji names. Source: JRA-VAN spec (Oracle Q5).
    private static readonly IReadOnlyDictionary<string, string> TrackNames =
        new Dictionary<string, string>
        {
            ["01"] = "Sapporo",  ["02"] = "Hakodate", ["03"] = "Fukushima", ["04"] = "Niigata",
            ["05"] = "Tokyo",    ["06"] = "Nakayama", ["07"] = "Chukyo",    ["08"] = "Kyoto",
            ["09"] = "Hanshin",  ["10"] = "Kokura",
        };

    // Phase 7: recency weights (most-recent first), f(pos) = 1/pos for top-5 else 0.
    private static readonly double[] FormWeights = { 0.5, 0.3, 0.2 };

    private static (string last3Str, double formScore) ComputeLast3(
        List<(DateTime Date, int Finish)>? hist, DateTime raceDate)
    {
        if (hist == null || hist.Count == 0) return ("—-—-—", 0.0);
        var picks = new List<int>(3);
        foreach (var h in hist)
        {
            if (h.Date >= raceDate) continue;
            picks.Add(h.Finish);
            if (picks.Count == 3) break;
        }
        if (picks.Count == 0) return ("—-—-—", 0.0);
        var parts = new string[3];
        double score = 0.0;
        for (int i = 0; i < 3; i++)
        {
            if (i < picks.Count)
            {
                parts[i] = picks[i].ToString();
                if (picks[i] >= 1 && picks[i] <= 5) score += FormWeights[i] * (1.0 / picks[i]);
            }
            else parts[i] = "—";
        }
        return (string.Join("-", parts), score);
    }

    private static string TrackName(string? code)
    {
        if (string.IsNullOrWhiteSpace(code)) return "";
        var padded = code.Trim().PadLeft(2, '0');
        return TrackNames.TryGetValue(padded, out var name) ? name : padded;
    }

    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly Services.SettingsService _settings;

    public RacesController(IDbContextFactory<AppDbContext> dbFactory, Services.SettingsService settings)
    {
        _dbFactory = dbFactory;
        _settings = settings;
    }

    [HttpGet]
    public async Task<IActionResult> GetRaces()
    {
        try
        {
            using var db = await _dbFactory.CreateDbContextAsync();

            // Phase 14: trim default window to roughly the last 2 weeks + any upcoming.
            // ~5 race days × 36 races = ~180 rows for the typical operator view. Older
            // races are still reachable via /api/races?from=YYYY-MM-DD (future endpoint).
            var jstNow = DateTime.UtcNow.AddHours(9);
            var windowStart = jstNow.AddDays(-14);
            var races = await db.Races
                .Where(r => r.SortTime != null && r.SortTime >= windowStart)
                .OrderBy(r => r.SortTime)
                .AsNoTracking()
                .ToListAsync();

            // Phase 14: ETag based on (race count, max race LastUpdated, max breeding_horses
            // LastUpdated). The breeding_horses signal ensures that an HN-name backfill
            // (Phase 15) invalidates the cached response even when no race row was touched.
            DateTime? maxLastUpdated = races.Count > 0
                ? races.Max(r => r.LastUpdated)
                : (DateTime?)null;
            var maxBreedingUpdated = await db.BreedingHorses.AsNoTracking()
                .MaxAsync(b => (DateTime?)b.LastUpdated);
            var etag = $"\"races-{races.Count}-{maxLastUpdated?.Ticks ?? 0}-{maxBreedingUpdated?.Ticks ?? 0}\"";
            Response.Headers["Cache-Control"] = "no-cache"; // re-validate every time, but allow 304
            Response.Headers["ETag"] = etag;
            var ifNoneMatch = Request.Headers["If-None-Match"].ToString();
            if (!string.IsNullOrEmpty(ifNoneMatch) && ifNoneMatch == etag)
            {
                return StatusCode(StatusCodes.Status304NotModified);
            }

            var raceIds = races.Select(r => r.RaceId).ToHashSet();
            var allEntries = await db.RaceEntries.AsNoTracking()
                .Where(e => raceIds.Contains(e.RaceId))
                .ToListAsync();

            // Only load the horses we'll actually need — joining on the entry HorseIds plus
            // any sire/dam/bms ids those horses reference. Loading all 654K rows per request
            // was the perf bug behind the 120s timeout.
            var entryHorseIds = allEntries.Select(e => e.HorseId).Where(s => !string.IsNullOrEmpty(s)).Distinct().ToList();
            var horsesForEntries = await db.Horses.AsNoTracking()
                .Where(h => entryHorseIds.Contains(h.HorseId))
                .ToListAsync();
            var horseLookup = horsesForEntries.ToDictionary(h => h.HorseId, h => h);

            // Pedigree-ancestor names live in breeding_horses (HansyokuNum-keyed). Most sire/dam
            // IDs (especially foreign sires like "11xxxxxxxx") never appear in horses at all —
            // they exist only as KETTO3_INFO slots inside UM records, backfilled into breeding_horses.
            var ancestorIds = horsesForEntries
                .SelectMany(h => new[] { h.SireId, h.DamId, h.BmsId })
                .Where(s => !string.IsNullOrEmpty(s))
                .Distinct()
                .ToList();
            var breedingLookup = await db.BreedingHorses.AsNoTracking()
                .Where(b => ancestorIds.Contains(b.HansyokuNum))
                .ToDictionaryAsync(b => b.HansyokuNum, b => new { b.NameJa, b.NameEn });
            // Some sires/dams are themselves runners — also resolve from horseLookup.
            var entriesByRace = allEntries.GroupBy(e => e.RaceId).ToDictionary(g => g.Key, g => g.ToList());

            // Career W/S (wins / starts) per horse, computed across ALL race_entries (not just the
            // ones loaded for this response). A "start" is any entry with a real finishing position
            // (FinishPos > 0); a "win" is FinishPos == 1. Pre-race entries have NULL FinishPos and
            // don't count. One grouped query — index on HorseId makes this cheap.
            var careerStats = await db.RaceEntries.AsNoTracking()
                .Where(e => entryHorseIds.Contains(e.HorseId) && e.FinishPos != null && e.FinishPos > 0)
                .GroupBy(e => e.HorseId)
                .Select(g => new
                {
                    HorseId = g.Key,
                    Starts  = g.Count(),
                    Wins    = g.Count(e => e.FinishPos == 1)
                })
                .ToListAsync();
            var recordByHorse = careerStats.ToDictionary(s => s.HorseId, s => $"{s.Wins}/{s.Starts}");

            // Phase 7: last-3 finishes per horse (most recent first), strictly BEFORE the current
            // race's date so that past-race rows still show the prior three rather than including
            // the race being viewed. Pull every completed entry for our horses + its race_date,
            // group by horse, sort desc once, then slice per-entry.
            var horseFinishHistory = await (
                from e in db.RaceEntries.AsNoTracking()
                join r in db.Races.AsNoTracking() on e.RaceId equals r.RaceId
                where entryHorseIds.Contains(e.HorseId!) && e.FinishPos != null && e.FinishPos > 0
                select new { e.HorseId, r.RaceDate, Finish = e.FinishPos!.Value }
            ).ToListAsync();
            var finishesByHorse = horseFinishHistory
                .GroupBy(x => x.HorseId!)
                .ToDictionary(
                    g => g.Key,
                    g => g.OrderByDescending(x => x.RaceDate).Select(x => (x.RaceDate, x.Finish)).ToList()
                );


            string ResolveAncestorName(string? id)
            {
                if (string.IsNullOrEmpty(id)) return "";
                if (horseLookup.TryGetValue(id, out var h))
                    return !string.IsNullOrEmpty(h.NameEn) ? h.NameEn : h.NameJa;
                if (breedingLookup.TryGetValue(id, out var b))
                    return !string.IsNullOrEmpty(b.NameEn) ? b.NameEn : b.NameJa;
                return "";
            }

            var upcomingByDate = new Dictionary<string, List<object>>();
            var pastByDate = new Dictionary<string, List<object>>();
            // SortTime is stored as JST wall-clock (Kind=Unspecified). Compare in JST so races
            // flip from "upcoming" to "past" at actual post time, not 9 hours later.
            var now = DateTime.UtcNow.AddHours(9);

            foreach (var race in races)
            {
                var cleanDate = race.RaceDate.ToString("yyyy-MM-dd");
                var sortTimeUtc = race.SortTime ?? DateTime.UtcNow;

                var raceInfo = new
                {
                    race_id = race.RaceId,
                    race_name = race.NameJa ?? "",
                    race_number = race.RaceNumber ?? 0,
                    place = race.TrackCode ?? "Unknown",
                    place_name = TrackName(race.TrackCode),
                    time = race.SortTime?.ToString("HH:mm") ?? "TBA",
                    sort_time = sortTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss"),
                    // Proper ISO-8601 with JST offset so the frontend can convert to any
                    // local timezone via new Date(sort_time_iso). sort_time (no offset)
                    // is kept for backward-compat with parseRaceSortTime.
                    sort_time_iso = race.SortTime?.ToString("yyyy-MM-ddTHH:mm:ss") + "+09:00",
                    clean_date = cleanDate,
                    history_refreshed = race.HistoryRefreshed,
                    // Phase 11 backward: HR payouts — emitted as a raw JSON string for the
                    // frontend to parse only when it needs to enrich past-race recap chips
                    // (◎ Win / Q Box / T Box) with the actual ¥ amount that paid out.
                    results_json = race.ResultsJson
                };

                var raceEntries = entriesByRace.TryGetValue(race.RaceId, out var re) ? re : new List<Data.Entities.RaceEntry>();
                var entries = raceEntries
                    .OrderBy(e => e.PostPosition ?? 0)
                    .Select(e =>
                    {
                        horseLookup.TryGetValue(e.HorseId ?? "", out var horse);
                        finishesByHorse.TryGetValue(e.HorseId ?? "", out var hist);
                        var (last3Str, formScore) = ComputeLast3(hist, race.RaceDate);

                        return (object)new
                        {
                            Horse_ID = e.HorseId,
                            Horse = horse?.NameEn ?? horse?.NameJa ?? "",
                            PP = e.PostPosition ?? 0,
                            BK = e.Bracket ?? 0,
                            Record = recordByHorse.TryGetValue(e.HorseId ?? "", out var rec) ? rec : "",
                            Last3 = last3Str,
                            Form_Score = formScore,
                            Sire = ResolveAncestorName(horse?.SireId),
                            Sire_ID = horse?.SireId ?? "",
                            Dam = ResolveAncestorName(horse?.DamId),
                            Dam_ID = horse?.DamId ?? "",
                            BMS = ResolveAncestorName(horse?.BmsId),
                            BMS_ID = horse?.BmsId ?? "",
                            Odds = e.Odds?.ToString("F1") ?? "",
                            Fav = e.FavRank?.ToString() ?? "",
                            Finish = e.FinishPos?.ToString() ?? ""
                        };
                    })
                    .ToList();

                var raceObj = new
                {
                    info = raceInfo,
                    entries = entries
                };

                var bucket = sortTimeUtc > now ? upcomingByDate : pastByDate;
                if (!bucket.ContainsKey(cleanDate))
                    bucket[cleanDate] = new List<object>();
                bucket[cleanDate].Add(raceObj);
            }

            // Combined map for clients (like tv.html) that just want all races by date.
            var combinedByDate = new Dictionary<string, List<object>>();
            foreach (var kv in upcomingByDate) combinedByDate[kv.Key] = new List<object>(kv.Value);
            foreach (var kv in pastByDate)
            {
                if (!combinedByDate.TryGetValue(kv.Key, out var list))
                {
                    list = new List<object>();
                    combinedByDate[kv.Key] = list;
                }
                list.AddRange(kv.Value);
            }

            return Ok(new
            {
                upcoming_races_by_date = upcomingByDate.ToDictionary(k => k.Key, v => v.Value.ToArray()),
                past_races_by_date     = pastByDate.ToDictionary(k => k.Key, v => v.Value.ToArray()),
                races_by_date          = combinedByDate.ToDictionary(k => k.Key, v => v.Value.ToArray()),
                top_picks              = Array.Empty<object>()
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }

    // /api/config GET/POST is owned by ConfigController — don't duplicate here.

    [HttpGet("../marks")]
    public IActionResult GetMarks() => Ok(new
    {
        version = 2,
        marks = new { },
        raceMeta = new { }
    });

    [HttpPost("../marks")]
    public IActionResult PostMarks([FromBody] object marks) => Ok(new { status = "ok" });

    [HttpGet("../lists")]
    public IActionResult GetLists() => Ok(new
    {
        favorites = "",
        watchlist = ""
    });

    // ── Phase 2+ stubs ───────────────────────────────────────────────────────

    [HttpPost("enrich-horse-info")]
    public IActionResult EnrichHorseInfo() => Ok(new
    {
        updated_rows    = 0,
        updated_races   = 0,
        unique_horses   = 0,
        fetch_candidates = 0,
    });

    [HttpPost("day/import-results")]
    public IActionResult ImportResults() => Ok(new { status = "not_implemented", message = "" });

    [HttpPost("{race_id}/refresh-history")]
    public IActionResult RefreshHistory(string race_id) => Ok(new { status = "not_implemented" });

    [HttpPost("upcoming/refresh")]
    public IActionResult RefreshUpcoming() => Ok(new
    {
        status        = "not_implemented",
        updated_races = 0,
        updated_rows  = 0,
        failed_races  = 0,
    });

    [HttpPost("prefetch/apply")]
    public IActionResult PrefetchApply() => Ok(new { status = "not_implemented" });

    public sealed record BetEstimateItem(string race_id, int honmei_post, int[] box_posts);
    public sealed record BetEstimateRequest(BetEstimateItem[] races);

    // Aggregate of one bet-type box (Q or T) within a race estimate.
    private sealed record BoxResolution(int ResolvedTickets, int MissingTickets,
        int? MinPayout, int? MaxPayout, int? MinNet, int? MaxNet);
    private sealed record AllHitResolution(int? MinNet, int? MaxNet);

    /// <summary>
    /// For each combination in the user's box, look up the matching odds slot in
    /// OddsJson and compute payout = stake × odds. Returns min/max across the box
    /// (the "if any combination hits, what's our expected payout range" view).
    /// All Hit = win.payout + qMin + tMin / qMax + tMax minus purchase total.
    /// </summary>
    private static (BoxResolution Q, BoxResolution T, AllHitResolution AllHit) ResolveBoxPayouts(
        string? oddsJson,
        int[] boxPosts,
        int qStake,
        int tStake,
        int winStake,
        int qTickets,
        int tTickets,
        int purchaseTotal,
        double? winOddsForAllHit)
    {
        // Generate all C(n,2) and C(n,3) combinations of box positions, sorted ascending.
        var sortedBox = boxPosts.OrderBy(p => p).ToArray();
        var qCombos = new List<int[]>();
        for (int i = 0; i < sortedBox.Length; i++)
            for (int j = i + 1; j < sortedBox.Length; j++)
                qCombos.Add(new[] { sortedBox[i], sortedBox[j] });
        var tCombos = new List<int[]>();
        for (int i = 0; i < sortedBox.Length; i++)
            for (int j = i + 1; j < sortedBox.Length; j++)
                for (int k = j + 1; k < sortedBox.Length; k++)
                    tCombos.Add(new[] { sortedBox[i], sortedBox[j], sortedBox[k] });

        // No odds data → everything missing.
        if (string.IsNullOrEmpty(oddsJson))
        {
            return (
                new BoxResolution(0, qTickets, null, null, null, null),
                new BoxResolution(0, tTickets, null, null, null, null),
                new AllHitResolution(null, null));
        }

        // Parse OddsJson once → dict keyed by sorted-combo string.
        var quinellaLookup = new Dictionary<string, decimal>();
        var trioLookup = new Dictionary<string, decimal>();
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(oddsJson);
            ExtractOddsArray(doc.RootElement, "quinella", quinellaLookup);
            ExtractOddsArray(doc.RootElement, "trio", trioLookup);
        }
        catch (System.Text.Json.JsonException) { /* empty lookups */ }

        var qPayouts = qCombos
            .Select(c => quinellaLookup.TryGetValue(ComboKey(c), out var o) ? (int?)Math.Round(qStake * (double)o) : null)
            .ToList();
        var tPayouts = tCombos
            .Select(c => trioLookup.TryGetValue(ComboKey(c), out var o) ? (int?)Math.Round(tStake * (double)o) : null)
            .ToList();

        var qResolved = qPayouts.Count(p => p.HasValue);
        var tResolved = tPayouts.Count(p => p.HasValue);
        var qResolvedValues = qPayouts.Where(p => p.HasValue).Select(p => p!.Value).ToList();
        var tResolvedValues = tPayouts.Where(p => p.HasValue).Select(p => p!.Value).ToList();

        int? qMin = qResolvedValues.Count > 0 ? qResolvedValues.Min() : null;
        int? qMax = qResolvedValues.Count > 0 ? qResolvedValues.Max() : null;
        int? tMin = tResolvedValues.Count > 0 ? tResolvedValues.Min() : null;
        int? tMax = tResolvedValues.Count > 0 ? tResolvedValues.Max() : null;

        var qBox = new BoxResolution(qResolved, qTickets - qResolved,
            qMin, qMax,
            qMin.HasValue ? qMin - purchaseTotal : null,
            qMax.HasValue ? qMax - purchaseTotal : null);
        var tBox = new BoxResolution(tResolved, tTickets - tResolved,
            tMin, tMax,
            tMin.HasValue ? tMin - purchaseTotal : null,
            tMax.HasValue ? tMax - purchaseTotal : null);

        int? winPayout = winOddsForAllHit.HasValue ? (int?)Math.Round(winStake * winOddsForAllHit.Value) : null;
        int? allHitMin = (winPayout.HasValue && qMin.HasValue && tMin.HasValue)
            ? winPayout + qMin + tMin - purchaseTotal : null;
        int? allHitMax = (winPayout.HasValue && qMax.HasValue && tMax.HasValue)
            ? winPayout + qMax + tMax - purchaseTotal : null;

        return (qBox, tBox, new AllHitResolution(allHitMin, allHitMax));
    }

    private static void ExtractOddsArray(System.Text.Json.JsonElement root, string key, Dictionary<string, decimal> dest)
    {
        if (!root.TryGetProperty(key, out var arr) || arr.ValueKind != System.Text.Json.JsonValueKind.Array) return;
        foreach (var slot in arr.EnumerateArray())
        {
            if (!slot.TryGetProperty("combo", out var comboEl) || comboEl.ValueKind != System.Text.Json.JsonValueKind.Array) continue;
            if (!slot.TryGetProperty("odds", out var oddsEl)) continue;
            var combo = comboEl.EnumerateArray().Select(e => e.GetInt32()).OrderBy(n => n).ToArray();
            var odds = oddsEl.GetDecimal();
            dest[ComboKey(combo)] = odds;
        }
    }

    private static string ComboKey(int[] combo) => string.Join("-", combo);

    [HttpPost("bet-estimate")]
    public async Task<IActionResult> BetEstimate([FromBody] BetEstimateRequest body)
    {
        if (body?.races == null || body.races.Length == 0)
            return Ok(new { estimates = new Dictionary<string, object>() });

        var stakes = await _settings.GetBetStakesAsync();
        var winStake  = stakes.Win;
        var qStake    = stakes.Quinella;
        var tStake    = stakes.Trio;

        var raceIds = body.races.Select(r => r.race_id).Where(s => !string.IsNullOrWhiteSpace(s)).Distinct().ToList();
        using var db = await _dbFactory.CreateDbContextAsync();
        var entries = await db.RaceEntries.AsNoTracking()
            .Where(e => raceIds.Contains(e.RaceId))
            .Select(e => new { e.RaceId, e.PostPosition, e.Odds })
            .ToListAsync();
        var byRace = entries.GroupBy(e => e.RaceId!)
            .ToDictionary(g => g.Key, g => g.ToDictionary(e => e.PostPosition ?? 0, e => e.Odds));

        // Phase 11 forward: pull OddsJson (O2 + O5) for forward Q/T estimates.
        var oddsByRace = await db.Races.AsNoTracking()
            .Where(r => raceIds.Contains(r.RaceId) && r.OddsJson != null)
            .Select(r => new { r.RaceId, r.OddsJson })
            .ToDictionaryAsync(r => r.RaceId, r => r.OddsJson!);

        var estimates = new Dictionary<string, object>(body.races.Length);
        foreach (var item in body.races)
        {
            if (string.IsNullOrWhiteSpace(item.race_id)) continue;
            var boxN = item.box_posts?.Length ?? 0;
            var qTickets = boxN >= 2 ? boxN * (boxN - 1) / 2 : 0;
            var tTickets = boxN >= 3 ? boxN * (boxN - 1) * (boxN - 2) / 6 : 0;
            var totalTickets = 1 + qTickets + tTickets;
            // Per-leg purchase: 1 Win ticket at winStake + qTickets at qStake + tTickets at tStake.
            var purchaseTotal = winStake + qTickets * qStake + tTickets * tStake;

            double? winOdds = null;
            if (byRace.TryGetValue(item.race_id, out var postMap)
                && postMap.TryGetValue(item.honmei_post, out var od)
                && od.HasValue && od.Value > 0)
            {
                winOdds = (double)od.Value;
            }

            object winObj;
            if (winOdds.HasValue)
            {
                var winPayout = winStake * winOdds.Value;
                winObj = new
                {
                    odds    = winOdds.Value,
                    payout  = (int)Math.Round(winPayout),
                    net     = (int)Math.Round(winPayout - purchaseTotal),
                };
            }
            else
            {
                winObj = new { odds = (double?)null, payout = (int?)null, net = (int?)null };
            }

            // Phase 11 forward: resolve Q Box / T Box payouts from race.OddsJson if present.
            var (quinellaBox, trioBox, allHit) = ResolveBoxPayouts(
                oddsByRace.TryGetValue(item.race_id, out var oj) ? oj : null,
                item.box_posts ?? Array.Empty<int>(),
                qStake,
                tStake,
                winStake,
                qTickets,
                tTickets,
                purchaseTotal,
                winOdds);

            var warnings = new List<string>();
            var status = "ok";
            if (quinellaBox.MissingTickets > 0 || trioBox.MissingTickets > 0)
            {
                status = "partial";
                if (!oddsByRace.ContainsKey(item.race_id))
                    warnings.Add("No O2/O5 odds ingested for this race yet — Q/T estimates unavailable.");
                else if (quinellaBox.MissingTickets > 0)
                    warnings.Add($"Q Box: {quinellaBox.MissingTickets}/{qTickets} combos missing odds.");
            }

            estimates[item.race_id] = new
            {
                status,
                raceId   = item.race_id,
                purchase = new { total = purchaseTotal, tickets = totalTickets,
                                 winStake, quinellaStake = qStake, trioStake = tStake },
                win      = winObj,
                quinellaBox = new
                {
                    tickets         = qTickets,
                    resolvedTickets = quinellaBox.ResolvedTickets,
                    missingTickets  = quinellaBox.MissingTickets,
                    minPayout       = quinellaBox.MinPayout,
                    maxPayout       = quinellaBox.MaxPayout,
                    minNet          = quinellaBox.MinNet,
                    maxNet          = quinellaBox.MaxNet,
                },
                trioBox = new
                {
                    tickets         = tTickets,
                    resolvedTickets = trioBox.ResolvedTickets,
                    missingTickets  = trioBox.MissingTickets,
                    minPayout       = trioBox.MinPayout,
                    maxPayout       = trioBox.MaxPayout,
                    minNet          = trioBox.MinNet,
                    maxNet          = trioBox.MaxNet,
                },
                allHit  = new { minNet = allHit.MinNet, maxNet = allHit.MaxNet },
                warnings,
                message = status == "ok" ? "" : "Some Q/T combinations have no odds yet.",
            };
        }
        return Ok(new { estimates });
    }
}
