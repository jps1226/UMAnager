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

            // For now, load only the most recent 300 races (roughly 1-2 weeks) to avoid
            // massive response payloads. This prevents the UI from choking on 19MB+ responses.
            // TODO: Implement proper pagination with query parameters once build/EF Core issues are resolved.
            var races = await db.Races
                .OrderByDescending(r => r.SortTime ?? DateTime.MinValue)
                .Take(300)
                .OrderBy(r => r.SortTime)
                .AsNoTracking()
                .ToListAsync();

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
                .ToDictionaryAsync(b => b.HansyokuNum, b => b.NameJa);
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
                return breedingLookup.TryGetValue(id, out var name) ? name : "";
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
                    history_refreshed = race.HistoryRefreshed
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

    [HttpPost("bet-estimate")]
    public async Task<IActionResult> BetEstimate([FromBody] BetEstimateRequest body)
    {
        if (body?.races == null || body.races.Length == 0)
            return Ok(new { estimates = new Dictionary<string, object>() });

        var stake = await _settings.GetIntAsync(Services.SettingsService.Keys.BetEstimateStakeYen,
                                                Services.SettingsService.Defaults.BetEstimateStakeYen);
        if (stake <= 0) stake = Services.SettingsService.Defaults.BetEstimateStakeYen;

        var raceIds = body.races.Select(r => r.race_id).Where(s => !string.IsNullOrWhiteSpace(s)).Distinct().ToList();
        using var db = await _dbFactory.CreateDbContextAsync();
        var entries = await db.RaceEntries.AsNoTracking()
            .Where(e => raceIds.Contains(e.RaceId))
            .Select(e => new { e.RaceId, e.PostPosition, e.Odds })
            .ToListAsync();
        var byRace = entries.GroupBy(e => e.RaceId!)
            .ToDictionary(g => g.Key, g => g.ToDictionary(e => e.PostPosition ?? 0, e => e.Odds));

        var estimates = new Dictionary<string, object>(body.races.Length);
        foreach (var item in body.races)
        {
            if (string.IsNullOrWhiteSpace(item.race_id)) continue;
            var boxN = item.box_posts?.Length ?? 0;
            var qTickets = boxN >= 2 ? boxN * (boxN - 1) / 2 : 0;
            var tTickets = boxN >= 3 ? boxN * (boxN - 1) * (boxN - 2) / 6 : 0;
            var totalTickets = 1 + qTickets + tTickets;
            var purchaseTotal = stake * totalTickets;

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
                var winPayout = stake * winOdds.Value;
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

            var warnings = new List<string> { "Quinella and Trio odds are not yet ingested — only the Win leg is computed." };

            estimates[item.race_id] = new
            {
                status   = "partial",
                raceId   = item.race_id,
                purchase = new { total = purchaseTotal, tickets = totalTickets, stake },
                win      = winObj,
                quinellaBox = new
                {
                    tickets         = qTickets,
                    resolvedTickets = 0,
                    missingTickets  = qTickets,
                    minPayout       = (int?)null,
                    maxPayout       = (int?)null,
                    minNet          = (int?)null,
                    maxNet          = (int?)null,
                },
                trioBox = new
                {
                    tickets         = tTickets,
                    resolvedTickets = 0,
                    missingTickets  = tTickets,
                    minPayout       = (int?)null,
                    maxPayout       = (int?)null,
                    minNet          = (int?)null,
                    maxNet          = (int?)null,
                },
                allHit  = new { minNet = (int?)null, maxNet = (int?)null },
                warnings,
                message = "Q Box / T Box / All Hit pending O2/O3 odds ingest.",
            };
        }
        return Ok(new { estimates });
    }
}
