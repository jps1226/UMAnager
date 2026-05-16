using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/races")]
public sealed class RacesController : ControllerBase
{
    private readonly IDbContextFactory<AppDbContext> _dbFactory;

    public RacesController(IDbContextFactory<AppDbContext> dbFactory)
    {
        _dbFactory = dbFactory;
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

            string ResolveAncestorName(string? id)
            {
                if (string.IsNullOrEmpty(id)) return "";
                if (horseLookup.TryGetValue(id, out var h))
                    return !string.IsNullOrEmpty(h.NameEn) ? h.NameEn : h.NameJa;
                return breedingLookup.TryGetValue(id, out var name) ? name : "";
            }

            var upcomingByDate = new Dictionary<string, List<object>>();
            var pastByDate = new Dictionary<string, List<object>>();
            var now = DateTime.UtcNow;

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
                    time = race.SortTime?.ToString("HH:mm") ?? "TBA",
                    sort_time = sortTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss"),
                    clean_date = cleanDate,
                    history_refreshed = race.HistoryRefreshed
                };

                var raceEntries = entriesByRace.TryGetValue(race.RaceId, out var re) ? re : new List<Data.Entities.RaceEntry>();
                var entries = raceEntries
                    .OrderBy(e => e.PostPosition ?? 0)
                    .Select(e =>
                    {
                        horseLookup.TryGetValue(e.HorseId ?? "", out var horse);

                        return (object)new
                        {
                            Horse_ID = e.HorseId,
                            Horse = horse?.NameEn ?? horse?.NameJa ?? "",
                            PP = e.PostPosition ?? 0,
                            BK = e.Bracket ?? 0,
                            Record = recordByHorse.TryGetValue(e.HorseId ?? "", out var rec) ? rec : "",
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

            return Ok(new
            {
                upcoming_races_by_date = upcomingByDate.ToDictionary(k => k.Key, v => v.Value.ToArray()),
                past_races_by_date = pastByDate.ToDictionary(k => k.Key, v => v.Value.ToArray()),
                top_picks = Array.Empty<object>()
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }

    [HttpGet("../config")]
    public IActionResult GetConfig() => Ok(new
    {
        ui = new
        {
            riskSlider = 50,
            betSafetyIndicator = false,
            voteSortingTop = true,
            autoFetchPastResults = true,
            prefetchRaceCheck = false,
            debugConsole = false,
            autoLockPastVotes = false,
            showConsole = true,
            highlightAutoBets = false,
            highlightFallbackBridge = false,
            tvModeSplitPercent = 50,
            tvModePanelsFlipped = false,
            raceTableColumns = new object[]
            {
                new { key = "Shirushi", visible = true },
                new { key = "BK", visible = true },
                new { key = "PP", visible = true },
                new { key = "Horse", visible = true },
                new { key = "Record", visible = true },
                new { key = "Sire", visible = true },
                new { key = "Dam", visible = true },
                new { key = "BMS", visible = true },
                new { key = "Odds", visible = true },
                new { key = "Fav", visible = true },
                new { key = "Finish", visible = true }
            },
            formulaWeights = new
            {
                oddsCap = 100,
                formMultiplier = 100,
                freshnessBonus = 3,
                freshnessBreakeven = 10,
                pedigreeMultiplier = 30
            }
        },
        sidebarTabs = new
        {
            raceDatabase = true,
            pedigreeLists = true,
            autoPickStrategy = true,
            weekendWatchlist = true
        },
        backend = new
        {
            dataEngine = "jv"
        }
    });

    [HttpPost("../config")]
    public IActionResult PostConfig([FromBody] object config) => Ok(new { status = "ok" });

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

    [HttpPost("bet-estimate")]
    public IActionResult BetEstimate() => Ok(new { estimates = new { } });
}
