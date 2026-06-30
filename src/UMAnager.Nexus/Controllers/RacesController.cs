// ============================================================
// FILE: RacesController.cs
// LAYER: API (api/races) — the heaviest endpoint in the app
// PURPOSE: Builds the full race-card payload (pedigree names, last-3 form w/ Ninki-Δ,
//          sire-fit %, jockey/trainer rolling stats, career record, odds) with ETag/304
//          + in-memory body cache. Also calendar skeleton, per-day detail (?date=),
//          odds-history series, and the voting-tab bet-estimate.
// KEY DEPENDENCIES: AppDbContext, SettingsService, SirePerformanceService,
//          JockeyTrainerStatsService, IMemoryCache.
// CAUTION: The /api/races ETag is data-keyed "races-v13-…" — BUMP vN on any response-shape
//          change or browsers serve a stale 304 body. SortTime is JST-wall-clock-in-UTC.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
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

    // Phase 28: U+2460–U+2471 = ①–⑱ (circled digits 1-18, covers max JRA field size).
    private static string CircledRank(int? rank) =>
        rank is >= 1 and <= 18 ? ((char)(0x245F + rank.Value)).ToString() : "";

    // Phase 28 v2: Ninki-to-Finish Delta scoring. Replaces the earlier
    // field-strength discount. Delta = favRank - finish.
    //   Pattern A (Δ > 3): under-the-radar overachiever bonus.
    //   Pattern B (Δ < 0 AND favRank ≤ 5): burned-favorite penalty.
    //     Gated on favRank so a 13th-fav finishing 16th doesn't get punished —
    //     a longshot running like a longshot isn't a market betrayal.
    // Recency weights below amplify the most-recent run by ~2.5× vs run #3.
    private const double DeltaBonusThreshold     = 3.0;
    private const double DeltaBonusPerUnit       = 0.10;
    private const double DeltaPenaltyPerUnit     = 0.08;
    private const int    BurnedFavoriteMaxNinki  = 5;

    // Phase 28 (finish): field-size weighting of the overachiever bonus. Beating the
    // market by Δ in an 18-horse field is a deeper result than in an 8-horse field, so
    // the bonus scales by (field size / reference). This re-introduces the original spec's
    // "discount wins in weak fields" intent, but only on the positive side and within the
    // v3 Ninki-Δ framework. The burned-favorite penalty is field-agnostic — a fancied horse
    // flopping is bad regardless of how many rivals it had.
    private const double FieldSizeReference = 14.0; // ~JRA average field
    private const double FieldFactorMin     = 0.6;  // small/weak field → muted credit
    private const double FieldFactorMax     = 1.3;  // big/deep field → amplified credit

    private static double FieldFactor(int fieldSize) =>
        fieldSize > 0 ? Math.Clamp(fieldSize / FieldSizeReference, FieldFactorMin, FieldFactorMax) : 1.0;

    private static double RunScore(int fin, int? favRank, int fieldSize)
    {
        if (fin <= 0) return 0.0;
        double raw = (fin <= 5) ? (1.0 / fin) : 0.0;
        if (favRank is null or < 1) return raw;
        double delta = favRank.Value - fin;
        if (delta > DeltaBonusThreshold)
            raw += (delta - DeltaBonusThreshold) * DeltaBonusPerUnit * FieldFactor(fieldSize);
        else if (delta < 0 && favRank.Value <= BurnedFavoriteMaxNinki)
            raw += delta * DeltaPenaltyPerUnit;
        return raw;
    }

    private static (string last3Str, double formScore, string fieldsStr) ComputeLast3(
        List<(DateTime Date, int Finish, int? FavRank, int FieldSize)>? hist, DateTime raceDate)
    {
        if (hist == null || hist.Count == 0) return ("—-—-—", 0.0, "—-—-—");
        var picks = new List<(int Finish, int? FavRank, int FieldSize)>(3);
        foreach (var h in hist)
        {
            if (h.Date >= raceDate) continue;
            picks.Add((h.Finish, h.FavRank, h.FieldSize));
            if (picks.Count == 3) break;
        }
        if (picks.Count == 0) return ("—-—-—", 0.0, "—-—-—");
        var parts = new string[3];
        var fields = new string[3];
        double score = 0.0;
        for (int i = 0; i < 3; i++)
        {
            if (i < picks.Count)
            {
                var (fin, favRank, fieldSize) = picks[i];
                parts[i]  = fin.ToString() + CircledRank(favRank);
                fields[i] = fieldSize > 0 ? fieldSize.ToString() : "—";
                score += FormWeights[i] * RunScore(fin, favRank, fieldSize);
            }
            else { parts[i] = "—"; fields[i] = "—"; }
        }
        return (string.Join("-", parts), score, string.Join("-", fields));
    }

    private static string TrackName(string? code)
    {
        if (string.IsNullOrWhiteSpace(code)) return "";
        var padded = code.Trim().PadLeft(2, '0');
        return TrackNames.TryGetValue(padded, out var name) ? name : padded;
    }

    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly Services.SettingsService _settings;
    private readonly Services.SirePerformanceService _sirePerf;
    private readonly Services.JockeyTrainerStatsService _jtStats;
    private readonly IMemoryCache _cache;

    // Fixed key — one slot holds the most-recent serialized response + its ETag.
    private const string RacesCacheKey = "races-v1-response";

    public RacesController(
        IDbContextFactory<AppDbContext> dbFactory,
        Services.SettingsService settings,
        Services.SirePerformanceService sirePerf,
        Services.JockeyTrainerStatsService jtStats,
        IMemoryCache cache)
    {
        _dbFactory = dbFactory;
        _settings = settings;
        _sirePerf = sirePerf;
        _jtStats = jtStats;
        _cache = cache;
    }

    [HttpGet]
    public async Task<IActionResult> GetRaces()
    {
        var _sw = System.Diagnostics.Stopwatch.StartNew();
        long tRacesLoaded = 0, tDataFetched = 0, tBuilt = 0;
        try
        {
            // Phase 38: optional ?date=YYYY-MM-DD returns one JST race-day's full detail,
            // bypassing both the rolling window and the shared response cache. Powers the
            // calendar's lazy day-load so any day with data can be opened on demand.
            var dateParam = Request.Query["date"].ToString();
            bool dateFiltered = DateOnly.TryParse(dateParam, out var reqDate);

            // ── FAST PATH: ETag string still fresh (< 30 s) — zero DB queries ─────────
            // Caching the computed ETag string avoids all 5 ETag-related DB round-trips
            // on every request. The 30 s TTL is well below the 5-minute live-odds floor.
            // Date-filtered calls skip the shared cache entirely (occasional, on-demand).
            const string EtagCacheKey = "races-etag-v10";
            string? fastEtag = null;
            if (!dateFiltered && _cache.TryGetValue<string>(EtagCacheKey, out fastEtag))
            {
                Response.Headers["Cache-Control"] = "no-cache";
                Response.Headers["ETag"] = fastEtag!;
                if (Request.Headers["If-None-Match"].ToString() == fastEtag)
                    return StatusCode(StatusCodes.Status304NotModified);
                if (_cache.TryGetValue<(string Tag, byte[] Bytes)>(RacesCacheKey, out var fb) && fb.Tag == fastEtag)
                {
                    Response.Headers["X-Races-Timing"] = $"fastbody={_sw.ElapsedMilliseconds}ms";
                    return File(fb.Bytes, "application/json; charset=utf-8");
                }
                // Body cache was evicted (server restart / memory pressure) — fall through
                // with fastEtag set so we skip recomputing the ETag below.
            }

            // ── SLOW PATH: single DB context for ETag computation + full build ─────────
            // Races are loaded once and reused by both the ETag check and the response
            // builder — no double-query on cold cache paths.
            using var db = await _dbFactory.CreateDbContextAsync();
            var jstNow = DateTime.UtcNow.AddHours(9);
            var windowStart = jstNow.AddDays(-14); // 14-day rolling window (the calendar skeleton + lazy day-load cover anything older)
            List<Data.Entities.Race> races;
            if (dateFiltered)
            {
                // SortTime is a timestamptz holding JST wall-clock numbers; the windowed query
                // compares it against a Kind=Utc value (UtcNow.AddHours(9)). Match that convention
                // — a Kind=Utc midnight of the requested JST day — so Npgsql accepts the parameter
                // and the bounds line up with cleanDate bucketing.
                var dayStart = DateTime.SpecifyKind(reqDate.ToDateTime(TimeOnly.MinValue), DateTimeKind.Utc);
                var dayEnd = dayStart.AddDays(1);
                races = await db.Races
                    .Where(r => r.SortTime != null && r.SortTime >= dayStart && r.SortTime < dayEnd)
                    .OrderBy(r => r.SortTime)
                    .AsNoTracking()
                    .ToListAsync();
            }
            else
            {
                races = await db.Races
                    .Where(r => r.SortTime != null && r.SortTime >= windowStart)
                    .OrderBy(r => r.SortTime)
                    .AsNoTracking()
                    .ToListAsync();
            }

            tRacesLoaded = _sw.ElapsedMilliseconds;
            // ETag / 304 / shared-cache only apply to the default windowed request. A
            // date-filtered call is served fresh with no-store (see header below).
            string etag = "";
            if (!dateFiltered)
            {
                if (fastEtag == null)
                {
                    // ETag cache miss: compute from the already-loaded races + 4 more queries.
                    DateTime? maxLastUpdated = races.Count > 0 ? races.Max(r => r.LastUpdated) : null;
                    var raceIdsEtag = races.Select(r => r.RaceId).ToHashSet();
                    var maxBreedingUpdated = await db.BreedingHorses.AsNoTracking()
                        .MaxAsync(b => (DateTime?)b.LastUpdated);
                    var maxEntryUpdated = raceIdsEtag.Count > 0
                        ? await db.RaceEntries.AsNoTracking()
                            .Where(e => raceIdsEtag.Contains(e.RaceId))
                            .MaxAsync(e => (DateTime?)e.UpdatedAt)
                        : null;
                    var maxJockeyRefresh = await db.Database
                        .SqlQueryRaw<DateTime?>("SELECT MAX(stats_refreshed_at) AS \"Value\" FROM jockeys")
                        .FirstOrDefaultAsync();
                    var maxTrainerRefresh = await db.Database
                        .SqlQueryRaw<DateTime?>("SELECT MAX(stats_refreshed_at) AS \"Value\" FROM trainers")
                        .FirstOrDefaultAsync();
                    var jtTicks = Math.Max(maxJockeyRefresh?.Ticks ?? 0, maxTrainerRefresh?.Ticks ?? 0);
                    etag = $"\"races-v14-{races.Count}-{maxLastUpdated?.Ticks ?? 0}-{maxBreedingUpdated?.Ticks ?? 0}-{maxEntryUpdated?.Ticks ?? 0}-{jtTicks}\"";
                    _cache.Set(EtagCacheKey, etag,
                        new MemoryCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(30) });
                }
                else
                {
                    etag = fastEtag!; // body was evicted but ETag is still valid
                }

                Response.Headers["Cache-Control"] = "no-cache";
                Response.Headers["ETag"] = etag;
                if (Request.Headers["If-None-Match"].ToString() == etag)
                    return StatusCode(StatusCodes.Status304NotModified);
                if (_cache.TryGetValue<(string Tag, byte[] Bytes)>(RacesCacheKey, out var cachedBody) && cachedBody.Tag == etag)
                {
                    Response.Headers["X-Races-Timing"] = $"slowbody={_sw.ElapsedMilliseconds}ms;racesLoad={tRacesLoaded}ms";
                    return File(cachedBody.Bytes, "application/json; charset=utf-8");
                }
            }
            else
            {
                Response.Headers["Cache-Control"] = "no-store";
            }

            // ── Full build ────────────────────────────────────────────────────────────
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
                select new { e.HorseId, e.RaceId, r.RaceDate, Finish = e.FinishPos!.Value, e.FavRank, r.Surface, r.Distance, e.PerformanceJson }
            ).ToListAsync();

            // Phase 43: the horse's OWN record split by surface and distance bucket, derived from
            // the same finished-entry ⋈ race history above (every past start already carries its
            // race's surface + distance). Parallels Sire_Fit but for the individual horse. Keyed by
            // horse → list of (surface, distance-bucket, finish) for cheap per-entry aggregation.
            var histSplitByHorse = horseFinishHistory
                .GroupBy(x => x.HorseId!)
                .ToDictionary(
                    g => g.Key,
                    g => g.Select(x => (
                            Surface: x.Surface ?? "",
                            Bucket: Services.SirePerformanceService.DistanceBucket(x.Distance),
                            Finish: x.Finish))
                          .ToList());

            // Phase 28 (finish): field size of each past race the runners contested, so the
            // Ninki-Δ overachiever bonus can be weighted by how deep that field was. One grouped
            // count over the races our horses actually ran in (cache-miss path only).
            var histRaceIds = horseFinishHistory.Select(x => x.RaceId).Distinct().ToList();
            var fieldSizeByRace = histRaceIds.Count > 0
                ? await db.RaceEntries.AsNoTracking()
                    .Where(e => histRaceIds.Contains(e.RaceId))
                    .GroupBy(e => e.RaceId)
                    .Select(g => new { RaceId = g.Key, N = g.Count() })
                    .ToDictionaryAsync(x => x.RaceId, x => x.N)
                : new Dictionary<string, int>();

            var finishesByHorse = horseFinishHistory
                .GroupBy(x => x.HorseId!)
                .ToDictionary(
                    g => g.Key,
                    g => g.OrderByDescending(x => x.RaceDate)
                          .Select(x => (x.RaceDate, x.Finish, x.FavRank,
                                        FieldSize: fieldSizeByRace.TryGetValue(x.RaceId, out var n) ? n : 0))
                          .ToList()
                );

            // s52: each horse's prior runs (date + surface + distance), most-recent first, for the
            // cold-engine "fresh longshot / surface-switch" PREVIEW overlay. Built from the same
            // finished-entry history above. Per entry we take the most recent run strictly BEFORE the
            // viewed race (mirrors ComputeLast3's date gate) → days-since-last + last surface/distance.
            var priorRunsByHorse = horseFinishHistory
                .GroupBy(x => x.HorseId!)
                .ToDictionary(
                    g => g.Key,
                    g => g.OrderByDescending(x => x.RaceDate)
                          // s54: also carry the run's PerformanceJson (corner positions) so the card can
                          // show the horse's PREDICTED running style (front-runner/closer) from its last start.
                          .Select(x => (x.RaceDate, Surface: x.Surface ?? "", x.Distance, Perf: x.PerformanceJson ?? ""))
                          .ToList()
                );

            // Race class is now stored on the races table (Oracle Q20: JyokenCD slot 5
            // parsed from RA bytes at ingest time). No per-request computation needed —
            // just emit race.RaceClass straight into the API payload below.

            // Phase 9: load sire_performance MV once per request (~5–10K rows) and
            // index by (sire_id, surface, bucket) for O(1) lookup per entry. Min
            // sample size of 10 starts — below that, sire_fit is null to avoid
            // noisy 100%/0% from rare sires.
            const int MinSireStarts = 10;
            var sirePerfRows = await _sirePerf.LoadAllAsync();
            var sirePerfLookup = sirePerfRows
                .Where(r => r.Starts >= MinSireStarts)
                .ToDictionary(
                    r => (r.SireId, r.Surface ?? "", r.Bucket ?? ""),
                    r => (r.WinPct, r.PlacePct, r.Starts));

            // Phase 8: jockey/trainer rolling stats. Indexed by code for O(1) lookup per entry.
            // MinJockeyStarts=30 (90d), MinTrainerStarts=20 (180d) floor — below these, stats
            // are surfaced as null and contribute nothing to scoring (avoids noisy 1-ride 100%).
            const int MinJockeyStarts = 30;
            const int MinTrainerStarts = 20;
            var jockeyStats = (await _jtStats.LoadAllJockeysAsync())
                .ToDictionary(j => j.Code, j => j);
            var trainerStats = (await _jtStats.LoadAllTrainersAsync())
                .ToDictionary(t => t.Code, t => t);
            tDataFetched = _sw.ElapsedMilliseconds;


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
                    // Phase 43: distance (meters) + surface (turf/dirt) for the race-card chip
                    // and the surface/distance scoring split. Already stored on races; just
                    // surface them so the frontend stops seeing undefined (the AI-export block
                    // already reads info.distance/info.surface).
                    distance = race.Distance,
                    surface = race.Surface ?? "",
                    time = race.SortTime?.ToString("HH:mm") ?? "TBA",
                    sort_time = sortTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss"),
                    // Proper ISO-8601 with JST offset so the frontend can convert to any
                    // local timezone via new Date(sort_time_iso). sort_time (no offset)
                    // is kept for backward-compat with parseRaceSortTime.
                    sort_time_iso = race.SortTime?.ToString("yyyy-MM-ddTHH:mm:ss") + "+09:00",
                    clean_date = cleanDate,
                    history_refreshed = race.HistoryRefreshed,
                    // JRA-canonical race class from JyokenCD slot 5 (Oracle Q20).
                    // "debut" / "maiden" / "1win" / "2win" / "3win" / "open" / "other" / null.
                    // Set at RA-ingest time; never changes.
                    race_class = race.RaceClass,
                    // s55: cancellation. JRA-VAN データ区分 (DataStatus) = 9 means レース中止 (race cancelled);
                    // there is no separate 順延/postponed code — a scrubbed slot just flips to 9 and any
                    // rescheduled running appears as a brand-new card (Oracle 2026-06-27). Surface a simple
                    // bool so the frontend (esp. TV mode) can badge the dead slot.
                    cancelled = race.DataStatus == 9,
                    // Phase 11 backward: HR payouts — emitted as a raw JSON string for the
                    // frontend to parse only when it needs to enrich past-race recap chips
                    // (◎ Win / Q Box / T Box) with the actual ¥ amount that paid out.
                    results_json = race.ResultsJson
                };

                var raceEntries = entriesByRace.TryGetValue(race.RaceId, out var re) ? re : new List<Data.Entities.RaceEntry>();
                var raceSurface = race.Surface ?? "";
                var raceBucket = Services.SirePerformanceService.DistanceBucket(race.Distance);
                // A race already run keeps only what the review grid displays — the heavy PRE-race
                // handicapping payload (sire-fit, surface/distance splits, jockey/trainer rolling stats,
                // layoff + last-start style) is dead weight once the result is in. Skipping it for past
                // races roughly halves their JSON, and they dominate the window between meetings — the
                // biggest lever on page-load size + parse time. Older days reload full via /api/races?date=.
                var racePast = !(sortTimeUtc > now);
                var entries = raceEntries
                    .OrderBy(e => e.PostPosition ?? 0)
                    .Select(e =>
                    {
                        horseLookup.TryGetValue(e.HorseId ?? "", out var horse);
                        finishesByHorse.TryGetValue(e.HorseId ?? "", out var hist);
                        var (last3Str, formScore, last3Fields) = ComputeLast3(hist, race.RaceDate);

                        if (racePast)
                        {
                            return (object)new
                            {
                                Horse_ID = e.HorseId,
                                Horse = horse?.NameEn ?? horse?.NameJa ?? "",
                                PP = e.PostPosition ?? 0,
                                BK = e.Bracket ?? 0,
                                Record = recordByHorse.TryGetValue(e.HorseId ?? "", out var recPast) ? recPast : "",
                                Last3 = last3Str,
                                Last3_Fields = last3Fields,
                                Sire = ResolveAncestorName(horse?.SireId),
                                Sire_ID = horse?.SireId ?? "",
                                Dam = ResolveAncestorName(horse?.DamId),
                                Dam_ID = horse?.DamId ?? "",
                                BMS = ResolveAncestorName(horse?.BmsId),
                                BMS_ID = horse?.BmsId ?? "",
                                Odds = e.Odds?.ToString("F1") ?? "",
                                Prev_Odds = e.PrevOdds?.ToString("F1") ?? "",
                                Fav = e.FavRank?.ToString() ?? "",
                                Finish = e.FinishPos?.ToString() ?? "",
                                Scratched = e.Scratched,
                                Sex = e.Sex,
                                Jockey = e.JockeyName ?? "",
                            };
                        }

                        // s52: layoff + last-start surface/distance for the preview overlay. Most
                        // recent prior run strictly before this race's date (null/"" when debut).
                        int? daysSinceLast = null;
                        string lastSurface = "";
                        int? lastDistance = null;
                        string lastPerf = "";
                        if (priorRunsByHorse.TryGetValue(e.HorseId ?? "", out var priorRuns))
                        {
                            foreach (var pr in priorRuns) // already desc by date → first match = most recent
                            {
                                if (pr.RaceDate >= race.RaceDate) continue;
                                daysSinceLast = (int)(race.RaceDate - pr.RaceDate).TotalDays;
                                lastSurface = pr.Surface;
                                lastDistance = pr.Distance;
                                lastPerf = pr.Perf; // corner positions of the last start → predicted run style
                                break;
                            }
                        }

                        // Phase 9: sire-fit % for THIS race's (surface, bucket). null when
                        // the sire's sample in the bucket is below MinSireStarts.
                        decimal? sireFitPct = null;
                        decimal? sirePlaceFitPct = null;
                        int? sireFitStarts = null;
                        var sireId = horse?.SireId;
                        if (!string.IsNullOrEmpty(sireId) && !string.IsNullOrEmpty(raceSurface) && !string.IsNullOrEmpty(raceBucket))
                        {
                            if (sirePerfLookup.TryGetValue((sireId, raceSurface, raceBucket), out var sp))
                            {
                                sireFitPct = sp.WinPct;
                                sirePlaceFitPct = sp.PlacePct;
                                sireFitStarts = sp.Starts;
                            }
                        }

                        // Phase 8: jockey/trainer rolling stats. Surface raw rates + A/E. The
                        // frontend gates the scoring contribution behind min-sample floors; we
                        // still emit small-sample names so the column doesn't go blank entirely.
                        Services.JockeyTrainerStatsService.JockeyStat? jStat = null;
                        if (!string.IsNullOrEmpty(e.JockeyCode))
                            jockeyStats.TryGetValue(e.JockeyCode, out jStat);

                        Services.JockeyTrainerStatsService.TrainerStat? tStat = null;
                        if (!string.IsNullOrEmpty(e.TrainerCode))
                            trainerStats.TryGetValue(e.TrainerCode, out tStat);

                        bool jQualifies = jStat != null && (jStat.Starts ?? 0) >= MinJockeyStarts;
                        bool tQualifies = tStat != null && (tStat.Starts ?? 0) >= MinTrainerStarts;

                        // Phase 43: this horse's OWN win%/place% on THIS race's surface and
                        // distance bucket. Min 3 starts in the split → null (horses race far
                        // less than sires, so the sire-fit floor of 10 would blank almost
                        // everyone). Low-weight tiebreaker in the JS scorer, mirroring Sire_Fit.
                        const int MinHorseSplitStarts = 3;
                        int? surfaceStarts = null, distStarts = null;
                        decimal? surfaceWinPct = null, surfacePlacePct = null;
                        decimal? distWinPct = null, distPlacePct = null;
                        // Jump (障害) is a separate discipline — never mix jump and flat history.
                        // Surface-fit is already clean (it matches the exact surface, and "jump"
                        // only equals "jump"). The distance-fit split is otherwise surface-agnostic
                        // (turf↔dirt distance aptitude transfers), so we must additionally gate it
                        // on discipline so a horse's jump-3000m runs don't pollute its flat-long
                        // distance fit — and vice-versa. (~51 horses in our data carry both.)
                        bool raceIsJump = raceSurface == "jump";
                        if (histSplitByHorse.TryGetValue(e.HorseId ?? "", out var hSplit))
                        {
                            if (!string.IsNullOrEmpty(raceSurface))
                            {
                                var sf = hSplit.Where(x => x.Surface == raceSurface).ToList();
                                if (sf.Count >= MinHorseSplitStarts)
                                {
                                    surfaceStarts   = sf.Count;
                                    surfaceWinPct   = Math.Round(100m * sf.Count(x => x.Finish == 1) / sf.Count, 1);
                                    surfacePlacePct = Math.Round(100m * sf.Count(x => x.Finish <= 3) / sf.Count, 1);
                                }
                            }
                            if (!string.IsNullOrEmpty(raceBucket))
                            {
                                var ds = hSplit.Where(x => x.Bucket == raceBucket
                                        && (raceIsJump ? x.Surface == "jump" : x.Surface != "jump"))
                                    .ToList();
                                if (ds.Count >= MinHorseSplitStarts)
                                {
                                    distStarts   = ds.Count;
                                    distWinPct   = Math.Round(100m * ds.Count(x => x.Finish == 1) / ds.Count, 1);
                                    distPlacePct = Math.Round(100m * ds.Count(x => x.Finish <= 3) / ds.Count, 1);
                                }
                            }
                        }

                        return (object)new
                        {
                            Horse_ID = e.HorseId,
                            Horse = horse?.NameEn ?? horse?.NameJa ?? "",
                            PP = e.PostPosition ?? 0,
                            BK = e.Bracket ?? 0,
                            Record = recordByHorse.TryGetValue(e.HorseId ?? "", out var rec) ? rec : "",
                            Last3 = last3Str,
                            Last3_Fields = last3Fields,   // Phase 28: field size per past run (popover context)
                            Form_Score = formScore,
                            Sire = ResolveAncestorName(horse?.SireId),
                            Sire_ID = horse?.SireId ?? "",
                            Sire_Fit = sireFitPct,
                            Sire_Place_Fit = sirePlaceFitPct,
                            Sire_Starts = sireFitStarts,
                            // Phase 43: horse's own surface/distance record (null below 3 starts).
                            Surface_Win_Pct   = surfaceWinPct,
                            Surface_Place_Pct = surfacePlacePct,
                            Surface_Starts    = surfaceStarts,
                            Dist_Win_Pct      = distWinPct,
                            Dist_Place_Pct    = distPlacePct,
                            Dist_Starts       = distStarts,
                            Dam = ResolveAncestorName(horse?.DamId),
                            Dam_ID = horse?.DamId ?? "",
                            BMS = ResolveAncestorName(horse?.BmsId),
                            BMS_ID = horse?.BmsId ?? "",
                            Odds = e.Odds?.ToString("F1") ?? "",
                            Prev_Odds = e.PrevOdds?.ToString("F1") ?? "",
                            Fav = e.FavRank?.ToString() ?? "",
                            // s52: cold-engine preview inputs (layoff + last-start surface/distance).
                            Days_Since_Last = daysSinceLast,
                            Last_Surface = lastSurface,
                            Last_Distance = lastDistance,
                            // s54: last start's PerformanceJson (corners/l3f) → frontend infers predicted run style.
                            Last_Perf = lastPerf,
                            Finish = e.FinishPos?.ToString() ?? "",
                            // SE 異常区分 1/2/3 (取消/除外) → horse removed from betting; the frontend
                            // bet-line synthesizer (buildRaceBetLines) shrinks the ticket. Only set
                            // once results settle; false pre-race. (中止 code 4 is NOT a scratch.)
                            Scratched = e.Scratched,
                            // s55: SE 性別コード → TV-mode gender sign. 1=牡 colt, 2=牝 filly/mare, 3=セ gelding, 0/null=unknown.
                            Sex = e.Sex,
                            Jockey      = jStat?.NameEn ?? jStat?.NameJa ?? e.JockeyName ?? "",
                            Jockey_Code = e.JockeyCode ?? "",
                            Jockey_Starts = jStat?.Starts,
                            Jockey_Win_Pct  = jQualifies ? jStat!.WinPct  : null,
                            Jockey_Place_Pct= jQualifies ? jStat!.PlacePct: null,
                            Jockey_AE       = jQualifies ? jStat!.Ae     : null,
                            Trainer      = tStat?.NameEn ?? tStat?.NameJa ?? "",
                            Trainer_Code = e.TrainerCode ?? "",
                            Trainer_Starts = tStat?.Starts,
                            Trainer_Win_Pct  = tQualifies ? tStat!.WinPct  : null,
                            Trainer_Place_Pct= tQualifies ? tStat!.PlacePct: null,
                            Trainer_AE       = tQualifies ? tStat!.Ae     : null
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

            var responsePayload = new
            {
                upcoming_races_by_date = upcomingByDate.ToDictionary(k => k.Key, v => v.Value.ToArray()),
                past_races_by_date     = pastByDate.ToDictionary(k => k.Key, v => v.Value.ToArray()),
                races_by_date          = combinedByDate.ToDictionary(k => k.Key, v => v.Value.ToArray()),
                top_picks              = Array.Empty<object>()
            };
            tBuilt = _sw.ElapsedMilliseconds;
            var responseBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
                responsePayload,
                new System.Text.Json.JsonSerializerOptions { PropertyNamingPolicy = null });
            // Date-filtered responses are not cached in the shared slot (it's keyed to the
            // windowed body); only the default request populates it.
            if (!dateFiltered)
                _cache.Set(RacesCacheKey, (etag, responseBytes),
                    new MemoryCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(4) });
            Response.Headers["X-Races-Timing"] = $"build_total={_sw.ElapsedMilliseconds}ms;racesLoad={tRacesLoaded}ms;"
                + $"dataFetch={tDataFetched - tRacesLoaded}ms;assemble={tBuilt - tDataFetched}ms;serialize={_sw.ElapsedMilliseconds - tBuilt}ms;"
                + $"bodyBytes={responseBytes.Length};cached={!dateFiltered}";
            return File(responseBytes, "application/json; charset=utf-8");
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }

    // Phase 38: lightweight calendar skeleton. Returns every race day in the DB as
    // { date, count, timeline } with NO entry data, so the dashboard calendar can highlight
    // and navigate to any day with races. The heavy per-day detail loads on demand via
    // GET /api/races?date=YYYY-MM-DD. Date strings match GetRaces' cleanDate bucketing
    // (RaceDate.ToString) so the two line up exactly. Cached 60 s to absorb rapid reloads.
    [HttpGet("calendar")]
    public async Task<IActionResult> GetCalendar()
    {
        const string CalCacheKey = "races-calendar-v1";
        if (_cache.TryGetValue<byte[]>(CalCacheKey, out var cached) && cached is not null)
        {
            Response.Headers["Cache-Control"] = "no-cache";
            return File(cached, "application/json; charset=utf-8");
        }

        using var db = await _dbFactory.CreateDbContextAsync();
        var now = DateTime.UtcNow.AddHours(9);
        var rows = await db.Races.AsNoTracking()
            .Where(r => r.SortTime != null)
            .Select(r => new { r.RaceDate, r.SortTime })
            .ToListAsync();

        var days = rows
            .GroupBy(r => r.RaceDate.ToString("yyyy-MM-dd"))
            .Select(g => new
            {
                date = g.Key,
                count = g.Count(),
                // A day is "upcoming" if any of its races are still ahead of post time.
                timeline = g.Any(x => x.SortTime > now) ? "upcoming" : "past"
            })
            .OrderBy(d => d.date)
            .ToList();

        var bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
            new { days },
            new System.Text.Json.JsonSerializerOptions { PropertyNamingPolicy = null });
        _cache.Set(CalCacheKey, bytes,
            new MemoryCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(60) });
        Response.Headers["Cache-Control"] = "no-cache";
        return File(bytes, "application/json; charset=utf-8");
    }

    // /api/config GET/POST is owned by ConfigController — don't duplicate here.

    // Phase 37: odds-over-time series for one race. Returns one entry per horse with the
    // ordered list of (timestamp, odds) snapshots captured while betting was open. The
    // frontend draws a multi-line trend graph; names are joined client-side from the
    // already-loaded race entries, so we keep this payload lean.
    [HttpGet("{raceId}/odds-history")]
    public async Task<IActionResult> GetOddsHistory(string raceId)
    {
        if (string.IsNullOrWhiteSpace(raceId)) return BadRequest(new { error = "raceId required" });

        using var db = await _dbFactory.CreateDbContextAsync();
        var rows = await db.OddsHistory.AsNoTracking()
            .Where(h => h.RaceId == raceId)
            .OrderBy(h => h.CapturedAt)
            .Select(h => new { h.HorseId, h.PostPosition, h.Odds, h.FavRank, h.CapturedAt })
            .ToListAsync();

        var series = rows
            .GroupBy(r => r.HorseId)
            .Select(g => new
            {
                horse_id      = g.Key,
                post_position = g.Select(x => x.PostPosition).LastOrDefault(),
                points        = g.Select(x => new
                {
                    t    = x.CapturedAt.ToUniversalTime().ToString("o"),
                    odds = (double)x.Odds,
                    fav  = x.FavRank
                }).ToList()
            })
            .OrderBy(s => s.post_position ?? 999)
            .ToList();

        // no-store: live-ish data, cheap query, and we never want a stale trend served.
        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new { race_id = raceId, series });
    }

    [HttpGet("../marks")]
    public IActionResult GetMarks() => Ok(new
    {
        version = 2,
        marks = new { },
        raceMeta = new { }
    });

    [HttpPost("../marks")]
    public IActionResult PostMarks([FromBody] object marks) => Ok(new { status = "ok" });

    // /api/lists is owned by ListsController — duplicate empty stub removed 2026-05-18.

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
