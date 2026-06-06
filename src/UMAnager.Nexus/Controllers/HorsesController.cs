// ============================================================
// FILE: HorsesController.cs
// LAYER: API (api/horses)
// PURPOSE: Horse search (racing/breeding filter) and the Phase 31 deep-dive profile —
//          hero stats, full race history, 3-gen pedigree, surface×distance grid,
//          sire-performance rows, progeny, and vote history, in one endpoint.
// KEY DEPENDENCIES: AppDbContext, SirePerformanceService (DistanceBucket).
// CAUTION: Bridges the two ID spaces — a passed HansyokuNum is resolved to its KettoNum
//          runner by NameJa; the dam bridge is cross-verified via the BMS.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;
using UMAnager.Nexus.Services;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/horses")]
public sealed class HorsesController : ControllerBase
{
    private static readonly IReadOnlyDictionary<string, string> TrackNames =
        new Dictionary<string, string>
        {
            ["01"] = "Sapporo",  ["02"] = "Hakodate", ["03"] = "Fukushima", ["04"] = "Niigata",
            ["05"] = "Tokyo",    ["06"] = "Nakayama", ["07"] = "Chukyo",    ["08"] = "Kyoto",
            ["09"] = "Hanshin",  ["10"] = "Kokura",
        };

    private static string TrackName(string? code)
    {
        if (string.IsNullOrWhiteSpace(code)) return "";
        var padded = code.Trim().PadLeft(2, '0');
        return TrackNames.TryGetValue(padded, out var name) ? name : padded;
    }

    private static string CircledRank(int? rank) =>
        rank is >= 1 and <= 18 ? ((char)(0x245F + rank.Value)).ToString() : "";

    private readonly IDbContextFactory<AppDbContext> _dbFactory;

    public HorsesController(IDbContextFactory<AppDbContext> dbFactory) => _dbFactory = dbFactory;

    // Watchlist/Bloodlines add dropdown searches `searchableHorses`, which is
    // only the entries on currently loaded race cards. Horses not entered this
    // week (e.g. Lebensstil between starts) never appear. This endpoint queries
    // the full horses + breeding_horses tables so the dropdown can find them.
    // type=racing  → horses only (active runners, KettoNum IDs)
    // type=breeding → breeding_horses only (sires/dams, HansyokuNum IDs)
    // default       → both tables merged (legacy behaviour)
    [HttpGet("search")]
    public async Task<IActionResult> Search([FromQuery] string q, [FromQuery] string? type, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(q) || q.Trim().Length < 2)
            return Ok(new { results = Array.Empty<object>() });

        var query = q.Trim();
        var like = $"%{query}%";
        var idPrefix = $"{query}%";

        using var db = await _dbFactory.CreateDbContextAsync(ct);

        var includeRacing  = type is null || type == "racing";
        var includeBreeding = type is null || type == "breeding";

        var horses = includeRacing
            ? await db.Horses.AsNoTracking()
                .Where(h => EF.Functions.ILike(h.NameEn ?? "", like)
                         || EF.Functions.Like(h.NameJa, like)
                         || EF.Functions.Like(h.HorseId, idPrefix))
                .OrderByDescending(h => h.BirthYear)
                .Take(20)
                .Select(h => new { id = h.HorseId, name = h.NameEn ?? h.NameJa, name_ja = h.NameJa, birth_year = h.BirthYear, source = "horse" })
                .ToListAsync(ct)
            : [];

        var breeders = includeBreeding
            ? await db.BreedingHorses.AsNoTracking()
                .Where(b => EF.Functions.ILike(b.NameEn ?? "", like)
                         || EF.Functions.Like(b.NameJa, like)
                         || EF.Functions.Like(b.HansyokuNum, idPrefix))
                .Take(20)
                .Select(b => new { id = b.HansyokuNum, name = b.NameEn ?? b.NameJa, name_ja = b.NameJa, birth_year = (int?)null, source = "breeder" })
                .ToListAsync(ct)
            : [];

        var combined = horses.Concat(breeders)
            .GroupBy(x => x.id)
            .Select(g => g.First())
            .Take(20)
            .ToList();

        return Ok(new { results = combined });
    }

    /// <summary>
    /// Phase 31: Full horse profile — hero stats, career history, 3-gen pedigree,
    /// surface×distance grid, sire-performance rows, and vote history.
    /// Single endpoint so the Horse tab opens with one request.
    /// </summary>
    [HttpGet("{id}/profile")]
    public async Task<IActionResult> Profile(string id, CancellationToken ct)
    {
        using var db = await _dbFactory.CreateDbContextAsync(ct);

        var horse = await db.Horses.AsNoTracking()
            .FirstOrDefaultAsync(h => h.HorseId == id, ct);

        // The id may be a HansyokuNum (breeding ID space) rather than a KettoNum — e.g. a
        // sire/dam name clicked from a race card, where Sire_ID/Dam_ID are HansyokuNums.
        // Bridge to the JRA-runner KettoNum by NameJa (same heuristic as the pedigree
        // links), then continue as if that runner had been requested.
        if (horse == null)
        {
            var breed = await db.BreedingHorses.AsNoTracking()
                .FirstOrDefaultAsync(b => b.HansyokuNum == id, ct);
            if (breed != null && !string.IsNullOrEmpty(breed.NameJa))
            {
                horse = await db.Horses.AsNoTracking()
                    .Where(h => h.NameJa == breed.NameJa && h.SireId != null && h.SireId != "")
                    .OrderByDescending(h => h.BirthYear)
                    .FirstOrDefaultAsync(ct)
                    ?? await db.Horses.AsNoTracking()
                        .Where(h => h.NameJa == breed.NameJa)
                        .OrderByDescending(h => h.BirthYear)
                        .FirstOrDefaultAsync(ct);
            }
        }
        if (horse == null) return NotFound(new { error = "Horse not found" });

        // From here on, use the resolved runner's KettoNum so the history/vote lookups
        // (which key on this id) line up even when the caller passed a HansyokuNum.
        id = horse.HorseId;

        // ── All race entries for this horse joined to races ───────────────────
        var historyRaw = await (
            from re in db.RaceEntries.AsNoTracking()
            join r in db.Races.AsNoTracking() on re.RaceId equals r.RaceId
            where re.HorseId == id
            orderby r.RaceDate descending, r.RaceNumber descending
            select new
            {
                re.RaceId,
                re.PostPosition,
                re.FinishPos,
                re.Odds,
                re.FavRank,
                re.JockeyName,
                r.RaceDate,
                r.SortTime,
                r.TrackCode,
                r.Distance,
                r.Surface,
                r.RaceClass,
                r.NameJa,
                r.RaceNumber
            }
        ).ToListAsync(ct);

        // ── Career stats (only runs with a real finish position) ──────────────
        var finished = historyRaw.Where(e => e.FinishPos is > 0).ToList();
        int careerStarts = finished.Count;
        int careerWins   = finished.Count(e => e.FinishPos == 1);
        int careerPlaces = finished.Count(e => e.FinishPos == 2);
        int careerShows  = finished.Count(e => e.FinishPos is >= 1 and <= 3);

        // ── Last-5 form strip ─────────────────────────────────────────────────
        var last5Parts = finished.Take(5)
            .Select(e => e.FinishPos!.Value.ToString() + CircledRank(e.FavRank))
            .ToList();
        var last5Form = last5Parts.Count > 0 ? string.Join("-", last5Parts) : "—";

        // ── Surface × distance grid ───────────────────────────────────────────
        var surfaceGrid = finished
            .Where(e => !string.IsNullOrEmpty(e.Surface) && e.Distance is > 0)
            .GroupBy(e => (Surface: e.Surface!, Bucket: SirePerformanceService.DistanceBucket(e.Distance)))
            .Select(g => (object)new
            {
                surface = g.Key.Surface,
                bucket  = g.Key.Bucket,
                starts  = g.Count(),
                wins    = g.Count(e => e.FinishPos == 1),
                places  = g.Count(e => e.FinishPos is >= 1 and <= 3)
            })
            .ToList();

        var bestGroup = finished
            .Where(e => !string.IsNullOrEmpty(e.Surface) && e.Distance is > 0)
            .GroupBy(e => (Surface: e.Surface!, Bucket: SirePerformanceService.DistanceBucket(e.Distance)))
            .Select(g => (surf: g.Key.Surface, bucket: g.Key.Bucket,
                          wins: g.Count(e => e.FinishPos == 1), starts: g.Count()))
            .OrderByDescending(g => g.wins).ThenByDescending(g => g.starts)
            .FirstOrDefault();

        // ── Upcoming entry (most-recent race not yet run) ─────────────────────
        var jstNow = DateTime.UtcNow.AddHours(9);
        var upcomingRow = historyRaw
            .Where(e => (!e.FinishPos.HasValue || e.FinishPos == 0)
                     && e.SortTime.HasValue && e.SortTime.Value > jstNow)
            .OrderBy(e => e.SortTime)
            .FirstOrDefault();

        // ── Field sizes (for history context) ────────────────────────────────
        var histRaceIds = historyRaw.Select(e => e.RaceId).Distinct().ToList();
        var fieldSizes = histRaceIds.Count > 0
            ? await db.RaceEntries.AsNoTracking()
                .Where(e => histRaceIds.Contains(e.RaceId) && e.FinishPos > 0)
                .GroupBy(e => e.RaceId)
                .Select(g => new { RaceId = g.Key, N = g.Count() })
                .ToDictionaryAsync(x => x.RaceId, x => x.N, ct)
            : new Dictionary<string, int>();

        // ── Pedigree resolution (3 generations) ──────────────────────────────
        // The hard part: a runner's SireId/DamId/BmsId are HansyokuNums (breeding ID
        // space), which never match the horses table's KettoNum PK. So the sire/dam
        // resolve to a NAME via breeding_horses, but to get the GRANDPARENTS we must
        // find the sire/dam again as JRA runners (in horses, by name) and read THEIR
        // SireId/DamId. We bridge by NameJa, preferring a row that actually carries
        // pedigree (SireId not null), and cross-verify the dam via the BMS:
        //   dam's sire ≡ this horse's BMS, so a runner-dam whose SireId == BmsId is
        //   the right mare even when the name is shared by multiple horses.

        // Names of the gen-2 ancestors (and BMS) from breeding_horses.
        var gen2BreedIds = new List<string>();
        void AddBreedId(string? v) { if (!string.IsNullOrEmpty(v)) gen2BreedIds.Add(v); }
        AddBreedId(horse.SireId); AddBreedId(horse.DamId); AddBreedId(horse.BmsId);
        var breedNames = gen2BreedIds.Count > 0
            ? await db.BreedingHorses.AsNoTracking()
                .Where(b => gen2BreedIds.Contains(b.HansyokuNum))
                .ToDictionaryAsync(b => b.HansyokuNum,
                    b => (NameJa: b.NameJa, NameEn: b.NameEn), ct)
            : new Dictionary<string, (string NameJa, string? NameEn)>();

        breedNames.TryGetValue(horse.SireId ?? "", out var sireBreed);
        breedNames.TryGetValue(horse.DamId  ?? "", out var damBreed);

        // Bridge sire → runner row (by NameJa, must carry pedigree). The sire is a
        // stallion; same-name collisions are rare, so first-with-pedigree is safe.
        Data.Entities.Horse? sireRunner = null;
        if (!string.IsNullOrEmpty(sireBreed.NameJa))
            sireRunner = await db.Horses.AsNoTracking()
                .Where(h => h.NameJa == sireBreed.NameJa && h.SireId != null && h.SireId != "")
                .OrderByDescending(h => h.BirthYear)
                .FirstOrDefaultAsync(ct);

        // Bridge dam → runner row, cross-verified by BMS (dam.SireId == horse.BmsId).
        // Falls back to plain name+pedigree match if the verified lookup misses.
        Data.Entities.Horse? damRunner = null;
        if (!string.IsNullOrEmpty(damBreed.NameJa))
        {
            if (!string.IsNullOrEmpty(horse.BmsId))
                damRunner = await db.Horses.AsNoTracking()
                    .Where(h => h.NameJa == damBreed.NameJa && h.SireId == horse.BmsId)
                    .FirstOrDefaultAsync(ct);
            damRunner ??= await db.Horses.AsNoTracking()
                .Where(h => h.NameJa == damBreed.NameJa && h.SireId != null && h.SireId != "")
                .OrderByDescending(h => h.BirthYear)
                .FirstOrDefaultAsync(ct);
        }

        // Gen-3 ancestor IDs (HansyokuNums). Maternal grandsire is the BMS directly.
        string? ssId = sireRunner?.SireId;          // paternal grandsire
        string? sdId = sireRunner?.DamId;           // paternal granddam
        string? dsId = horse.BmsId;                 // maternal grandsire (authoritative)
        string? ddId = damRunner?.DamId;            // maternal granddam

        // Resolve all ancestor IDs to names. Most are HansyokuNums → breeding_horses;
        // a few may themselves be KettoNum runners → horses.
        var allAncIds = new HashSet<string>(StringComparer.Ordinal);
        void AddId(string? v) { if (!string.IsNullOrEmpty(v)) allAncIds.Add(v); }
        AddId(horse.SireId); AddId(horse.DamId); AddId(horse.BmsId);
        AddId(ssId); AddId(sdId); AddId(dsId); AddId(ddId);
        var ancList = allAncIds.ToList();

        var breedingAncs = ancList.Count > 0
            ? await db.BreedingHorses.AsNoTracking()
                .Where(b => ancList.Contains(b.HansyokuNum))
                .ToDictionaryAsync(b => b.HansyokuNum,
                    b => (NameJa: b.NameJa, NameEn: b.NameEn), ct)
            : new Dictionary<string, (string NameJa, string? NameEn)>();
        var horseAncs = ancList.Count > 0
            ? await db.Horses.AsNoTracking()
                .Where(h => ancList.Contains(h.HorseId))
                .ToDictionaryAsync(h => h.HorseId, h => (NameJa: h.NameJa, NameEn: h.NameEn), ct)
            : new Dictionary<string, (string NameJa, string? NameEn)>();

        string ResolveAnc(string? ancId)
        {
            if (string.IsNullOrEmpty(ancId)) return "";
            if (horseAncs.TryGetValue(ancId, out var h))
                return !string.IsNullOrEmpty(h.NameEn) ? h.NameEn : h.NameJa;
            if (breedingAncs.TryGetValue(ancId, out var b))
                return !string.IsNullOrEmpty(b.NameEn) ? b.NameEn : b.NameJa;
            return "";
        }

        // The pedigree boxes link to ancestors' own profiles, but a slot's ID is a
        // HansyokuNum (breeding space) — the /profile endpoint keys on KettoNum. Bridge
        // each ancestor to its JRA-runner KettoNum by NameJa so the box becomes clickable
        // (only when a runner row exists; foreign-only ancestors stay non-clickable).
        string AncNameJa(string? id)
        {
            if (string.IsNullOrEmpty(id)) return "";
            if (horseAncs.TryGetValue(id, out var h)) return h.NameJa;
            if (breedingAncs.TryGetValue(id, out var b)) return b.NameJa;
            return "";
        }
        var ancNames = new[] { horse.SireId, horse.DamId, ssId, sdId, dsId, ddId }
            .Select(AncNameJa).Where(s => !string.IsNullOrEmpty(s)).Distinct().ToList();
        var runnerRows = ancNames.Count > 0
            ? await db.Horses.AsNoTracking()
                .Where(h => ancNames.Contains(h.NameJa))
                .Select(h => new { h.HorseId, h.NameJa, h.SireId, h.BirthYear })
                .ToListAsync(ct)
            : new();
        // Prefer a runner that carries pedigree, then the most recent — same heuristic
        // as the sire/dam bridge above.
        var runnerByName = runnerRows
            .GroupBy(r => r.NameJa)
            .ToDictionary(g => g.Key,
                g => g.OrderByDescending(x => !string.IsNullOrEmpty(x.SireId))
                      .ThenByDescending(x => x.BirthYear)
                      .First().HorseId);
        string RunnerId(string? ancId)
        {
            var nm = AncNameJa(ancId);
            return !string.IsNullOrEmpty(nm) && runnerByName.TryGetValue(nm, out var rid) ? rid : "";
        }
        // sire/dam have authoritative bridges (dam cross-verified by BMS); prefer those.
        var sireRunnerId = sireRunner?.HorseId ?? RunnerId(horse.SireId);
        var damRunnerId  = damRunner?.HorseId  ?? RunnerId(horse.DamId);

        // ── Progeny ───────────────────────────────────────────────────────────
        // Offspring reference this horse by its HansyokuNum (breeding ID), which the
        // horses table doesn't store for the horse itself. Bridge by NameJa to find
        // this horse's HansyokuNum(s), then pull horses whose Sire/Dam points to them.
        var selfHansyoku = await db.BreedingHorses.AsNoTracking()
            .Where(b => b.NameJa == horse.NameJa)
            .Select(b => b.HansyokuNum)
            .ToListAsync(ct);
        List<object> progenyList = [];
        int progenyTotal = 0;
        if (selfHansyoku.Count > 0)
        {
            progenyTotal = await db.Horses.AsNoTracking().CountAsync(
                h => (h.SireId != null && selfHansyoku.Contains(h.SireId))
                  || (h.DamId  != null && selfHansyoku.Contains(h.DamId)), ct);
            var kids = await db.Horses.AsNoTracking()
                .Where(h => (h.SireId != null && selfHansyoku.Contains(h.SireId))
                         || (h.DamId  != null && selfHansyoku.Contains(h.DamId)))
                .OrderByDescending(h => h.BirthYear)
                .Take(60)
                .Select(h => new { h.HorseId, h.NameEn, h.NameJa, h.BirthYear, h.SireId })
                .ToListAsync(ct);
            progenyList = kids.Select(k => (object)new
            {
                horse_id   = k.HorseId,
                name       = k.NameEn ?? k.NameJa,
                birth_year = k.BirthYear,
                role       = (k.SireId != null && selfHansyoku.Contains(k.SireId)) ? "sire" : "dam"
            }).ToList();
        }

        // ── Sire performance (all surface/bucket combos for this horse's sire) ─
        List<object> sirePerfList;
        if (!string.IsNullOrEmpty(horse.SireId))
        {
            var spRows = await db.Database
                .SqlQueryRaw<SirePerformanceService.SirePerfRow>(
                    "SELECT sire_id AS \"SireId\", surface AS \"Surface\", " +
                    "distance_bucket AS \"Bucket\", starts AS \"Starts\", wins AS \"Wins\", " +
                    "places AS \"Places\", win_pct AS \"WinPct\", place_pct AS \"PlacePct\" " +
                    "FROM sire_performance WHERE sire_id = {0}", horse.SireId)
                .ToListAsync(ct);
            sirePerfList = spRows.Select(r => (object)new
            {
                surface   = r.Surface,
                bucket    = r.Bucket,
                win_pct   = r.WinPct,
                place_pct = r.PlacePct,
                starts    = r.Starts,
                wins      = r.Wins
            }).ToList();
        }
        else sirePerfList = [];

        // ── Vote history for this horse ───────────────────────────────────────
        var voteRows = await db.VoteHistory.AsNoTracking()
            .Where(v => v.HorseId == id)
            .OrderByDescending(v => v.VotedAt)
            .ToListAsync(ct);

        var votedRaceIds = voteRows.Select(v => v.RaceId).Distinct().ToList();
        Dictionary<string, (DateTime Date, string? TrackCode, string? NameJa)> votedRaceMap;
        if (votedRaceIds.Count > 0)
        {
            var raceInfo = await db.Races.AsNoTracking()
                .Where(r => votedRaceIds.Contains(r.RaceId))
                .Select(r => new { r.RaceId, r.RaceDate, r.TrackCode, r.NameJa })
                .ToListAsync(ct);
            votedRaceMap = raceInfo.ToDictionary(r => r.RaceId,
                r => (r.RaceDate, r.TrackCode, r.NameJa));
        }
        else votedRaceMap = new();

        var voteHistory = voteRows.Select(v =>
        {
            votedRaceMap.TryGetValue(v.RaceId, out var ri);
            return (object)new
            {
                race_id    = v.RaceId,
                race_date  = ri.Date != default ? ri.Date.ToString("yyyy-MM-dd") : "",
                track_name = TrackName(ri.TrackCode),
                race_name  = ri.NameJa ?? "",
                mark       = v.Mark,
                voted_at   = v.VotedAt.ToString("yyyy-MM-dd")
            };
        }).ToList();

        // ── History list (all entries, client paginates to last-10) ──────────
        var historyList = historyRaw.Select(e => (object)new
        {
            race_id     = e.RaceId,
            race_date   = e.RaceDate.ToString("yyyy-MM-dd"),
            track_name  = TrackName(e.TrackCode),
            race_number = e.RaceNumber,
            distance    = e.Distance,
            surface     = e.Surface ?? "",
            race_class  = e.RaceClass ?? "",
            race_name   = e.NameJa ?? "",
            finish      = e.FinishPos,
            odds        = e.Odds.HasValue ? e.Odds.Value.ToString("F1") : (string?)null,
            fav_rank    = e.FavRank,
            jockey      = e.JockeyName ?? "",
            field_size  = fieldSizes.TryGetValue(e.RaceId, out var fs) ? fs : 0,
            is_upcoming = e.SortTime.HasValue && e.SortTime.Value > jstNow
        }).ToList();

        // ── Assemble response ─────────────────────────────────────────────────
        var response = new
        {
            horse_id     = horse.HorseId,
            name_en      = horse.NameEn ?? "",
            name_ja      = horse.NameJa ?? "",
            birth_year   = horse.BirthYear,
            sire_id      = horse.SireId ?? "",
            sire_name    = ResolveAnc(horse.SireId),
            dam_id       = horse.DamId ?? "",
            dam_name     = ResolveAnc(horse.DamId),
            bms_id       = horse.BmsId ?? "",
            bms_name     = ResolveAnc(horse.BmsId),
            career_starts = careerStarts,
            career_wins   = careerWins,
            career_places = careerPlaces,
            career_shows  = careerShows,
            last5_form    = last5Form,
            best_surface  = bestGroup.surf ?? "",
            best_bucket   = bestGroup.bucket ?? "",
            current_odds    = upcomingRow?.Odds?.ToString("F1"),
            current_fav     = upcomingRow?.FavRank,
            current_race_id = upcomingRow?.RaceId,
            pedigree = new
            {
                sire_id        = horse.SireId ?? "",
                sire_name      = ResolveAnc(horse.SireId),
                sire_runner_id = sireRunnerId,
                sire_sire_id   = ssId ?? "",
                sire_sire_name = ResolveAnc(ssId),
                sire_sire_runner_id = RunnerId(ssId),
                sire_dam_id    = sdId ?? "",
                sire_dam_name  = ResolveAnc(sdId),
                sire_dam_runner_id = RunnerId(sdId),
                dam_id         = horse.DamId ?? "",
                dam_name       = ResolveAnc(horse.DamId),
                dam_runner_id  = damRunnerId,
                dam_sire_id    = dsId ?? "",
                dam_sire_name  = ResolveAnc(dsId),
                dam_sire_runner_id = RunnerId(dsId),
                dam_dam_id     = ddId ?? "",
                dam_dam_name   = ResolveAnc(ddId),
                dam_dam_runner_id = RunnerId(ddId)
            },
            surface_grid  = surfaceGrid,
            sire_perf     = sirePerfList,
            vote_history  = voteHistory,
            progeny       = progenyList,
            progeny_total = progenyTotal,
            history       = historyList
        };

        return Ok(response);
    }
}
