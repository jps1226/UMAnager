// ============================================================
// FILE: ListsController.cs
// LAYER: API (api/lists)
// PURPOSE: Persists the operator's Bloodlines (breeding horses) + Watchlist (active runners)
//          to user_horse_lists (raw SQL). Stores only IDs on the wire; rejoins names from
//          horses + breeding_horses on GET (format "id#name").
// KEY DEPENDENCIES: AppDbContext (raw SQL — user_horse_lists predates the EF schema).
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Controllers;

/// <summary>
/// Persists the operator's Bloodlines (breeding-horse tracking, sires/dams/BMS)
/// and Watchlist (active runners) lists to user_horse_lists
/// (snake-case, raw SQL — table predates the EF schema). Frontend stores them as
/// newline-separated strings of horse IDs (with optional space-separated metadata
/// after the ID); we keep that shape on the wire and only persist the ID column.
/// Replaced the empty stub on 2026-05-18.
/// </summary>
[ApiController]
[Route("api/lists")]
public sealed class ListsController : ControllerBase
{
    private readonly IDbContextFactory<AppDbContext> _dbFactory;

    public ListsController(IDbContextFactory<AppDbContext> dbFactory) => _dbFactory = dbFactory;

    public sealed record ListsPayload(string? bloodlines, string? watchlist);

    [HttpGet]
    public async Task<IActionResult> Get(CancellationToken ct)
    {
        using var db = await _dbFactory.CreateDbContextAsync(ct);
        var rows = await db.Database
            .SqlQueryRaw<ListRow>("SELECT horse_id AS \"HorseId\", list_type AS \"ListType\" FROM user_horse_lists WHERE horse_id IS NOT NULL")
            .ToListAsync(ct);

        // Resolve names from BOTH horses (KettoNum, runners) and breeding_horses
        // (HansyokuNum, sires/dams). Frontend writes "id#name"; we rebuild that
        // format on GET so the sidebar shows names even after a fresh page load.
        var ids = rows.Select(r => r.HorseId).Where(s => !string.IsNullOrEmpty(s)).Distinct().ToList();
        var horseNames = await db.Horses.AsNoTracking()
            .Where(h => ids.Contains(h.HorseId))
            .Select(h => new { h.HorseId, Name = h.NameEn ?? h.NameJa })
            .ToDictionaryAsync(x => x.HorseId, x => x.Name ?? "", ct);
        var breedingNames = await db.BreedingHorses.AsNoTracking()
            .Where(b => ids.Contains(b.HansyokuNum))
            .Select(b => new { b.HansyokuNum, Name = b.NameEn ?? b.NameJa })
            .ToDictionaryAsync(x => x.HansyokuNum, x => x.Name ?? "", ct);

        string Format(string id)
        {
            var name = "";
            if (horseNames.TryGetValue(id, out var n1) && !string.IsNullOrEmpty(n1)) name = n1;
            else if (breedingNames.TryGetValue(id, out var n2) && !string.IsNullOrEmpty(n2)) name = n2;
            return string.IsNullOrEmpty(name) ? id : $"{id}#{name}";
        }

        var bloodlines = string.Join("\n", rows.Where(r => r.ListType == "bloodlines").Select(r => Format(r.HorseId)));
        var watchlist = string.Join("\n", rows.Where(r => r.ListType == "watchlist").Select(r => Format(r.HorseId)));
        return Ok(new { bloodlines, watchlist });
    }

    [HttpPost]
    public async Task<IActionResult> Post([FromBody] ListsPayload payload, CancellationToken ct)
    {
        var bld = ParseLines(payload?.bloodlines);
        var wat = ParseLines(payload?.watchlist);

        using var db = await _dbFactory.CreateDbContextAsync(ct);
        using var tx = await db.Database.BeginTransactionAsync(ct);

        await SyncListAsync(db, "bloodlines", bld, ct);
        await SyncListAsync(db, "watchlist", wat, ct);

        await tx.CommitAsync(ct);
        return Ok(new { status = "ok", bloodlines = bld.Count, watchlist = wat.Count });
    }

    private static List<string> ParseLines(string? text)
    {
        if (string.IsNullOrWhiteSpace(text)) return new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<string>();
        foreach (var raw in text.Split('\n'))
        {
            var line = raw.Trim();
            if (line.Length == 0) continue;
            // Accept "id", "id#name", or "id name" — strip everything after the
            // first separator. user_horse_lists only stores the id; the GET path
            // re-joins names from horses/breeding_horses.
            var firstSep = line.IndexOfAny(new[] { '#', ' ', '\t' });
            var id = firstSep > 0 ? line.Substring(0, firstSep) : line;
            if (id.Length == 0 || !seen.Add(id)) continue;
            result.Add(id);
        }
        return result;
    }

    private static async Task SyncListAsync(AppDbContext db, string listType, List<string> ids, CancellationToken ct)
    {
        // Wipe + bulk insert. user_horse_lists is tiny (tens of rows), so the
        // simpler "DELETE WHERE list_type = X; INSERT all current" pattern is fine
        // and avoids diff logic. UNIQUE (horse_id, list_type) guards duplicates.
        await db.Database.ExecuteSqlRawAsync(
            "DELETE FROM user_horse_lists WHERE list_type = @p0",
            new NpgsqlParameter("p0", NpgsqlDbType.Text) { Value = listType });

        if (ids.Count == 0) return;

        var values = new List<string>(ids.Count);
        var pars = new List<NpgsqlParameter>(ids.Count + 1);
        pars.Add(new NpgsqlParameter("plt", NpgsqlDbType.Text) { Value = listType });
        for (int i = 0; i < ids.Count; i++)
        {
            values.Add($"(@p{i}, @plt, NOW())");
            pars.Add(new NpgsqlParameter($"p{i}", NpgsqlDbType.Text) { Value = ids[i] });
        }

        var sql = "INSERT INTO user_horse_lists (horse_id, list_type, created_at) VALUES " +
                  string.Join(", ", values) +
                  " ON CONFLICT (horse_id, list_type) DO NOTHING";
        await db.Database.ExecuteSqlRawAsync(sql, pars.ToArray(), ct);
    }

    public sealed record ListRow(string HorseId, string ListType);
}
