using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Controllers;

/// <summary>
/// Persists the operator's Favorites and Watchlist lists to user_horse_lists
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

    public sealed record ListsPayload(string? favorites, string? watchlist);

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

        var favorites = string.Join("\n", rows.Where(r => r.ListType == "favorites").Select(r => Format(r.HorseId)));
        var watchlist = string.Join("\n", rows.Where(r => r.ListType == "watchlist").Select(r => Format(r.HorseId)));
        return Ok(new { favorites, watchlist });
    }

    [HttpPost]
    public async Task<IActionResult> Post([FromBody] ListsPayload payload, CancellationToken ct)
    {
        var fav = ParseLines(payload?.favorites);
        var wat = ParseLines(payload?.watchlist);

        using var db = await _dbFactory.CreateDbContextAsync(ct);
        using var tx = await db.Database.BeginTransactionAsync(ct);

        await SyncListAsync(db, "favorites", fav, ct);
        await SyncListAsync(db, "watchlist", wat, ct);

        await tx.CommitAsync(ct);
        return Ok(new { status = "ok", favorites = fav.Count, watchlist = wat.Count });
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
