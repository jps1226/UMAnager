using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/horses")]
public sealed class HorsesController : ControllerBase
{
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
}
