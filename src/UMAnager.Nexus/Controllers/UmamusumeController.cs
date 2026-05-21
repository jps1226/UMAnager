using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Controllers;

/// <summary>
/// "Uma Musume mode" — a fun toggle that auto-adds every Uma Musume Pretty Derby
/// character's real-horse counterpart to the Bloodlines list. The roster lives
/// in Resources/umamusume_characters.json; toggling ON resolves each name against
/// breeding_horses (preferred — HansyokuNum is the bloodline ID) with a horses
/// fallback (KettoNum). Characters flagged still_racing are skipped (they belong
/// in Watchlist, not Bloodlines). Toggle OFF removes only IDs the mode added,
/// preserving anything the operator manually curated.
/// </summary>
[ApiController]
[Route("api/umamusume")]
public sealed class UmamusumeController : ControllerBase
{
    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly IWebHostEnvironment _env;

    public UmamusumeController(IDbContextFactory<AppDbContext> dbFactory, IWebHostEnvironment env)
    {
        _dbFactory = dbFactory;
        _env = env;
    }

    private sealed record Character(string name_en, string name_ja, bool still_racing, string[]? aliases);
    private sealed record CharactersFile(int version, string last_curated, Character[] characters);

    public sealed record ApplyRequest(bool enabled);
    public sealed record ResolvedChar(string name_en, string? matched_id, string? matched_source, bool still_racing);

    [HttpGet("characters")]
    public async Task<IActionResult> List(CancellationToken ct)
    {
        var file = LoadCharacterFile();
        if (file is null) return Problem("umamusume_characters.json missing from Resources folder");

        var resolved = await ResolveAllAsync(file.characters, ct);

        // Mode is "on" if we previously added anything and didn't clear it.
        using var db = await _dbFactory.CreateDbContextAsync(ct);
        var addedJson = await GetAppStateAsync(db, "umamusume_added_ids", ct);
        int addedCount = 0;
        if (!string.IsNullOrWhiteSpace(addedJson))
        {
            try { addedCount = JsonSerializer.Deserialize<List<string>>(addedJson)?.Count ?? 0; }
            catch { /* corrupt state — treat as off */ }
        }

        return Ok(new
        {
            version = file.version,
            last_curated = file.last_curated,
            total = file.characters.Length,
            still_racing = file.characters.Count(c => c.still_racing),
            resolved = resolved.Count(r => r.matched_id != null),
            enabled = addedCount > 0,
            added_count = addedCount,
            characters = resolved
        });
    }

    [HttpPost("apply")]
    public async Task<IActionResult> Apply([FromBody] ApplyRequest req, CancellationToken ct)
    {
        var file = LoadCharacterFile();
        if (file is null) return Problem("umamusume_characters.json missing from Resources folder");

        using var db = await _dbFactory.CreateDbContextAsync(ct);
        using var tx = await db.Database.BeginTransactionAsync(ct);

        if (req.enabled)
        {
            // Resolve every non-still-racing character to a breeding_horses or
            // horses ID. Skip still_racing characters (they belong in Watchlist).
            var resolved = await ResolveAllAsync(file.characters, ct);
            var ids = resolved
                .Where(r => !r.still_racing && !string.IsNullOrEmpty(r.matched_id))
                .Select(r => r.matched_id!)
                .Distinct()
                .ToList();

            // De-dupe against whatever's already in bloodlines so we only record
            // the IDs we actually inserted — so toggle-OFF can remove just ours.
            var existing = await db.Database
                .SqlQueryRaw<string>("SELECT horse_id AS \"Value\" FROM user_horse_lists WHERE list_type = 'bloodlines' AND horse_id IS NOT NULL")
                .ToListAsync(ct);
            var existingSet = new HashSet<string>(existing, StringComparer.Ordinal);
            var newIds = ids.Where(id => !existingSet.Contains(id)).ToList();

            foreach (var id in newIds)
            {
                await db.Database.ExecuteSqlRawAsync(
                    "INSERT INTO user_horse_lists (list_type, horse_id) VALUES ('bloodlines', {0}) ON CONFLICT DO NOTHING",
                    new object[] { id }, ct);
            }

            // Record exactly which IDs we just added so a future toggle-OFF only
            // removes those. Anything in `existing` was operator-curated; leave alone.
            await SetAppStateAsync(db, "umamusume_added_ids", JsonSerializer.Serialize(newIds), ct);

            await tx.CommitAsync(ct);
            return Ok(new
            {
                status = "enabled",
                resolved_total = ids.Count,
                inserted = newIds.Count,
                already_present = ids.Count - newIds.Count,
                unresolved = resolved.Count(r => !r.still_racing && string.IsNullOrEmpty(r.matched_id)),
                still_racing = resolved.Count(r => r.still_racing),
                unresolved_names = resolved.Where(r => !r.still_racing && string.IsNullOrEmpty(r.matched_id)).Select(r => r.name_en).ToList()
            });
        }
        else
        {
            // Remove only what we added previously.
            var addedJson = await GetAppStateAsync(db, "umamusume_added_ids", ct);
            var addedIds = string.IsNullOrWhiteSpace(addedJson)
                ? new List<string>()
                : (JsonSerializer.Deserialize<List<string>>(addedJson) ?? new List<string>());

            int removed = 0;
            foreach (var id in addedIds)
            {
                removed += await db.Database.ExecuteSqlRawAsync(
                    "DELETE FROM user_horse_lists WHERE list_type = 'bloodlines' AND horse_id = {0}",
                    new object[] { id }, ct);
            }

            await SetAppStateAsync(db, "umamusume_added_ids", "[]", ct);
            await tx.CommitAsync(ct);
            return Ok(new { status = "disabled", removed });
        }
    }

    private CharactersFile? LoadCharacterFile()
    {
        // Try ContentRoot/Resources first (where the build copies it), then fall
        // back to walking up from CWD for `dotnet run` from src/UMAnager.Nexus.
        var candidates = new[]
        {
            Path.Combine(_env.ContentRootPath, "Resources", "umamusume_characters.json"),
            Path.Combine(AppContext.BaseDirectory, "Resources", "umamusume_characters.json"),
        };
        var path = candidates.FirstOrDefault(System.IO.File.Exists);
        if (path is null) return null;

        var json = System.IO.File.ReadAllText(path);
        return JsonSerializer.Deserialize<CharactersFile>(json, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        });
    }

    private async Task<List<ResolvedChar>> ResolveAllAsync(Character[] chars, CancellationToken ct)
    {
        using var db = await _dbFactory.CreateDbContextAsync(ct);

        // Pre-load all candidate names from both tables. Tens of thousands of
        // rows max — cheaper than a per-char round-trip.
        var breeders = await db.BreedingHorses.AsNoTracking()
            .Select(b => new { b.HansyokuNum, b.NameJa, b.NameEn })
            .ToListAsync(ct);
        var horses = await db.Horses.AsNoTracking()
            .Select(h => new { h.HorseId, h.NameJa, h.NameEn })
            .ToListAsync(ct);

        var breederByEn = breeders.Where(b => !string.IsNullOrEmpty(b.NameEn))
            .GroupBy(b => b.NameEn!.Trim(), StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g => g.First().HansyokuNum, StringComparer.OrdinalIgnoreCase);
        var breederByJa = breeders.Where(b => !string.IsNullOrEmpty(b.NameJa))
            .GroupBy(b => b.NameJa.Trim(), StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First().HansyokuNum, StringComparer.Ordinal);
        var horseByEn = horses.Where(h => !string.IsNullOrEmpty(h.NameEn))
            .GroupBy(h => h.NameEn!.Trim(), StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g => g.First().HorseId, StringComparer.OrdinalIgnoreCase);
        var horseByJa = horses.Where(h => !string.IsNullOrEmpty(h.NameJa))
            .GroupBy(h => h.NameJa.Trim(), StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First().HorseId, StringComparer.Ordinal);

        var results = new List<ResolvedChar>(chars.Length);
        foreach (var c in chars)
        {
            string? id = null;
            string? source = null;

            // Try every spelling (canonical + aliases) against breeders first,
            // then horses. Breeder wins because HansyokuNum is what the pedigree
            // highlight system keys on for sires/dams/BMS.
            foreach (var name in EnumerateNames(c))
            {
                if (breederByEn.TryGetValue(name, out var hid)) { id = hid; source = "breeding"; break; }
                if (breederByJa.TryGetValue(name, out hid))     { id = hid; source = "breeding"; break; }
                if (horseByEn.TryGetValue(name, out hid))       { id = hid; source = "horse";    break; }
                if (horseByJa.TryGetValue(name, out hid))       { id = hid; source = "horse";    break; }
            }

            results.Add(new ResolvedChar(c.name_en, id, source, c.still_racing));
        }
        return results;
    }

    private static IEnumerable<string> EnumerateNames(Character c)
    {
        if (!string.IsNullOrWhiteSpace(c.name_en)) yield return c.name_en.Trim();
        if (!string.IsNullOrWhiteSpace(c.name_ja)) yield return c.name_ja.Trim();
        if (c.aliases is not null)
            foreach (var a in c.aliases) if (!string.IsNullOrWhiteSpace(a)) yield return a.Trim();
    }

    private static async Task<string?> GetAppStateAsync(AppDbContext db, string key, CancellationToken ct)
    {
        // app_state columns are quoted PascalCase ("Key"/"Value") because EF
        // maps property names verbatim to Postgres identifiers.
        var rows = await db.Database
            .SqlQueryRaw<string>("SELECT \"Value\" FROM app_state WHERE \"Key\" = {0}", key)
            .ToListAsync(ct);
        return rows.FirstOrDefault();
    }

    private static async Task SetAppStateAsync(AppDbContext db, string key, string value, CancellationToken ct)
    {
        await db.Database.ExecuteSqlRawAsync(
            "INSERT INTO app_state (\"Key\", \"Value\", \"UpdatedAt\") VALUES ({0}, {1}, now()) " +
            "ON CONFLICT (\"Key\") DO UPDATE SET \"Value\" = EXCLUDED.\"Value\", \"UpdatedAt\" = now()",
            new object[] { key, value }, ct);
    }
}
