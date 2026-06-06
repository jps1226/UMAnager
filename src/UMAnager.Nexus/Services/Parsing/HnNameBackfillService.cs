// ============================================================
// FILE: HnNameBackfillService.cs
// LAYER: Parsing (one-shot backfill, idempotent)
// PURPOSE: Scans raw_staging HN records (BLDN) and UPDATEs breeding_horses.NameEn with the romaji
//          name. Enriches only — never inserts rows (BreedingHorseBackfillService owns creation).
// KEY DEPENDENCIES: AppDbContext, HnRecordParser.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services.Parsing;

/// <summary>
/// Scans raw_staging for HN records (from BLDN ingest), parses out the romaji name,
/// and UPDATEs breeding_horses.NameEn. Idempotent — re-runnable after each BLDN pull.
///
/// Does NOT insert new breeding_horses rows. UM-driven BreedingHorseBackfillService is
/// the authoritative source of row creation; this service only enriches existing rows.
/// </summary>
public sealed class HnNameBackfillService
{
    private const int BatchRecords = 2000;

    private readonly IDbContextFactory<AppDbContext> _contextFactory;
    private readonly ILogger<HnNameBackfillService> _logger;

    public HnNameBackfillService(
        IDbContextFactory<AppDbContext> contextFactory,
        ILogger<HnNameBackfillService> logger)
    {
        _contextFactory = contextFactory;
        _logger = logger;
    }

    public async Task<(int Scanned, int Updated)> BackfillAsync(CancellationToken ct = default)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        long lastId = 0;
        int totalScanned = 0;
        int totalUpdated = 0;

        while (!ct.IsCancellationRequested)
        {
            using var ctx = _contextFactory.CreateDbContext();
            var batch = await ctx.RawStagingRecords.AsNoTracking()
                .Where(r => r.RecordType == "HN" && r.Id > lastId)
                .OrderBy(r => r.Id)
                .Take(BatchRecords)
                .Select(r => new { r.Id, r.RawBytes })
                .ToListAsync(ct);

            if (batch.Count == 0) break;

            var bag = new Dictionary<string, string>(capacity: batch.Count);
            foreach (var row in batch)
            {
                totalScanned++;
                var parsed = HnRecordParser.Parse(row.RawBytes);
                if (parsed == null || parsed.NameEn == null) continue;
                bag[parsed.HansyokuNum] = parsed.NameEn; // last-wins per batch
            }

            if (bag.Count > 0)
                totalUpdated += await BulkUpdateAsync(ctx, bag, ct);

            lastId = batch[^1].Id;
            _logger.LogInformation(
                "HN name backfill: scanned={Scanned}, updated={Updated}, cursor={Cursor}",
                totalScanned, totalUpdated, lastId);
        }

        sw.Stop();
        _logger.LogInformation(
            "HN name backfill complete: scanned={Scanned}, updated={Updated}, duration={Ms}ms",
            totalScanned, totalUpdated, sw.ElapsedMilliseconds);

        return (totalScanned, totalUpdated);
    }

    private static async Task<int> BulkUpdateAsync(
        AppDbContext ctx, Dictionary<string, string> bag, CancellationToken ct)
    {
        // Bulk update via VALUES join — one round-trip per chunk regardless of row count.
        const int ChunkRows = 5000;
        int total = 0;

        var items = bag.ToList();
        for (int start = 0; start < items.Count; start += ChunkRows)
        {
            var chunk = items.Skip(start).Take(ChunkRows).ToList();
            var values = new List<string>(chunk.Count);
            var parameters = new List<object>(chunk.Count * 2);
            int p = 0;

            foreach (var kv in chunk)
            {
                values.Add($"(@p{p}, @p{p+1})");
                parameters.Add(kv.Key);
                parameters.Add(kv.Value);
                p += 2;
            }

            var sql = $@"
                UPDATE breeding_horses bh
                SET ""NameEn"" = v.name_en, ""LastUpdated"" = NOW()
                FROM (VALUES {string.Join(", ", values)}) AS v(hansyoku, name_en)
                WHERE bh.""HansyokuNum"" = v.hansyoku
                  AND (bh.""NameEn"" IS NULL OR bh.""NameEn"" <> v.name_en)";

            total += await ctx.Database.ExecuteSqlRawAsync(sql, parameters.ToArray(), ct);
        }

        return total;
    }
}
