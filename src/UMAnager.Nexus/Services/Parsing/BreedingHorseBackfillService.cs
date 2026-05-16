using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System.Text;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services.Parsing;

/// <summary>
/// One-shot backfill: reads every UM record in raw_staging, extracts the 14 KETTO3_INFO
/// pedigree-ancestor slots, and upserts them into breeding_horses.
///
/// Slot layout per JV_UM_UMA: 14 slots × 46 bytes each, starting at offset 205 (1-indexed).
/// Within each slot:  HansyokuNum (offset 1, len 10), Bamei (offset 11, len 36, Shift-JIS).
/// </summary>
public sealed class BreedingHorseBackfillService
{
    private const int SlotsStart  = 204; // 0-indexed
    private const int SlotSize    = 46;
    private const int SlotCount   = 14;
    private const int BatchRecords = 2000;

    private static readonly Encoding ShiftJis = Encoding.GetEncoding(932);

    private readonly IDbContextFactory<AppDbContext> _contextFactory;
    private readonly ILogger<BreedingHorseBackfillService> _logger;

    public BreedingHorseBackfillService(
        IDbContextFactory<AppDbContext> contextFactory,
        ILogger<BreedingHorseBackfillService> logger)
    {
        _contextFactory = contextFactory;
        _logger = logger;
    }

    public async Task<(int Scanned, int Upserted)> BackfillAsync(CancellationToken ct = default)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        long lastId = 0;
        int totalScanned = 0;
        int totalUpserted = 0;

        while (!ct.IsCancellationRequested)
        {
            using var ctx = _contextFactory.CreateDbContext();
            var batch = await ctx.RawStagingRecords.AsNoTracking()
                .Where(r => r.RecordType == "UM" && r.Id > lastId)
                .OrderBy(r => r.Id)
                .Take(BatchRecords)
                .Select(r => new { r.Id, r.RawBytes })
                .ToListAsync(ct);

            if (batch.Count == 0) break;

            // Dedupe within batch and across the union of slots we extract.
            var bag = new Dictionary<string, string>(capacity: batch.Count * SlotCount);

            foreach (var row in batch)
            {
                totalScanned++;
                ExtractInto(row.RawBytes, bag);
            }

            if (bag.Count > 0)
                totalUpserted += await BulkUpsertAsync(ctx, bag, ct);

            lastId = batch[^1].Id;
            _logger.LogInformation(
                "Breeding-horse backfill: scanned={Scanned}, upserted={Upserted}, cursor={Cursor}",
                totalScanned, totalUpserted, lastId);
        }

        sw.Stop();
        _logger.LogInformation(
            "Breeding-horse backfill complete: scanned={Scanned}, upserted={Upserted}, duration={Ms}ms",
            totalScanned, totalUpserted, sw.ElapsedMilliseconds);

        return (totalScanned, totalUpserted);
    }

    private static void ExtractInto(byte[] raw, Dictionary<string, string> bag)
    {
        var minLen = SlotsStart + SlotSize * SlotCount;
        if (raw.Length < minLen) return;

        var span = raw.AsSpan();
        for (int i = 0; i < SlotCount; i++)
        {
            var slot = span.Slice(SlotsStart + i * SlotSize, SlotSize);

            var idBytes = slot.Slice(0, 10);
            var hansyokuNum = Encoding.ASCII.GetString(idBytes).Trim();
            if (string.IsNullOrEmpty(hansyokuNum) || hansyokuNum == "0000000000")
                continue;

            var nameBytes = slot.Slice(10, 36);
            var nameJa = ShiftJis.GetString(nameBytes).Trim();
            if (string.IsNullOrEmpty(nameJa)) continue;

            // First wins per batch — same HansyokuNum maps to the same name in practice.
            if (!bag.ContainsKey(hansyokuNum))
                bag[hansyokuNum] = nameJa;
        }
    }

    private static async Task<int> BulkUpsertAsync(
        AppDbContext ctx, Dictionary<string, string> bag, CancellationToken ct)
    {
        // Chunk SQL parameters to stay well under PG's 65535 limit (3 params per row).
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
                values.Add($"(@p{p}, @p{p+1}, NOW())");
                parameters.Add(kv.Key);
                parameters.Add(kv.Value);
                p += 2;
            }

            var sql = $@"
                INSERT INTO breeding_horses (""HansyokuNum"", ""NameJa"", ""LastUpdated"")
                VALUES {string.Join(", ", values)}
                ON CONFLICT (""HansyokuNum"") DO NOTHING";

            total += await ctx.Database.ExecuteSqlRawAsync(sql, parameters.ToArray(), ct);
        }

        return total;
    }
}
