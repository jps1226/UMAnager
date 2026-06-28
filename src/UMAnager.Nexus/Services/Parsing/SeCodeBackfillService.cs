// ============================================================
// FILE: SeCodeBackfillService.cs
// LAYER: Parsing (one-shot backfill, idempotent)
// PURPOSE: Re-parses staged SE records to fill the JockeyCode / TrainerCode columns on existing
//          race_entries (Phase 8). Writes only when both codes resolve and differ from current.
// KEY DEPENDENCIES: AppDbContext, SeRecordParser.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services.Parsing;

/// <summary>
/// Phase 8 — re-parses staged SE records to fill the new JockeyCode / TrainerCode columns
/// on existing race_entries rows. Idempotent: only writes when both codes resolve and the
/// row's current value differs.
/// </summary>
public sealed class SeCodeBackfillService
{
    private const int BatchRecords = 3000;

    private readonly IDbContextFactory<AppDbContext> _contextFactory;
    private readonly ILogger<SeCodeBackfillService> _logger;

    public SeCodeBackfillService(
        IDbContextFactory<AppDbContext> contextFactory,
        ILogger<SeCodeBackfillService> logger)
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
                .Where(r => r.RecordType == "SE" && r.Id > lastId)
                .OrderBy(r => r.Id)
                .Take(BatchRecords)
                .Select(r => new { r.Id, r.RawBytes })
                .ToListAsync(ct);

            if (batch.Count == 0) break;

            // (RaceId, HorseId) -> (jockey, trainer, sex). Last record wins per (RaceId, HorseId).
            var bag = new Dictionary<(string, string), (string? j, string? t, short? sex)>(batch.Count);
            foreach (var row in batch)
            {
                totalScanned++;
                try
                {
                    var entry = SeRecordParser.Parse(row.RawBytes);
                    if (string.IsNullOrEmpty(entry.RaceId) || string.IsNullOrEmpty(entry.HorseId)) continue;
                    if (entry.JockeyCode == null && entry.TrainerCode == null && entry.Sex == null) continue;
                    bag[(entry.RaceId, entry.HorseId)] = (entry.JockeyCode, entry.TrainerCode, entry.Sex);
                }
                catch { /* skip malformed records */ }
            }

            if (bag.Count > 0)
                totalUpdated += await BulkUpdateAsync(ctx, bag, ct);

            lastId = batch[^1].Id;
        }

        sw.Stop();
        _logger.LogInformation(
            "SE code backfill complete: scanned={Scanned}, updated={Updated}, duration={Ms}ms",
            totalScanned, totalUpdated, sw.ElapsedMilliseconds);

        return (totalScanned, totalUpdated);
    }

    private static async Task<int> BulkUpdateAsync(
        AppDbContext ctx,
        Dictionary<(string, string), (string? j, string? t, short? sex)> bag,
        CancellationToken ct)
    {
        const int ChunkRows = 4000;
        int total = 0;

        var items = bag.ToList();
        for (int start = 0; start < items.Count; start += ChunkRows)
        {
            var chunk = items.Skip(start).Take(ChunkRows).ToList();
            var values = new List<string>(chunk.Count);
            var parameters = new List<object>(chunk.Count * 5);
            int p = 0;

            foreach (var kv in chunk)
            {
                values.Add($"(@p{p}, @p{p+1}, @p{p+2}, @p{p+3}, @p{p+4})");
                parameters.Add(new NpgsqlParameter($"p{p}",   NpgsqlDbType.Varchar)  { Value = kv.Key.Item1 });
                parameters.Add(new NpgsqlParameter($"p{p+1}", NpgsqlDbType.Varchar)  { Value = kv.Key.Item2 });
                parameters.Add(new NpgsqlParameter($"p{p+2}", NpgsqlDbType.Varchar)  { Value = (object?)kv.Value.j ?? DBNull.Value });
                parameters.Add(new NpgsqlParameter($"p{p+3}", NpgsqlDbType.Varchar)  { Value = (object?)kv.Value.t ?? DBNull.Value });
                parameters.Add(new NpgsqlParameter($"p{p+4}", NpgsqlDbType.Smallint) { Value = (object?)kv.Value.sex ?? DBNull.Value });
                p += 5;
            }

            var sql = $@"
                UPDATE race_entries re
                SET ""JockeyCode""  = COALESCE(v.j, re.""JockeyCode""),
                    ""TrainerCode"" = COALESCE(v.t, re.""TrainerCode""),
                    ""Sex""         = COALESCE(v.sex, re.""Sex""),
                    ""UpdatedAt""   = NOW()
                FROM (VALUES {string.Join(", ", values)}) AS v(race_id, horse_id, j, t, sex)
                WHERE re.""RaceId""  = v.race_id
                  AND re.""HorseId"" = v.horse_id
                  AND (re.""JockeyCode""  IS DISTINCT FROM COALESCE(v.j, re.""JockeyCode"")
                    OR re.""TrainerCode"" IS DISTINCT FROM COALESCE(v.t, re.""TrainerCode"")
                    OR re.""Sex""         IS DISTINCT FROM COALESCE(v.sex, re.""Sex""))";

            total += await ctx.Database.ExecuteSqlRawAsync(sql, parameters.ToArray(), ct);
        }

        return total;
    }
}
