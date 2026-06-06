// ============================================================
// FILE: JockeyTrainerIngestService.cs
// LAYER: Parsing (ingest, idempotent)
// PURPOSE: Scans raw_staging for KS (jockey) + CH (trainer) master records and upserts them into
//          the snake_case jockeys / trainers tables (Phase 8). Re-runnable.
// KEY DEPENDENCIES: AppDbContext, KsRecordParser, ChRecordParser.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services.Parsing;

/// <summary>
/// Phase 8 — scans raw_staging for KS (jockey master) and CH (trainer master) records,
/// upserts into the snake_case jockeys / trainers tables. Idempotent and re-runnable.
/// </summary>
public sealed class JockeyTrainerIngestService
{
    private const int BatchRecords = 2000;

    private readonly IDbContextFactory<AppDbContext> _contextFactory;
    private readonly ILogger<JockeyTrainerIngestService> _logger;

    public JockeyTrainerIngestService(
        IDbContextFactory<AppDbContext> contextFactory,
        ILogger<JockeyTrainerIngestService> logger)
    {
        _contextFactory = contextFactory;
        _logger = logger;
    }

    public async Task<(int Scanned, int Upserted)> IngestJockeysAsync(CancellationToken ct = default)
        => await IngestAsync("KS", ct);

    public async Task<(int Scanned, int Upserted)> IngestTrainersAsync(CancellationToken ct = default)
        => await IngestAsync("CH", ct);

    private async Task<(int Scanned, int Upserted)> IngestAsync(string type, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        long lastId = 0;
        int totalScanned = 0;
        int totalUpserted = 0;

        while (!ct.IsCancellationRequested)
        {
            using var ctx = _contextFactory.CreateDbContext();
            var batch = await ctx.RawStagingRecords.AsNoTracking()
                .Where(r => r.RecordType == type && r.Id > lastId)
                .OrderBy(r => r.Id)
                .Take(BatchRecords)
                .Select(r => new { r.Id, r.RawBytes })
                .ToListAsync(ct);

            if (batch.Count == 0) break;

            // Aggregate per code (last-wins within batch — later staged record is fresher).
            var bag = new Dictionary<string, (string? nameJa, string? nameEn)>(batch.Count);
            foreach (var row in batch)
            {
                totalScanned++;
                if (type == "KS")
                {
                    var p = KsRecordParser.Parse(row.RawBytes);
                    if (p == null) continue;
                    bag[p.JockeyCode] = (p.NameJa, p.NameEn);
                }
                else
                {
                    var p = ChRecordParser.Parse(row.RawBytes);
                    if (p == null) continue;
                    bag[p.TrainerCode] = (p.NameJa, p.NameEn);
                }
            }

            if (bag.Count > 0)
                totalUpserted += await BulkUpsertAsync(ctx, type, bag, ct);

            lastId = batch[^1].Id;
        }

        sw.Stop();
        _logger.LogInformation(
            "{Type} ingest complete: scanned={Scanned}, upserted={Upserted}, duration={Ms}ms",
            type, totalScanned, totalUpserted, sw.ElapsedMilliseconds);

        return (totalScanned, totalUpserted);
    }

    private static async Task<int> BulkUpsertAsync(
        AppDbContext ctx, string type,
        Dictionary<string, (string? nameJa, string? nameEn)> bag,
        CancellationToken ct)
    {
        const int ChunkRows = 4000;
        int total = 0;
        var (table, codeCol, jaCol, enCol) = type == "KS"
            ? ("jockeys",  "jockey_code",  "jockey_name_japanese",  "jockey_name_romaji")
            : ("trainers", "trainer_code", "trainer_name_japanese", "trainer_name_romaji");

        var items = bag.ToList();
        for (int start = 0; start < items.Count; start += ChunkRows)
        {
            var chunk = items.Skip(start).Take(ChunkRows).ToList();
            var values = new List<string>(chunk.Count);
            var parameters = new List<object>(chunk.Count * 3);
            int p = 0;

            foreach (var kv in chunk)
            {
                values.Add($"(@p{p}, @p{p+1}, @p{p+2}, NOW())");
                parameters.Add(new NpgsqlParameter($"p{p}",   NpgsqlDbType.Varchar) { Value = kv.Key });
                parameters.Add(new NpgsqlParameter($"p{p+1}", NpgsqlDbType.Text)    { Value = (object?)kv.Value.nameJa ?? DBNull.Value });
                parameters.Add(new NpgsqlParameter($"p{p+2}", NpgsqlDbType.Varchar) { Value = (object?)kv.Value.nameEn ?? DBNull.Value });
                p += 3;
            }

            var sql = $@"
                INSERT INTO {table} ({codeCol}, {jaCol}, {enCol}, last_updated)
                VALUES {string.Join(", ", values)}
                ON CONFLICT ({codeCol}) DO UPDATE SET
                    {jaCol} = COALESCE(excluded.{jaCol}, {table}.{jaCol}),
                    {enCol} = COALESCE(excluded.{enCol}, {table}.{enCol}),
                    last_updated = NOW()";

            total += await ctx.Database.ExecuteSqlRawAsync(sql, parameters.ToArray(), ct);
        }

        return total;
    }
}
