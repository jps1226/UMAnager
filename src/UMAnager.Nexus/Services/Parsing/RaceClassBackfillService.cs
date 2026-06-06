// ============================================================
// FILE: RaceClassBackfillService.cs
// LAYER: Parsing (one-shot backfill, idempotent)
// PURPOSE: Re-parses RA records to fill races.RaceClass from JyokenCD slot 5 (the original parser
//          never read it, so historical races have RaceClass NULL). Mirrors SurfaceBackfillService.
// KEY DEPENDENCIES: AppDbContext, RaRecordParser.MapJyokenCdToRaceClass.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services.Parsing;

/// <summary>
/// One-shot backfill for races.RaceClass — pulled from JyokenCD slot 5 (offset 635,
/// len 3) of every RA record in raw_staging per Oracle Q20. The original RaRecord
/// parser didn't read this field, so historical races have RaceClass NULL.
///
/// Mirrors SurfaceBackfillService: re-parse RA bytes with the fixed parser, bulk
/// UPDATE via VALUES join. Idempotent; safe to re-run.
/// </summary>
public sealed class RaceClassBackfillService
{
    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly ILogger<RaceClassBackfillService> _logger;

    public RaceClassBackfillService(IDbContextFactory<AppDbContext> dbFactory, ILogger<RaceClassBackfillService> logger)
    {
        _dbFactory = dbFactory;
        _logger = logger;
    }

    public async Task<(int scanned, int updated)> BackfillAsync(CancellationToken ct = default)
    {
        using var db = await _dbFactory.CreateDbContextAsync(ct);

        var raRaw = await db.RawStagingRecords
            .Where(r => r.RecordType == "RA")
            .Select(r => r.RawBytes)
            .ToListAsync(ct);

        // Last write wins per RaceId — raw_staging is roughly chronological, so the
        // final lifecycle update for each race holds the authoritative JyokenCD.
        var byClass = new Dictionary<string, string>();
        int scanned = 0;
        foreach (var raw in raRaw)
        {
            scanned++;
            try
            {
                var parsed = RaRecordParser.Parse(raw);
                if (string.IsNullOrEmpty(parsed.RaceId) || string.IsNullOrEmpty(parsed.RaceClass)) continue;
                byClass[parsed.RaceId] = parsed.RaceClass;
            }
            catch
            {
                // Skip malformed records.
            }
        }

        if (byClass.Count == 0)
        {
            _logger.LogInformation("[RaceClassBackfill] No race-class values resolved from {Scanned} RA records.", scanned);
            return (scanned, 0);
        }

        int totalUpdated = 0;
        const int batchSize = 1000;
        var entries = byClass.ToList();
        for (int i = 0; i < entries.Count; i += batchSize)
        {
            var batch = entries.Skip(i).Take(batchSize).ToList();
            var valueRows = new List<string>(batch.Count);
            var parameters = new List<NpgsqlParameter>(batch.Count * 2);
            int p = 0;
            foreach (var (raceId, raceClass) in batch)
            {
                valueRows.Add($"(@p{p}, @p{p + 1})");
                parameters.Add(new NpgsqlParameter($"p{p}", NpgsqlDbType.Text) { Value = raceId });
                parameters.Add(new NpgsqlParameter($"p{p + 1}", NpgsqlDbType.Text) { Value = raceClass });
                p += 2;
            }

            var sql =
                "UPDATE races r SET \"RaceClass\" = v.race_class, \"LastUpdated\" = NOW() " +
                "FROM (VALUES " + string.Join(", ", valueRows) + ") AS v(race_id, race_class) " +
                "WHERE r.\"RaceId\" = v.race_id " +
                "  AND (r.\"RaceClass\" IS NULL OR r.\"RaceClass\" = '' OR r.\"RaceClass\" <> v.race_class)";

            totalUpdated += await db.Database.ExecuteSqlRawAsync(sql, parameters.ToArray(), ct);
        }

        _logger.LogInformation("[RaceClassBackfill] Scanned {Scanned} RA records, resolved {Resolved} races, updated {Updated} rows.",
            scanned, byClass.Count, totalUpdated);
        return (scanned, totalUpdated);
    }
}
