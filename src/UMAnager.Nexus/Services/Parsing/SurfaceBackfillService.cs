// ============================================================
// FILE: SurfaceBackfillService.cs
// LAYER: Parsing (one-shot backfill, idempotent)
// PURPOSE: Re-parses every RA record with the fixed TrackCD logic and bulk-UPDATEs races.Surface
//          (the original parser never matched, leaving Surface NULL — which blocked the sire-perf MV).
// KEY DEPENDENCIES: AppDbContext, RaRecordParser.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services.Parsing;

/// <summary>
/// One-shot backfill for races.Surface. The original RaRecordParser checked the
/// TrackCD field for "1"/"2" (single digit) but real JV-Link codes are 2-digit
/// (10-19 turf, 21-29 dirt, 50-59 jump). Result: every existing race row has
/// Surface = NULL/empty, which blocked the Phase 9 sire_performance MV.
///
/// This service re-parses every RA record in raw_staging with the fixed parser
/// and bulk-UPDATEs races.Surface. Idempotent — re-runnable after each ingest.
/// </summary>
public sealed class SurfaceBackfillService
{
    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly ILogger<SurfaceBackfillService> _logger;

    public SurfaceBackfillService(IDbContextFactory<AppDbContext> dbFactory, ILogger<SurfaceBackfillService> logger)
    {
        _dbFactory = dbFactory;
        _logger = logger;
    }

    public async Task<(int scanned, int updated)> BackfillAsync(CancellationToken ct = default)
    {
        using var db = await _dbFactory.CreateDbContextAsync(ct);

        // Pull every RA record's raw bytes. ~9K records → ~11MB at 1273 bytes each. Fine.
        var raRaw = await db.RawStagingRecords
            .Where(r => r.RecordType == "RA")
            .Select(r => r.RawBytes)
            .ToListAsync(ct);

        // Latest record wins per RaceId (DataKubun lifecycle 1→2→…→7 means later
        // rows carry the authoritative final value).
        var bySurface = new Dictionary<string, string>(); // raceId → surface
        int scanned = 0;
        foreach (var raw in raRaw)
        {
            scanned++;
            try
            {
                var parsed = RaRecordParser.Parse(raw);
                if (string.IsNullOrEmpty(parsed.RaceId) || string.IsNullOrEmpty(parsed.Surface)) continue;
                bySurface[parsed.RaceId] = parsed.Surface;  // last write wins; raw_staging is roughly chronological
            }
            catch
            {
                // Skip malformed records; not our concern in this backfill.
            }
        }

        if (bySurface.Count == 0)
        {
            _logger.LogInformation("[SurfaceBackfill] No surface values resolved from {Scanned} RA records.", scanned);
            return (scanned, 0);
        }

        // Bulk update via a VALUES table — same idiom as HnNameBackfillService.
        int totalUpdated = 0;
        const int batchSize = 1000;
        var entries = bySurface.ToList();
        for (int i = 0; i < entries.Count; i += batchSize)
        {
            var batch = entries.Skip(i).Take(batchSize).ToList();
            var valueRows = new List<string>(batch.Count);
            var parameters = new List<NpgsqlParameter>(batch.Count * 2);
            int p = 0;
            foreach (var (raceId, surface) in batch)
            {
                valueRows.Add($"(@p{p}, @p{p + 1})");
                parameters.Add(new NpgsqlParameter($"p{p}", NpgsqlDbType.Text) { Value = raceId });
                parameters.Add(new NpgsqlParameter($"p{p + 1}", NpgsqlDbType.Text) { Value = surface });
                p += 2;
            }

            var sql =
                "UPDATE races r SET \"Surface\" = v.surface, \"LastUpdated\" = NOW() " +
                "FROM (VALUES " + string.Join(", ", valueRows) + ") AS v(race_id, surface) " +
                "WHERE r.\"RaceId\" = v.race_id " +
                "  AND (r.\"Surface\" IS NULL OR r.\"Surface\" = '' OR r.\"Surface\" <> v.surface)";

            totalUpdated += await db.Database.ExecuteSqlRawAsync(sql, parameters.ToArray(), ct);
        }

        _logger.LogInformation("[SurfaceBackfill] Scanned {Scanned} RA records, resolved {Resolved} unique races, updated {Updated} rows.",
            scanned, bySurface.Count, totalUpdated);
        return (scanned, totalUpdated);
    }
}
