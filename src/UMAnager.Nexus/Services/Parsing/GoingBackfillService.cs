// ============================================================
// FILE: GoingBackfillService.cs
// LAYER: Parsing (one-shot backfill, idempotent)
// PURPOSE: Re-parses every RA record in raw_staging and bulk-UPDATEs races.Weather/TurfGoing/
//          DirtGoing (Oracle 2026-06-22, RA bytes 888/889/890). The going only populates post-race
//          (DataKubun 3+); the dedup keeps the latest RA per race, so settled races carry it.
// KEY DEPENDENCIES: AppDbContext, RaRecordParser. Mirrors SurfaceBackfillService.
// LAST DOCUMENTED: 2026-06-22
// ============================================================
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services.Parsing;

/// <summary>
/// One-shot backfill for races.Weather / TurfGoing / DirtGoing (the cold "value" engine's first
/// Group-B factor). Re-parses every RA record in raw_staging and bulk-UPDATEs the three columns.
/// Idempotent — re-runnable after each ingest (only writes rows where a non-null going was resolved).
/// </summary>
public sealed class GoingBackfillService
{
    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly ILogger<GoingBackfillService> _logger;

    public GoingBackfillService(IDbContextFactory<AppDbContext> dbFactory, ILogger<GoingBackfillService> logger)
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

        // raceId → (weather, turf, dirt). Last non-null write wins (raw_staging is roughly chronological,
        // and the going only fills in on the later/settled RA records).
        var byRace = new Dictionary<string, (short? w, short? t, short? d)>();
        int scanned = 0;
        foreach (var raw in raRaw)
        {
            scanned++;
            try
            {
                var p = RaRecordParser.Parse(raw);
                if (string.IsNullOrEmpty(p.RaceId)) continue;
                if (p.Weather == null && p.TurfGoing == null && p.DirtGoing == null) continue; // pre-race blank
                byRace.TryGetValue(p.RaceId, out var cur);
                byRace[p.RaceId] = (p.Weather ?? cur.w, p.TurfGoing ?? cur.t, p.DirtGoing ?? cur.d);
            }
            catch
            {
                // Skip malformed records.
            }
        }

        if (byRace.Count == 0)
        {
            _logger.LogInformation("[GoingBackfill] No going values resolved from {Scanned} RA records.", scanned);
            return (scanned, 0);
        }

        int totalUpdated = 0;
        const int batchSize = 1000;
        var entries = byRace.ToList();
        for (int i = 0; i < entries.Count; i += batchSize)
        {
            var batch = entries.Skip(i).Take(batchSize).ToList();
            var valueRows = new List<string>(batch.Count);
            var parameters = new List<NpgsqlParameter>(batch.Count * 4);
            int p = 0;
            foreach (var (raceId, g) in batch)
            {
                valueRows.Add($"(@p{p}, @p{p + 1}, @p{p + 2}, @p{p + 3})");
                parameters.Add(new NpgsqlParameter($"p{p}",     NpgsqlDbType.Text)     { Value = raceId });
                parameters.Add(new NpgsqlParameter($"p{p + 1}", NpgsqlDbType.Smallint) { Value = (object?)g.w ?? DBNull.Value });
                parameters.Add(new NpgsqlParameter($"p{p + 2}", NpgsqlDbType.Smallint) { Value = (object?)g.t ?? DBNull.Value });
                parameters.Add(new NpgsqlParameter($"p{p + 3}", NpgsqlDbType.Smallint) { Value = (object?)g.d ?? DBNull.Value });
                p += 4;
            }

            // COALESCE keeps any existing value when this batch's column is null (don't blank a set going).
            var sql =
                "UPDATE races r SET " +
                "\"Weather\"   = COALESCE(v.weather, r.\"Weather\"), " +
                "\"TurfGoing\" = COALESCE(v.turf,    r.\"TurfGoing\"), " +
                "\"DirtGoing\" = COALESCE(v.dirt,    r.\"DirtGoing\"), " +
                "\"LastUpdated\" = NOW() " +
                "FROM (VALUES " + string.Join(", ", valueRows) + ") AS v(race_id, weather, turf, dirt) " +
                "WHERE r.\"RaceId\" = v.race_id " +
                "  AND (r.\"Weather\" IS DISTINCT FROM COALESCE(v.weather, r.\"Weather\") " +
                "    OR r.\"TurfGoing\" IS DISTINCT FROM COALESCE(v.turf, r.\"TurfGoing\") " +
                "    OR r.\"DirtGoing\" IS DISTINCT FROM COALESCE(v.dirt, r.\"DirtGoing\"))";

            totalUpdated += await db.Database.ExecuteSqlRawAsync(sql, parameters.ToArray(), ct);
        }

        _logger.LogInformation("[GoingBackfill] Scanned {Scanned} RA records, resolved {Resolved} races, updated {Updated} rows.",
            scanned, byRace.Count, totalUpdated);
        return (scanned, totalUpdated);
    }
}
