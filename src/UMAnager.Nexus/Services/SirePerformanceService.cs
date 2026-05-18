using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Phase 9: sire-fit % by (sire_id × surface × distance bucket).
///
/// Computed from race_entries ⋈ races ⋈ horses — no SK-record parsing needed.
/// We already carry ~68K finished entries spanning 1999–2026 across ~680 sires,
/// which is the authoritative body of work for sire performance on our system.
///
/// Distance buckets (CLAUDE.md): sprint ≤1400, mile 1401–1800, middle 1801–2200, long ≥2201.
/// Surface: 'turf' / 'dirt' (normalized from races.Surface).
///
/// EnsureSchemaAsync is idempotent (CREATE … IF NOT EXISTS) and runs at startup.
/// RefreshAsync runs CONCURRENTLY — needs the unique index to do so.
/// </summary>
public sealed class SirePerformanceService
{
    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly ILogger<SirePerformanceService> _logger;

    public SirePerformanceService(IDbContextFactory<AppDbContext> dbFactory, ILogger<SirePerformanceService> logger)
    {
        _dbFactory = dbFactory;
        _logger = logger;
    }

    public async Task EnsureSchemaAsync(CancellationToken ct = default)
    {
        using var db = await _dbFactory.CreateDbContextAsync(ct);

        const string ddl = @"
CREATE MATERIALIZED VIEW IF NOT EXISTS sire_performance AS
SELECT
    h.""SireId""                                        AS sire_id,
    r.""Surface""                                       AS surface,
    CASE
        WHEN r.""Distance"" <= 1400 THEN 'sprint'
        WHEN r.""Distance"" <= 1800 THEN 'mile'
        WHEN r.""Distance"" <= 2200 THEN 'middle'
        ELSE 'long'
    END                                                  AS distance_bucket,
    COUNT(*)                                             AS starts,
    COUNT(*) FILTER (WHERE re.""FinishPos"" = 1)         AS wins,
    COUNT(*) FILTER (WHERE re.""FinishPos"" BETWEEN 1 AND 3) AS places,
    ROUND(100.0 * COUNT(*) FILTER (WHERE re.""FinishPos"" = 1) / COUNT(*), 2) AS win_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE re.""FinishPos"" BETWEEN 1 AND 3) / COUNT(*), 2) AS place_pct
FROM race_entries re
JOIN races  r ON r.""RaceId""  = re.""RaceId""
JOIN horses h ON h.""HorseId"" = re.""HorseId""
WHERE h.""SireId"" IS NOT NULL
  AND h.""SireId"" <> ''
  AND re.""FinishPos"" IS NOT NULL
  AND re.""FinishPos"" > 0
  AND r.""Surface""  IS NOT NULL
  AND r.""Surface"" <> ''
  AND r.""Distance"" IS NOT NULL
  AND r.""Distance"" > 0
GROUP BY h.""SireId"", r.""Surface"", distance_bucket
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS ux_sire_performance_key
    ON sire_performance (sire_id, surface, distance_bucket);
";
        await db.Database.ExecuteSqlRawAsync(ddl, ct);
        _logger.LogInformation("[SirePerf] EnsureSchema complete (MV + unique index ready).");
    }

    /// <summary>
    /// Refresh the MV. First call (after EnsureSchema) cannot use CONCURRENTLY
    /// because the view is empty (WITH NO DATA). Subsequent calls use CONCURRENTLY
    /// so reads never block.
    /// </summary>
    public async Task RefreshAsync(CancellationToken ct = default)
    {
        using var db = await _dbFactory.CreateDbContextAsync(ct);

        var populated = await db.Database
            .SqlQueryRaw<bool>("SELECT relispopulated AS \"Value\" FROM pg_class WHERE relname = 'sire_performance'")
            .FirstOrDefaultAsync(ct);

        var sql = populated
            ? "REFRESH MATERIALIZED VIEW CONCURRENTLY sire_performance"
            : "REFRESH MATERIALIZED VIEW sire_performance";

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await db.Database.ExecuteSqlRawAsync(sql, ct);
        sw.Stop();
        _logger.LogInformation("[SirePerf] Refresh ({Mode}) complete in {Ms}ms.",
            populated ? "concurrent" : "initial", sw.ElapsedMilliseconds);
    }

    /// <summary>Bucket helper mirroring the MV CASE — used by the API to look up.</summary>
    public static string DistanceBucket(int? distance)
    {
        if (distance is null || distance <= 0) return "";
        if (distance <= 1400) return "sprint";
        if (distance <= 1800) return "mile";
        if (distance <= 2200) return "middle";
        return "long";
    }

    public sealed record SirePerfRow(string SireId, string Surface, string Bucket, int Starts, int Wins, int Places, decimal WinPct, decimal PlacePct);

    /// <summary>
    /// Pull the entire MV as a list. ~5–10K rows worst case; load once per /api/races
    /// request and index in-memory. Cheap (single seq scan of a small table).
    /// </summary>
    public async Task<List<SirePerfRow>> LoadAllAsync(CancellationToken ct = default)
    {
        using var db = await _dbFactory.CreateDbContextAsync(ct);
        return await db.Database
            .SqlQueryRaw<SirePerfRow>(
                "SELECT sire_id AS \"SireId\", surface AS \"Surface\", distance_bucket AS \"Bucket\", " +
                "starts AS \"Starts\", wins AS \"Wins\", places AS \"Places\", " +
                "win_pct AS \"WinPct\", place_pct AS \"PlacePct\" " +
                "FROM sire_performance")
            .ToListAsync(ct);
    }
}
