// ============================================================
// FILE: JockeyTrainerStatsService.cs
// LAYER: Service
// PURPOSE: Computes rolling Win%/Place%/A-E for jockeys (90d) and trainers (180d) via SQL over
//          race_entries, with a self-calibrating expected-win baseline by favorite rank. LoadAll*
//          serve the cached stats into RacesController.
// KEY DEPENDENCIES: AppDbContext (writes the snake_case jockeys/trainers stat columns).
// CAUTION: Min-sample floors (30 jockey / 20 trainer starts) gate display + scoring elsewhere.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Phase 8 — rolling Win% / Place% / A/E (Actual/Expected) for jockeys (90d) and trainers (180d).
///
/// A/E uses an empirically-derived expected-win probability by favorite rank, computed over
/// the same window. P(win | fav_rank=k) self-calibrates to JRA's actual market accuracy — no
/// fixed table to drift out of date. Jockey/trainer A/E = Σ wins ÷ Σ P(win | their mount's rank).
///
/// 1.00 ≈ market-neutral. &gt;1.15 = beating the market. &lt;0.85 = drag.
/// Min sample floors (30 jockey starts, 20 trainer starts) gate display + scoring contribution.
/// </summary>
public sealed class JockeyTrainerStatsService
{
    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly ILogger<JockeyTrainerStatsService> _logger;

    public JockeyTrainerStatsService(IDbContextFactory<AppDbContext> dbFactory, ILogger<JockeyTrainerStatsService> logger)
    {
        _dbFactory = dbFactory;
        _logger = logger;
    }

    public async Task RefreshAsync(CancellationToken ct = default)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var db = await _dbFactory.CreateDbContextAsync(ct);

        await RefreshJockeysAsync(db, ct);
        await RefreshTrainersAsync(db, ct);

        sw.Stop();
        _logger.LogInformation("[JockeyTrainerStats] Refresh complete in {Ms}ms.", sw.ElapsedMilliseconds);
    }

    private static async Task RefreshJockeysAsync(AppDbContext db, CancellationToken ct)
    {
        const string sql = @"
WITH window_entries AS (
    SELECT re.""JockeyCode"" AS code, re.""FavRank"" AS rank, re.""FinishPos"" AS pos
    FROM race_entries re
    JOIN races r ON r.""RaceId"" = re.""RaceId""
    WHERE r.""RaceDate"" >= (CURRENT_DATE - INTERVAL '90 days')
      AND r.""RaceDate"" <= CURRENT_DATE
      AND re.""JockeyCode"" IS NOT NULL
      AND re.""FinishPos"" IS NOT NULL
      AND re.""FinishPos"" > 0
),
rank_baseline AS (
    SELECT rank,
           AVG(CASE WHEN pos = 1 THEN 1.0 ELSE 0 END) AS p_win
    FROM window_entries
    WHERE rank IS NOT NULL AND rank BETWEEN 1 AND 18
    GROUP BY rank
),
agg AS (
    SELECT
        we.code,
        COUNT(*)                                          AS starts,
        COUNT(*) FILTER (WHERE we.pos = 1)                AS wins,
        COUNT(*) FILTER (WHERE we.pos BETWEEN 1 AND 3)    AS places,
        COALESCE(SUM(rb.p_win), 0)                        AS expected_wins
    FROM window_entries we
    LEFT JOIN rank_baseline rb ON rb.rank = we.rank
    GROUP BY we.code
)
UPDATE jockeys j
SET starts_90d   = agg.starts,
    wins_90d     = agg.wins,
    places_90d   = agg.places,
    win_pct_90d  = ROUND(agg.wins::numeric   / NULLIF(agg.starts, 0), 4),
    place_pct_90d= ROUND(agg.places::numeric / NULLIF(agg.starts, 0), 4),
    ae_90d       = ROUND(agg.wins::numeric   / NULLIF(agg.expected_wins, 0), 4),
    stats_refreshed_at = NOW()
FROM agg
WHERE j.jockey_code = agg.code;

-- Reset rows with no recent rides (so stale numbers don't linger).
UPDATE jockeys
SET starts_90d = 0, wins_90d = 0, places_90d = 0,
    win_pct_90d = NULL, place_pct_90d = NULL, ae_90d = NULL,
    stats_refreshed_at = NOW()
WHERE jockey_code NOT IN (
    SELECT DISTINCT re.""JockeyCode""
    FROM race_entries re
    JOIN races r ON r.""RaceId"" = re.""RaceId""
    WHERE r.""RaceDate"" >= (CURRENT_DATE - INTERVAL '90 days')
      AND re.""JockeyCode"" IS NOT NULL
);
";
        await db.Database.ExecuteSqlRawAsync(sql, ct);
    }

    private static async Task RefreshTrainersAsync(AppDbContext db, CancellationToken ct)
    {
        const string sql = @"
WITH window_entries AS (
    SELECT re.""TrainerCode"" AS code, re.""FavRank"" AS rank, re.""FinishPos"" AS pos
    FROM race_entries re
    JOIN races r ON r.""RaceId"" = re.""RaceId""
    WHERE r.""RaceDate"" >= (CURRENT_DATE - INTERVAL '180 days')
      AND r.""RaceDate"" <= CURRENT_DATE
      AND re.""TrainerCode"" IS NOT NULL
      AND re.""FinishPos"" IS NOT NULL
      AND re.""FinishPos"" > 0
),
rank_baseline AS (
    SELECT rank,
           AVG(CASE WHEN pos = 1 THEN 1.0 ELSE 0 END) AS p_win
    FROM window_entries
    WHERE rank IS NOT NULL AND rank BETWEEN 1 AND 18
    GROUP BY rank
),
agg AS (
    SELECT
        we.code,
        COUNT(*)                                          AS starts,
        COUNT(*) FILTER (WHERE we.pos = 1)                AS wins,
        COUNT(*) FILTER (WHERE we.pos BETWEEN 1 AND 3)    AS places,
        COALESCE(SUM(rb.p_win), 0)                        AS expected_wins
    FROM window_entries we
    LEFT JOIN rank_baseline rb ON rb.rank = we.rank
    GROUP BY we.code
)
UPDATE trainers t
SET starts_180d   = agg.starts,
    wins_180d     = agg.wins,
    places_180d   = agg.places,
    win_pct_180d  = ROUND(agg.wins::numeric   / NULLIF(agg.starts, 0), 4),
    place_pct_180d= ROUND(agg.places::numeric / NULLIF(agg.starts, 0), 4),
    ae_180d       = ROUND(agg.wins::numeric   / NULLIF(agg.expected_wins, 0), 4),
    stats_refreshed_at = NOW()
FROM agg
WHERE t.trainer_code = agg.code;

UPDATE trainers
SET starts_180d = 0, wins_180d = 0, places_180d = 0,
    win_pct_180d = NULL, place_pct_180d = NULL, ae_180d = NULL,
    stats_refreshed_at = NOW()
WHERE trainer_code NOT IN (
    SELECT DISTINCT re.""TrainerCode""
    FROM race_entries re
    JOIN races r ON r.""RaceId"" = re.""RaceId""
    WHERE r.""RaceDate"" >= (CURRENT_DATE - INTERVAL '180 days')
      AND re.""TrainerCode"" IS NOT NULL
);
";
        await db.Database.ExecuteSqlRawAsync(sql, ct);
    }

    public sealed record JockeyStat(
        string Code, string? NameJa, string? NameEn,
        int? Starts, int? Wins, int? Places,
        decimal? WinPct, decimal? PlacePct, decimal? Ae);

    public sealed record TrainerStat(
        string Code, string? NameJa, string? NameEn,
        int? Starts, int? Wins, int? Places,
        decimal? WinPct, decimal? PlacePct, decimal? Ae);

    public async Task<List<JockeyStat>> LoadAllJockeysAsync(CancellationToken ct = default)
    {
        using var db = await _dbFactory.CreateDbContextAsync(ct);
        return await db.Database
            .SqlQueryRaw<JockeyStat>(@"
                SELECT jockey_code AS ""Code"",
                       jockey_name_japanese AS ""NameJa"",
                       jockey_name_romaji   AS ""NameEn"",
                       starts_90d AS ""Starts"", wins_90d AS ""Wins"", places_90d AS ""Places"",
                       win_pct_90d AS ""WinPct"", place_pct_90d AS ""PlacePct"", ae_90d AS ""Ae""
                FROM jockeys
                WHERE starts_90d IS NOT NULL AND starts_90d > 0")
            .ToListAsync(ct);
    }

    public async Task<List<TrainerStat>> LoadAllTrainersAsync(CancellationToken ct = default)
    {
        using var db = await _dbFactory.CreateDbContextAsync(ct);
        return await db.Database
            .SqlQueryRaw<TrainerStat>(@"
                SELECT trainer_code AS ""Code"",
                       trainer_name_japanese AS ""NameJa"",
                       trainer_name_romaji   AS ""NameEn"",
                       starts_180d AS ""Starts"", wins_180d AS ""Wins"", places_180d AS ""Places"",
                       win_pct_180d AS ""WinPct"", place_pct_180d AS ""PlacePct"", ae_180d AS ""Ae""
                FROM trainers
                WHERE starts_180d IS NOT NULL AND starts_180d > 0")
            .ToListAsync(ct);
    }
}
