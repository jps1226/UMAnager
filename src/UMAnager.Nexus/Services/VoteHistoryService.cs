using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Phase 30: persists a row to vote_history whenever the user applies marks to OrePro,
/// and serves the badge/history data the dashboard uses to show "Voted N×" on repeat runners.
/// </summary>
public sealed class VoteHistoryService
{
    private readonly IDbContextFactory<AppDbContext> _db;
    private readonly ILogger<VoteHistoryService> _logger;

    public VoteHistoryService(IDbContextFactory<AppDbContext> db, ILogger<VoteHistoryService> logger)
    {
        _db     = db;
        _logger = logger;
    }

    /// <summary>
    /// Upserts one row per (race_id, horse_id). Called after a successful OrePro apply so
    /// re-applying a race just refreshes the mark/timestamp instead of duplicating.
    /// </summary>
    public async Task RecordVotesAsync(string raceId, IEnumerable<(string horseId, string mark)> votes)
    {
        var voteList = votes.Where(v => !string.IsNullOrWhiteSpace(v.horseId)).ToList();
        if (voteList.Count == 0) return;

        try
        {
            await using var db = await _db.CreateDbContextAsync();
            var now = DateTime.UtcNow;

            foreach (var (horseId, mark) in voteList)
            {
                await db.Database.ExecuteSqlRawAsync(@"
                    INSERT INTO vote_history (""HorseId"", ""RaceId"", ""Mark"", ""VotedAt"")
                    VALUES ({0}, {1}, {2}, {3})
                    ON CONFLICT (""RaceId"", ""HorseId"")
                    DO UPDATE SET ""Mark"" = EXCLUDED.""Mark"", ""VotedAt"" = EXCLUDED.""VotedAt""
                ", horseId, raceId, mark, now);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[VoteHistory] Failed to record votes for race {RaceId} (non-fatal)", raceId);
        }
    }

    /// <summary>
    /// Returns per-horse vote counts (for badge display) plus the 50 most recent vote rows
    /// with horse names (for the sidebar history section).
    /// </summary>
    public async Task<VoteHistoryResponse> GetHistoryAsync()
    {
        try
        {
            await using var db = await _db.CreateDbContextAsync();

            // Per-horse list of race_ids voted. The frontend counts only the races OTHER
            // than the one being viewed, so a horse you've only voted in the current race
            // shows no "Voted N×" badge — the badge means "you backed this horse before."
            var pairs = await db.VoteHistory
                .Select(v => new { v.HorseId, v.RaceId })
                .ToListAsync();
            var races = pairs
                .GroupBy(x => x.HorseId)
                .ToDictionary(g => g.Key, g => g.Select(x => x.RaceId).Distinct().ToList());
            var counts = races.ToDictionary(kv => kv.Key, kv => kv.Value.Count);

            // Recent 50 rows joined to horses for names.
            var recent = await (
                from v in db.VoteHistory
                join h in db.Horses on v.HorseId equals h.HorseId into hj
                from h in hj.DefaultIfEmpty()
                orderby v.VotedAt descending
                select new VoteHistoryItem
                {
                    HorseId = v.HorseId,
                    Name    = h != null ? (h.NameEn ?? h.NameJa ?? v.HorseId) : v.HorseId,
                    RaceId  = v.RaceId,
                    Mark    = v.Mark,
                    VotedAt = v.VotedAt,
                }
            ).Take(50).ToListAsync();

            return new VoteHistoryResponse { Counts = counts, Races = races, Recent = recent };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[VoteHistory] GetHistoryAsync failed (non-fatal)");
            return new VoteHistoryResponse();
        }
    }
}

public sealed class VoteHistoryResponse
{
    public Dictionary<string, int>          Counts { get; set; } = new();
    public Dictionary<string, List<string>> Races  { get; set; } = new(); // horse_id → race_ids voted
    public List<VoteHistoryItem>            Recent { get; set; } = new();
}

public sealed class VoteHistoryItem
{
    public string   HorseId { get; set; } = "";
    public string   Name    { get; set; } = "";
    public string   RaceId  { get; set; } = "";
    public string   Mark    { get; set; } = "";
    public DateTime VotedAt { get; set; }
}
