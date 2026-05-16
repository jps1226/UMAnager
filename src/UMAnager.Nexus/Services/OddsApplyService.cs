using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;
using UMAnager.Nexus.Services.Parsing;

namespace UMAnager.Nexus.Services;

public sealed class OddsApplyService
{
    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly ILogger<OddsApplyService> _logger;

    public OddsApplyService(IDbContextFactory<AppDbContext> dbFactory, ILogger<OddsApplyService> logger)
    {
        _dbFactory = dbFactory;
        _logger    = logger;
    }

    public async Task<(int RecordsProcessed, int EntriesUpdated)> ApplyAllPendingAsync(CancellationToken ct)
    {
        await using var db = await _dbFactory.CreateDbContextAsync(ct);

        var o1Records = await db.RawStagingRecords
            .Where(r => r.RecordType == "O1" && !r.IsProcessed)
            .AsNoTracking()
            .ToListAsync(ct);

        if (o1Records.Count == 0)
        {
            _logger.LogInformation("[OddsApply] No unprocessed O1 records found.");
            return (0, 0);
        }

        _logger.LogInformation("[OddsApply] Processing {Count} O1 records...", o1Records.Count);

        int entriesUpdated = 0;
        var processedIds   = new List<long>(o1Records.Count);

        foreach (var staging in o1Records)
        {
            try
            {
                var (raceId, slots) = O1RecordParser.Parse(staging.RawBytes.AsSpan());
                if (string.IsNullOrWhiteSpace(raceId) || slots.Count == 0)
                {
                    processedIds.Add(staging.Id);
                    continue;
                }

                foreach (var slot in slots)
                {
                    // Umaban (馬番) maps to PostPosition in race_entries.
                    var entry = await db.RaceEntries
                        .FirstOrDefaultAsync(e => e.RaceId == raceId && e.PostPosition == slot.HorseNumber, ct);

                    if (entry is null) continue;

                    entry.Odds    = slot.WinOdds > 0 ? slot.WinOdds : entry.Odds;
                    entry.FavRank = slot.FavRank > 0 ? slot.FavRank : entry.FavRank;
                    entriesUpdated++;
                }

                processedIds.Add(staging.Id);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[OddsApply] Failed to parse O1 record Id={Id}", staging.Id);
                processedIds.Add(staging.Id); // Mark as processed to avoid infinite retry
            }
        }

        // Flush entry updates.
        await db.SaveChangesAsync(ct);

        // Mark staging records as processed in a separate context to avoid tracking conflicts.
        await using var db2 = await _dbFactory.CreateDbContextAsync(ct);
        await db2.RawStagingRecords
            .Where(r => processedIds.Contains(r.Id))
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.IsProcessed, true), ct);

        _logger.LogInformation("[OddsApply] Done. Records={Records}, EntriesUpdated={Entries}",
            processedIds.Count, entriesUpdated);

        return (processedIds.Count, entriesUpdated);
    }
}
