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

    public async Task<OddsApplyResult> ApplyAllPendingAsync(CancellationToken ct)
    {
        await using var db = await _dbFactory.CreateDbContextAsync(ct);

        var o1Records = await db.RawStagingRecords
            .Where(r => r.RecordType == "O1" && !r.IsProcessed)
            .AsNoTracking()
            .ToListAsync(ct);

        if (o1Records.Count == 0)
        {
            _logger.LogInformation("[OddsApply] No unprocessed O1 records found.");
            return new OddsApplyResult(0, 0, Array.Empty<string>());
        }

        _logger.LogInformation("[OddsApply] Processing {Count} O1 records...", o1Records.Count);

        int entriesUpdated = 0;
        var processedIds   = new List<long>(o1Records.Count);
        var touchedRaces   = new HashSet<string>(StringComparer.Ordinal);
        var historyRows    = new List<Data.Entities.OddsHistory>();
        var capturedAt     = DateTime.UtcNow; // one timestamp for this apply batch

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

                bool anyEntryTouched = false;
                foreach (var slot in slots)
                {
                    // Umaban (馬番) maps to PostPosition in race_entries.
                    var entry = await db.RaceEntries
                        .FirstOrDefaultAsync(e => e.RaceId == raceId && e.PostPosition == slot.HorseNumber, ct);

                    if (entry is null) continue;

                    if (slot.WinOdds > 0 && slot.WinOdds != entry.Odds)
                    {
                        entry.PrevOdds = entry.Odds; // snapshot before overwrite for ↑↓ indicator
                        entry.Odds     = slot.WinOdds;

                        // Phase 37: append a time-series point only when the value actually
                        // changed (this branch). Auto-dedupes consecutive identical odds.
                        historyRows.Add(new Data.Entities.OddsHistory
                        {
                            RaceId       = raceId,
                            HorseId      = entry.HorseId,
                            PostPosition = entry.PostPosition,
                            Odds         = slot.WinOdds,
                            FavRank      = slot.FavRank > 0 ? slot.FavRank : entry.FavRank,
                            CapturedAt   = capturedAt,
                        });
                    }
                    if (slot.FavRank > 0) entry.FavRank = slot.FavRank;
                    entriesUpdated++;
                    anyEntryTouched = true;
                }

                if (anyEntryTouched) touchedRaces.Add(raceId);
                processedIds.Add(staging.Id);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[OddsApply] Failed to parse O1 record Id={Id}", staging.Id);
                processedIds.Add(staging.Id); // Mark as processed to avoid infinite retry
            }
        }

        // Append odds-history snapshots in the same transaction as the entry updates.
        if (historyRows.Count > 0) db.OddsHistory.AddRange(historyRows);

        // Flush entry updates.
        await db.SaveChangesAsync(ct);

        // Mark staging records as processed in a separate context to avoid tracking conflicts.
        await using var db2 = await _dbFactory.CreateDbContextAsync(ct);
        await db2.RawStagingRecords
            .Where(r => processedIds.Contains(r.Id))
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.IsProcessed, true), ct);

        _logger.LogInformation("[OddsApply] Done. Records={Records}, EntriesUpdated={Entries}, Races={Races}, HistoryPoints={History}",
            processedIds.Count, entriesUpdated, touchedRaces.Count, historyRows.Count);

        return new OddsApplyResult(processedIds.Count, entriesUpdated, touchedRaces);
    }
}

public sealed record OddsApplyResult(int RecordsProcessed, int EntriesUpdated, IReadOnlyCollection<string> TouchedRaceIds);
