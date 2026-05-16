using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using UMAnager.Nexus.Data;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Services.Parsing;

public class DifnRecordParsingService
{
    private readonly IDbContextFactory<AppDbContext> _contextFactory;
    private readonly ILogger<DifnRecordParsingService> _logger;

    public DifnRecordParsingService(
        IDbContextFactory<AppDbContext> contextFactory,
        ILogger<DifnRecordParsingService> logger)
    {
        _contextFactory = contextFactory;
        _logger = logger;
    }

    public async Task<ParsingStats> ParseAllRecordsAsync(CancellationToken ct = default)
    {
        var stats = new ParsingStats();
        var sw = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            _logger.LogInformation("Starting DIFN record parsing: UM → RA → SE");

            // Parse in strict order: UM first (no dependencies), then RA, then SE
            await ParseRecordsByTypeAsync("UM", ParseUmRecord, stats, ct);
            await ParseRecordsByTypeAsync("RA", ParseRaRecord, stats, ct);
            await ParseRecordsByTypeAsync("SE", ParseSeRecord, stats, ct);

            sw.Stop();
            stats.DurationMs = (int)sw.ElapsedMilliseconds;

            _logger.LogInformation(
                "DIFN parsing complete: UM={ParsedUm} RA={ParsedRa} SE={ParsedSe} Failed={Failed} Duration={Duration}ms",
                stats.ParsedUm, stats.ParsedRa, stats.ParsedSe, stats.FailedCount, stats.DurationMs);

            return stats;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DIFN parsing failed: {Error}", ex.Message);
            stats.ErrorMessage = ex.Message;
            return stats;
        }
    }

    private async Task ParseRecordsByTypeAsync<T>(
        string recordType,
        Func<byte[], T?> parseFunc,
        ParsingStats stats,
        CancellationToken ct)
        where T : class
    {
        const int BatchSize = 1000;  // kmy-keiba proven safe default

        using var context = _contextFactory.CreateDbContext();
        long lastProcessedId = 0;
        int batchNum = 0;

        while (true)
        {
            var batch = await context.RawStagingRecords
                .Where(r => r.RecordType == recordType && !r.IsProcessed && r.Id > lastProcessedId)
                .OrderBy(r => r.Id)
                .Take(BatchSize)
                .ToListAsync(ct);

            if (batch.Count == 0)
                break;

            batchNum++;
            var parsedEntities = new List<T>();
            var idsToMark = new List<long>();

            foreach (var rawRecord in batch)
            {
                try
                {
                    var parsed = parseFunc(rawRecord.RawBytes);
                    if (parsed != null)
                    {
                        parsedEntities.Add(parsed);
                        idsToMark.Add(rawRecord.Id);

                        switch (recordType)
                        {
                            case "UM":
                                stats.ParsedUm++;
                                break;
                            case "RA":
                                stats.ParsedRa++;
                                break;
                            case "SE":
                                stats.ParsedSe++;
                                break;
                        }
                    }
                }
                catch (Exception ex)
                {
                    stats.FailedCount++;
                    LogParsingError(recordType, rawRecord.RawBytes, ex);
                }
            }

            if (parsedEntities.Count > 0)
            {
                try
                {
                    // Use UPSERT (INSERT ... ON CONFLICT DO UPDATE) to handle duplicate keys
                    await UpsertEntitiesAsync(context, recordType, parsedEntities, ct);

                    _logger.LogInformation(
                        "[{RecordType}] Batch {BatchNum}: Saved {Count} records (Total: {Total})",
                        recordType, batchNum, parsedEntities.Count,
                        recordType == "UM" ? stats.ParsedUm : recordType == "RA" ? stats.ParsedRa : stats.ParsedSe);

                    if (idsToMark.Count > 0)
                    {
                        using var markContext = _contextFactory.CreateDbContext();
                        var recordsToMark = await markContext.RawStagingRecords
                            .Where(r => idsToMark.Contains(r.Id))
                            .ToListAsync(ct);

                        foreach (var record in recordsToMark)
                        {
                            record.IsProcessed = true;
                        }

                        await markContext.SaveChangesAsync(ct);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[{RecordType}] Batch {BatchNum} upsert failed: {Error}", recordType, batchNum, ex.Message);
                    stats.FailedCount += parsedEntities.Count;
                }
            }

            lastProcessedId = batch.Last().Id;
        }

        _logger.LogInformation("[{RecordType}] Completed {TotalBatches} batches", recordType, batchNum);
    }

    private async Task UpsertEntitiesAsync<T>(AppDbContext context, string recordType, List<T> entities, CancellationToken ct)
        where T : class
    {
        if (entities.Count == 0)
            return;

        switch (recordType)
        {
            case "UM":
                // Use raw SQL INSERT ... ON CONFLICT to handle duplicates server-side
                var horses = entities.Cast<Horse>().ToList();

                var values = new List<string>();
                var parameters = new List<object>();
                int paramIndex = 0;

                foreach (var horse in horses)
                {
                    var horseIdParam = $"@p{paramIndex}";
                    var nameJaParam = $"@p{paramIndex + 1}";
                    var nameEnParam = $"@p{paramIndex + 2}";
                    var birthYearParam = $"@p{paramIndex + 3}";
                    var sireIdParam = $"@p{paramIndex + 4}";
                    var damIdParam = $"@p{paramIndex + 5}";
                    var bmsIdParam = $"@p{paramIndex + 6}";

                    values.Add($"({horseIdParam}, {nameJaParam}, {nameEnParam}, {birthYearParam}, {sireIdParam}, {damIdParam}, {bmsIdParam}, NOW())");
                    parameters.Add(horse.HorseId ?? "");
                    parameters.Add(horse.NameJa ?? "");
                    parameters.Add(horse.NameEn ?? "");
                    parameters.Add(horse.BirthYear > 0 ? horse.BirthYear : (int?)null);
                    parameters.Add(horse.SireId ?? "");
                    parameters.Add(horse.DamId ?? "");
                    parameters.Add(horse.BmsId ?? "");

                    paramIndex += 7;
                }

                if (values.Count > 0)
                {
                    var sql = $@"
                        INSERT INTO horses (""HorseId"", ""NameJa"", ""NameEn"", ""BirthYear"", ""SireId"", ""DamId"", ""BmsId"", ""LastUpdated"")
                        VALUES {string.Join(", ", values)}
                        ON CONFLICT (""HorseId"") DO NOTHING";

                    await context.Database.ExecuteSqlRawAsync(sql, parameters.ToArray(), ct);
                }
                break;

            case "RA":
                var races = entities.Cast<Race>()
                    .GroupBy(r => r.RaceId)
                    .Select(g => g
                        .OrderByDescending(r => r.DataStatus)
                        .ThenByDescending(r => r.LastModified ?? DateOnly.MinValue)
                        .First())
                    .ToList();
                var raceValues = new List<string>();
                var raceParams = new List<object>();
                int raceParamIndex = 0;

                foreach (var race in races)
                {
                    raceValues.Add($"(@p{raceParamIndex}, @p{raceParamIndex+1}, @p{raceParamIndex+2}, @p{raceParamIndex+3}, @p{raceParamIndex+4}, @p{raceParamIndex+5}, @p{raceParamIndex+6}, @p{raceParamIndex+7}, @p{raceParamIndex+8}, @p{raceParamIndex+9}, NOW())");
                    raceParams.Add(race.RaceId ?? "");
                    raceParams.Add(race.RaceDate);
                    raceParams.Add(race.TrackCode ?? "");
                    raceParams.Add(race.RaceNumber);
                    raceParams.Add(race.NameJa ?? "");
                    raceParams.Add(race.Distance);
                    raceParams.Add(race.Surface ?? "");
                    raceParams.Add(race.DataStatus);
                    raceParams.Add((object?)race.LastModified ?? DBNull.Value);
                    raceParams.Add((object?)race.SortTime ?? DBNull.Value);
                    raceParamIndex += 10;
                }

                if (raceValues.Count > 0)
                {
                    var raceSql = $@"
                        INSERT INTO races (""RaceId"", ""RaceDate"", ""TrackCode"", ""RaceNumber"", ""NameJa"", ""Distance"", ""Surface"", ""DataStatus"", ""LastModified"", ""SortTime"", ""LastUpdated"")
                        VALUES {string.Join(", ", raceValues)}
                        ON CONFLICT (""RaceId"") DO UPDATE SET
                            ""TrackCode""    = excluded.""TrackCode"",
                            ""RaceNumber""   = excluded.""RaceNumber"",
                            ""NameJa""       = excluded.""NameJa"",
                            ""Distance""     = excluded.""Distance"",
                            ""Surface""      = excluded.""Surface"",
                            ""DataStatus""   = excluded.""DataStatus"",
                            ""LastModified"" = excluded.""LastModified"",
                            ""SortTime""     = excluded.""SortTime"",
                            ""LastUpdated""  = excluded.""LastUpdated""
                        WHERE excluded.""DataStatus"" > races.""DataStatus""
                           OR (excluded.""DataStatus"" = races.""DataStatus""
                               AND COALESCE(excluded.""LastModified"", '1900-01-01') > COALESCE(races.""LastModified"", '1900-01-01'))";

                    await context.Database.ExecuteSqlRawAsync(raceSql, raceParams.ToArray(), ct);
                }
                break;

            case "SE":
                var entries = entities.Cast<RaceEntry>()
                    .GroupBy(e => new { e.RaceId, e.HorseId })
                    .Select(g => g
                        .OrderByDescending(e => e.DataStatus)
                        .ThenByDescending(e => e.LastModified ?? DateOnly.MinValue)
                        .First())
                    .ToList();
                var seValues = new List<string>();
                var seParams = new List<object>();
                int seParamIndex = 0;

                foreach (var entry in entries)
                {
                    seValues.Add($"(@p{seParamIndex}, @p{seParamIndex+1}, @p{seParamIndex+2}, @p{seParamIndex+3}, @p{seParamIndex+4}, @p{seParamIndex+5}, @p{seParamIndex+6}, @p{seParamIndex+7}, @p{seParamIndex+8}, @p{seParamIndex+9}, @p{seParamIndex+10}, NOW())");
                    seParams.Add(entry.RaceId ?? "");
                    seParams.Add(entry.HorseId ?? "");
                    seParams.Add(entry.PostPosition);
                    seParams.Add(entry.Bracket);
                    seParams.Add(entry.Weight);
                    seParams.Add(entry.JockeyName ?? "");
                    // Preserve NULLs for pre-race rows. Pre-race SE records have empty bytes at
                    // offsets 360/364/335 (Odds/FavRank/FinishPos); the parser returns null. Coalescing
                    // to 0 here makes the UI think a race has happened. (Bug fix 2026-05-16.)
                    seParams.Add((object?)entry.Odds      ?? DBNull.Value);
                    seParams.Add((object?)entry.FavRank   ?? DBNull.Value);
                    seParams.Add((object?)entry.FinishPos ?? DBNull.Value);
                    seParams.Add(entry.DataStatus);
                    seParams.Add((object?)entry.LastModified ?? DBNull.Value);
                    seParamIndex += 11;
                }

                if (seValues.Count > 0)
                {
                    var seSql = $@"
                        INSERT INTO race_entries (""RaceId"", ""HorseId"", ""PostPosition"", ""Bracket"", ""Weight"", ""JockeyName"", ""Odds"", ""FavRank"", ""FinishPos"", ""DataStatus"", ""LastModified"", ""UpdatedAt"")
                        VALUES {string.Join(", ", seValues)}
                        ON CONFLICT (""RaceId"", ""HorseId"") DO UPDATE SET
                            ""PostPosition"" = excluded.""PostPosition"",
                            ""Bracket""      = excluded.""Bracket"",
                            ""Weight""       = excluded.""Weight"",
                            ""JockeyName""   = excluded.""JockeyName"",
                            ""Odds""         = excluded.""Odds"",
                            ""FavRank""      = excluded.""FavRank"",
                            ""FinishPos""    = excluded.""FinishPos"",
                            ""DataStatus""   = excluded.""DataStatus"",
                            ""LastModified"" = excluded.""LastModified"",
                            ""UpdatedAt""    = excluded.""UpdatedAt""
                        WHERE excluded.""DataStatus"" > race_entries.""DataStatus""
                           OR (excluded.""DataStatus"" = race_entries.""DataStatus""
                               AND COALESCE(excluded.""LastModified"", '1900-01-01') > COALESCE(race_entries.""LastModified"", '1900-01-01'))";

                    await context.Database.ExecuteSqlRawAsync(seSql, seParams.ToArray(), ct);
                }
                break;
        }
    }

    private Horse? ParseUmRecord(byte[] rawBytes)
    {
        try
        {
            return UmRecordParser.Parse(rawBytes);
        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException($"UM record parse error: {ex.Message}", ex);
        }
    }

    private Race? ParseRaRecord(byte[] rawBytes)
    {
        try
        {
            return RaRecordParser.Parse(rawBytes);
        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException($"RA record parse error: {ex.Message}", ex);
        }
    }

    private RaceEntry? ParseSeRecord(byte[] rawBytes)
    {
        try
        {
            return SeRecordParser.Parse(rawBytes);
        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException($"SE record parse error: {ex.Message}", ex);
        }
    }

    private void LogParsingError(string recordType, byte[] rawRecord, Exception ex)
    {
        // Only log hex for first few records to avoid massive log files
        // Subsequent errors just log the message
        var hexString = BitConverter.ToString(rawRecord.Take(50).ToArray()) + "...";
        _logger.LogDebug(
            "Parse error [{RecordType}]: {Error} | Hex (first 50 bytes): {Hex}",
            recordType, ex.Message, hexString);
    }
}

public class ParsingStats
{
    public int ParsedUm { get; set; }
    public int ParsedRa { get; set; }
    public int ParsedSe { get; set; }
    public int FailedCount { get; set; }
    public int DurationMs { get; set; }
    public string? ErrorMessage { get; set; }
}
