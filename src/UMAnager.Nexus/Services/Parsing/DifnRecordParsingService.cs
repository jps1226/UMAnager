// ============================================================
// FILE: DifnRecordParsingService.cs
// LAYER: Parsing (orchestrator)
// PURPOSE: Drains raw_staging in strict order UM→RA→SE→HR→O2/O5, calling each *RecordParser and
//          UPSERTing with DataStatus-guarded conflict resolution (only newer data wins). HR mutates
//          races.ResultsJson; O2/O5 merge into races.OddsJson. Marks staging rows IsProcessed.
// KEY DEPENDENCIES: AppDbContext, all *RecordParser classes.
// CALLED BY: NexusPipeServer after every STREAM_*_COMPLETE (and JvLinkController.parse-records).
// CAUTION: Idempotent (only !IsProcessed rows). Uses typed NpgsqlParameter for nullables (DBNull).
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using UMAnager.Nexus.Data;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Services.Parsing;

public class DifnRecordParsingService
{
    private readonly IDbContextFactory<AppDbContext> _contextFactory;
    private readonly ILogger<DifnRecordParsingService> _logger;
    private readonly PipelineHealthService _health;

    public DifnRecordParsingService(
        IDbContextFactory<AppDbContext> contextFactory,
        ILogger<DifnRecordParsingService> logger,
        PipelineHealthService health)
    {
        _contextFactory = contextFactory;
        _logger = logger;
        _health = health;
    }

    public async Task<ParsingStats> ParseAllRecordsAsync(CancellationToken ct = default)
    {
        var stats = new ParsingStats();
        var sw = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            _logger.LogInformation("Starting DIFN record parsing: UM → RA → SE");

            // Parse in strict order: UM first (no dependencies), then RA, then SE,
            // then HR (race payouts — references races by RaceId, so must come after RA),
            // then O2 + O5 odds (also reference races by RaceId; merge into OddsJson).
            await ParseRecordsByTypeAsync("UM", ParseUmRecord, stats, ct);
            await ParseRecordsByTypeAsync("RA", ParseRaRecord, stats, ct);
            await ParseRecordsByTypeAsync("SE", ParseSeRecord, stats, ct);
            await ParseHrRecordsAsync(stats, ct);
            await ParseOddsRecordsAsync(stats, ct);

            sw.Stop();
            stats.DurationMs = (int)sw.ElapsedMilliseconds;

            _logger.LogInformation(
                "DIFN parsing complete: UM={ParsedUm} RA={ParsedRa} SE={ParsedSe} HR={ParsedHr} O2={O2} O5={O5} Failed={Failed} Duration={Duration}ms",
                stats.ParsedUm, stats.ParsedRa, stats.ParsedSe, stats.ParsedHr, stats.ParsedO2, stats.ParsedO5, stats.FailedCount, stats.DurationMs);

            _health.RecordSuccess("parse");   // T1-1: the parse completed (no throw) — a healthy tick.
            return stats;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DIFN parsing failed: {Error}", ex.Message);
            stats.ErrorMessage = ex.Message;
            _health.RecordFailure("parse", ex.Message);   // T1-1: was silent before — now alerts on repeat.
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
                    raceValues.Add($"(@p{raceParamIndex}, @p{raceParamIndex+1}, @p{raceParamIndex+2}, @p{raceParamIndex+3}, @p{raceParamIndex+4}, @p{raceParamIndex+5}, @p{raceParamIndex+6}, @p{raceParamIndex+7}, @p{raceParamIndex+8}, @p{raceParamIndex+9}, @p{raceParamIndex+10}, NOW())");
                    raceParams.Add(NullableParam($"p{raceParamIndex + 0}", NpgsqlDbType.Text,        race.RaceId ?? ""));
                    raceParams.Add(NullableParam($"p{raceParamIndex + 1}", NpgsqlDbType.TimestampTz, DateTime.SpecifyKind(race.RaceDate, DateTimeKind.Utc)));
                    raceParams.Add(NullableParam($"p{raceParamIndex + 2}", NpgsqlDbType.Text,        race.TrackCode));
                    raceParams.Add(NullableParam($"p{raceParamIndex + 3}", NpgsqlDbType.Integer,     race.RaceNumber));
                    raceParams.Add(NullableParam($"p{raceParamIndex + 4}", NpgsqlDbType.Text,        race.NameJa));
                    raceParams.Add(NullableParam($"p{raceParamIndex + 5}", NpgsqlDbType.Integer,     race.Distance));
                    raceParams.Add(NullableParam($"p{raceParamIndex + 6}", NpgsqlDbType.Text,        race.Surface));
                    raceParams.Add(NullableParam($"p{raceParamIndex + 7}", NpgsqlDbType.Smallint,    race.DataStatus));
                    raceParams.Add(NullableParam($"p{raceParamIndex + 8}", NpgsqlDbType.Date,        race.LastModified));
                    raceParams.Add(NullableParam($"p{raceParamIndex + 9}", NpgsqlDbType.TimestampTz, race.SortTime));
                    raceParams.Add(NullableParam($"p{raceParamIndex + 10}", NpgsqlDbType.Text,       race.RaceClass));
                    raceParamIndex += 11;
                }

                if (raceValues.Count > 0)
                {
                    var raceSql = $@"
                        INSERT INTO races (""RaceId"", ""RaceDate"", ""TrackCode"", ""RaceNumber"", ""NameJa"", ""Distance"", ""Surface"", ""DataStatus"", ""LastModified"", ""SortTime"", ""RaceClass"", ""LastUpdated"")
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
                            ""RaceClass""    = COALESCE(excluded.""RaceClass"", races.""RaceClass""),
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
                    seValues.Add($"(@p{seParamIndex}, @p{seParamIndex+1}, @p{seParamIndex+2}, @p{seParamIndex+3}, @p{seParamIndex+4}, @p{seParamIndex+5}, @p{seParamIndex+6}, @p{seParamIndex+7}, @p{seParamIndex+8}, @p{seParamIndex+9}, @p{seParamIndex+10}, @p{seParamIndex+11}, @p{seParamIndex+12}, NOW())");
                    seParams.Add(NullableParam($"p{seParamIndex + 0}",  NpgsqlDbType.Text,     entry.RaceId ?? ""));
                    seParams.Add(NullableParam($"p{seParamIndex + 1}",  NpgsqlDbType.Text,     entry.HorseId ?? ""));
                    seParams.Add(NullableParam($"p{seParamIndex + 2}",  NpgsqlDbType.Integer,  entry.PostPosition));
                    seParams.Add(NullableParam($"p{seParamIndex + 3}",  NpgsqlDbType.Integer,  entry.Bracket));
                    seParams.Add(NullableParam($"p{seParamIndex + 4}",  NpgsqlDbType.Integer,  entry.Weight));
                    seParams.Add(NullableParam($"p{seParamIndex + 5}",  NpgsqlDbType.Text,     entry.JockeyName));
                    // Preserve NULLs for pre-race rows. Pre-race SE records have empty bytes at
                    // offsets 360/364/335 (Odds/FavRank/FinishPos); the parser returns null. Coalescing
                    // to 0 here makes the UI think a race has happened. (Bug fix 2026-05-16.)
                    seParams.Add(NullableParam($"p{seParamIndex + 6}",  NpgsqlDbType.Numeric,  entry.Odds));
                    seParams.Add(NullableParam($"p{seParamIndex + 7}",  NpgsqlDbType.Integer,  entry.FavRank));
                    seParams.Add(NullableParam($"p{seParamIndex + 8}",  NpgsqlDbType.Integer,  entry.FinishPos));
                    seParams.Add(NullableParam($"p{seParamIndex + 9}",  NpgsqlDbType.Smallint, entry.DataStatus));
                    seParams.Add(NullableParam($"p{seParamIndex + 10}", NpgsqlDbType.Date,     entry.LastModified));
                    seParams.Add(NullableParam($"p{seParamIndex + 11}", NpgsqlDbType.Varchar,  entry.JockeyCode));
                    seParams.Add(NullableParam($"p{seParamIndex + 12}", NpgsqlDbType.Varchar,  entry.TrainerCode));
                    seParamIndex += 13;
                }

                if (seValues.Count > 0)
                {
                    var seSql = $@"
                        INSERT INTO race_entries (""RaceId"", ""HorseId"", ""PostPosition"", ""Bracket"", ""Weight"", ""JockeyName"", ""Odds"", ""FavRank"", ""FinishPos"", ""DataStatus"", ""LastModified"", ""JockeyCode"", ""TrainerCode"", ""UpdatedAt"")
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
                            ""JockeyCode""   = COALESCE(excluded.""JockeyCode"", race_entries.""JockeyCode""),
                            ""TrainerCode""  = COALESCE(excluded.""TrainerCode"", race_entries.""TrainerCode""),
                            ""UpdatedAt""    = excluded.""UpdatedAt""
                        WHERE excluded.""DataStatus"" > race_entries.""DataStatus""
                           OR (excluded.""DataStatus"" = race_entries.""DataStatus""
                               AND COALESCE(excluded.""LastModified"", '1900-01-01') > COALESCE(race_entries.""LastModified"", '1900-01-01'))";

                    await context.Database.ExecuteSqlRawAsync(seSql, seParams.ToArray(), ct);
                }
                break;
        }
    }

    // EF Core's ExecuteSqlRawAsync routes plain object[] params through its type-mapping layer,
    // which has no mapping for System.DBNull — passing DBNull.Value crashes the batch. Typed
    // NpgsqlParameters bypass that mapping and accept DBNull.Value directly.
    private static NpgsqlParameter NullableParam(string name, NpgsqlDbType type, object? value)
        => new(name, type) { Value = value ?? DBNull.Value };

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

    /// <summary>
    /// Parse HR (Haray / payout) records and UPDATE races.results_json with the JSON blob.
    /// Doesn't fit the entity-upsert pipeline because HR mutates existing rows rather than
    /// inserting new ones — separate batched UPDATE per chunk.
    /// </summary>
    private async Task ParseHrRecordsAsync(ParsingStats stats, CancellationToken ct)
    {
        const int BatchSize = 500;
        using var context = _contextFactory.CreateDbContext();
        long lastProcessedId = 0;
        int batchNum = 0;

        while (true)
        {
            var batch = await context.RawStagingRecords
                .Where(r => r.RecordType == "HR" && !r.IsProcessed && r.Id > lastProcessedId)
                .OrderBy(r => r.Id)
                .Take(BatchSize)
                .ToListAsync(ct);
            if (batch.Count == 0) break;
            batchNum++;

            var parsed = new List<(string RaceId, string Json)>();
            var idsToMark = new List<long>();
            foreach (var raw in batch)
            {
                try
                {
                    var result = HrRecordParser.Parse(raw.RawBytes);
                    if (result != null)
                    {
                        parsed.Add((result.RaceId, result.ResultsJson));
                        idsToMark.Add(raw.Id);
                        stats.ParsedHr++;
                    }
                    else
                    {
                        // Unparseable (e.g. truncated) — mark processed so we don't retry forever.
                        idsToMark.Add(raw.Id);
                    }
                }
                catch (Exception ex)
                {
                    stats.FailedCount++;
                    LogParsingError("HR", raw.RawBytes, ex);
                }
            }

            // Dedupe by RaceId — if multiple HR records arrive for the same race
            // (provisional then final), keep the last (later in batch order ≈ later in file).
            var byRace = parsed.GroupBy(p => p.RaceId)
                .ToDictionary(g => g.Key, g => g.Last().Json);

            if (byRace.Count > 0)
            {
                // Single batched UPDATE using a VALUES clause: cheaper than N round-trips.
                var values = new List<string>(byRace.Count);
                var pgParams = new List<object>(byRace.Count * 2);
                int idx = 0;
                foreach (var kv in byRace)
                {
                    values.Add($"(@p{idx}, @p{idx + 1}::jsonb)");
                    pgParams.Add(new NpgsqlParameter($"p{idx}", NpgsqlDbType.Text) { Value = kv.Key });
                    pgParams.Add(new NpgsqlParameter($"p{idx + 1}", NpgsqlDbType.Text) { Value = kv.Value });
                    idx += 2;
                }
                var sql = $@"
                    UPDATE races AS r
                    SET ""ResultsJson"" = v.json, ""LastUpdated"" = NOW()
                    FROM (VALUES {string.Join(", ", values)}) AS v(race_id, json)
                    WHERE r.""RaceId"" = v.race_id";
                await context.Database.ExecuteSqlRawAsync(sql, pgParams.ToArray(), ct);

                _logger.LogInformation("[HR] Batch {BatchNum}: Updated {Count} races (Total: {Total})",
                    batchNum, byRace.Count, stats.ParsedHr);
            }

            if (idsToMark.Count > 0)
            {
                using var markContext = _contextFactory.CreateDbContext();
                var toMark = await markContext.RawStagingRecords.Where(r => idsToMark.Contains(r.Id)).ToListAsync(ct);
                foreach (var rec in toMark) rec.IsProcessed = true;
                await markContext.SaveChangesAsync(ct);
            }

            lastProcessedId = batch.Last().Id;
        }

        _logger.LogInformation("[HR] Completed {TotalBatches} batches", batchNum);
    }

    /// <summary>
    /// Parse O2 (Quinella) + O5 (Trio) odds records and merge into races.OddsJson.
    /// Both record types produce slot arrays that go into the same JSON blob under
    /// "quinella" and "trio" keys respectively. Latest record wins per race.
    /// </summary>
    private async Task ParseOddsRecordsAsync(ParsingStats stats, CancellationToken ct)
    {
        await ParseOddsTypeAsync("O2", "quinella", OddsRecordParser.ParseO2,
            v => stats.ParsedO2 = v, stats, ct);
        await ParseOddsTypeAsync("O5", "trio", OddsRecordParser.ParseO5,
            v => stats.ParsedO5 = v, stats, ct);
    }

    private async Task ParseOddsTypeAsync(
        string recordType,
        string jsonKey,
        Func<byte[], OddsRecordParser.OddsParseResult?> parseFunc,
        Action<int> assignCounter,
        ParsingStats stats,
        CancellationToken ct)
    {
        const int BatchSize = 200;  // smaller — O5 records are 12KB each
        using var context = _contextFactory.CreateDbContext();
        long lastProcessedId = 0;
        int batchNum = 0;
        int totalParsed = 0;

        while (true)
        {
            var batch = await context.RawStagingRecords
                .Where(r => r.RecordType == recordType && !r.IsProcessed && r.Id > lastProcessedId)
                .OrderBy(r => r.Id)
                .Take(BatchSize)
                .ToListAsync(ct);
            if (batch.Count == 0) break;
            batchNum++;

            // Per race: latest record wins (provisional → final).
            var bySlot = new Dictionary<string, List<OddsRecordParser.OddsSlot>>();
            var idsToMark = new List<long>();
            foreach (var raw in batch)
            {
                try
                {
                    var parsed = parseFunc(raw.RawBytes);
                    if (parsed != null)
                    {
                        bySlot[parsed.RaceId] = parsed.Slots; // overwrite — last wins
                        totalParsed++;
                    }
                    idsToMark.Add(raw.Id);
                }
                catch (Exception ex)
                {
                    stats.FailedCount++;
                    LogParsingError(recordType, raw.RawBytes, ex);
                }
            }

            if (bySlot.Count > 0)
            {
                // Merge: jsonb_set(coalesce(OddsJson, '{}'::jsonb), '{key}', new_array).
                // Per-race UPDATE in one batched statement using FROM (VALUES ...).
                var values = new List<string>(bySlot.Count);
                var pgParams = new List<object>(bySlot.Count * 2);
                int idx = 0;
                foreach (var kv in bySlot)
                {
                    var slotJson = System.Text.Json.JsonSerializer.Serialize(
                        kv.Value.Select(s => new { combo = s.Combo, odds = s.Odds, rank = s.Rank }));
                    values.Add($"(@p{idx}, @p{idx + 1}::jsonb)");
                    pgParams.Add(new NpgsqlParameter($"p{idx}", NpgsqlDbType.Text)
                        { Value = kv.Key });
                    pgParams.Add(new NpgsqlParameter($"p{idx + 1}", NpgsqlDbType.Text)
                        { Value = slotJson });
                    idx += 2;
                }
                // Merge into OddsJson via the JSONB || operator. Both the empty default
                // and the new object are built with jsonb_build_object() — avoids any
                // literal {braces} in the SQL that EF's String.Format would choke on.
                var jsonKeyParam = $"@k_{batchNum}";
                pgParams.Add(new NpgsqlParameter(jsonKeyParam.TrimStart('@'), NpgsqlDbType.Text) { Value = jsonKey });
                var sql = "UPDATE races AS r " +
                          "SET \"OddsJson\" = COALESCE(r.\"OddsJson\", jsonb_build_object()) || jsonb_build_object(" + jsonKeyParam + ", v.slots), " +
                          "\"LastUpdated\" = NOW() " +
                          "FROM (VALUES " + string.Join(", ", values) + ") AS v(race_id, slots) " +
                          "WHERE r.\"RaceId\" = v.race_id";
                await context.Database.ExecuteSqlRawAsync(sql, pgParams.ToArray(), ct);

                _logger.LogInformation("[{Type}] Batch {Batch}: Updated {Count} races ({Key}, Total: {Total})",
                    recordType, batchNum, bySlot.Count, jsonKey, totalParsed);
            }

            if (idsToMark.Count > 0)
            {
                using var markContext = _contextFactory.CreateDbContext();
                var toMark = await markContext.RawStagingRecords.Where(r => idsToMark.Contains(r.Id)).ToListAsync(ct);
                foreach (var rec in toMark) rec.IsProcessed = true;
                await markContext.SaveChangesAsync(ct);
            }

            lastProcessedId = batch.Last().Id;
        }

        assignCounter(totalParsed);
        _logger.LogInformation("[{Type}] Completed {Batches} batches, {Total} parsed", recordType, batchNum, totalParsed);
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
    public int ParsedHr { get; set; }
    public int ParsedO2 { get; set; }
    public int ParsedO5 { get; set; }
    public int FailedCount { get; set; }
    public int DurationMs { get; set; }
    public string? ErrorMessage { get; set; }
}
