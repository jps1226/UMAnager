namespace UMAnager.Ingestion.Service;

using UMAnager.Common;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly SyncStateRepository _syncStateRepo;
    private readonly JVLinkClient _jvLink;
    private readonly IConfiguration _config;

    public Worker(
        ILogger<Worker> logger,
        SyncStateRepository syncStateRepo,
        JVLinkClient jvLink,
        IConfiguration config)
    {
        _logger = logger;
        _syncStateRepo = syncStateRepo;
        _jvLink = jvLink;
        _config = config;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("UMAnager Ingestion Service starting");

        try
        {
            // Initialize database schema
            await _syncStateRepo.InitializeDatabaseAsync();

            // Initialize JV-Link
            string softwareId = _config["JVLink:SoftwareId"] ?? "UMANAGER20";
            _jvLink.Initialize(softwareId);

            // Load sync state
            var syncState = await _syncStateRepo.LoadAsync();
            _logger.LogInformation(
                "Loaded sync state: umTimestamp={UmTimestamp}, racesTimestamp={RacesTimestamp}, syncCount={SyncCount}",
                syncState.LastTimestampUm, syncState.LastTimestampRaces, syncState.SyncCount);

            // Run first sync immediately, then wait for scheduled execution
            await PerformSyncAsync(syncState, stoppingToken);

            _logger.LogInformation("Initial sync complete. Service running in idle mode.");
            _logger.LogInformation("Scheduled data pulls: Thursday/Friday/Saturday (implemented in Phase 3)");

            // Keep the service running until cancellation
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Service cancellation requested");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Unhandled exception in Worker");
            await _syncStateRepo.RecordErrorAsync($"Unhandled exception: {ex.Message}");
            throw;
        }
        finally
        {
            _logger.LogInformation("UMAnager Ingestion Service stopping");
            _jvLink?.Close();
            _jvLink?.Dispose();
        }
    }

    private async Task PerformSyncAsync(SyncState syncState, CancellationToken cancellationToken)
    {
        try
        {
            string serviceKey = _config["JVLink:ServiceKey"] ?? "";
            if (string.IsNullOrEmpty(serviceKey))
                throw new InvalidOperationException("JVLink:ServiceKey not configured");

            _logger.LogInformation("Starting data sync");

            // Phase 2: Bootstrap horse master data
            bool needsBootstrap = _syncStateRepo.NeedsBootstrap(syncState);
            if (needsBootstrap)
            {
                _logger.LogInformation("Performing master data bootstrap (Option=4). User interaction may be required.");
                await PerformBootstrapAsync(syncState, cancellationToken);
            }

            // Phase 3: Fetch weekly races (TOKURACETCOV with option=2)
            _logger.LogInformation("Performing weekly data fetch (Option=2)");
            await PerformWeeklyFetchAsync(syncState, cancellationToken);

            _logger.LogInformation("Data sync complete");
        }
        catch (JVLinkException ex)
        {
            _logger.LogError(ex, "JV-Link error during sync: {ErrorCode}", ex.ErrorCode);
            await _syncStateRepo.RecordErrorAsync($"JV-Link error: {ex.ErrorCode}");
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during data sync");
            await _syncStateRepo.RecordErrorAsync($"Sync error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Phase 2: Bootstrap horse master data (UM records) from JRA-VAN.
    /// Opens DIFN dataspec with Option=4 (setup, downloads all historical data).
    /// Reads UM records, parses them, and inserts into horses table.
    /// </summary>
    private async Task PerformBootstrapAsync(SyncState syncState, CancellationToken cancellationToken)
    {
        const string dataSpec = "DIFN";
        const int bootstrapOption = 4; // Option 4: setup mode, downloads all data
        string fromTime = "19860101000000"; // JRA-VAN era start (1986-01-01)

        // Disable FK constraints during bulk import (allows self-referential inserts in any order)
        await _syncStateRepo.DisableForeignKeyConstraintsAsync();

        try
        {
            _logger.LogInformation("Opening {DataSpec} with Option={Option} (bootstrap mode)", dataSpec, bootstrapOption);

            var openResult = await _jvLink.OpenAndWaitForDownloadAsync(dataSpec, fromTime, bootstrapOption, cancellationToken);
        _logger.LogInformation(
            "Download complete: readCount={ReadCount}, downloadCount={DownloadCount}, lastFileTimestamp={LastFileTimestamp}",
            openResult.ReadCount, openResult.DownloadCount, openResult.LastFileTimestamp);

        // Read UM records in a loop
        int umRecordsRead = 0;
        int umRecordsInserted = 0;
        int umRecordsFailed = 0;
        int totalRecordsProcessed = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            var result = _jvLink.ReadRecord();

            // Status codes:
            // > 0 = bytes read
            // -1 = file boundary (end of current file, timestamp updated)
            // 0 = EOF (done reading all files)
            if (result.StatusCode == 0)
            {
                _logger.LogInformation("EOF reached. Total records processed: {Total}", totalRecordsProcessed);
                break;
            }

            if (result.StatusCode == -1)
            {
                // File boundary encountered
                // Per JRA-VAN spec for Option=4: save the current timestamp and continue
                _logger.LogInformation("File boundary after {Records} UM records. Last file: {FileName}", umRecordsRead, result.FileName);

                // Save the current file timestamp at this boundary for crash recovery
                string currentTimestamp = _jvLink.GetCurrentFileTimestamp();
                if (!string.IsNullOrEmpty(currentTimestamp))
                {
                    try
                    {
                        await _syncStateRepo.SaveTimestampAsync("DIFN", currentTimestamp);
                        _logger.LogInformation("Saved DIFN file boundary timestamp: {Timestamp}", currentTimestamp);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to save file boundary timestamp");
                    }
                }

                // Continue reading next file — JVRead continues the stream automatically
                continue;
            }

            if (result.StatusCode <= -2)
            {
                _logger.LogError("Error reading record: status={Status}", result.StatusCode);
                continue;
            }

            // Log first few records to understand structure
            totalRecordsProcessed++;
            if (totalRecordsProcessed <= 5)
            {
                _logger.LogDebug("Record {Num}: status={Status}, size={Size}, data={Data}",
                    totalRecordsProcessed, result.StatusCode, result.Data.Length,
                    result.Data.Length > 0 ? System.Text.Encoding.UTF8.GetString(result.Data[..Math.Min(50, result.Data.Length)]) : "(empty)");
            }

            // Decode the raw bytes as CP932 (Shift-JIS)
            string recordLine = JVEncoding.DecodeRecord(result.Data);

            // Strip CR/LF from end of record (JRA-VAN terminates every record with 0x0D 0x0A)
            recordLine = recordLine.TrimEnd('\r', '\n');

            // Check record type (first 2 characters)
            if (recordLine.Length < 2)
            {
                continue;
            }

            string recordType = recordLine[..2];

            // Log first few record types to debug
            if (umRecordsRead + umRecordsFailed < 50)
                _logger.LogDebug("Record type: {Type} (first 20 chars: {Preview})", recordType, recordLine[..Math.Min(20, recordLine.Length)]);

            // Only process UM (Horse Master) records; skip all others
            if (recordType != "UM")
            {
                // Skip unwanted record types using JVSkip to avoid processing them
                if (recordType is "RA" or "SE" or "KS" or "CH" or "BR" or "BN" or "RC")
                {
                    _jvLink.Skip();
                }
                // Don't count skipped records as failures
                continue;
            }

            // Try to parse as UM record
            var horse = UMRecordParser.ParseUMRecord(recordLine);
            if (horse == null)
            {
                umRecordsFailed++;
                continue;
            }

            // Validate pedigree completeness
            if (!UMRecordParser.HasCompletePedigree(horse))
            {
                _logger.LogWarning(
                    "Incomplete pedigree for horse {HorseId}: sire={Sire}, dam={Dam}, bms={BMS}",
                    horse.HorseId, horse.SireId, horse.DamId, horse.BroodmareSireId);
            }

            // Insert or update horse in database
            try
            {
                await _syncStateRepo.InsertOrUpdateHorseAsync(horse);
                umRecordsInserted++;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to insert horse {HorseId}", horse.HorseId);
                umRecordsFailed++;
            }

            umRecordsRead++;

            // Log progress every 500 records
            if (umRecordsRead % 500 == 0)
                _logger.LogInformation("Progress: {ReadCount} records read, {InsertedCount} inserted", umRecordsRead, umRecordsInserted);
        }

            _logger.LogInformation(
                "Bootstrap complete: {ReadCount} records read, {InsertedCount} inserted, {FailedCount} failed",
                umRecordsRead, umRecordsInserted, umRecordsFailed);

            // Update sync state with new timestamp
            if (!string.IsNullOrEmpty(openResult.LastFileTimestamp))
            {
                await _syncStateRepo.SaveTimestampAsync("DIFN", openResult.LastFileTimestamp);
                _logger.LogInformation("Sync state updated: lastFileTimestamp={Timestamp}", openResult.LastFileTimestamp);
            }

            _jvLink.Close();
        }
        finally
        {
            // Re-enable FK constraints after bulk import
            await _syncStateRepo.EnableForeignKeyConstraintsAsync();
        }
    }

    /// <summary>
    /// Phase 3: Fetch weekly race and entry data (RA, SE, UM records).
    /// Opens TOKURACETCOV dataspec with Option=2 (incremental, respects FromTime).
    /// Parses RA (race), SE (entry), and UM (horse master) records.
    /// Saves timestamp per file boundary (-1) for crash recovery.
    /// </summary>
    private async Task PerformWeeklyFetchAsync(SyncState syncState, CancellationToken cancellationToken)
    {
        const string dataSpec = "TOKURACETCOV"; // 4 dataspecs: TOKU (race entries), RACE (race meta), TCOV (UM pedigree), no SNPN (CK stats)
        const int weeklyOption = 2; // Option 2: incremental fetch within current week cycle
        string fromTime = _syncStateRepo.GetFromTime(syncState, "RACE"); // Use last saved races timestamp

        _logger.LogInformation("Opening {DataSpec} with Option={Option} (weekly fetch). FromTime={FromTime}", dataSpec, weeklyOption, fromTime);

        var openResult = await _jvLink.OpenAndWaitForDownloadAsync(dataSpec, fromTime, weeklyOption, cancellationToken);

        if (openResult.NoData)
        {
            _logger.LogInformation("Weekly fetch: no new data since {FromTime}", fromTime);
            return;
        }

        _logger.LogInformation(
            "Download complete: readCount={ReadCount}, downloadCount={DownloadCount}, lastFileTimestamp={LastFileTimestamp}",
            openResult.ReadCount, openResult.DownloadCount, openResult.LastFileTimestamp);

        // Read records in a loop, dispatching by record type
        int raRecordsRead = 0, raRecordsInserted = 0, raRecordsFailed = 0;
        int seRecordsRead = 0, seRecordsInserted = 0, seRecordsFailed = 0;
        int umRecordsRead = 0, umRecordsInserted = 0, umRecordsFailed = 0;
        string currentRaceId = "";

        while (!cancellationToken.IsCancellationRequested)
        {
            var result = _jvLink.ReadRecord();

            // Status codes:
            // > 0 = bytes read
            // -1 = file boundary (save timestamp, move to next file)
            // 0 = EOF (done reading)
            if (result.StatusCode == 0)
            {
                _logger.LogInformation("EOF reached");
                break;
            }

            if (result.StatusCode == -1)
            {
                // File boundary: save timestamp for crash recovery
                _logger.LogDebug("File boundary encountered. Saving timestamp.");
                string currentFileTimestamp = _jvLink.GetCurrentFileTimestamp();
                if (!string.IsNullOrEmpty(currentFileTimestamp))
                {
                    try
                    {
                        await _syncStateRepo.SaveTimestampAsync("RACE", currentFileTimestamp);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to save timestamp for file boundary");
                    }
                }
                continue;
            }

            if (result.StatusCode <= -2)
            {
                _logger.LogError("Error reading record: status={Status}", result.StatusCode);
                continue;
            }

            // Decode the raw bytes as CP932 (Shift-JIS)
            string recordLine = JVEncoding.DecodeRecord(result.Data);

            // Dispatch by record type (first 2 bytes)
            if (recordLine.Length < 2)
                continue;

            string recordType = recordLine.Substring(0, 2);

            if (recordType == "RA")
            {
                raRecordsRead++;
                var race = RARecordParser.ParseRARecord(recordLine);
                if (race == null)
                {
                    raRecordsFailed++;
                    continue;
                }

                try
                {
                    await _syncStateRepo.InsertOrUpdateRaceAsync(race);
                    raRecordsInserted++;
                    currentRaceId = race.RaceId;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to insert race {RaceId}", race.RaceId);
                    raRecordsFailed++;
                }
            }
            else if (recordType == "SE")
            {
                seRecordsRead++;
                var entry = SERecordParser.ParseSERecord(recordLine, currentRaceId);
                if (entry == null)
                {
                    seRecordsFailed++;
                    continue;
                }

                try
                {
                    await _syncStateRepo.InsertOrUpdateRaceEntryAsync(entry);
                    seRecordsInserted++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to insert entry {RaceId}/{HorseId}", entry.RaceId, entry.HorseId);
                    seRecordsFailed++;
                }
            }
            else if (recordType == "UM")
            {
                umRecordsRead++;
                var horse = UMRecordParser.ParseUMRecord(recordLine);
                if (horse == null)
                {
                    umRecordsFailed++;
                    continue;
                }

                try
                {
                    await _syncStateRepo.InsertOrUpdateHorseAsync(horse);
                    umRecordsInserted++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to insert horse {HorseId}", horse.HorseId);
                    umRecordsFailed++;
                }
            }
            // Silently skip other record types (e.g., CK which we're not downloading anyway)

            // Log progress every 100 records total
            int totalRead = raRecordsRead + seRecordsRead + umRecordsRead;
            if (totalRead > 0 && totalRead % 100 == 0)
                _logger.LogInformation(
                    "Progress: RA={RaRead}/{RaInserted}, SE={SeRead}/{SeInserted}, UM={UmRead}/{UmInserted}",
                    raRecordsRead, raRecordsInserted, seRecordsRead, seRecordsInserted, umRecordsRead, umRecordsInserted);
        }

        int totalInserted = raRecordsInserted + seRecordsInserted + umRecordsInserted;
        int totalFailed = raRecordsFailed + seRecordsFailed + umRecordsFailed;

        _logger.LogInformation(
            "Weekly fetch complete: RA={RaRead}/{RaInserted}/{RaFailed}, SE={SeRead}/{SeInserted}/{SeFailed}, UM={UmRead}/{UmInserted}/{UmFailed} (total inserted={TotalInserted})",
            raRecordsRead, raRecordsInserted, raRecordsFailed,
            seRecordsRead, seRecordsInserted, seRecordsFailed,
            umRecordsRead, umRecordsInserted, umRecordsFailed,
            totalInserted);

        // Final timestamp save (should have been saved per file, but do it again to be sure)
        if (!string.IsNullOrEmpty(openResult.LastFileTimestamp))
        {
            try
            {
                await _syncStateRepo.SaveTimestampAsync("RACE", openResult.LastFileTimestamp);
                _logger.LogInformation("Final sync state update: lastFileTimestamp={Timestamp}", openResult.LastFileTimestamp);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to save final timestamp");
            }
        }

        _jvLink.Close();
    }
}
