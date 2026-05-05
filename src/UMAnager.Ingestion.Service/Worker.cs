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
            string softwareId = _config["JVLink:SoftwareId"] ?? "UMANAGER-2.0";
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

            // Phase 3: TODO - Pull weekly races (TOKURACETCOVSNPN) with option=2

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

        _logger.LogInformation("Opening {DataSpec} with Option={Option} (bootstrap mode)", dataSpec, bootstrapOption);

        var openResult = await _jvLink.OpenAndWaitForDownloadAsync(dataSpec, fromTime, bootstrapOption, cancellationToken);
        _logger.LogInformation(
            "Download complete: readCount={ReadCount}, downloadCount={DownloadCount}, lastFileTimestamp={LastFileTimestamp}",
            openResult.ReadCount, openResult.DownloadCount, openResult.LastFileTimestamp);

        // Read UM records in a loop
        int umRecordsRead = 0;
        int umRecordsInserted = 0;
        int umRecordsFailed = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            var result = _jvLink.ReadRecord();

            // Status codes:
            // > 0 = bytes read
            // -1 = file boundary (move to next file)
            // 0 = EOF (done reading)
            if (result.StatusCode == 0)
            {
                _logger.LogInformation("EOF reached");
                break;
            }

            if (result.StatusCode == -1)
            {
                _logger.LogDebug("File boundary encountered");
                continue;
            }

            if (result.StatusCode <= -2)
            {
                _logger.LogError("Error reading record: status={Status}", result.StatusCode);
                continue;
            }

            // Decode the raw bytes as CP932 (Shift-JIS)
            string recordLine = JVEncoding.DecodeRecord(result.Data);

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
            await _syncStateRepo.SaveTimestampAsync("UM", openResult.LastFileTimestamp);
            _logger.LogInformation("Sync state updated: lastFileTimestamp={Timestamp}", openResult.LastFileTimestamp);
        }

        _jvLink.Close();
    }
}
