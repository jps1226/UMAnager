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

            // Determine which data pulls to perform
            bool needsBootstrap = _syncStateRepo.NeedsBootstrap(syncState);
            // int bootstrapOption = needsBootstrap ? 4 : 0; // option 4 = setup, 0 = normal (Phase 2)
            // int weeklyOption = 2; // option 2 = this week (Phase 3)

            // TODO: Phase 2 - Bootstrap horse master (DIFN) with option=(needsBootstrap ? 4 : 1)
            // TODO: Phase 3 - Pull weekly races (TOKURACESNPN) with option=2
            // TODO: Parse RA, SE, CK records and insert into database
            // TODO: Update sync state timestamp on success

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
}
