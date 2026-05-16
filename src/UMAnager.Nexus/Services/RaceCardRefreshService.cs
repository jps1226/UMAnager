namespace UMAnager.Nexus.Services;

public sealed class RaceCardRefreshService : BackgroundService
{
    private static readonly TimeSpan TickInterval     = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan RefreshThreshold = TimeSpan.FromHours(4);

    private readonly SidecarBridge   _bridge;
    private readonly AppStateService _appState;
    private readonly ILogger<RaceCardRefreshService> _logger;

    public RaceCardRefreshService(
        SidecarBridge bridge,
        AppStateService appState,
        ILogger<RaceCardRefreshService> logger)
    {
        _bridge   = bridge;
        _appState = appState;
        _logger   = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await Task.Yield(); // Don't block host startup

        _logger.LogInformation("RaceCardRefreshService started (tick={Tick}m, threshold={Threshold}h)",
            TickInterval.TotalMinutes, RefreshThreshold.TotalHours);

        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(TickInterval, ct);
            await CheckAndRefreshAsync(ct);
        }
    }

    // Called by the 15-min tick AND by the manual endpoint.
    public async Task<string> TriggerNowAsync(CancellationToken ct = default)
    {
        if (!_bridge.IsConnected)
            return "Sidecar not connected — command not sent.";

        if (_bridge.IngestionStatus == "Streaming")
            return "Stream already in progress — skipped.";

        // JV-Link Option=2 cursor: prior lastfiletimestamp from JVOpen, or "00000000000000" on first run.
        var fromTime = await _appState.GetStringAsync(AppStateService.Keys.TokuFileCursor)
                       ?? "00000000000000";

        _bridge.IngestionStatus = "Streaming";
        await _bridge.CommandQueue.Writer.WriteAsync(
            $"{{\"command\":\"STREAM_TOKU\",\"from_time\":\"{fromTime}\"}}", ct);

        // Record trigger time for the throttle. Cursor (TokuFileCursor) is persisted by the
        // pipe receiver on STREAM_TOKU_COMPLETE — never here, never with DateTime.UtcNow.
        await _appState.SetTimestampAsync(AppStateService.Keys.LastRacePlanDownload, DateTime.UtcNow);

        _logger.LogInformation("STREAM_TOKU command enqueued for race card refresh (from_time={FromTime})", fromTime);
        return "STREAM_TOKU enqueued.";
    }

    private async Task CheckAndRefreshAsync(CancellationToken ct)
    {
        try
        {
            var last = await _appState.GetTimestampAsync(AppStateService.Keys.LastRacePlanDownload);
            var elapsed = last.HasValue ? DateTime.UtcNow - last.Value : TimeSpan.MaxValue;

            if (elapsed < RefreshThreshold)
            {
                _logger.LogDebug("Race card refresh skipped — last run {Elapsed:hh\\:mm} ago", elapsed);
                return;
            }

            _logger.LogInformation("Race card refresh threshold reached ({Elapsed:hh\\:mm} since last run) — triggering",
                elapsed == TimeSpan.MaxValue ? TimeSpan.Zero : elapsed);

            var result = await TriggerNowAsync(ct);
            _logger.LogInformation("Race card refresh trigger result: {Result}", result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Race card refresh check failed: {Error}", ex.Message);
        }
    }
}
