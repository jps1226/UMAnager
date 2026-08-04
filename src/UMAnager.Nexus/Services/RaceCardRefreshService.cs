// ============================================================
// FILE: RaceCardRefreshService.cs
// LAYER: Background service
// PURPOSE: Keeps weekend race cards fresh. TriggerNowAsync enqueues STREAM_TOKU with the
//          persisted toku_file_cursor; a 15-min self-tick fires it when the 4-hour throttle
//          (last_race_plan_download) has elapsed. Also invoked directly by AWAITING_POSTS ticks.
// KEY DEPENDENCIES: SidecarBridge, AppStateService.
// CAUTION: Records the trigger time (throttle) here, but the cursor is advanced by the pipe
//          server on STREAM_TOKU_COMPLETE — never set the cursor to UtcNow here.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
namespace UMAnager.Nexus.Services;

public sealed class RaceCardRefreshService : BackgroundService
{
    private static readonly TimeSpan TickInterval     = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan RefreshThreshold = TimeSpan.FromHours(4);
    // Mirrors the DIFN safety backoff: a blocked JVOpen cannot be cancelled, so avoid repeatedly
    // relaunching Sidecar into the same known-bad TOKU call while we investigate the cause.
    private static readonly TimeSpan FailureBackoff    = TimeSpan.FromHours(12);

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

        if (_bridge.IngestionStatus is "Streaming" or "Maintenance")
            return $"Ingest busy ({_bridge.IngestionStatus}) — skipped.";

        var lastFailure = await _appState.GetTimestampAsync(AppStateService.Keys.LastRacePlanDownloadFailedAt);
        if (lastFailure.HasValue && DateTime.UtcNow - lastFailure.Value < FailureBackoff)
        {
            _logger.LogWarning("STREAM_TOKU is still in backoff after a watchdog reset at {FailedAt}; not re-enqueueing.",
                lastFailure.Value.ToString("O"));
            return "STREAM_TOKU is in a 12-hour backoff after a watchdog reset — command not sent.";
        }

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
            // JV-Van maintenance back-off: don't fire a doomed STREAM_TOKU into a server that's
            // returning rc=-504 — it just hangs until the watchdog resets it (the noise behind the
            // 2026-06-16 false alarm). Mirrors the orchestrator's 6h-stale-guarded maintenance read.
            if (await IsMaintenanceActiveAsync())
            {
                _logger.LogDebug("Race card refresh skipped — JV-Van under maintenance (backing off).");
                return;
            }

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

    // JV-Van maintenance is active if a maintenance completion (rc=-504) was stamped within the
    // last 6h. Same signal + stale-guard the LiveOrchestrator backs off on; cleared (Value="") on
    // the next successful stream, so this reads false again as soon as JV-Van recovers.
    private async Task<bool> IsMaintenanceActiveAsync()
    {
        var ts = await _appState.GetTimestampAsync(AppStateService.Keys.MaintenanceDetectedAt);
        return ts.HasValue && DateTime.UtcNow - ts.Value <= TimeSpan.FromHours(6);
    }
}
