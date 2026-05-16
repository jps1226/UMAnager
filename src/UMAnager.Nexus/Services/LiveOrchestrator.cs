using System.Threading.Channels;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Background loop mirroring kmy-keiba's <c>DownloadScheduler.LoopAsync</c>:
/// - Single long-lived <c>while(true)</c> task.
/// - Inner 1-second countdown to the next interval boundary, interruptible by force-update or pause.
/// - Each tick: recompute desired phase, commit transition if changed, then act per current phase.
/// - LIVE phase: enqueue an odds fetch for races in the live window.
/// - RACES_POPULATED / WAITING phases: no fetch yet (populate-polling lands in Phase 6).
/// </summary>
public sealed class LiveOrchestrator : BackgroundService
{
    private readonly PhaseService _phase;
    private readonly SettingsService _settings;
    private readonly OddsFetchService _odds;
    private readonly ResultsFetchService _results;
    private readonly SidecarBridge _bridge;
    private readonly ILogger<LiveOrchestrator> _logger;

    private readonly Channel<bool> _forceTickChannel = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropOldest });

    public DateTime? LastTickAtUtc   { get; private set; }
    public DateTime? NextTickEtaUtc  { get; private set; }
    public AppPhase  LastObservedPhase { get; private set; } = AppPhase.WAITING_FOR_RACES;

    public LiveOrchestrator(
        PhaseService phase,
        SettingsService settings,
        OddsFetchService odds,
        ResultsFetchService results,
        SidecarBridge bridge,
        ILogger<LiveOrchestrator> logger)
    {
        _phase    = phase;
        _settings = settings;
        _odds     = odds;
        _results  = results;
        _bridge   = bridge;
        _logger   = logger;
    }

    public void RequestForceTick() => _forceTickChannel.Writer.TryWrite(true);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("[Orchestrator] Started.");

        // Brief grace period for Nexus startup (DB seed, Sidecar pipe handshake).
        try { await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken); }
        catch (OperationCanceledException) { return; }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunTickAsync(stoppingToken);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[Orchestrator] Tick threw — sleeping 30s before retry.");
                try { await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken); }
                catch (OperationCanceledException) { break; }
            }
        }

        _logger.LogInformation("[Orchestrator] Stopped.");
    }

    private async Task RunTickAsync(CancellationToken ct)
    {
        // 1. Update phase if data state has changed.
        var desired = await _phase.DetermineDesiredPhaseAsync(DateTime.UtcNow);
        if (desired != LastObservedPhase)
        {
            await _phase.SetPhaseAsync(desired);
            LastObservedPhase = desired;
        }

        var paused = await _phase.IsLivePollPausedAsync();
        var interval = await GetIntervalForPhaseAsync(desired);

        // 2. Act per phase (skip work if paused or Sidecar offline; still keep the loop ticking).
        if (!paused && _bridge.IsConnected && desired == AppPhase.LIVE_OPERATIONS)
        {
            var (oddsResult, oddsCount, _) = await _odds.EnqueueForLiveWindowAsync(ct);
            var (resultsResult, _)         = await _results.EnqueueForTodayAsync(ct);
            _logger.LogInformation("[Orchestrator] LIVE tick: odds={Odds}({Count} races), results={Results}.",
                oddsResult, oddsCount, resultsResult);
        }
        else
        {
            _logger.LogDebug("[Orchestrator] {Phase} tick (paused={Paused}, connected={Connected}) — no fetch.",
                desired, paused, _bridge.IsConnected);
        }

        LastTickAtUtc = DateTime.UtcNow;
        NextTickEtaUtc = LastTickAtUtc.Value.Add(interval);

        // 3. Wait the interval, interruptible by force-tick or cancellation. Mirrors kmy-keiba's
        //    1s-granularity countdown so the pause flag and force button feel responsive.
        await WaitInterruptiblyAsync(interval, ct);
    }

    private async Task<TimeSpan> GetIntervalForPhaseAsync(AppPhase phase) => phase switch
    {
        AppPhase.LIVE_OPERATIONS  => await _settings.GetLiveOddsIntervalAsync(),
        AppPhase.RACES_POPULATED  => await _settings.GetTimeSpanAsync(
                                        SettingsService.Keys.OddsPollIntervalPrelive,
                                        SettingsService.Defaults.OddsPollIntervalPrelive),
        AppPhase.WAITING_FOR_RACES => await _settings.GetTimeSpanAsync(
                                        SettingsService.Keys.PopulatePollInterval,
                                        SettingsService.Defaults.PopulatePollInterval),
        _ => TimeSpan.FromMinutes(5),
    };

    private async Task WaitInterruptiblyAsync(TimeSpan total, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.Add(total);
        while (DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            if (_forceTickChannel.Reader.TryRead(out _))
            {
                _logger.LogInformation("[Orchestrator] Force-tick requested — breaking wait.");
                return;
            }
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
        }
    }
}
