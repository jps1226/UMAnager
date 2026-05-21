using System.Threading.Channels;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

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
    private readonly IDiscordNotifier _discord;
    private readonly IDbContextFactory<AppDbContext> _dbFactory;
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
        IDiscordNotifier discord,
        IDbContextFactory<AppDbContext> dbFactory,
        ILogger<LiveOrchestrator> logger)
    {
        _phase     = phase;
        _settings  = settings;
        _odds      = odds;
        _results   = results;
        _bridge    = bridge;
        _discord   = discord;
        _dbFactory = dbFactory;
        _logger    = logger;
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
                _ = _discord.NotifyOrchestratorErrorAsync("Tick threw", ex);
                try { await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken); }
                catch (OperationCanceledException) { break; }
            }
        }

        _logger.LogInformation("[Orchestrator] Stopped.");
    }

    private async Task RunTickAsync(CancellationToken ct)
    {
        // 1. Update phase if data state has changed. Fire Discord notifications on every
        //    transition; additionally fire RacePlanPopulated on WAITING_FOR_RACES → RACES_POPULATED
        //    (the moment the weekend's race plan first lands per CLAUDE.md Phase 6 spec).
        var previousPhase = LastObservedPhase;
        var desired = await _phase.DetermineDesiredPhaseAsync(DateTime.UtcNow);
        if (desired != previousPhase)
        {
            await _phase.SetPhaseAsync(desired);
            LastObservedPhase = desired;
            _ = _discord.NotifyPhaseChangedAsync(previousPhase, desired);

            if (previousPhase == AppPhase.WAITING_FOR_RACES && desired == AppPhase.RACES_POPULATED)
                _ = NotifyRacePlanPopulatedAsync();
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
        else if (!paused && _bridge.IsConnected && desired == AppPhase.RACES_POPULATED)
        {
            var datesEnqueued = await _odds.EnqueueForUpcomingDatesAsync(ct);
            _logger.LogInformation("[Orchestrator] RACES_POPULATED tick: prelive odds enqueued for {Count} date(s).", datesEnqueued);
        }
        else
        {
            _logger.LogDebug("[Orchestrator] {Phase} tick (paused={Paused}, connected={Connected}) — no fetch.",
                desired, paused, _bridge.IsConnected);
        }

        LastTickAtUtc = DateTime.UtcNow;

        // 3. Wait the interval, interruptible by force-tick or cancellation. Mirrors kmy-keiba's
        //    1s-granularity countdown so the pause flag and force button feel responsive.
        //    In RACES_POPULATED, also cap the sleep so we wake up exactly when the live window
        //    opens for the next race — otherwise the hourly cadence can miss the boundary by
        //    up to (interval - 1) minutes and start 5-min ticks late.
        var effectiveInterval = await ClampForLiveBoundaryAsync(desired, interval);
        NextTickEtaUtc = LastTickAtUtc.Value.Add(effectiveInterval);
        await WaitInterruptiblyAsync(effectiveInterval, ct);
    }

    private static readonly TimeZoneInfo JstZone = TimeZoneInfo.FindSystemTimeZoneById("Tokyo Standard Time");

    private async Task<TimeSpan> ClampForLiveBoundaryAsync(AppPhase phase, TimeSpan interval)
    {
        if (phase != AppPhase.RACES_POPULATED) return interval;

        var liveWindowMinutes = await _settings.GetIntAsync(
            SettingsService.Keys.LiveWindowMinutes,
            SettingsService.Defaults.LiveWindowMinutes);

        // SortTime is stored Kind=Utc with JST wall-clock values (same convention as PhaseService).
        var jstNowUnspec = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, JstZone);
        var jstNow       = DateTime.SpecifyKind(jstNowUnspec, DateTimeKind.Utc);

        await using var db = await _dbFactory.CreateDbContextAsync();
        var nextRaceSortTime = await db.Races.AsNoTracking()
            .Where(r => r.SortTime != null && r.SortTime > jstNow)
            .OrderBy(r => r.SortTime)
            .Select(r => r.SortTime!.Value)
            .FirstOrDefaultAsync();

        if (nextRaceSortTime == default) return interval;

        // Time remaining until (first race - liveWindowMinutes). +1s puts us just inside the window.
        var untilFlip = (nextRaceSortTime - jstNow) - TimeSpan.FromMinutes(liveWindowMinutes) + TimeSpan.FromSeconds(1);
        if (untilFlip <= TimeSpan.Zero) return interval; // already inside the window; next tick will flip

        return untilFlip < interval ? untilFlip : interval;
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

    private async Task NotifyRacePlanPopulatedAsync()
    {
        try
        {
            await using var db = await _dbFactory.CreateDbContextAsync();
            // Summarize the next race day (the one that just appeared). Grouping by date and taking
            // the earliest gives us "this weekend's first day" without baking in a JST calendar gate.
            var today = DateTime.UtcNow.Date;
            var upcomingDay = await db.Races.AsNoTracking()
                .Where(r => r.RaceDate >= today)
                .OrderBy(r => r.RaceDate)
                .Select(r => r.RaceDate)
                .FirstOrDefaultAsync();

            if (upcomingDay == default) return;

            var dayRaces = await db.Races.AsNoTracking()
                .Where(r => r.RaceDate == upcomingDay)
                .Select(r => new { r.TrackCode })
                .ToListAsync();

            var tracks = dayRaces.Select(r => r.TrackCode ?? "?").Distinct().OrderBy(s => s).ToList();
            await _discord.NotifyRacePlanPopulatedAsync(upcomingDay.ToString("yyyy-MM-dd"), dayRaces.Count, tracks);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Orchestrator] RacePlanPopulated summary failed (notification skipped).");
        }
    }

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
