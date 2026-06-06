// ============================================================
// FILE: LiveOrchestrator.cs
// LAYER: Background service — the master clock
// PURPOSE: Single interruptible loop (mirrors kmy-keiba's DownloadScheduler). Each tick:
//          recompute desired phase, commit + Discord-ping on change, then act per phase —
//          LIVE fetches odds+results, RACES_POPULATED/AWAITING_ODDS refresh odds, AWAITING_POSTS
//          polls race cards. Interval is phase-derived (with pre-live ramp + live-boundary clamp).
// KEY DEPENDENCIES: PhaseService, SettingsService, OddsFetchService, ResultsFetchService,
//          SidecarBridge, IDiscordNotifier, AppStateService, RaceCardRefreshService.
// CAUTION: RequestForceTick breaks the wait; the pipe server calls it after relevant ingests.
//          Honors the pause flag and only fetches when the Sidecar is connected.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
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
    private readonly AppStateService _appState;
    private readonly RaceCardRefreshService _raceCardRefresh;
    private readonly RaceCardRtFetchService _raceCardRtFetch;
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
        AppStateService appState,
        RaceCardRefreshService raceCardRefresh,
        RaceCardRtFetchService raceCardRtFetch,
        ILogger<LiveOrchestrator> logger)
    {
        _phase           = phase;
        _settings        = settings;
        _odds            = odds;
        _results         = results;
        _bridge          = bridge;
        _discord         = discord;
        _dbFactory       = dbFactory;
        _appState        = appState;
        _raceCardRefresh = raceCardRefresh;
        _raceCardRtFetch = raceCardRtFetch;
        _logger          = logger;
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

            // Race plan landed: fire the "populated" notification on the first transition
            // out of WAITING_FOR_RACES. Now lands in AWAITING_POSTS (Thu evening JST) since
            // post positions are not yet drawn. If posts + odds arrive simultaneously (rare),
            // the transition may skip directly to AWAITING_ODDS or RACES_POPULATED.
            if (previousPhase == AppPhase.WAITING_FOR_RACES
                && (desired == AppPhase.AWAITING_POSTS
                    || desired == AppPhase.AWAITING_ODDS
                    || desired == AppPhase.RACES_POPULATED))
                _ = NotifyRacePlanPopulatedAsync();

            // Post positions confirmed: AWAITING_POSTS → AWAITING_ODDS (or RACES_POPULATED if
            // odds land in the same batch — G1 early draw scenario).
            if (previousPhase == AppPhase.AWAITING_POSTS
                && (desired == AppPhase.AWAITING_ODDS || desired == AppPhase.RACES_POPULATED))
                _ = NotifyPostPositionsConfirmedAsync();
        }

        var paused = await _phase.IsLivePollPausedAsync();
        var interval = await GetIntervalForPhaseAsync(desired);

        // 2. Weekly UM refresh — runs in any non-LIVE phase when Sidecar is idle.
        if (desired != AppPhase.LIVE_OPERATIONS && _bridge.IsConnected && !paused)
            await MaybeEnqueueUmRefreshAsync(ct);

        // 3. Act per phase (skip work if paused or Sidecar offline; still keep the loop ticking).
        if (!paused && _bridge.IsConnected && desired == AppPhase.LIVE_OPERATIONS)
        {
            var (oddsResult, oddsCount, _) = await _odds.EnqueueForLiveWindowAsync(ct);
            var (resultsResult, _)         = await _results.EnqueueForTodayAsync(ct);
            _logger.LogInformation("[Orchestrator] LIVE tick: odds={Odds}({Count} races), results={Results}.",
                oddsResult, oddsCount, resultsResult);
        }
        else if (!paused && _bridge.IsConnected
                 && (desired == AppPhase.RACES_POPULATED || desired == AppPhase.AWAITING_ODDS))
        {
            var datesEnqueued = await _odds.EnqueueForUpcomingDatesAsync(ct);
            _logger.LogInformation("[Orchestrator] {Phase} tick: odds enqueued for {Count} date(s).", desired, datesEnqueued);
        }
        else if (!paused && _bridge.IsConnected && desired == AppPhase.AWAITING_POSTS)
        {
            // Poll the real-time race card via JVRTOpen("0B15") — delivers finalized posts
            // (DataStatus 1→2) the instant they're drawn, with none of the option=2 "this week"
            // batch-sync lag that previously stranded us here for hours (Oracle 2026-06-04).
            var enqueued = await _raceCardRtFetch.EnqueueForPendingPostDatesAsync(ct);
            _logger.LogInformation("[Orchestrator] AWAITING_POSTS tick: 0B15 race-card fetch enqueued for {Count} date(s).", enqueued);
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
        if (phase != AppPhase.RACES_POPULATED && phase != AppPhase.AWAITING_ODDS && phase != AppPhase.AWAITING_POSTS) return interval;

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

    private async Task<TimeSpan> GetIntervalForPhaseAsync(AppPhase phase)
    {
        return phase switch
        {
            AppPhase.LIVE_OPERATIONS   => await _settings.GetLiveOddsIntervalAsync(),
            AppPhase.RACES_POPULATED   => await GetRacesPopulatedIntervalAsync(),
            AppPhase.AWAITING_ODDS     => await _settings.GetTimeSpanAsync(
                                             SettingsService.Keys.OddsPollIntervalAwaiting,
                                             SettingsService.Defaults.OddsPollIntervalAwaiting),
            AppPhase.AWAITING_POSTS    => await _settings.GetTimeSpanAsync(
                                             SettingsService.Keys.PostsPollInterval,
                                             SettingsService.Defaults.PostsPollInterval),
            AppPhase.WAITING_FOR_RACES => await _settings.GetTimeSpanAsync(
                                             SettingsService.Keys.PopulatePollInterval,
                                             SettingsService.Defaults.PopulatePollInterval),
            _ => TimeSpan.FromMinutes(5),
        };
    }

    /// <summary>
    /// Returns the RACES_POPULATED polling interval, switching to the faster ramp cadence when
    /// JST-now is within <c>prelive_ramp_window_minutes</c> of the first upcoming race post time.
    /// This lets odds refresh tighten from 1h → 15m in the ~2h window before the card opens,
    /// without requiring live-window proximity (which is handled separately by ClampForLiveBoundaryAsync).
    /// </summary>
    private async Task<TimeSpan> GetRacesPopulatedIntervalAsync()
    {
        var normalInterval = await _settings.GetTimeSpanAsync(
            SettingsService.Keys.OddsPollIntervalPrelive,
            SettingsService.Defaults.OddsPollIntervalPrelive);

        var rampWindow = await _settings.GetIntAsync(
            SettingsService.Keys.PreliveRampWindowMinutes,
            SettingsService.Defaults.PreliveRampWindowMinutes);

        var rampInterval = await _settings.GetTimeSpanAsync(
            SettingsService.Keys.OddsPollIntervalPreliveRamp,
            SettingsService.Defaults.OddsPollIntervalPreliveRamp);

        var jstNowUnspec = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, JstZone);
        var jstNow       = DateTime.SpecifyKind(jstNowUnspec, DateTimeKind.Utc);

        await using var db = await _dbFactory.CreateDbContextAsync();
        var nextRaceSortTime = await db.Races.AsNoTracking()
            .Where(r => r.SortTime != null && r.SortTime > jstNow)
            .OrderBy(r => r.SortTime)
            .Select(r => r.SortTime!.Value)
            .FirstOrDefaultAsync();

        if (nextRaceSortTime == default) return normalInterval;

        var minutesUntilFirst = (nextRaceSortTime - jstNow).TotalMinutes;
        if (minutesUntilFirst <= rampWindow)
        {
            _logger.LogDebug(
                "[Orchestrator] Pre-live ramp active: {Min:F0}m until first race (window={Window}m). Using {Ramp} interval.",
                minutesUntilFirst, rampWindow, rampInterval);
            return rampInterval;
        }

        return normalInterval;
    }

    private async Task MaybeEnqueueUmRefreshAsync(CancellationToken ct)
    {
        if (_bridge.IngestionStatus == "Streaming") return;

        var lastRefresh = await _appState.GetTimestampAsync(AppStateService.Keys.LastUmRefresh);
        if (lastRefresh.HasValue && (DateTime.UtcNow - lastRefresh.Value).TotalDays < 7) return;

        _logger.LogInformation("[Orchestrator] Weekly UM refresh due (last={Last}). Enqueueing STREAM_DIFN.",
            lastRefresh?.ToString("O") ?? "never");
        _bridge.StagedRecordCount = 0;
        _bridge.IngestionStatus   = "Streaming";
        await _bridge.CommandQueue.Writer.WriteAsync("{\"command\":\"STREAM_DIFN\"}", ct);
    }

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

    private async Task NotifyPostPositionsConfirmedAsync()
    {
        try
        {
            await using var db = await _dbFactory.CreateDbContextAsync();
            var jstNowUnspec = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, JstZone);
            var jstNow       = DateTime.SpecifyKind(jstNowUnspec, DateTimeKind.Utc);

            var today = DateTime.UtcNow.Date;
            var upcomingDay = await db.Races.AsNoTracking()
                .Where(r => r.RaceDate >= today && r.SortTime != null && r.SortTime > jstNow)
                .OrderBy(r => r.RaceDate)
                .Select(r => r.RaceDate)
                .FirstOrDefaultAsync();

            if (upcomingDay == default) return;

            var raceCount = await db.Races.AsNoTracking()
                .Where(r => r.RaceDate >= upcomingDay && r.SortTime != null && r.SortTime > jstNow)
                .CountAsync();

            await _discord.NotifyPostPositionsConfirmedAsync(upcomingDay.ToString("yyyy-MM-dd"), raceCount);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Orchestrator] NotifyPostPositionsConfirmed failed (notification skipped).");
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
