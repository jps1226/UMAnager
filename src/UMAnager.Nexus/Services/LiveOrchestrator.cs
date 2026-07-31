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
using System.Diagnostics;
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
    private readonly PipelineHealthService _health;
    private readonly ILogger<LiveOrchestrator> _logger;

    // T1-3: if ingest sits in "Streaming" longer than this with no completion, the Sidecar is assumed
    // hung (it never dropped the pipe, so the error path never fired). Reset the lock and restart Sidecar.
    // A normal stream completes in seconds-to-low-minutes; 10m is comfortably past any legitimate run.
    private static readonly TimeSpan StreamingWatchdogTimeout = TimeSpan.FromMinutes(10);

    // A stuck weekly UM refresh should not be re-enqueued every watchdog cycle overnight. Back off long
    // enough for Jonathan/Iris to investigate while live race-card/odds/result fetches can continue.
    private static readonly TimeSpan UmRefreshFailureBackoff = TimeSpan.FromHours(12);

    private readonly Channel<bool> _forceTickChannel = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropOldest });

    public DateTime? LastTickAtUtc   { get; private set; }
    public DateTime? NextTickEtaUtc  { get; private set; }
    public AppPhase  LastObservedPhase { get; private set; } = AppPhase.WAITING_FOR_RACES;

    // Non-null while JRA-VAN is under maintenance (rc=-504): the UTC time the flag was last
    // stamped. Drives the back-off interval and is surfaced in /api/orchestrator/status.
    public DateTime? MaintenanceSinceUtc { get; private set; }

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
        PipelineHealthService health,
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
        _health          = health;
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
                _health.RecordSuccess("orchestrator-tick");
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[Orchestrator] Tick threw — sleeping 30s before retry.");
                _health.RecordFailure("orchestrator-tick", ex.Message);
                _ = _discord.NotifyOrchestratorErrorAsync("Tick threw", ex);
                try { await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken); }
                catch (OperationCanceledException) { break; }
            }
        }

        _logger.LogInformation("[Orchestrator] Stopped.");
    }

    private async Task RunTickAsync(CancellationToken ct)
    {
        // 0. Streaming-lock watchdog (T1-3) — recover from a Sidecar that hung mid-stream without
        //    dropping the pipe (so the error path never reset IngestionStatus). Runs before anything
        //    that guards on "Streaming", so a stuck lock is freed at the top of the tick.
        await CheckStreamingWatchdogAsync();

        // 0b. Daily raw_staging retention (T1-2) — trim re-streamed duplicate UM/SE/RA rows so the
        //     5GB bloat can't rebuild. Internally gated to once/24h and skipped while streaming.
        await MaybeRunRetentionAsync(ct);

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

        // JV-Link maintenance back-off (Oracle 2026-06-07). The pipe server stamps
        // maintenance_detected_at on an rc=-504 completion and clears it on the next successful
        // stream. While active we still run the normal per-phase fetch below (it doubles as the
        // probe that detects recovery), but widen the wait so we don't hammer a down server.
        MaintenanceSinceUtc = await GetActiveMaintenanceSinceAsync();

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

        // Maintenance back-off wins over the phase/clamp cadence — probe gently, don't hammer a
        // server we already know is down. Applied AFTER the live-boundary clamp so it can't be
        // shrunk back to the 5-min live tick while JRA is in maintenance.
        if (MaintenanceSinceUtc.HasValue)
        {
            var maintInterval = await _settings.GetTimeSpanAsync(
                SettingsService.Keys.MaintenanceRetryInterval,
                SettingsService.Defaults.MaintenanceRetryInterval);
            if (maintInterval > effectiveInterval) effectiveInterval = maintInterval;
            _logger.LogInformation(
                "[Orchestrator] JRA-VAN maintenance active since {Since:o} — backing off to {Interval}.",
                MaintenanceSinceUtc.Value, effectiveInterval);
        }

        // The watchdog runs at the top of each tick, so long non-live sleeps must be capped while a
        // stream is active. Otherwise a 10-minute watchdog cannot fire until the normal one-hour
        // AWAITING_POSTS/RACES_POPULATED cadence wakes back up.
        effectiveInterval = CapIntervalForStreamingWatchdog(effectiveInterval);

        NextTickEtaUtc = LastTickAtUtc.Value.Add(effectiveInterval);
        await WaitInterruptiblyAsync(effectiveInterval, ct);
    }

    private static readonly TimeZoneInfo JstZone = TimeZoneInfo.FindSystemTimeZoneById("Tokyo Standard Time");

    /// <summary>
    /// Returns the time the JRA-VAN maintenance flag was stamped, or null if not in maintenance.
    /// A stale flag (&gt; 6h old) is treated as cleared — a safety net in case the clearing
    /// completion was somehow missed (e.g. orchestrator paused across the whole window).
    /// </summary>
    private async Task<DateTime?> GetActiveMaintenanceSinceAsync()
    {
        var ts = await _appState.GetTimestampAsync(AppStateService.Keys.MaintenanceDetectedAt);
        if (!ts.HasValue) return null;
        // GetTimestampAsync may hand back a Local- or Unspecified-kind DateTime (DateTime.TryParse
        // of the stored "...Z" string). This box runs on Eastern time, so compare in UTC explicitly
        // or the 6h staleness window would be off by the UTC offset.
        var tsUtc = ts.Value.Kind == DateTimeKind.Utc ? ts.Value : ts.Value.ToUniversalTime();
        if (DateTime.UtcNow - tsUtc > TimeSpan.FromHours(6)) return null;
        return tsUtc;
    }

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

    private TimeSpan CapIntervalForStreamingWatchdog(TimeSpan interval)
    {
        if (_bridge.IngestionStatus != "Streaming" || _bridge.StreamingSinceUtc is null) return interval;

        var elapsed = DateTime.UtcNow - _bridge.StreamingSinceUtc.Value;
        var remaining = StreamingWatchdogTimeout - elapsed;
        var watchdogInterval = remaining <= TimeSpan.Zero
            ? TimeSpan.FromSeconds(1)
            : remaining + TimeSpan.FromSeconds(5);

        if (watchdogInterval < interval)
        {
            _logger.LogInformation("[Orchestrator] Stream active on {Command}; capping next tick to {Interval} for watchdog.",
                _bridge.ActiveStreamCommand ?? "unknown command", watchdogInterval);
            return watchdogInterval;
        }

        return interval;
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

    // T1-3: free a stuck ingest lock. A hung Sidecar that never sends a completion (and never drops
    // the pipe) leaves IngestionStatus = "Streaming" forever, silently killing all fetches for the
    // rest of an unattended weekend. If we've been "Streaming" past the timeout, assume the stream is
    // dead: reset the lock, restart Sidecar so the blocked COM call dies, and back off the weekly
    // DIFN job if that was the command that wedged.
    private async Task CheckStreamingWatchdogAsync()
    {
        var since = _bridge.StreamingSinceUtc;

        // Healthy tick: not mid-stream, or a stream still within the timeout. Record SUCCESS so the
        // step reflects CURRENT health and recovers to green after an isolated intervention — the
        // watchdog firing is the safety net WORKING, so it must not show "DOWN / last ok never"
        // forever once things recover. (Only a stream that stays stuck across ticks now alerts.)
        if (_bridge.IngestionStatus != "Streaming" || since is null
            || DateTime.UtcNow - since.Value <= StreamingWatchdogTimeout)
        {
            _health.RecordSuccess("streaming-watchdog");
            return;
        }

        var stuckFor = DateTime.UtcNow - since.Value;
        var activeCommand = _bridge.ActiveStreamCommand;
        _logger.LogWarning(
            "[Orchestrator] Streaming-lock watchdog: ingest stuck 'Streaming' for {Mins:F0}m with no completion on {Command} — resetting to Idle and restarting Sidecar.",
            stuckFor.TotalMinutes, activeCommand ?? "unknown command");

        if (string.Equals(activeCommand, "STREAM_DIFN", StringComparison.OrdinalIgnoreCase))
        {
            await _appState.SetTimestampAsync(AppStateService.Keys.LastUmRefreshFailedAt, DateTime.UtcNow);
            _logger.LogWarning("[Orchestrator] STREAM_DIFN wedged; backing off weekly UM refresh for {Hours:F0}h.",
                UmRefreshFailureBackoff.TotalHours);
        }

        _bridge.ActiveStreamCommand = null;
        _bridge.IngestionStatus = "Idle";   // setter clears StreamingSinceUtc; lock is freed
        RestartSidecarAfterWatchdog(activeCommand ?? "unknown");

        _health.RecordFailure("streaming-watchdog",
            $"Ingest stuck 'Streaming' {stuckFor.TotalMinutes:F0}m with no completion — auto-reset to Idle; Sidecar restarted.");
    }

    private void RestartSidecarAfterWatchdog(string activeCommand)
    {
        try
        {
            foreach (var p in Process.GetProcessesByName("UMAnager.Sidecar"))
            {
                try
                {
                    _logger.LogWarning("[Orchestrator] Killing stuck UMAnager.Sidecar PID {Pid} after watchdog ({Command}).",
                        p.Id, activeCommand);
                    p.Kill(entireProcessTree: true);
                    p.WaitForExit(5000);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "[Orchestrator] Failed to kill UMAnager.Sidecar PID {Pid}.", p.Id);
                }
                finally { p.Dispose(); }
            }

            var root = FindRepoRoot();
            if (root is null)
            {
                _logger.LogError("[Orchestrator] Could not locate UMAnager repo root; Sidecar not restarted.");
                return;
            }

            var sidecarExe = Path.Combine(root, "src", "UMAnager.Sidecar", "bin", "Release", "net8.0-windows", "win-x86", "UMAnager.Sidecar.exe");
            if (!File.Exists(sidecarExe))
            {
                _logger.LogError("[Orchestrator] Sidecar exe missing; Sidecar not restarted: {Path}", sidecarExe);
                return;
            }

            Directory.CreateDirectory(Path.Combine(root, "logs"));
            var sidecarLog = Path.Combine(root, "logs", "sidecar.log");
            var cmd = $@"cd /d ""{root}"" && ""{sidecarExe}"" >> ""{sidecarLog}"" 2>&1";
            var proc = Process.Start(new ProcessStartInfo
            {
                FileName = "cmd.exe",
                Arguments = "/c \"" + cmd + "\"",
                WorkingDirectory = root,
                UseShellExecute = false,
                CreateNoWindow = true,
            });
            _logger.LogWarning("[Orchestrator] Restarted Sidecar wrapper PID {Pid} after watchdog.", proc?.Id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Orchestrator] Sidecar watchdog restart failed.");
        }
    }

    private static string? FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "launch-services.ps1"))
                && Directory.Exists(Path.Combine(dir.FullName, "src")))
                return dir.FullName;
            dir = dir.Parent;
        }
        return null;
    }

    // T1-2: keep raw_staging from regrowing the duplicate bloat. JV-Link re-streams the whole horse
    // master weekly and re-streams each weekend's entries on every odds/results refresh, so the table
    // accumulates ~7-11 stale copies of every UM/SE/RA record. The parsed tables (horses/races/
    // race_entries) already hold the deduplicated truth, and the backfills only ever need the newest
    // copy per key — so this trims everything but the newest staged row per logical key, at most once
    // per day. Skipped while streaming to avoid contending with the inserts.
    private async Task MaybeRunRetentionAsync(CancellationToken ct)
    {
        if (_bridge.IngestionStatus == "Streaming") return;

        var last = await _appState.GetTimestampAsync(AppStateService.Keys.LastRawStagingRetention);
        if (last.HasValue && (DateTime.UtcNow - last.Value).TotalHours < 24) return;

        try
        {
            await using var db = await _dbFactory.CreateDbContextAsync(ct);
            db.Database.SetCommandTimeout(300);

            // Keep only the newest staged row (max Id) per logical key; delete older duplicates.
            // Byte offsets are 1-indexed per the JRA-VAN spec (PG substring is 1-indexed too):
            //   UM HorseId = bytes 12-21 ; RA RaceId = bytes 12-27 ; SE RaceId 12-27 + HorseId 31-40.
            // Never touches the newest row of a key (Id < keep_id), so an as-yet-unparsed newest copy
            // is always preserved for the parser.
            const string umSql = @"
                DELETE FROM raw_staging r USING (
                    SELECT max(""Id"") AS keep_id, substring(""RawBytes"" FROM 12 FOR 10) AS k
                    FROM raw_staging WHERE ""RecordType"" = 'UM' GROUP BY k
                ) g
                WHERE r.""RecordType"" = 'UM'
                  AND substring(r.""RawBytes"" FROM 12 FOR 10) = g.k
                  AND r.""Id"" < g.keep_id";
            const string raSql = @"
                DELETE FROM raw_staging r USING (
                    SELECT max(""Id"") AS keep_id, substring(""RawBytes"" FROM 12 FOR 16) AS k
                    FROM raw_staging WHERE ""RecordType"" = 'RA' GROUP BY k
                ) g
                WHERE r.""RecordType"" = 'RA'
                  AND substring(r.""RawBytes"" FROM 12 FOR 16) = g.k
                  AND r.""Id"" < g.keep_id";
            const string seSql = @"
                DELETE FROM raw_staging r USING (
                    SELECT max(""Id"") AS keep_id,
                           substring(""RawBytes"" FROM 12 FOR 16) || substring(""RawBytes"" FROM 31 FOR 10) AS k
                    FROM raw_staging WHERE ""RecordType"" = 'SE' GROUP BY k
                ) g
                WHERE r.""RecordType"" = 'SE'
                  AND substring(r.""RawBytes"" FROM 12 FOR 16) || substring(r.""RawBytes"" FROM 31 FOR 10) = g.k
                  AND r.""Id"" < g.keep_id";

            var trimmed = await db.Database.ExecuteSqlRawAsync(umSql, ct);
            trimmed += await db.Database.ExecuteSqlRawAsync(raSql, ct);
            trimmed += await db.Database.ExecuteSqlRawAsync(seSql, ct);

            await _appState.SetTimestampAsync(AppStateService.Keys.LastRawStagingRetention, DateTime.UtcNow);
            if (trimmed > 0)
                _logger.LogInformation("[Orchestrator] raw_staging retention: trimmed {Count} duplicate UM/SE/RA rows.", trimmed);
            _health.RecordSuccess("retention");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Orchestrator] raw_staging retention failed: {Error}", ex.Message);
            _health.RecordFailure("retention", ex.Message);
        }
    }

    private async Task MaybeEnqueueUmRefreshAsync(CancellationToken ct)
    {
        if (_bridge.IngestionStatus == "Streaming") return;

        var lastFailure = await _appState.GetTimestampAsync(AppStateService.Keys.LastUmRefreshFailedAt);
        if (lastFailure.HasValue && DateTime.UtcNow - lastFailure.Value < UmRefreshFailureBackoff)
        {
            _logger.LogWarning("[Orchestrator] Weekly UM refresh is still in backoff after a stuck STREAM_DIFN at {FailedAt}; skipping this tick.",
                lastFailure.Value.ToString("O"));
            return;
        }

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
