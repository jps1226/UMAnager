// ============================================================
// FILE: PhaseService.cs
// LAYER: Service — owns the AppPhase state machine
// PURPOSE: Reads/writes the current phase (app_state.app_phase) and computes the DESIRED phase
//          purely from data state (DetermineDesiredPhaseAsync): WAITING_FOR_RACES →
//          AWAITING_POSTS → AWAITING_ODDS → RACES_POPULATED → LIVE_OPERATIONS, scoped to the
//          nearest upcoming race day. Also owns the live-poll pause flag.
// KEY DEPENDENCIES: AppDbContext, AppStateService, SettingsService.
// CAUTION: SortTime is Kind=Utc holding JST wall-clock — all comparisons use JST-now. The
//          method is a PURE read; LiveOrchestrator decides whether to commit the transition.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services;

public enum AppPhase
{
    WAITING_FOR_RACES,
    AWAITING_POSTS,    // Race plan ingested but no post positions drawn yet (Thu evening JST)
    AWAITING_ODDS,     // Posts confirmed (PostPosition > 0) but no odds published yet (Fri morning JST)
    RACES_POPULATED,
    LIVE_OPERATIONS,
}

/// <summary>
/// Owns the three-phase app state machine. Phase is stored as a string in app_state under key "app_phase".
/// Transitions are data-driven (not calendar-driven) per the rules in CLAUDE.md.
/// </summary>
public sealed class PhaseService
{
    private readonly IDbContextFactory<AppDbContext> _contextFactory;
    private readonly AppStateService _appState;
    private readonly SettingsService _settings;
    private readonly ILogger<PhaseService> _logger;

    public PhaseService(
        IDbContextFactory<AppDbContext> contextFactory,
        AppStateService appState,
        SettingsService settings,
        ILogger<PhaseService> logger)
    {
        _contextFactory = contextFactory;
        _appState = appState;
        _settings = settings;
        _logger = logger;
    }

    public async Task<AppPhase> GetPhaseAsync()
    {
        var raw = await _appState.GetStringAsync(AppStateService.Keys.AppPhase);
        return Enum.TryParse<AppPhase>(raw, out var p) ? p : AppPhase.WAITING_FOR_RACES;
    }

    public async Task SetPhaseAsync(AppPhase phase)
    {
        var current = await GetPhaseAsync();
        if (current == phase) return;

        await _appState.SetStringAsync(AppStateService.Keys.AppPhase, phase.ToString());
        _logger.LogInformation("[Phase] Transition: {Old} -> {New}", current, phase);
    }

    private static readonly TimeZoneInfo JstZone = TimeZoneInfo.FindSystemTimeZoneById("Tokyo Standard Time");

    /// <summary>
    /// Computes the desired phase from current data state. Pure read — does NOT write.
    /// Caller decides whether to commit the transition (so phase-change side effects fire exactly once).
    /// </summary>
    /// <remarks>
    /// CRITICAL: `Race.SortTime` is stored with DateTimeKind.Utc but the value is JST wall-clock
    /// (HHMM from JV-Link, not converted). We compare against JST-now to match that convention.
    /// </remarks>
    public async Task<AppPhase> DetermineDesiredPhaseAsync(DateTime utcNow)
    {
        using var ctx = _contextFactory.CreateDbContext();

        var liveWindowMinutes = await _settings.GetIntAsync(
            SettingsService.Keys.LiveWindowMinutes,
            SettingsService.Defaults.LiveWindowMinutes);

        // SortTime is stored Kind=Utc with JST wall-clock values (see RaRecordParser). Match that.
        var jstNowUnspec   = TimeZoneInfo.ConvertTimeFromUtc(utcNow, JstZone);
        var jstNow         = DateTime.SpecifyKind(jstNowUnspec, DateTimeKind.Utc);
        var liveWindowStart = jstNow.AddMinutes(-liveWindowMinutes);
        var liveWindowEnd   = jstNow.AddMinutes(liveWindowMinutes);

        var anyLive = await ctx.Races
            .AsNoTracking()
            .AnyAsync(r => r.SortTime != null
                        && r.SortTime >= liveWindowStart
                        && r.SortTime <= liveWindowEnd);

        if (anyLive) return AppPhase.LIVE_OPERATIONS;

        var anyFuture = await ctx.Races
            .AsNoTracking()
            .AnyAsync(r => r.SortTime != null && r.SortTime > jstNow);

        if (!anyFuture) return AppPhase.WAITING_FOR_RACES;

        // Phase 34 (revised): split the pre-live window into two phases, scoped to the
        // NEAREST upcoming race day. JRA publishes posts/odds per race day, not per weekend
        // — Sunday's posts don't draw until ~12 PM JST Saturday (after Saturday's card runs).
        // Scoping to "the next race day with any upcoming race" lets the cycle progress as:
        //   Saturday's posts land  → AWAITING_ODDS
        //   Saturday's odds land   → RACES_POPULATED
        //   Saturday races run     → LIVE_OPERATIONS
        //   Saturday's last race finishes → next-upcoming-day becomes Sunday → AWAITING_POSTS
        //   Sunday's posts land    → AWAITING_ODDS  (cycle repeats)
        //
        // AWAITING_POSTS  — nearest upcoming race day has at least one race with no post drawn yet.
        // AWAITING_ODDS   — every race on the nearest upcoming day has posts, but no odds yet.
        // RACES_POPULATED — every race on the nearest upcoming day has both posts AND odds.
        var nearestDay = await ctx.Races
            .AsNoTracking()
            .Where(r => r.SortTime != null && r.SortTime > jstNow)
            .OrderBy(r => r.SortTime)
            .Select(r => (DateTime?)r.RaceDate)
            .FirstOrDefaultAsync();

        if (nearestDay == null) return AppPhase.AWAITING_POSTS; // shouldn't happen given anyFuture above

        var nearestDayRaceCount = await ctx.Races
            .AsNoTracking()
            .CountAsync(r => r.RaceDate == nearestDay && r.SortTime != null && r.SortTime > jstNow);

        var nearestDayRacesWithPostsCount = await ctx.RaceEntries
            .AsNoTracking()
            .Where(e => e.PostPosition != null && e.PostPosition > 0
                     && ctx.Races.Any(r => r.RaceId == e.RaceId
                                       && r.RaceDate == nearestDay
                                       && r.SortTime != null
                                       && r.SortTime > jstNow))
            .Select(e => e.RaceId)
            .Distinct()
            .CountAsync();

        var allRacesHavePosts = nearestDayRaceCount > 0
                                && nearestDayRacesWithPostsCount >= nearestDayRaceCount;
        if (!allRacesHavePosts) return AppPhase.AWAITING_POSTS;

        var anyNearestDayOdds = await ctx.RaceEntries
            .AsNoTracking()
            .AnyAsync(e => e.Odds != null
                        && ctx.Races.Any(r => r.RaceId == e.RaceId
                                           && r.RaceDate == nearestDay
                                           && r.SortTime != null
                                           && r.SortTime > jstNow));

        return anyNearestDayOdds ? AppPhase.RACES_POPULATED : AppPhase.AWAITING_ODDS;
    }

    public async Task<bool> IsLivePollPausedAsync()
    {
        var raw = await _appState.GetStringAsync(AppStateService.Keys.LivePollPaused);
        return string.Equals(raw, "true", StringComparison.OrdinalIgnoreCase);
    }

    public Task SetLivePollPausedAsync(bool paused)
        => _appState.SetStringAsync(AppStateService.Keys.LivePollPaused, paused ? "true" : "false");
}
