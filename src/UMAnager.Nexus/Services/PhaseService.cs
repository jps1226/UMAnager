using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services;

public enum AppPhase
{
    WAITING_FOR_RACES,
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

        return anyFuture ? AppPhase.RACES_POPULATED : AppPhase.WAITING_FOR_RACES;
    }

    public async Task<bool> IsLivePollPausedAsync()
    {
        var raw = await _appState.GetStringAsync(AppStateService.Keys.LivePollPaused);
        return string.Equals(raw, "true", StringComparison.OrdinalIgnoreCase);
    }

    public Task SetLivePollPausedAsync(bool paused)
        => _appState.SetStringAsync(AppStateService.Keys.LivePollPaused, paused ? "true" : "false");
}
