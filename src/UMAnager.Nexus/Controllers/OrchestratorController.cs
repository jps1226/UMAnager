// ============================================================
// FILE: OrchestratorController.cs
// LAYER: API (api/orchestrator)
// PURPOSE: Visibility + manual control over the phase machine: status (phase, paused,
//          last/next tick), force-tick, pause/resume live polling, and manual day-recap trigger.
// KEY DEPENDENCIES: LiveOrchestrator, PhaseService, DayRecapNotifier, AppDbContext.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;
using UMAnager.Nexus.Services;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/orchestrator")]
public sealed class OrchestratorController : ControllerBase
{
    private readonly LiveOrchestrator _orchestrator;
    private readonly PhaseService _phase;
    private readonly SettingsService _settings;
    private readonly DayRecapNotifier _dayRecap;
    private readonly BetWinNotifier _betWin;
    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly PipelineHealthService _health;

    public OrchestratorController(LiveOrchestrator orchestrator, PhaseService phase, SettingsService settings,
        DayRecapNotifier dayRecap, BetWinNotifier betWin, IDbContextFactory<AppDbContext> dbFactory, PipelineHealthService health)
    {
        _orchestrator = orchestrator;
        _phase = phase;
        _settings = settings;
        _dayRecap = dayRecap;
        _betWin = betWin;
        _dbFactory = dbFactory;
        _health = health;
    }

    [HttpGet("status")]
    public async Task<IActionResult> GetStatus()
    {
        var phase = await _phase.GetPhaseAsync();
        var paused = await _phase.IsLivePollPausedAsync();
        return Ok(new
        {
            phase = phase.ToString(),
            paused,
            last_tick_utc      = _orchestrator.LastTickAtUtc,
            next_tick_eta_utc  = _orchestrator.NextTickEtaUtc,
            last_observed_phase = _orchestrator.LastObservedPhase.ToString(),
            // JV-Link maintenance (rc=-504): non-null while JRA-VAN is down; the loop is backed off.
            maintenance               = _orchestrator.MaintenanceSinceUtc != null,
            maintenance_since_utc     = _orchestrator.MaintenanceSinceUtc,
            // T1-1: per-step pipeline health (parse / orchestrator-tick / streaming-watchdog). A step
            // with consecutive_failures > 0 (or a stale seconds_since_success) is the loud signal that
            // used to be a silent log line.
            pipeline_health           = _health.Snapshot(),
        });
    }

    [HttpPost("force-tick")]
    public IActionResult ForceTick()
    {
        _orchestrator.RequestForceTick();
        return Accepted(new { status = "Force-tick requested." });
    }

    [HttpPost("pause")]
    public async Task<IActionResult> Pause()
    {
        await _phase.SetLivePollPausedAsync(true);
        return Ok(new { paused = true });
    }

    [HttpPost("resume")]
    public async Task<IActionResult> Resume()
    {
        await _phase.SetLivePollPausedAsync(false);
        _orchestrator.RequestForceTick(); // immediate catch-up
        return Ok(new { paused = false });
    }

    /// <summary>
    /// Manually trigger the day recap for a specific JST date (yyyy-MM-dd).
    /// Use this if the automated recap missed the window (e.g. HR records arrived
    /// in a batch with no RA records before this fix was deployed).
    /// POST /api/orchestrator/trigger-recap?date=2026-05-23
    /// </summary>
    [HttpPost("trigger-recap")]
    public async Task<IActionResult> TriggerRecap([FromQuery] string date, CancellationToken ct)
    {
        if (!DateTime.TryParseExact(date, "yyyy-MM-dd", null,
                System.Globalization.DateTimeStyles.None, out var parsed))
            return BadRequest(new { error = "date must be yyyy-MM-dd" });

        var utcMidnight = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        var raceIds = await db.Races.AsNoTracking()
            .Where(r => r.RaceDate == utcMidnight)
            .Select(r => r.RaceId)
            .ToListAsync(ct);

        if (raceIds.Count == 0)
            return NotFound(new { error = $"No races found for {date}", hint = "Check date is JST calendar date in yyyy-MM-dd" });

        await _dayRecap.EvaluateAndNotifyAsync(raceIds, ct);
        return Accepted(new { triggered_for = date, race_count = raceIds.Count });
    }

    /// <summary>
    /// Manually re-run the per-race win-ping check for a specific JST date (yyyy-MM-dd). Use this to
    /// catch up races that were skipped by a bug (BetWinNotifier tracks races it's already handled in
    /// app_state.bet_win_notified_race_ids — clear the ones you want re-checked from that set first,
    /// or nothing will happen since it's idempotent by design).
    /// POST /api/orchestrator/trigger-bet-win-recheck?date=2026-07-04
    /// </summary>
    [HttpPost("trigger-bet-win-recheck")]
    public async Task<IActionResult> TriggerBetWinRecheck([FromQuery] string date, CancellationToken ct)
    {
        if (!DateTime.TryParseExact(date, "yyyy-MM-dd", null,
                System.Globalization.DateTimeStyles.None, out var parsed))
            return BadRequest(new { error = "date must be yyyy-MM-dd" });

        var utcMidnight = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        var raceIds = await db.Races.AsNoTracking()
            .Where(r => r.RaceDate == utcMidnight)
            .Select(r => r.RaceId)
            .ToListAsync(ct);

        if (raceIds.Count == 0)
            return NotFound(new { error = $"No races found for {date}", hint = "Check date is JST calendar date in yyyy-MM-dd" });

        await _betWin.EvaluateAndNotifyAsync(raceIds, ct);
        return Accepted(new { triggered_for = date, race_count = raceIds.Count });
    }
}
