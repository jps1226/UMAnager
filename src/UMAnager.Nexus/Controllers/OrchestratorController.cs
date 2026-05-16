using Microsoft.AspNetCore.Mvc;
using UMAnager.Nexus.Services;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/orchestrator")]
public sealed class OrchestratorController : ControllerBase
{
    private readonly LiveOrchestrator _orchestrator;
    private readonly PhaseService _phase;
    private readonly SettingsService _settings;

    public OrchestratorController(LiveOrchestrator orchestrator, PhaseService phase, SettingsService settings)
    {
        _orchestrator = orchestrator;
        _phase = phase;
        _settings = settings;
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
}
