// ============================================================
// FILE: PipelineHealthService.cs
// LAYER: Service (singleton — shared live health state)
// PURPOSE: Make data-pipeline failures LOUD instead of silent. Each critical step (parse,
//          orchestrator tick, streaming watchdog) records success/failure here; a step that fails
//          N times in a row fires ONE Discord alert. Born from the 2026-06-13 incident where the
//          results parse threw on every tick (raw_staging timeout) with zero visible signal and was
//          only caught by the operator noticing missing results by eye. See TECH_DEBT.md T1-1.
// SURFACED: /api/orchestrator/status → pipeline_health (for a UI header indicator).
// NOTE: state is intentionally IN-MEMORY — it's a live signal, and a Nexus restart both clears it
//       and resolves most hang conditions. Not persisted across restarts by design.
// LAST DOCUMENTED: 2026-06-13
// ============================================================
using System.Collections.Concurrent;

namespace UMAnager.Nexus.Services;

public sealed class PipelineHealthService
{
    // Fire a Discord alert once a step has failed this many times consecutively. Low enough to catch
    // a real outage fast (the live tick is ~5 min, so 3 ≈ 15 min of failures), high enough to ride
    // out a single transient blip without crying wolf.
    private const int AlertAfterConsecutiveFailures = 3;

    public sealed record StepHealth(
        DateTime? LastSuccessUtc,
        DateTime? LastFailureUtc,
        int ConsecutiveFailures,
        string? LastError,
        bool Alerted);

    private readonly ConcurrentDictionary<string, StepHealth> _steps = new();
    private readonly IServiceScopeFactory _scopes;
    private readonly ILogger<PipelineHealthService> _logger;

    public PipelineHealthService(IServiceScopeFactory scopes, ILogger<PipelineHealthService> logger)
    {
        _scopes = scopes;
        _logger = logger;
    }

    public void RecordSuccess(string step)
    {
        _steps.AddOrUpdate(step,
            _ => new StepHealth(DateTime.UtcNow, null, 0, null, false),
            (_, cur) => cur with { LastSuccessUtc = DateTime.UtcNow, ConsecutiveFailures = 0, LastError = null, Alerted = false });
    }

    public void RecordFailure(string step, string error)
    {
        var updated = _steps.AddOrUpdate(step,
            _ => new StepHealth(null, DateTime.UtcNow, 1, error, false),
            (_, cur) => cur with { LastFailureUtc = DateTime.UtcNow, ConsecutiveFailures = cur.ConsecutiveFailures + 1, LastError = error });

        _logger.LogWarning("[Health] Step '{Step}' failed ({N} in a row): {Error}", step, updated.ConsecutiveFailures, error);

        // Cross the threshold exactly once → fire a single alert (re-armed on the next success).
        if (updated.ConsecutiveFailures >= AlertAfterConsecutiveFailures && !updated.Alerted)
        {
            _steps[step] = updated with { Alerted = true };
            FireAlert(step, updated.ConsecutiveFailures, error);
        }
    }

    private void FireAlert(string step, int n, string error)
    {
        // Fire-and-forget; resolve IDiscordNotifier in a fresh scope so this singleton never captures
        // a scoped dependency.
        _ = Task.Run(async () =>
        {
            try
            {
                using var scope = _scopes.CreateScope();
                var discord = scope.ServiceProvider.GetRequiredService<IDiscordNotifier>();
                await discord.NotifyOrchestratorErrorAsync(
                    $"⚠️ Pipeline step **{step}** has failed {n}× in a row. Latest error: {error}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[Health] Failed to fire Discord alert for step {Step}", step);
            }
        });
    }

    /// <summary>Per-step snapshot for the status endpoint. Keys are step names.</summary>
    public IReadOnlyDictionary<string, object> Snapshot()
    {
        var now = DateTime.UtcNow;
        return _steps.ToDictionary(
            kv => kv.Key,
            kv => (object)new
            {
                last_success_utc      = kv.Value.LastSuccessUtc,
                last_failure_utc      = kv.Value.LastFailureUtc,
                consecutive_failures  = kv.Value.ConsecutiveFailures,
                last_error            = kv.Value.LastError,
                seconds_since_success = kv.Value.LastSuccessUtc.HasValue
                    ? (int?)(now - kv.Value.LastSuccessUtc.Value).TotalSeconds : null,
                healthy               = kv.Value.ConsecutiveFailures == 0,
            });
    }
}
