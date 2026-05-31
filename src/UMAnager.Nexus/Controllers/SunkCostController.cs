using Microsoft.AspNetCore.Mvc;
using UMAnager.Nexus.Services;

namespace UMAnager.Nexus.Controllers;

/// <summary>
/// Sunk-cost tally: cumulative ¥ wagered on placed (locked) bets vs ¥ won back, derived on
/// read from the marks blob + race results. See <see cref="SunkCostService"/>.
/// </summary>
[ApiController]
[Route("api/sunk-cost")]
public sealed class SunkCostController : ControllerBase
{
    private readonly SunkCostService _sunk;
    public SunkCostController(SunkCostService sunk) => _sunk = sunk;

    [HttpGet]
    public async Task<IActionResult> Get(CancellationToken ct)
        => Ok(await _sunk.GetSummaryAsync(ct));

    public sealed record ResetRequest(string? Date);

    /// <summary>Reset the tally to count only races on/after the given JST date (default: today).</summary>
    [HttpPost("reset")]
    public async Task<IActionResult> Reset([FromBody] ResetRequest? body, CancellationToken ct)
    {
        DateTime? date = null;
        if (!string.IsNullOrWhiteSpace(body?.Date) && DateTime.TryParse(body!.Date, out var d))
            date = d;
        await _sunk.ResetAsync(date, ct);
        return Ok(await _sunk.GetSummaryAsync(ct));
    }
}
