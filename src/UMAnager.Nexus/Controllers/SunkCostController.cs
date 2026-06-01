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

    public sealed record BackfillRequest(string? Mode, int? Stake);

    /// <summary>Retroactively stamp a frozen bet profile on every locked race so the tally
    /// prices history as actually bet. Default: Default-OrePro 4-horse @ ¥10,000.</summary>
    [HttpPost("backfill")]
    public async Task<IActionResult> Backfill([FromBody] BackfillRequest? body, CancellationToken ct)
    {
        var mode = (body?.Mode == "custom") ? "custom" : "orepro_default";
        var stake = (body?.Stake is int s && s > 0) ? s : 10000;
        var count = await _sunk.BackfillProfilesForLockedAsync(mode, stake, ct);
        var summary = await _sunk.GetSummaryAsync(ct);
        return Ok(new { stamped = count, mode, stake, summary });
    }

    /// <summary>Import a batch of OrePro-history records (parsed from a 俺プロフ export).
    /// Body: { "records": [ { "raceId","track","num","name","purchase","payout","marks" }, ... ] }
    /// or just the array. Deduped by raceId; import is the authoritative all-time tally.</summary>
    [HttpPost("import")]
    public async Task<IActionResult> Import([FromBody] System.Text.Json.JsonElement body, CancellationToken ct)
    {
        var records = body;
        if (body.ValueKind == System.Text.Json.JsonValueKind.Object
            && body.TryGetProperty("records", out var r)) records = r;
        var total = await _sunk.MergeImportedBetsAsync(records, ct);
        var summary = await _sunk.GetSummaryAsync(ct);
        return Ok(new { ledgerTotal = total, summary });
    }

    /// <summary>Clear the imported OrePro-history ledger.</summary>
    [HttpPost("import/clear")]
    public async Task<IActionResult> ClearImport(CancellationToken ct)
    {
        await _sunk.ClearImportedBetsAsync(ct);
        return Ok(await _sunk.GetSummaryAsync(ct));
    }
}
