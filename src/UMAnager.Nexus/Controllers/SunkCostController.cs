// ============================================================
// FILE: SunkCostController.cs
// LAYER: API (api/sunk-cost)
// PURPOSE: Sunk-cost tally surface — GET summary (¥ staked/won/net), reset cutoff, retroactive
//          profile backfill, and import of REAL marks from a parsed OrePro history export.
// KEY DEPENDENCIES: SunkCostService.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
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

    /// <summary>Per-race recap for DISPLAY — each placed race's staked/won/net + the ticket labels
    /// that actually hit, scored from the bet you really placed (not your marks). Consumed by the
    /// TV ticker and the dashboard win badges so a "win" never shows on a race you didn't bet.
    /// Optional <c>?date=YYYY-MM-DD</c> (JST) scopes to a single race day (TV's day-of-action view).
    /// The sunk-cost reset cutoff is NOT applied here (that's a tally-only concept).</summary>
    [HttpGet("per-race")]
    public async Task<IActionResult> PerRace([FromQuery] string? date, CancellationToken ct)
    {
        DateTime? day = null;
        if (!string.IsNullOrWhiteSpace(date) && DateTime.TryParse(date, out var d))
            day = DateTime.SpecifyKind(d.Date, DateTimeKind.Utc);
        var recap = await _sunk.GetPerRaceRecapAsync(day, applyResetCutoff: false, ct);
        return Ok(recap);
    }

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

    /// <summary>Import REAL marks from a parsed 俺プロフ export. Body: { "records":[ {
    /// "track","num","purchase","payout","marks":[{"sym","horse"}] }, ... ] } (or the bare
    /// array). Resolves each race by track+race#+horse-set → our race_id, writes the marks,
    /// locks the race, and stamps the actual ¥. So past races show your real marks.</summary>
    [HttpPost("import")]
    public async Task<IActionResult> Import([FromBody] System.Text.Json.JsonElement body, CancellationToken ct)
    {
        var arr = body;
        if (body.ValueKind == System.Text.Json.JsonValueKind.Object
            && body.TryGetProperty("records", out var r)) arr = r;

        var records = new List<SunkCostService.ImportRecord>();
        if (arr.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            foreach (var rec in arr.EnumerateArray())
            {
                if (rec.ValueKind != System.Text.Json.JsonValueKind.Object) continue;
                string track = rec.TryGetProperty("track", out var t) && t.ValueKind == System.Text.Json.JsonValueKind.String ? t.GetString() ?? "" : "";
                int num = rec.TryGetProperty("num", out var n) && n.ValueKind == System.Text.Json.JsonValueKind.Number && n.TryGetInt32(out var nv) ? nv : 0;
                int purchase = rec.TryGetProperty("purchase", out var p) && p.ValueKind == System.Text.Json.JsonValueKind.Number && p.TryGetInt32(out var pv) ? pv : 0;
                int payout = rec.TryGetProperty("payout", out var pa) && pa.ValueKind == System.Text.Json.JsonValueKind.Number && pa.TryGetInt32(out var av) ? av : 0;
                var marks = new List<(string, string)>();
                if (rec.TryGetProperty("marks", out var ms) && ms.ValueKind == System.Text.Json.JsonValueKind.Array)
                    foreach (var m in ms.EnumerateArray())
                    {
                        var sym = m.TryGetProperty("sym", out var sy) && sy.ValueKind == System.Text.Json.JsonValueKind.String ? sy.GetString() : null;
                        var horse = m.TryGetProperty("horse", out var ho) && ho.ValueKind == System.Text.Json.JsonValueKind.String ? ho.GetString() : null;
                        if (!string.IsNullOrWhiteSpace(sym) && !string.IsNullOrWhiteSpace(horse)) marks.Add((sym!, horse!.Trim()));
                    }
                if (!string.IsNullOrWhiteSpace(track) && num > 0 && marks.Count > 0)
                    records.Add(new SunkCostService.ImportRecord(track.Trim(), num, purchase, payout, marks));
            }
        }

        var result = await _sunk.ImportMarksFromRecordsAsync(records, ct);
        var summary = await _sunk.GetSummaryAsync(ct);
        return Ok(new { result.RacesMatched, result.MarksWritten, result.Unresolved, summary });
    }
}
