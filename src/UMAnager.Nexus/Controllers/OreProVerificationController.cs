// ============================================================
// FILE: OreProVerificationController.cs
// LAYER: API (api/orepro/verification)
// PURPOSE: Read the saved daily OrePro-vs-UMAnager reconciliation and explicitly
//          rerun a PUBLIC, read-only OrePro check for a JST date.
// ============================================================
using System.Globalization;
using Microsoft.AspNetCore.Mvc;
using UMAnager.Nexus.Services;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/orepro/verification")]
public sealed class OreProVerificationController : ControllerBase
{
    private readonly OreProDailyVerificationService _verification;

    public OreProVerificationController(OreProDailyVerificationService verification)
        => _verification = verification;

    [HttpGet]
    public async Task<IActionResult> Get([FromQuery] string date)
    {
        if (!TryParseJstDate(date, out var dateKey))
            return BadRequest(new { error = "date must be a JST calendar date in yyyy-MM-dd format" });

        var stored = await _verification.GetStoredAsync(dateKey);
        return stored is null
            ? NotFound(new { error = $"No OrePro verification has been recorded for {dateKey}." })
            : Ok(stored);
    }

    /// <summary>
    /// Explicit catch-up/recheck for one already-settled JST date. This only reads the public
    /// OrePro profile endpoint and writes UMAnager's local audit evidence; it never accesses a
    /// login/session, cart, or bet-submission endpoint.
    /// </summary>
    [HttpPost("run")]
    public async Task<IActionResult> Run([FromQuery] string date, CancellationToken ct)
    {
        if (!TryParseJstDate(date, out var dateKey))
            return BadRequest(new { error = "date must be a JST calendar date in yyyy-MM-dd format" });

        var jstDate = DateTime.SpecifyKind(
            DateTime.ParseExact(dateKey, "yyyy-MM-dd", CultureInfo.InvariantCulture), DateTimeKind.Utc);
        return Ok(await _verification.VerifyAsync(jstDate, ct));
    }

    private static bool TryParseJstDate(string? raw, out string dateKey)
    {
        dateKey = "";
        if (!DateTime.TryParseExact(raw, "yyyy-MM-dd", CultureInfo.InvariantCulture,
                DateTimeStyles.None, out var parsed))
            return false;
        dateKey = parsed.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        return true;
    }
}
