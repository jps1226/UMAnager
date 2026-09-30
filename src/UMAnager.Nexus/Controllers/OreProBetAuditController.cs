// ============================================================
// FILE: OreProBetAuditController.cs
// LAYER: API (api/orepro/bet-audit)
// PURPOSE: Read-only inspection of durable per-Apply evidence. Records are written
//          only by the server-side OrePro Apply paths.
// ============================================================
using System.Globalization;
using Microsoft.AspNetCore.Mvc;
using UMAnager.Nexus.Services;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/orepro/bet-audit")]
public sealed class OreProBetAuditController : ControllerBase
{
    private readonly OreProBetAuditService _audit;

    public OreProBetAuditController(OreProBetAuditService audit) => _audit = audit;

    [HttpGet]
    public async Task<IActionResult> Get([FromQuery] string? date)
    {
        if (!string.IsNullOrWhiteSpace(date) && !DateTime.TryParseExact(date, "yyyy-MM-dd", CultureInfo.InvariantCulture,
                DateTimeStyles.None, out _))
            return BadRequest(new { error = "date must be a JST calendar date in yyyy-MM-dd format" });

        var records = await _audit.GetAsync(date);
        return Ok(new { date, records });
    }
}
