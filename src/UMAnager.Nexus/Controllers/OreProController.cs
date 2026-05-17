using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using UMAnager.Nexus.Services;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/orepro")]
public sealed class OreProController : ControllerBase
{
    private readonly SettingsService _settings;
    private readonly OreProVoteApplyService _apply;
    private readonly AppStateService _appState;

    public OreProController(SettingsService settings, OreProVoteApplyService apply, AppStateService appState)
    {
        _settings = settings;
        _apply    = apply;
        _appState = appState;
    }

    /// <summary>
    /// Returns per-race apply/submit state so the dashboard can show badges that survive
    /// page refresh. Shape: { "&lt;jraRaceId&gt;": { appliedAt, submitted, submittedAt, marksCount, lastMessage } }.
    /// </summary>
    [HttpGet("apply-state")]
    public async Task<IActionResult> ApplyState()
    {
        var raw = await _appState.GetStringAsync(OreProVoteApplyService.ApplyStateKey);
        if (string.IsNullOrWhiteSpace(raw)) return Ok(new { });
        return Content(raw, "application/json");
    }

    public sealed class CompanionWindowRequest
    {
        public string? action { get; set; }
    }

    /// <summary>
    /// Legacy "companion window" endpoint that the frontend calls before Apply Votes. We no
    /// longer launch a browser — the OrePro flow is fully server-side via the session cookie.
    /// Returns ok if the cookie is configured so the frontend's pre-apply gate passes.
    /// </summary>
    [HttpPost("companion/window")]
    public async Task<IActionResult> CompanionWindow([FromBody] CompanionWindowRequest? body)
    {
        var cookie = (await _settings.GetStringAsync(SettingsService.Keys.OreProSessionCookie) ?? "").Trim();
        if (string.IsNullOrEmpty(cookie))
        {
            return Ok(new
            {
                status = "warn",
                message = "OrePro session cookie not configured. Open Settings → OrePro card, paste the Cookie header value from DevTools (after logging in to orepro.netkeiba.com), then retry.",
            });
        }
        return Ok(new
        {
            status = "ok",
            message = "OrePro session cookie configured. Marks are applied server-side; open OrePro in any browser to see/confirm the cart.",
        });
    }

    [HttpPost("votes/apply")]
    public async Task<IActionResult> Apply([FromBody] JsonElement payload, CancellationToken ct)
    {
        var result = await _apply.ApplyAsync(payload, ct);
        return Content(result.GetRawText(), "application/json");
    }
}
