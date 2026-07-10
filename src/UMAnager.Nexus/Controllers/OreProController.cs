// ============================================================
// FILE: OreProController.cs
// LAYER: API (api/orepro)
// PURPOSE: Thin HTTP surface over OreProVoteApplyService — apply marks to the OrePro cart
//          (optionally submit), the legacy companion-window gate, and per-race apply-state.
// KEY DEPENDENCIES: OreProVoteApplyService, SettingsService, AppStateService.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
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
    private readonly OreProCustomBetService _customBet;
    private readonly AppStateService _appState;

    public OreProController(SettingsService settings, OreProVoteApplyService apply, OreProCustomBetService customBet, AppStateService appState)
    {
        _settings  = settings;
        _apply     = apply;
        _customBet = customBet;
        _appState  = appState;
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

    public sealed class CookieCheckRequest
    {
        public string? race_id { get; set; }
    }

    /// <summary>
    /// Pre-flight "is the OrePro cookie actually logged in?" probe (no bets placed). Apply Day
    /// Votes calls this before placing anything so a dead cookie stops at zero bets; the Settings
    /// "Test OrePro Session" button uses it too. Pass race_id (the loaded card's first race) for
    /// the reliable shutuba-based check; omit it for a best-effort race-list probe.
    /// </summary>
    [HttpPost("cookie-check")]
    public async Task<IActionResult> CookieCheck([FromBody] CookieCheckRequest? body, CancellationToken ct)
    {
        var result = await _apply.CheckCookieAsync(body?.race_id, ct);
        return Content(result.GetRawText(), "application/json");
    }

    public sealed class LoginRequest
    {
        // Optional: log in with these instead of the stored credentials (e.g. a first-time
        // "type it each time" flow). When omitted, the stored orepro_login_id/password are used.
        public string? login_id { get; set; }
        public string? password { get; set; }
        public string? race_id { get; set; }
    }

    /// <summary>
    /// Logs in to netkeiba/OrePro server-side and re-mints the session cookie — no browser needed,
    /// works from a phone. Uses stored credentials unless login_id/password are supplied in the body.
    /// </summary>
    [HttpPost("login")]
    public async Task<IActionResult> Login([FromBody] LoginRequest? body, CancellationToken ct)
    {
        var result = await _apply.LoginAsync(body?.login_id, body?.password, body?.race_id, ct);
        return Content(result.GetRawText(), "application/json");
    }

    [HttpPost("votes/apply")]
    public async Task<IActionResult> Apply([FromBody] JsonElement payload, CancellationToken ct)
    {
        var result = await _apply.ApplyAsync(payload, ct);
        return Content(result.GetRawText(), "application/json");
    }

    /// <summary>
    /// POC: places an ARBITRARY multi-line custom ticket into the OrePro cart (easy-mode-off),
    /// decoupled from the ◎〇▲△ marks. Body:
    ///   { race_id, dry_run?, lines: [ { type, method, umaban, money } ] }
    /// type 1単 2複 3枠連 4馬連 5ワイド 6馬単 7三連複 8三連単; method 0通常 1フォーメーション 2BOX 3ながし;
    /// umaban = digits joined by '-' within a group, groups joined by '_' (e.g. "1-4-6", "5_1-3-8-12");
    /// money = per-combination yen. dry_run=true returns the constructed bet_ids + add URL without firing.
    /// </summary>
    [HttpPost("custom-bet/test")]
    public async Task<IActionResult> CustomBetTest([FromBody] JsonElement payload, CancellationToken ct)
    {
        var result = await _customBet.PlaceAsync(payload, ct);
        return Content(result.GetRawText(), "application/json");
    }
}
