// ============================================================
// FILE: MarksController.cs
// LAYER: API (api/marks)
// PURPOSE: Stores the user's marks (◎〇▲△X) + per-race meta as one verbatim JSON blob in
//          app_state.user_marks_blob. This blob is the shared truth read by BetWinNotifier,
//          DayRecapNotifier, and SunkCostService.
// KEY DEPENDENCIES: AppStateService.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using UMAnager.Nexus.Services;

namespace UMAnager.Nexus.Controllers;

/// <summary>
/// Persists the user's marks (◎〇▲△X) and race-level metadata as a single JSON blob in
/// app_state under <c>user_marks_blob</c>. The frontend POSTs the whole blob on every mark
/// change; we store it verbatim. GET returns the stored blob or an empty default.
/// </summary>
[ApiController]
[Route("api/marks")]
public sealed class MarksController : ControllerBase
{
    private const string StateKey = "user_marks_blob";

    private readonly AppStateService _state;
    public MarksController(AppStateService state) => _state = state;

    [HttpGet]
    public async Task<IActionResult> Get()
    {
        var raw = await _state.GetStringAsync(StateKey);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return Ok(new { version = 2, marks = new { }, raceMeta = new { } });
        }
        // Return the saved JSON verbatim so we don't reshape it on every roundtrip.
        return Content(raw, "application/json");
    }

    [HttpPost]
    public async Task<IActionResult> Post([FromBody] JsonElement body)
    {
        await _state.SetStringAsync(StateKey, body.GetRawText());
        return Ok(new { status = "ok" });
    }

    /// <summary>
    /// Returns the day-level bet COMPOSITION for a JST date. Shape:
    ///   { composition: { presetId, lines:[{type,yen}] } | null }
    /// The composition is opaque to the server (the frontend builds + scores it; applied bets
    /// freeze explicit lines that the server scores separately). Returns null when none stored.
    /// </summary>
    [HttpGet("day-bet/{date}")]
    public async Task<IActionResult> GetDayBet(string date)
    {
        var key = AppStateService.Keys.DayBetStructureKey(date);
        var raw = await _state.GetStringAsync(key);
        if (string.IsNullOrWhiteSpace(raw)) return Ok(new { composition = (object?)null });
        // Stored verbatim as JSON; return it parsed so the client gets { composition: {...} }.
        try { using var doc = JsonDocument.Parse(raw); return Ok(new { composition = doc.RootElement.Clone() }); }
        catch { return Ok(new { composition = (object?)null }); }
    }

    /// <summary>
    /// Saves the day-level bet composition for a JST date. Body: { composition: { presetId, lines:[...] } }.
    /// Temporary state — applies to all races that day and is ignored once the day passes. Lightly
    /// validated (must be an object carrying a non-empty lines array) then stored verbatim.
    /// </summary>
    [HttpPost("day-bet/{date}")]
    public async Task<IActionResult> SetDayBet(string date, [FromBody] JsonElement body)
    {
        if (!body.TryGetProperty("composition", out var comp) || comp.ValueKind != JsonValueKind.Object)
            return BadRequest(new { status = "error", message = "composition object required." });
        if (!comp.TryGetProperty("lines", out var lines) || lines.ValueKind != JsonValueKind.Array || lines.GetArrayLength() == 0)
            return BadRequest(new { status = "error", message = "composition.lines must be a non-empty array." });

        var key = AppStateService.Keys.DayBetStructureKey(date);
        await _state.SetStringAsync(key, comp.GetRawText());
        return Ok(new { status = "ok" });
    }
}
