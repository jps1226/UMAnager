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
}
