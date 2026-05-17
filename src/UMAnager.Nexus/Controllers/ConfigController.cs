using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using UMAnager.Nexus.Services;

namespace UMAnager.Nexus.Controllers;

/// <summary>
/// Persists the user's UI configuration (column order, formula weights, sidebar tabs, etc.)
/// as a single JSON blob in app_state under <c>user_config_blob</c>. GET returns the stored
/// blob if present, otherwise the built-in defaults.
/// </summary>
[ApiController]
[Route("api/config")]
public sealed class ConfigController : ControllerBase
{
    private const string StateKey = "user_config_blob";

    private static readonly object _defaultConfig = new
    {
        ui = new
        {
            riskSlider              = 50,
            betSafetyIndicator      = false,
            voteSortingTop          = true,
            autoFetchPastResults    = true,
            prefetchRaceCheck       = false,
            debugConsole            = false,
            autoLockPastVotes       = false,
            showConsole             = true,
            highlightAutoBets       = false,
            highlightFallbackBridge = false,
            tvModeSplitPercent      = 50,
            tvModePanelsFlipped     = false,
            raceTableColumns = new[]
            {
                new { key = "Shirushi", visible = true },
                new { key = "BK",      visible = true },
                new { key = "PP",      visible = true },
                new { key = "Horse",   visible = true },
                new { key = "Record",  visible = true },
                new { key = "Sire",    visible = true },
                new { key = "Dam",     visible = true },
                new { key = "BMS",     visible = true },
                new { key = "Odds",    visible = true },
                new { key = "Fav",     visible = true },
                new { key = "Finish",  visible = true },
            },
            formulaWeights = new
            {
                oddsCap             = 100,
                formMultiplier      = 100,
                freshnessBonus      = 3,
                freshnessBreakeven  = 10,
                pedigreeMultiplier  = 30,
            },
        },
        sidebarTabs = new
        {
            raceDatabase    = true,
            pedigreeLists   = true,
            autoPickStrategy = true,
            weekendWatchlist = true,
        },
        backend = new { dataEngine = "jv" },
    };

    private readonly AppStateService _state;
    public ConfigController(AppStateService state) => _state = state;

    [HttpGet]
    public async Task<IActionResult> Get()
    {
        var raw = await _state.GetStringAsync(StateKey);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return Ok(_defaultConfig);
        }
        return Content(raw, "application/json");
    }

    [HttpPost]
    public async Task<IActionResult> Post([FromBody] JsonElement body)
    {
        await _state.SetStringAsync(StateKey, body.GetRawText());
        return Ok(new { status = "ok" });
    }
}
