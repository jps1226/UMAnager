using System.Text.Json;
using System.Text.Json.Nodes;
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
            // Phase 12: dev console hidden by default; gated by Dev Mode.
            showConsole             = false,
            // Phase 12: master toggle for the Advanced Tools panel, scrape console,
            // and debug-mode setting rows. Off by default for the cleaner operator UI.
            devMode                 = false,
            highlightAutoBets       = false,
            highlightFallbackBridge = false,
            cleanPastRaceCards      = true,
            tvModeSplitPercent      = 50,
            tvModePanelsFlipped     = false,
            raceTableColumns = new[]
            {
                new { key = "Shirushi", visible = true },
                new { key = "BK",      visible = true },
                new { key = "PP",      visible = true },
                new { key = "Horse",   visible = true },
                new { key = "Record",  visible = true },
                new { key = "Last3",   visible = true },
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
                formWeight          = 80,
            },
        },
        sidebarTabs = new
        {
            pedigreeLists   = true,
            autoPickStrategy = true,
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
        // Overlay the persisted blob on top of defaults so newly-added default keys
        // (e.g. Phase 12's devMode, Phase 7's formWeight + Last3 column) appear immediately
        // without requiring the user to save settings again.
        try
        {
            var defaultsNode = JsonSerializer.SerializeToNode(_defaultConfig)!.AsObject();
            var savedNode    = JsonNode.Parse(raw)!.AsObject();
            MergeJsonObjects(defaultsNode, savedNode);
            return Content(defaultsNode.ToJsonString(), "application/json");
        }
        catch (JsonException)
        {
            return Content(raw, "application/json");
        }
    }

    private static void MergeJsonObjects(JsonObject target, JsonObject source)
    {
        foreach (var kv in source)
        {
            if (kv.Value is JsonObject srcObj && target[kv.Key] is JsonObject tgtObj)
            {
                MergeJsonObjects(tgtObj, srcObj);
            }
            else
            {
                target[kv.Key] = kv.Value?.DeepClone();
            }
        }
    }

    [HttpPost]
    public async Task<IActionResult> Post([FromBody] JsonElement body)
    {
        await _state.SetStringAsync(StateKey, body.GetRawText());
        return Ok(new { status = "ok" });
    }
}
