using Microsoft.AspNetCore.Mvc;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/config")]
public sealed class ConfigController : ControllerBase
{
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

    [HttpGet]
    public IActionResult Get() => Ok(_defaultConfig);

    [HttpPost]
    public IActionResult Post() => Ok(new { status = "ok" });
}
