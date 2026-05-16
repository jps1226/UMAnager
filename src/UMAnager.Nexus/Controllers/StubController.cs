using Microsoft.AspNetCore.Mvc;

namespace UMAnager.Nexus.Controllers;

/// <summary>
/// Stubs for endpoints not yet implemented. Returns 200 with a status field
/// so the frontend does not throw on optional calls during page load.
/// </summary>
[ApiController]
public sealed class StubController : ControllerBase
{
    // ── Prefetch ─────────────────────────────────────────────────────────────

    [HttpGet("api/prefetch-check")]
    public IActionResult PrefetchCheck() => Ok(new
    {
        hasUpdates    = false,
        enabled       = false,
        updatesByDate = new { },
        summary       = "",
    });

    // ── Snipe ────────────────────────────────────────────────────────────────

    [HttpPost("api/snipe")]
    public IActionResult Snipe() => Ok(new { status = "not_implemented" });

    // ── Scrape ───────────────────────────────────────────────────────────────

    [HttpPost("api/scrape")]
    public IActionResult Scrape() => Ok(new
    {
        status       = "not_implemented",
        cached_races = 0,
        data_engine  = "jv",
    });

    [HttpGet("api/scrape/log")]
    public IActionResult ScrapeLog() => Ok(new { logs = Array.Empty<string>() });

    // ── Day delete ───────────────────────────────────────────────────────────

    [HttpPost("api/day/delete")]
    public IActionResult DayDelete() => Ok(new { status = "not_implemented" });

    // ── OrePro ───────────────────────────────────────────────────────────────

    [HttpPost("api/orepro/companion/window")]
    public IActionResult OreProWindow() => Ok(new { status = "not_implemented", message = "" });

    [HttpPost("api/orepro/votes/apply")]
    public IActionResult OreProApply() => Ok(new { status = "not_implemented", message = "", results = Array.Empty<object>() });

    [HttpGet("api/orepro/results/last")]
    public IActionResult OreProResultsLast() => Ok(new { status = "no_data" });

    [HttpGet("api/orepro/results/history")]
    public IActionResult OreProResultsHistory() => Ok(new { });

    [HttpPost("api/orepro/results/sync")]
    public IActionResult OreProResultsSync() => Ok(new { status = "not_implemented" });

    // ── Data management ───────────────────────────────────────────────────────

    [HttpPost("api/data/backup")]
    public IActionResult Backup() => Ok(new { status = "not_implemented", filename = "", path = "" });

    [HttpPost("api/data/backup/restore")]
    public IActionResult BackupRestore() => Ok(new { status = "not_implemented" });

    [HttpPost("api/data/legacy/export")]
    public IActionResult LegacyExport() => Ok(new { status = "not_implemented" });

    [HttpPost("api/data/legacy/import")]
    public IActionResult LegacyImport() => Ok(new { status = "not_implemented" });

    [HttpPost("api/cache/clear")]
    public IActionResult CacheClear() => Ok(new { status = "not_implemented" });

    [HttpPost("api/dict/wipe")]
    public IActionResult DictWipe() => Ok(new { message = "not_implemented", cleared = 0 });

    // ── Server ────────────────────────────────────────────────────────────────

    [HttpPost("api/server/shutdown")]
    public IActionResult Shutdown() => Ok(new { status = "not_implemented" });

    // ── TV Mode ───────────────────────────────────────────────────────────────

    [HttpGet("api/gch/live-playback-json")]
    public IActionResult GchLivePlayback() => Ok(new { status = "not_implemented" });
}
