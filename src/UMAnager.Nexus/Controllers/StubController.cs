// ============================================================
// FILE: StubController.cs
// LAYER: API (various unimplemented routes)
// PURPOSE: Returns 200-with-status for endpoints the frontend optimistically calls but that
//          aren't implemented (prefetch, snipe, scrape, day-delete, backup, cache-clear, etc.),
//          so page load never throws on an optional call.
// CAUTION: Some routes here were superseded by real controllers (OreProController, GchController) —
//          those duplicates were removed; don't re-add them.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
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
    // companion/window and votes/apply are now handled by OreProController.

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
    // /api/gch/live-playback-json is now handled by GchController.
}
