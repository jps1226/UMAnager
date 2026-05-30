using Microsoft.AspNetCore.Mvc;
using UMAnager.Nexus.Services;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/votes")]
public sealed class VoteHistoryController : ControllerBase
{
    private readonly VoteHistoryService _votes;
    public VoteHistoryController(VoteHistoryService votes) => _votes = votes;

    /// <summary>
    /// Returns per-horse vote counts (for "Voted N×" badges) and the 50 most recent
    /// vote rows with horse names (for the sidebar history section).
    /// Shape: { counts: { horse_id: N }, recent: [{ horseId, name, raceId, mark, votedAt }] }
    /// </summary>
    [HttpGet("history")]
    public async Task<IActionResult> History()
    {
        var result = await _votes.GetHistoryAsync();
        return Ok(result);
    }
}
