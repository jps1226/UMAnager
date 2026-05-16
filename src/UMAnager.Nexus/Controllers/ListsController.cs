using Microsoft.AspNetCore.Mvc;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/lists")]
public sealed class ListsController : ControllerBase
{
    [HttpGet]
    public IActionResult Get() => Ok(new { favorites = "", watchlist = "" });

    [HttpPost]
    public IActionResult Post() => Ok(new { status = "ok" });
}
