using Microsoft.AspNetCore.Mvc;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/marks")]
public sealed class MarksController : ControllerBase
{
    [HttpGet]
    public IActionResult Get() => Ok(new
    {
        version  = 2,
        marks    = new { },
        raceMeta = new { },
    });

    [HttpPost]
    public IActionResult Post() => Ok(new { status = "ok" });
}
