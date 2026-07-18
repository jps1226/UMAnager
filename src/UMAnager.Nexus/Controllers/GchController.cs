// ============================================================
// FILE: GchController.cs
// LAYER: API (api/gch)
// PURPOSE: Proxies the GreenChannel live stream metadata used by TV mode, via a local Python
//          helper (tools/gch_stream_proxy.py) rather than calling sp.gch.jp/streaks.jp directly.
// KEY DEPENDENCIES: HttpClient (to the local helper only).
// LAST DOCUMENTED: 2026-07-17
// ============================================================
using Microsoft.AspNetCore.Mvc;

namespace UMAnager.Nexus.Controllers;

/// <summary>
/// The TV mode page calls <c>GET /api/gch/live-playback-json</c> and feeds
/// <c>data.playback</c> into the Streaks player SDK.
///
/// The actual JRA calls (sp.gch.jp/api/vij → streaks.jp playback) are geo-gated to Japan IPs.
/// Nexus.exe itself must stay OUTSIDE the VPN's split-tunnel — routing Nexus through the VPN
/// previously broke OrePro/Discord connectivity live (SocketException 10049, 18 missed win
/// notifications, ¥251,000 — see dev_log.md). So instead of calling JRA directly (as this
/// controller originally did, ported from v1's Python server.py), this now just relays to a
/// small standalone Python process (<c>tools/gch_stream_proxy.py</c>, run manually when TV mode
/// is wanted) that carries the exact same two-call logic and is the one thing added to
/// ProtonVPN's split-tunnel INCLUDE list. If that helper isn't running, this returns a clear
/// 502 telling the operator to start it — not a silent failure.
/// </summary>
[ApiController]
[Route("api/gch")]
public sealed class GchController : ControllerBase
{
    private const string ProxyUrl = "http://127.0.0.1:5057/live-playback-json";

    private readonly ILogger<GchController> _logger;

    public GchController(ILogger<GchController> logger) => _logger = logger;

    [HttpGet("live-playback-json")]
    public async Task<IActionResult> LivePlaybackJson(CancellationToken ct)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(20) };
        try
        {
            using var resp = await http.GetAsync(ProxyUrl, ct);
            var text = await resp.Content.ReadAsStringAsync(ct);
            if (!resp.IsSuccessStatusCode)
            {
                return StatusCode(502, new
                {
                    error = $"gch_stream_proxy.py returned HTTP {(int)resp.StatusCode}: {text}",
                });
            }
            return Content(text, "application/json");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "GCh local proxy unreachable");
            return StatusCode(502, new
            {
                error = "Could not reach tools/gch_stream_proxy.py on 127.0.0.1:5057 — " +
                        "run `python tools/gch_stream_proxy.py` (needs ProtonVPN split-tunnel " +
                        "set to include python.exe, connected to a Japan server) then retry.",
                detail = ex.Message,
            });
        }
    }
}
