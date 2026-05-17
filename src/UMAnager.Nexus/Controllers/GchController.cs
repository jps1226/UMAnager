using System.Net.Http.Headers;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;

namespace UMAnager.Nexus.Controllers;

/// <summary>
/// Server-side proxy for the GreenChannel live stream. The TV mode page calls
/// <c>GET /api/gch/live-playback-json</c> and feeds <c>data.playback</c> into the Streaks
/// player SDK. Two upstream calls:
///   1. GET https://sp.gch.jp/api/vij                                → { project_id, id, api_key }
///   2. GET https://playback.api.streaks.jp/v1/projects/&lt;pid&gt;/medias/&lt;id&gt;
///                                                                     (with X-Streaks-Api-Key header)
/// Ported behavior-identically from v1 server.py.
/// </summary>
[ApiController]
[Route("api/gch")]
public sealed class GchController : ControllerBase
{
    private const string UserAgent =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";

    private readonly ILogger<GchController> _logger;

    public GchController(ILogger<GchController> logger) => _logger = logger;

    [HttpGet("live-playback-json")]
    public async Task<IActionResult> LivePlaybackJson(CancellationToken ct)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        http.DefaultRequestHeaders.UserAgent.ParseAdd(UserAgent);
        http.DefaultRequestHeaders.Accept.ParseAdd("application/json,text/plain,*/*");

        // 1. Fetch the JRA project/media/api-key trio from gch's session endpoint.
        string projectId, mediaId, apiKey;
        try
        {
            using var vijReq = new HttpRequestMessage(HttpMethod.Get, "https://sp.gch.jp/api/vij");
            vijReq.Headers.Referrer = new Uri("https://sp.gch.jp/jra");
            using var vijResp = await http.SendAsync(vijReq, ct);
            if (!vijResp.IsSuccessStatusCode)
            {
                return StatusCode(502, new { error = $"gch /api/vij returned HTTP {(int)vijResp.StatusCode}" });
            }
            var vijText = await vijResp.Content.ReadAsStringAsync(ct);
            using var vij = JsonDocument.Parse(vijText);
            projectId = vij.RootElement.TryGetProperty("project_id", out var p) ? (p.GetString() ?? "") : "";
            mediaId   = vij.RootElement.TryGetProperty("id", out var i)         ? (i.GetString() ?? "") : "";
            apiKey    = vij.RootElement.TryGetProperty("api_key", out var k)    ? (k.GetString() ?? "") : "";
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "GCh session fetch failed");
            return StatusCode(502, new { error = $"gch session fetch failed: {ex.Message}" });
        }

        if (string.IsNullOrEmpty(projectId) || string.IsNullOrEmpty(mediaId) || string.IsNullOrEmpty(apiKey))
        {
            return StatusCode(502, new { error = "gch session response missing project_id/id/api_key" });
        }

        // 2. Use the api_key to fetch the actual playback metadata from streaks.jp.
        JsonElement playback;
        try
        {
            var url = $"https://playback.api.streaks.jp/v1/projects/{Uri.EscapeDataString(projectId)}/medias/{Uri.EscapeDataString(mediaId)}";
            using var playReq = new HttpRequestMessage(HttpMethod.Get, url);
            playReq.Headers.TryAddWithoutValidation("X-Streaks-Api-Key", apiKey);
            playReq.Headers.Referrer = new Uri("https://sp.gch.jp/");
            playReq.Headers.Add("Origin", "https://sp.gch.jp");
            using var playResp = await http.SendAsync(playReq, ct);
            if (!playResp.IsSuccessStatusCode)
            {
                return StatusCode(502, new { error = $"streaks playback returned HTTP {(int)playResp.StatusCode}" });
            }
            var playText = await playResp.Content.ReadAsStringAsync(ct);
            using var doc = JsonDocument.Parse(playText);
            playback = doc.RootElement.Clone();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "GCh playback fetch failed");
            return StatusCode(502, new { error = $"streaks playback fetch failed: {ex.Message}" });
        }

        return Ok(new
        {
            session = new { project_id = projectId, id = mediaId },
            playback,
        });
    }
}
