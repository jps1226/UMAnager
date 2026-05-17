using Microsoft.AspNetCore.Mvc;
using UMAnager.Nexus.Services;

namespace UMAnager.Nexus.Controllers;

[ApiController]
[Route("api/settings")]
public sealed class SettingsController : ControllerBase
{
    private readonly SettingsService _settings;
    private readonly IDiscordNotifier _discord;

    public SettingsController(SettingsService settings, IDiscordNotifier discord)
    {
        _settings = settings;
        _discord  = discord;
    }

    private static readonly string[] KnownKeys =
    {
        SettingsService.Keys.PopulatePollInterval,
        SettingsService.Keys.OddsPollIntervalPrelive,
        SettingsService.Keys.OddsPollIntervalLive,
        SettingsService.Keys.LiveWindowMinutes,
        SettingsService.Keys.DiscordWebhookUrl,
        SettingsService.Keys.OreProSessionCookie,
        SettingsService.Keys.OreProUserAgent,
        SettingsService.Keys.OreProNavToCompleteAfterSubmit,
        SettingsService.Keys.DisplayLocalTime,
    };

    [HttpGet]
    public async Task<IActionResult> GetAll()
    {
        var dict = new Dictionary<string, string?>(KnownKeys.Length);
        foreach (var k in KnownKeys)
            dict[k] = await _settings.GetStringAsync(k);
        return Ok(new
        {
            settings = dict,
            live_odds_hard_floor = SettingsService.LiveOddsHardFloor.ToString(),
        });
    }

    public sealed record SetValueRequest(string Value);

    [HttpPut("{key}")]
    public async Task<IActionResult> Set(string key, [FromBody] SetValueRequest body)
    {
        if (Array.IndexOf(KnownKeys, key) < 0)
            return BadRequest(new { error = $"Unknown settings key: {key}" });

        // Validate live odds interval against the hard floor before persisting.
        if (key == SettingsService.Keys.OddsPollIntervalLive
            && TimeSpan.TryParse(body.Value, out var ts)
            && ts < SettingsService.LiveOddsHardFloor)
        {
            return BadRequest(new { error = $"odds_poll_interval_live cannot be below the {SettingsService.LiveOddsHardFloor} hard floor." });
        }

        await _settings.SetStringAsync(key, body.Value);
        return Ok(new { key, value = body.Value });
    }

    [HttpPost("discord/test")]
    public async Task<IActionResult> TestDiscord(CancellationToken ct)
    {
        var url = await _settings.GetStringAsync(SettingsService.Keys.DiscordWebhookUrl);
        if (string.IsNullOrWhiteSpace(url))
            return BadRequest(new { error = "discord_webhook_url is not set." });

        await _discord.NotifyTestAsync(ct);
        return Ok(new { status = "Probe sent." });
    }
}
