// ============================================================
// FILE: DiscordNotifier.cs
// LAYER: Service (IDiscordNotifier)
// PURPOSE: Posts styled Discord webhook embeds — phase change, race-plan/posts/odds available,
//          mark-hit win, day recap, orchestrator error, test ping. Defines the MarkHit record.
// KEY DEPENDENCIES: IHttpClientFactory, SettingsService.
// CAUTION: Webhook URL is read per-send from app_settings; if unset, every Notify* is a silent no-op.
// ============================================================
using System.Net.Http.Json;

namespace UMAnager.Nexus.Services;

public sealed record MarkHit(string Mark, string HorseName, int Finish);

public interface IDiscordNotifier
{
    Task NotifyPhaseChangedAsync(AppPhase from, AppPhase to, CancellationToken ct = default);
    Task NotifyRacePlanPopulatedAsync(string raceDate, int raceCount, IEnumerable<string> tracks, CancellationToken ct = default);
    Task NotifyBetCardWonAsync(string raceId, string description, decimal payout, CancellationToken ct = default);
    /// <returns>true only if the webhook POST actually reached Discord with a 2xx response.</returns>
    Task<bool> NotifyMarkHitsAsync(string raceLabel, IEnumerable<string> hitPills, IEnumerable<MarkHit> hits, string? runningNetLine = null, CancellationToken ct = default);
    Task NotifyPostPositionsConfirmedAsync(string raceDate, int raceCount, CancellationToken ct = default);
    Task NotifyOddsAvailableAsync(string raceDate, IEnumerable<string> tracks, CancellationToken ct = default);
    Task NotifyDayRecapAsync(DayRecap recap, CancellationToken ct = default);
    Task<bool> NotifyBetReminderAsync(string raceDate, string slot, CancellationToken ct = default);
    Task<bool> NotifyWeekendCardPreflightFailedAsync(string expectedSaturday, string expectedSunday, IEnumerable<string> availableDates, CancellationToken ct = default);
    Task NotifyOrchestratorErrorAsync(string message, Exception? ex = null, CancellationToken ct = default);
    Task NotifyTestAsync(CancellationToken ct = default);
}

/// <summary>
/// Posts Discord webhook embeds. The payload intentionally mirrors the finance dashboard's
/// presentation: a concise mobile-friendly title, a colored embed, and named fields for detail.
/// The webhook URL is read from app_settings on every send so settings changes take effect without
/// a restart. Normal events and operational failures may use separate URLs.
/// </summary>
public sealed class DiscordNotifier : IDiscordNotifier
{
    private const int Blue = 0x3987E5;
    private const int Green = 0x2ECC71;
    private const int Amber = 0xE67E22;
    private const int Red = 0xE74C3C;

    private readonly IHttpClientFactory _httpFactory;
    private readonly SettingsService _settings;
    private readonly ILogger<DiscordNotifier> _logger;

    private sealed record EmbedField(string Name, string Value, bool Inline = false);

    public DiscordNotifier(IHttpClientFactory httpFactory, SettingsService settings, ILogger<DiscordNotifier> logger)
    {
        _httpFactory = httpFactory;
        _settings    = settings;
        _logger      = logger;
    }

    public async Task NotifyPhaseChangedAsync(AppPhase from, AppPhase to, CancellationToken ct = default)
        => await SendEmbedAsync($"🔄 Phase: {from} → {to}", Blue,
            new[] { new EmbedField("Phase transition", $"`{from}` → `{to}`") }, ct);

    private static readonly Dictionary<string, string> TrackNames = new()
    {
        ["01"] = "Sapporo", ["02"] = "Hakodate", ["03"] = "Fukushima", ["04"] = "Niigata",
        ["05"] = "Tokyo",   ["06"] = "Nakayama",  ["07"] = "Chukyo",    ["08"] = "Kyoto",
        ["09"] = "Hanshin", ["10"] = "Kokura",
    };

    public async Task NotifyRacePlanPopulatedAsync(string raceDate, int raceCount, IEnumerable<string> tracks, CancellationToken ct = default)
    {
        var trackList = FormatTracks(tracks);
        await SendEmbedAsync($"🏁 Race plan loaded — {raceDate}", Blue,
            new[] { new EmbedField("Races", $"{raceCount} across {trackList}") }, ct);
    }

    public async Task NotifyPostPositionsConfirmedAsync(string raceDate, int raceCount, CancellationToken ct = default)
        => await SendEmbedAsync($"🏇 Post positions confirmed — {raceDate}", Blue,
            new[] { new EmbedField("Card", $"{raceCount} races locked in. Awaiting odds.") }, ct);

    public async Task NotifyOddsAvailableAsync(string raceDate, IEnumerable<string> tracks, CancellationToken ct = default)
    {
        var trackList = FormatTracks(tracks);
        await SendEmbedAsync($"📊 Odds are live — {raceDate}", Blue,
            new[] { new EmbedField("Tracks", trackList) }, ct);
    }

    public async Task NotifyBetCardWonAsync(string raceId, string description, decimal payout, CancellationToken ct = default)
        => await SendEmbedAsync($"💰 Bet card won — {raceId}", Green,
            new[] { new EmbedField("Result", $"{description}\nPayout: ¥{payout:N0}") }, ct);

    public Task<bool> NotifyMarkHitsAsync(string raceLabel, IEnumerable<string> hitPills, IEnumerable<MarkHit> hits, string? runningNetLine = null, CancellationToken ct = default)
    {
        var pills = string.Join(" · ", hitPills);
        var hitList = hits.OrderBy(h => h.Finish).ToList();
        var fields = new List<EmbedField>
        {
            new("Winning lines", string.IsNullOrWhiteSpace(pills) ? "—" : pills),
        };
        if (hitList.Count > 0)
        {
            var detail = string.Join("\n", hitList.Select(h => $"{h.Mark} {h.HorseName} — {Ordinal(h.Finish)}"));
            fields.Add(new EmbedField("Hit horses", Limit(detail)));
        }
        if (!string.IsNullOrWhiteSpace(runningNetLine))
            fields.Add(new EmbedField("Running result", Limit(runningNetLine)));

        return SendEmbedAsync($"🏆 Win! — {raceLabel}", Green, fields, ct);
    }

    private static string Ordinal(int n) => n switch
    {
        1 => "1st", 2 => "2nd", 3 => "3rd",
        _ => $"{n}th"
    };

    public async Task NotifyDayRecapAsync(DayRecap recap, CancellationToken ct = default)
    {
        var netSign = recap.NetYen >= 0 ? "+" : "−";
        var netEmoji = recap.NetYen >= 0 ? "📈" : "📉";
        var fields = new List<EmbedField>
        {
            new("Bets placed", $"{recap.RacesMarked}/{recap.RacesTotal}"),
            new("Hit rate", $"{recap.RacesWon}/{recap.RacesMarked} bets won"),
            new("Money", $"Won: ¥{recap.TotalWonYen:N0}\nStaked: ¥{recap.TotalStakedYen:N0}"),
            new("Net", $"{netEmoji} {netSign}¥{Math.Abs(recap.NetYen):N0}"),
        };
        var lines = recap.WinningLines.Count > 0
            ? string.Join("\n", recap.WinningLines.Select(l => $"• {l}"))
            : "_(no hits today)_";
        fields.Add(new EmbedField("Winning lines", Limit(lines)));
        await SendEmbedAsync($"🏁 Day recap — {recap.DateKey}", recap.NetYen >= 0 ? Green : Red, fields, ct);
    }

    public Task<bool> NotifyBetReminderAsync(string raceDate, string slot, CancellationToken ct = default)
        => SendEmbedAsync("⏰ Bet reminder", Amber,
            new[]
            {
                new EmbedField("Card", $"{raceDate} JST"),
                new EmbedField("Reminder", $"It’s {slot} and no bets are locked yet.\nWhen ready, open the War Room and apply/submit your bets."),
            }, ct);

    public Task<bool> NotifyWeekendCardPreflightFailedAsync(string expectedSaturday, string expectedSunday, IEnumerable<string> availableDates, CancellationToken ct = default)
    {
        var available = availableDates.Any() ? string.Join(", ", availableDates.OrderBy(d => d)) : "none";
        return SendEmbedAsync("⚠️ Weekend race-card preflight failed", Amber,
            new[]
            {
                new EmbedField("Expected", $"{expectedSaturday} and {expectedSunday} JST"),
                new EmbedField("Available upcoming dates", available),
                new EmbedField("Action", "Check JVLinkAgent, JRA-VAN DNS, and the TOKURACESNPN stream before live operations."),
            }, ct, SettingsService.Keys.DiscordAlertWebhookUrl);
    }

    public async Task NotifyOrchestratorErrorAsync(string message, Exception? ex = null, CancellationToken ct = default)
    {
        var detail = ex is null ? message : $"{message}\n`{ex.GetType().Name}: {ex.Message}`";
        await SendEmbedAsync("🚨 Orchestrator error", Red,
            new[] { new EmbedField("Details", Limit(detail)) }, ct, SettingsService.Keys.DiscordAlertWebhookUrl);
    }

    public async Task NotifyTestAsync(CancellationToken ct = default)
        => await SendEmbedAsync("👋 UMAnager test ping", Blue,
            new[] { new EmbedField("Status", $"Webhook is wired up.\n{DateTime.UtcNow:HH:mm:ss} UTC") }, ct);

    private static string FormatTracks(IEnumerable<string> tracks)
    {
        var names = tracks.Select(t => TrackNames.GetValueOrDefault(t, t)).ToList();
        return names.Count switch
        {
            0 => "?",
            1 => names[0],
            2 => $"{names[0]} and {names[1]}",
            _ => string.Join(", ", names[..^1]) + ", and " + names[^1],
        };
    }

    private static string Limit(string value, int max = 1024)
        => value.Length <= max ? value : value[..(max - 1)] + "…";

    /// <returns>true only if Discord accepted the embed (2xx). False if the webhook is unset,
    /// rejected, rate-limited, or the request failed.</returns>
    private async Task<bool> SendEmbedAsync(string title, int color, IEnumerable<EmbedField> fields,
        CancellationToken ct, string? webhookKey = null)
    {
        var url = await _settings.GetStringAsync(webhookKey ?? SettingsService.Keys.DiscordWebhookUrl);
        if (string.IsNullOrWhiteSpace(url)) return false;

        try
        {
            var http = _httpFactory.CreateClient(nameof(DiscordNotifier));
            var payload = new
            {
                embeds = new[]
                {
                    new
                    {
                        title = Limit(title, 256),
                        color,
                        fields = fields.Select(f => new
                        {
                            name = Limit(f.Name, 256),
                            value = Limit(f.Value),
                            inline = f.Inline,
                        }).Take(25).ToArray(),
                    },
                },
            };
            using var resp = await http.PostAsJsonAsync(url, payload, ct);
            if (!resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync(ct);
                _logger.LogWarning("[Discord] Webhook returned {Status}: {Body}", (int)resp.StatusCode, body);
                return false;
            }
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Discord] Webhook POST failed.");
            return false;
        }
    }
}
