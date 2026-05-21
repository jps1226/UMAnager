using System.Net.Http.Json;

namespace UMAnager.Nexus.Services;

public sealed record MarkHit(string Mark, string HorseName, int Finish);

public interface IDiscordNotifier
{
    Task NotifyPhaseChangedAsync(AppPhase from, AppPhase to, CancellationToken ct = default);
    Task NotifyRacePlanPopulatedAsync(string raceDate, int raceCount, IEnumerable<string> tracks, CancellationToken ct = default);
    Task NotifyBetCardWonAsync(string raceId, string description, decimal payout, CancellationToken ct = default);
    Task NotifyMarkHitsAsync(string raceLabel, IEnumerable<string> hitPills, IEnumerable<MarkHit> hits, CancellationToken ct = default);
    Task NotifyOddsAvailableAsync(string raceDate, IEnumerable<string> tracks, CancellationToken ct = default);
    Task NotifyDayRecapAsync(DayRecap recap, CancellationToken ct = default);
    Task NotifyOrchestratorErrorAsync(string message, Exception? ex = null, CancellationToken ct = default);
    Task NotifyTestAsync(CancellationToken ct = default);
}

/// <summary>
/// Posts plain Discord webhook messages. Webhook URL is read from app_settings on every send
/// so changes from the settings UI take effect without a restart. If the URL is unset, all
/// sends are silent no-ops.
/// </summary>
public sealed class DiscordNotifier : IDiscordNotifier
{
    private readonly IHttpClientFactory _httpFactory;
    private readonly SettingsService _settings;
    private readonly ILogger<DiscordNotifier> _logger;

    public DiscordNotifier(IHttpClientFactory httpFactory, SettingsService settings, ILogger<DiscordNotifier> logger)
    {
        _httpFactory = httpFactory;
        _settings    = settings;
        _logger      = logger;
    }

    public Task NotifyPhaseChangedAsync(AppPhase from, AppPhase to, CancellationToken ct = default)
        => SendAsync($":arrows_clockwise: **Phase**: `{from}` → `{to}`", ct);

    private static readonly Dictionary<string, string> TrackNames = new()
    {
        ["01"] = "Sapporo", ["02"] = "Hakodate", ["03"] = "Fukushima", ["04"] = "Niigata",
        ["05"] = "Tokyo",   ["06"] = "Nakayama",  ["07"] = "Chukyo",    ["08"] = "Kyoto",
        ["09"] = "Hanshin", ["10"] = "Kokura",
    };

    public Task NotifyRacePlanPopulatedAsync(string raceDate, int raceCount, IEnumerable<string> tracks, CancellationToken ct = default)
    {
        var names = tracks.Select(t => TrackNames.GetValueOrDefault(t, t)).ToList();
        var trackList = names.Count switch
        {
            0 => "?",
            1 => names[0],
            2 => $"{names[0]} and {names[1]}",
            _ => string.Join(", ", names[..^1]) + ", and " + names[^1],
        };
        return SendAsync($":checkered_flag: **Race plan loaded** for `{raceDate}`: {raceCount} races across {trackList}.", ct);
    }

    public Task NotifyOddsAvailableAsync(string raceDate, IEnumerable<string> tracks, CancellationToken ct = default)
    {
        var names = tracks.Select(t => TrackNames.GetValueOrDefault(t, t)).ToList();
        var trackList = names.Count switch
        {
            0 => "?",
            1 => names[0],
            2 => $"{names[0]} and {names[1]}",
            _ => string.Join(", ", names[..^1]) + ", and " + names[^1],
        };
        return SendAsync($":bar_chart: **Odds are live** for `{raceDate}` — {trackList}.", ct);
    }

    public Task NotifyBetCardWonAsync(string raceId, string description, decimal payout, CancellationToken ct = default)
        => SendAsync($":moneybag: **Bet card won** on `{raceId}` — {description} → ¥{payout:N0}", ct);

    public Task NotifyMarkHitsAsync(string raceLabel, IEnumerable<string> hitPills, IEnumerable<MarkHit> hits, CancellationToken ct = default)
    {
        var pills = string.Join(" · ", hitPills);
        var hitList = hits.OrderBy(h => h.Finish).ToList();
        var content = $":trophy: **Win!** {raceLabel} — {pills}";
        if (hitList.Count > 0)
        {
            var detail = string.Join(" · ", hitList.Select(h => $"{h.Mark} {h.HorseName} ({Ordinal(h.Finish)})"));
            content += $"\n      {detail}";
        }
        return SendAsync(content, ct);
    }

    private static string Ordinal(int n) => n switch
    {
        1 => "1st", 2 => "2nd", 3 => "3rd",
        _ => $"{n}th"
    };

    public Task NotifyDayRecapAsync(DayRecap recap, CancellationToken ct = default)
    {
        var summary = $":checkered_flag: **Day Recap {recap.DateKey}** — {recap.RacesMarked}/{recap.RacesTotal} races marked";
        var hits    = $"◎ Win: **{recap.HonmeiHits}** · Q Box: **{recap.QBoxHits}** · T Box: **{recap.TBoxHits}**";
        var total   = $":moneybag: Estimated total winnings: **¥{recap.TotalWonYen:N0}**";

        string body;
        if (recap.WinningLines.Count > 0)
        {
            var lines = string.Join("\n", recap.WinningLines.Select(l => $"• {l}"));
            body = $"{summary}\n{hits}\n{total}\n{lines}";
        }
        else
        {
            body = $"{summary}\n{hits}\n_(no hits today)_";
        }
        return SendAsync(body, ct);
    }

    public Task NotifyOrchestratorErrorAsync(string message, Exception? ex = null, CancellationToken ct = default)
    {
        var detail = ex is null ? "" : $"\n```\n{ex.GetType().Name}: {ex.Message}\n```";
        return SendAsync($":rotating_light: **Orchestrator error**: {message}{detail}", ct);
    }

    public Task NotifyTestAsync(CancellationToken ct = default)
        => SendAsync($":wave: **UMAnager test ping** — webhook is wired up. ({DateTime.UtcNow:HH:mm:ss} UTC)", ct);

    private async Task SendAsync(string content, CancellationToken ct)
    {
        var url = await _settings.GetStringAsync(SettingsService.Keys.DiscordWebhookUrl);
        if (string.IsNullOrWhiteSpace(url)) return;

        try
        {
            var http = _httpFactory.CreateClient(nameof(DiscordNotifier));
            using var resp = await http.PostAsJsonAsync(url, new { content }, ct);
            if (!resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync(ct);
                _logger.LogWarning("[Discord] Webhook returned {Status}: {Body}", (int)resp.StatusCode, body);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Discord] Webhook POST failed.");
        }
    }
}
