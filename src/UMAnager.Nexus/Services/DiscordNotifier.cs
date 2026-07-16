// ============================================================
// FILE: DiscordNotifier.cs
// LAYER: Service (IDiscordNotifier)
// PURPOSE: Posts plain Discord webhook messages — phase change, race-plan/posts/odds available,
//          mark-hit win, day recap, orchestrator error, test ping. Defines the MarkHit record.
// KEY DEPENDENCIES: IHttpClientFactory, SettingsService.
// CAUTION: Webhook URL is read per-send from app_settings; if unset, every Notify* is a silent no-op.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Net.Http.Json;

namespace UMAnager.Nexus.Services;

public sealed record MarkHit(string Mark, string HorseName, int Finish);

public interface IDiscordNotifier
{
    Task NotifyPhaseChangedAsync(AppPhase from, AppPhase to, CancellationToken ct = default);
    Task NotifyRacePlanPopulatedAsync(string raceDate, int raceCount, IEnumerable<string> tracks, CancellationToken ct = default);
    Task NotifyBetCardWonAsync(string raceId, string description, decimal payout, CancellationToken ct = default);
    /// <returns>true only if the webhook POST actually reached Discord with a 2xx response. false if
    /// the webhook is unset, or Discord rejected/failed the request (e.g. 429 rate-limit). Callers that
    /// gate "already notified" bookkeeping on delivery MUST honour this — see BetWinNotifier.</returns>
    Task<bool> NotifyMarkHitsAsync(string raceLabel, IEnumerable<string> hitPills, IEnumerable<MarkHit> hits, string? runningNetLine = null, CancellationToken ct = default);
    Task NotifyPostPositionsConfirmedAsync(string raceDate, int raceCount, CancellationToken ct = default);
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

    public Task NotifyPostPositionsConfirmedAsync(string raceDate, int raceCount, CancellationToken ct = default)
        => SendAsync($":horse_racing: **Post positions confirmed** for `{raceDate}` — {raceCount} races locked in. Awaiting odds.", ct);

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

    public Task<bool> NotifyMarkHitsAsync(string raceLabel, IEnumerable<string> hitPills, IEnumerable<MarkHit> hits, string? runningNetLine = null, CancellationToken ct = default)
    {
        var pills = string.Join(" · ", hitPills);
        var hitList = hits.OrderBy(h => h.Finish).ToList();
        var content = $":trophy: **Win!** {raceLabel} — {pills}";
        if (hitList.Count > 0)
        {
            var detail = string.Join(" · ", hitList.Select(h => $"{h.Mark} {h.HorseName} ({Ordinal(h.Finish)})"));
            content += $"\n      {detail}";
        }
        if (!string.IsNullOrEmpty(runningNetLine))
            content += $"\n      {runningNetLine}";
        return SendAsync(content, ct);
    }

    private static string Ordinal(int n) => n switch
    {
        1 => "1st", 2 => "2nd", 3 => "3rd",
        _ => $"{n}th"
    };

    public Task NotifyDayRecapAsync(DayRecap recap, CancellationToken ct = default)
    {
        var summary = $":checkered_flag: **Day Recap {recap.DateKey}** — {recap.RacesMarked}/{recap.RacesTotal} bets placed";
        var hits    = $"Won **{recap.RacesWon}** of **{recap.RacesMarked}** placed bets";
        var total   = $":moneybag: Won: **¥{recap.TotalWonYen:N0}** · Staked: **¥{recap.TotalStakedYen:N0}**";
        var netSign = recap.NetYen >= 0 ? "+" : "−";
        var netEmoji = recap.NetYen >= 0 ? ":chart_with_upwards_trend:" : ":chart_with_downwards_trend:";
        var net     = $"{netEmoji} **Net: {netSign}¥{Math.Abs(recap.NetYen):N0}**";

        string body;
        if (recap.WinningLines.Count > 0)
        {
            var lines = string.Join("\n", recap.WinningLines.Select(l => $"• {l}"));
            body = $"{summary}\n{hits}\n{total}\n{net}\n{lines}";
        }
        else
        {
            body = $"{summary}\n{hits}\n{total}\n{net}\n_(no hits today)_";
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

    /// <returns>true only if Discord accepted the message (2xx). false if the webhook is unset, or the
    /// POST was rejected (e.g. HTTP 429 rate-limit) or threw. A non-2xx is NOT success — historically
    /// this method returned void, so a 429 (or any rejection) was logged but invisible to callers, which
    /// then marked the race "already notified" and silently dropped the ping (real incident, s60).</returns>
    private async Task<bool> SendAsync(string content, CancellationToken ct)
    {
        var url = await _settings.GetStringAsync(SettingsService.Keys.DiscordWebhookUrl);
        if (string.IsNullOrWhiteSpace(url)) return false;

        try
        {
            var http = _httpFactory.CreateClient(nameof(DiscordNotifier));
            using var resp = await http.PostAsJsonAsync(url, new { content }, ct);
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
