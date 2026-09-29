// ============================================================
// FILE: OreProDailyVerificationService.cs
// LAYER: Service (singleton)
// PURPOSE: Read OrePro's public daily profile totals after a JST race day settles and
//          compare them with UMAnager's stored ledger. This never uses a login, cookie,
//          cart, or any OrePro write endpoint.
// ============================================================
using System.Globalization;
using System.Text.Json;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Treats OrePro's public profile daily result as the external accounting reference.
/// A local frozen plan is only UMAnager's record; a mismatch is preserved for review,
/// never silently corrected and never sent back to OrePro.
/// </summary>
public sealed class OreProDailyVerificationService
{
    private const string DailyStatsUrl = "https://orepro.netkeiba.com/mydata/api_get_mydata.html";
    private const string PendingDatesStateKey = "orepro_daily_verification_pending_dates";

    private readonly IHttpClientFactory _httpFactory;
    private readonly SettingsService _settings;
    private readonly SunkCostService _sunkCost;
    private readonly AppStateService _state;
    private readonly ILogger<OreProDailyVerificationService> _logger;

    public OreProDailyVerificationService(
        IHttpClientFactory httpFactory,
        SettingsService settings,
        SunkCostService sunkCost,
        AppStateService state,
        ILogger<OreProDailyVerificationService> logger)
    {
        _httpFactory = httpFactory;
        _settings = settings;
        _sunkCost = sunkCost;
        _state = state;
        _logger = logger;
    }

    public static string StateKey(string jstDate) => $"orepro_daily_verification_{jstDate}";

    /// <summary>
    /// Fetches ONLY the public daily result aggregate for a JST race date, compares it with
    /// UMAnager's per-race ledger, and persists the evidence. Network/API failures are recorded
    /// as unavailable rather than treated as a zero result or a successful verification.
    /// </summary>
    public async Task<OreProDailyVerification> VerifyAsync(DateTime jstRaceDate, CancellationToken ct = default)
    {
        var date = DateTime.SpecifyKind(jstRaceDate.Date, DateTimeKind.Utc);
        var dateKey = date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var compactDate = date.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        var profileId = (await _settings.GetStringAsync(SettingsService.Keys.OreProPublicProfileId) ?? "").Trim();

        var umanager = await GetUMAnagerTotalsAsync(date, ct);
        if (!profileId.All(char.IsDigit) || profileId.Length < 1)
            return await PersistAsync(new OreProDailyVerification(
                Date: dateKey, Status: "unconfigured", VerifiedAtUtc: DateTime.UtcNow,
                ProfileId: null, UMAnager: umanager, OrePro: null,
                Message: "OrePro public profile ID is not configured."));

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, DailyStatsUrl)
            {
                // This public endpoint is a read-only aggregate lookup. It takes POST form data but
                // neither requires nor receives authenticated session state.
                Content = new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    ["action"] = "daily",
                    ["id"] = profileId,
                    ["output"] = "jsonp",
                })
            };
            var client = _httpFactory.CreateClient("OreProPublicAudit");
            using var response = await client.SendAsync(request, ct);
            var body = await response.Content.ReadAsStringAsync(ct);
            if (!response.IsSuccessStatusCode)
                return await PersistAsync(new OreProDailyVerification(
                    dateKey, "unavailable", DateTime.UtcNow, profileId, umanager, null,
                    $"OrePro public daily lookup returned HTTP {(int)response.StatusCode}."));

            if (!TryParseDailyTotals(body, compactDate, out var orePro, out var error))
                return await PersistAsync(new OreProDailyVerification(
                    dateKey, "unavailable", DateTime.UtcNow, profileId, umanager, null, error));

            var status = TotalsEqual(umanager, orePro) ? "match" : "mismatch";
            var message = status == "match"
                ? "OrePro public daily totals match UMAnager's ledger."
                : "OrePro public daily totals differ from UMAnager's ledger; OrePro remains the external reference.";
            var verification = await PersistAsync(new OreProDailyVerification(
                dateKey, status, DateTime.UtcNow, profileId, umanager, orePro, message));

            if (status == "mismatch")
                _logger.LogError("[OreProVerify] MISMATCH {Date}: UM races={UMRaces} hits={UMHits} staked=¥{UMStaked:N0} won=¥{UMWon:N0}; OrePro races={OreRaces} hits={OreHits} staked=¥{OreStaked:N0} won=¥{OreWon:N0}.",
                    dateKey, umanager.Races, umanager.Hits, umanager.StakedYen, umanager.WonYen,
                    orePro.Races, orePro.Hits, orePro.StakedYen, orePro.WonYen);
            else
                _logger.LogInformation("[OreProVerify] Match {Date}: {Races} races, ¥{Staked:N0} staked, ¥{Won:N0} won.",
                    dateKey, orePro.Races, orePro.StakedYen, orePro.WonYen);

            return verification;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[OreProVerify] Public daily lookup failed for {Date}.", dateKey);
            return await PersistAsync(new OreProDailyVerification(
                dateKey, "unavailable", DateTime.UtcNow, profileId, umanager, null,
                "OrePro public daily lookup failed; see application log for sanitized exception details."));
        }
    }

    public async Task<OreProDailyVerification?> GetStoredAsync(string jstDate)
    {
        var raw = await _state.GetStringAsync(StateKey(jstDate));
        if (string.IsNullOrWhiteSpace(raw)) return null;
        try { return JsonSerializer.Deserialize<OreProDailyVerification>(raw); }
        catch (JsonException) { return null; }
    }

    public async Task<IReadOnlyList<string>> GetPendingDateKeysAsync()
    {
        var raw = await _state.GetStringAsync(PendingDatesStateKey);
        if (string.IsNullOrWhiteSpace(raw)) return Array.Empty<string>();
        try
        {
            return JsonSerializer.Deserialize<string[]>(raw)
                ?.Where(d => DateTime.TryParseExact(d, "yyyy-MM-dd", CultureInfo.InvariantCulture,
                    DateTimeStyles.None, out _))
                .Distinct(StringComparer.Ordinal)
                .OrderBy(d => d, StringComparer.Ordinal)
                .ToArray() ?? Array.Empty<string>();
        }
        catch (JsonException)
        {
            return Array.Empty<string>();
        }
    }

    public static bool TryParseDailyTotals(string body, string compactDate, out DailyBetTotals totals, out string error)
    {
        totals = default!;
        error = "";
        try
        {
            var json = StripJsonp(body);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (!root.TryGetProperty("status", out var status) || !string.Equals(status.GetString(), "OK", StringComparison.OrdinalIgnoreCase))
            {
                error = "OrePro public daily lookup did not return status OK.";
                return false;
            }
            if (!root.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Object ||
                !data.TryGetProperty(compactDate, out var day) || day.ValueKind != JsonValueKind.Object ||
                !day.TryGetProperty("mydata", out var stats) || stats.ValueKind != JsonValueKind.Object)
            {
                error = $"OrePro has not published a daily result for {compactDate} yet.";
                return false;
            }

            if (!TryGetYen(stats, "bet_num", out var races) ||
                !TryGetYen(stats, "hit_num", out var hits) ||
                !TryGetYen(stats, "price", out var staked) ||
                !TryGetYen(stats, "payback", out var won) ||
                !TryGetYen(stats, "diff", out var net))
            {
                error = "OrePro public daily result was missing a required numeric total.";
                return false;
            }
            totals = new DailyBetTotals(races, hits, staked, won, net);
            return true;
        }
        catch (JsonException)
        {
            error = "OrePro public daily lookup returned malformed JSONP.";
            return false;
        }
    }

    private async Task<DailyBetTotals> GetUMAnagerTotalsAsync(DateTime date, CancellationToken ct)
    {
        var recap = await _sunkCost.GetPerRaceRecapAsync(date, applyResetCutoff: false, ct);
        var staked = recap.Sum(r => r.Staked);
        var won = recap.Sum(r => r.Won);
        return new DailyBetTotals(
            Races: recap.Count,
            Hits: recap.Count(r => r.Won > 0),
            StakedYen: staked,
            WonYen: won,
            NetYen: won - staked);
    }

    private async Task<OreProDailyVerification> PersistAsync(OreProDailyVerification verification)
    {
        await _state.SetStringAsync(StateKey(verification.Date), JsonSerializer.Serialize(verification));

        var pending = (await GetPendingDateKeysAsync()).ToHashSet(StringComparer.Ordinal);
        if (verification.Status is "match" or "mismatch")
            pending.Remove(verification.Date);
        else
            pending.Add(verification.Date);
        await _state.SetStringAsync(PendingDatesStateKey, JsonSerializer.Serialize(pending.OrderBy(d => d, StringComparer.Ordinal)));

        return verification;
    }

    private static bool TotalsEqual(DailyBetTotals left, DailyBetTotals right)
        => left.Races == right.Races && left.Hits == right.Hits && left.StakedYen == right.StakedYen &&
           left.WonYen == right.WonYen && left.NetYen == right.NetYen;

    private static bool TryGetYen(JsonElement obj, string property, out int value)
    {
        value = 0;
        if (!obj.TryGetProperty(property, out var el)) return false;
        var raw = el.ValueKind == JsonValueKind.String ? el.GetString() : el.GetRawText();
        return int.TryParse((raw ?? "").Replace(",", "", StringComparison.Ordinal), NumberStyles.Integer, CultureInfo.InvariantCulture, out value);
    }

    private static string StripJsonp(string body)
    {
        var trimmed = body.Trim();
        if (trimmed.StartsWith("(", StringComparison.Ordinal) && trimmed.EndsWith(")", StringComparison.Ordinal))
            return trimmed[1..^1];
        var first = trimmed.IndexOf('{');
        var last = trimmed.LastIndexOf('}');
        return first >= 0 && last > first ? trimmed[first..(last + 1)] : trimmed;
    }
}

public sealed record DailyBetTotals(int Races, int Hits, int StakedYen, int WonYen, int NetYen);

public sealed record OreProDailyVerification(
    string Date,
    string Status,
    DateTime VerifiedAtUtc,
    string? ProfileId,
    DailyBetTotals UMAnager,
    DailyBetTotals? OrePro,
    string Message);
