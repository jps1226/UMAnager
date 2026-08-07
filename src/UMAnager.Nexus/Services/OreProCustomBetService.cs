// ============================================================
// FILE: OreProCustomBetService.cs
// LAYER: Service
// PURPOSE: Drives ARBITRARY multi-line custom OrePro tickets (easy-mode-off) over plain HTTP
//          using the user's stored session cookie. This is the "actually DRIVE custom tickets"
//          path that complements OreProVoteApplyService (which only sets the ◎〇▲△ marks).
// KEY DEPENDENCIES: SettingsService (cookie/UA).
// PROTOCOL (reverse-engineered from custombet.har + custombet2.har, 2026-06-04; see memory
//          orepro-custom-bet-api). Verified end-to-end against both recordings:
//   PLACE   GET  /cart/?action=add&group=orebet_<race12>
//                &item_id[]=bet_list&item_id[]=auto_bet&item_value[]=1&item_value[]=1
//                &item_price[]=0&item_price[]=0&client_data[]=<JSON>&client_data[]=0
//                &input=UTF-8&output=jsonp&callback=...&_=<ms>
//           client_data[0] = JSON array of {bet_id, money(per-combo yen)}; client_data[1]="0".
//   CONFIRM POST /cart/  body: input=UTF-8&output=json&action=get&group=orebet_<race12>
//                → {"status":"OK","data":{"bet_list":{"_cd":"<the JSON array now in cart>"}}}
//   bet_id  = a<week_id>-<int(jyo_cd)>-<int(race_no)>_b<type>_c<method>_<umaban-groups>
//             type   1単 2複 3枠連 4馬連 5ワイド 6馬単 7三連複 8三連単
//             method 0通常 1フォーメーション 2BOX 3ながし
//             The prefix's three parts are scraped LIVE from the shutuba page JS vars
//             (var week_id / var jyo_cd / var race_no) — NOT guessed. Confirmed Tokyo(05)→a7-5-1,
//             Hanshin(09)→a7-9-1, both week_id=7 race_no=1.
// CAUTION: Custom bets are fully DECOUPLED from the ◎〇▲△ marks — arbitrary umaban. dry_run=true
//          builds the bet_ids + add URL WITHOUT firing, so the operator can eyeball before placing.
// STATUS:  POC (session 42). Manual lines in, no composer wiring yet.
// LAST DOCUMENTED: 2026-06-06
// ============================================================
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Places arbitrary multi-line custom tickets into the OrePro cart via the reverse-engineered
/// cart API. POC: callers pass explicit lines ({type, method, umaban, money}); the composer
/// that derives those lines from a race's resolved bet composition is wired separately later.
/// </summary>
public sealed class OreProCustomBetService
{
    private const string CartUrl        = "https://orepro.netkeiba.com/cart/";
    private const string ShutubaUrl     = "https://orepro.netkeiba.com/bet/shutuba.html";
    private const string BetReferer     = "https://orepro.netkeiba.com/bet/race_list.html";
    private const string UserSettingUrl = "https://orepro.netkeiba.com/mydata/api_post_user_setting.html";

    // 式別 / 方式 valid ranges (see header). Kept here so a bad payload is rejected before we
    // ever touch OrePro with a malformed bet_id.
    private static readonly int[] ValidTypes   = { 1, 2, 3, 4, 5, 6, 7, 8 };
    private static readonly int[] ValidMethods = { 0, 1, 2, 3 };

    private static readonly Regex VarWeekId = new(@"var\s+week_id\s*=\s*'([^']*)'", RegexOptions.Compiled);
    private static readonly Regex VarJyoCd  = new(@"var\s+jyo_cd\s*=\s*'([^']*)'",  RegexOptions.Compiled);
    private static readonly Regex VarRaceNo = new(@"var\s+race_no\s*=\s*'([^']*)'", RegexOptions.Compiled);

    private readonly ILogger<OreProCustomBetService> _logger;
    private readonly SettingsService _settings;
    private readonly AppStateService _appState;

    public OreProCustomBetService(ILogger<OreProCustomBetService> logger, SettingsService settings, AppStateService appState)
    {
        _logger   = logger;
        _settings = settings;
        _appState = appState;
    }

    private sealed record BetLine(int Type, int Method, string Umaban, int Money);

    /// <summary>
    /// Re-reads whatever cookies OrePro set during this call and writes them back to Settings —
    /// mirrors OreProVoteApplyService.PersistSessionCookieAsync. Without this, a session rotation
    /// that happens during the marks-first step (a separate earlier POST) never carries forward to
    /// THIS call, which seeds its own fresh CookieContainer from the stale stored string. Found live
    /// 2026-07-17: exactly this gap caused "not login" on the custom-bet placement right after a
    /// confirmed-working session had just been used successfully by an earlier, separate call.
    ///
    /// BUG FOUND same night: every cookie is seeded under TWO domain scopes (wildcard ".netkeiba.com"
    /// AND exact "orepro.netkeiba.com"), so a rotated cookie can leave the jar holding a stale value
    /// under the wildcard entry alongside the fresh one under the exact-host entry. Deduping by
    /// "whichever came first" could persist the stale copy. Fixed: prefer the non-wildcard (exact)
    /// cookie whenever both exist for the same name.
    /// </summary>
    private async Task PersistSessionCookieAsync(CookieContainer jar, CancellationToken ct)
    {
        try
        {
            var byName = new Dictionary<string, Cookie>();
            foreach (var host in new[] { "https://orepro.netkeiba.com/", "https://netkeiba.com/" })
                foreach (Cookie c in jar.GetCookies(new Uri(host)))
                {
                    if (!byName.TryGetValue(c.Name, out var existing) ||
                        (existing.Domain.StartsWith('.') && !c.Domain.StartsWith('.')))
                    {
                        byName[c.Name] = c;
                    }
                }
            if (byName.Count == 0) return;
            var header = string.Join("; ", byName.Values.Select(c => $"{c.Name}={c.Value}"));
            await _settings.SetStringAsync(SettingsService.Keys.OreProSessionCookie, header);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed persisting refreshed OrePro session cookie (non-fatal)");
        }
    }

    public async Task<JsonElement> PlaceAsync(JsonElement payload, CancellationToken ct)
    {
        // ── Parse + validate the request ────────────────────────────────────────
        var rawRaceId = (payload.TryGetProperty("race_id", out var ridEl) ? ridEl.GetString() ?? "" : "").Trim();
        if (string.IsNullOrEmpty(rawRaceId))
            return Err("race_id is required.");

        // 16-char JRA-VAN id → 12-char netkeiba id (drop MMDD), same convention as the apply path.
        var race12 = rawRaceId.Length == 16 ? rawRaceId[..4] + rawRaceId[8..] : rawRaceId;
        if (race12.Length != 12 || !race12.All(char.IsDigit))
            return Err($"race_id did not resolve to a 12-digit netkeiba id (got '{race12}').");

        var dryRun = payload.TryGetProperty("dry_run", out var dr) && dr.ValueKind == JsonValueKind.True;
        // Phase 37 (D/E): when true, after placing the custom lines into the cart we also COMMIT
        // them via api_post_mybet (same call the marks path uses) and record shared apply-state so
        // Auto Bet Day skips this race. Never honored on a dry run.
        var submitAfter = payload.TryGetProperty("submit_after_apply", out var sa) && sa.ValueKind == JsonValueKind.True;

        if (!payload.TryGetProperty("lines", out var linesEl) || linesEl.ValueKind != JsonValueKind.Array || linesEl.GetArrayLength() == 0)
            return Err("lines[] is required and must be a non-empty array of {type, method, umaban, money}.");

        var lines = new List<BetLine>();
        foreach (var l in linesEl.EnumerateArray())
        {
            int type   = GetInt(l, "type");
            int method = GetInt(l, "method");
            int money  = GetInt(l, "money");
            string umaban = (l.TryGetProperty("umaban", out var uEl) ? uEl.GetString() ?? "" : "").Trim();

            if (!ValidTypes.Contains(type))   return Err($"Invalid bet type {type} (must be 1-8: 1単 2複 3枠連 4馬連 5ワイド 6馬単 7三連複 8三連単).");
            if (!ValidMethods.Contains(method)) return Err($"Invalid method {method} (must be 0通常 1フォーメーション 2BOX 3ながし).");
            if (money <= 0)                   return Err("money (per-combination yen) must be > 0.");
            if (string.IsNullOrEmpty(umaban) || !Regex.IsMatch(umaban, @"^[0-9_\-]+$"))
                return Err($"umaban '{umaban}' is malformed (digits joined by '-' within a group, groups joined by '_').");

            lines.Add(new BetLine(type, method, umaban, money));
        }

        // ── Session / HTTP setup (mirrors OreProVoteApplyService) ────────────────
        var cookie = (await _settings.GetStringAsync(SettingsService.Keys.OreProSessionCookie) ?? "").Trim();
        if (string.IsNullOrEmpty(cookie))
            return Err("OrePro session cookie is not set. Open Settings → OrePro and paste the Cookie header value from DevTools.");
        var userAgent = (await _settings.GetStringAsync(SettingsService.Keys.OreProUserAgent) ?? "").Trim();
        if (string.IsNullOrEmpty(userAgent)) userAgent = SettingsService.Defaults.OreProUserAgent;

        var cookieJar = new CookieContainer();
        var oreproDomain = new Uri("https://orepro.netkeiba.com/");
        foreach (var pair in cookie.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var eq = pair.IndexOf('=');
            if (eq <= 0 || eq == pair.Length - 1) continue;
            var name = pair[..eq].Trim();
            var value = pair[(eq + 1)..].Trim();
            if (string.IsNullOrEmpty(name)) continue;
            try
            {
                cookieJar.Add(oreproDomain, new Cookie(name, value, "/", ".netkeiba.com"));
                cookieJar.Add(oreproDomain, new Cookie(name, value, "/", "orepro.netkeiba.com"));
            }
            catch { /* names CookieContainer rejects — ignore */ }
        }

        using var handler = new HttpClientHandler
        {
            UseCookies = false,
            CookieContainer = cookieJar,
            AutomaticDecompression = DecompressionMethods.All,
        };
        using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(25) };
        http.DefaultRequestHeaders.UserAgent.ParseAdd(userAgent);
        http.DefaultRequestHeaders.Add("Accept-Language", "ja,en;q=0.8");

        // ── Scrape the bet_id prefix parts from the shutuba page ─────────────────
        string shutubaHtml;
        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Get, $"{ShutubaUrl}?race_id={Uri.EscapeDataString(race12)}");
            req.Headers.Referrer = new Uri(BetReferer);
            req.Headers.Add("Origin", "https://orepro.netkeiba.com");
            using var resp = await http.SendAsync(req, ct);
            var bytes = await resp.Content.ReadAsByteArrayAsync(ct);
            var charset = (resp.Content.Headers.ContentType?.CharSet ?? "").Trim().Trim('"');
            Encoding enc;
            try { enc = string.IsNullOrEmpty(charset) ? Encoding.UTF8 : Encoding.GetEncoding(charset); }
            catch { enc = Encoding.UTF8; }
            shutubaHtml = enc.GetString(bytes);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Custom-bet: failed fetching shutuba for {Race}", race12);
            return Err($"Failed fetching OrePro shutuba page: {ex.Message}");
        }

        var weekId = VarWeekId.Match(shutubaHtml).Groups[1].Value.Trim();
        var jyoCd  = VarJyoCd.Match(shutubaHtml).Groups[1].Value.Trim();
        var raceNo = VarRaceNo.Match(shutubaHtml).Groups[1].Value.Trim();
        if (string.IsNullOrEmpty(weekId) || string.IsNullOrEmpty(jyoCd) || string.IsNullOrEmpty(raceNo))
        {
            // Missing JS vars almost always means a login redirect (expired cookie) rather than a real page.
            // Still persist — the response we just got may carry a fresher cookie than what we sent.
            await PersistSessionCookieAsync(cookieJar, ct);
            return Err($"Could not read week_id/jyo_cd/race_no from OrePro shutuba (week_id='{weekId}' jyo_cd='{jyoCd}' race_no='{raceNo}'). " +
                       "The session cookie is likely expired — re-paste it in Settings → OrePro.");
        }

        // prefix = a<week_id>-<int(jyo_cd)>-<int(race_no)>  (jyo/race leading zeros stripped)
        var prefix = $"a{weekId}-{int.Parse(jyoCd)}-{int.Parse(raceNo)}";

        // ── Build bet_ids + client_data ──────────────────────────────────────────
        var betEntries = lines.Select(l => new
        {
            bet_id = $"{prefix}_b{l.Type}_c{l.Method}_{l.Umaban}",
            money  = l.Money,
        }).ToList();
        var clientDataJson = JsonSerializer.Serialize(betEntries);

        // Full add URL (manual query build — item_id[] etc. repeat, and we must not double-encode).
        var cb = $"jQuery{Random.Shared.NextInt64(100000000000000, 999999999999999)}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        var ms = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var addUrl =
            $"{CartUrl}?callback={cb}&input=UTF-8&output=jsonp&action=add" +
            $"&group=orebet_{race12}" +
            "&item_id%5B%5D=bet_list&item_id%5B%5D=auto_bet" +
            "&item_value%5B%5D=1&item_value%5B%5D=1" +
            "&item_price%5B%5D=0&item_price%5B%5D=0" +
            $"&client_data%5B%5D={Uri.EscapeDataString(clientDataJson)}&client_data%5B%5D=0" +
            $"&_={ms}";

        if (dryRun)
        {
            return Json(new
            {
                status     = "dry-run",
                message    = "Dry run — nothing was placed. Eyeball the bet_ids and addUrl, then re-POST with dry_run=false.",
                raceId12   = race12,
                prefix,
                weekId, jyoCd, raceNo,
                betIds     = betEntries.Select(b => b.bet_id).ToArray(),
                clientData = clientDataJson,
                addUrl,
            });
        }

        // ── Switch the account into custom-bet mode (simple_bet=n) ──────────────
        // REQUIRED, confirmed 2026-06-06: a custom bet_list in the orebet_ bucket only RENDERS
        // (and is the active bet) when the account's 簡単投票 / easy-mode setting is OFF. The
        // browser fires this exact call right before the add; without it, OrePro keeps showing
        // the marks-derived auto-bet and our custom lines stay invisible even though they're in
        // the cart. NOTE: simple_bet is a GLOBAL account setting — turning it off here disables
        // marks→auto-bet generation on ALL races until it's turned back on.
        bool easyModeOff = false;
        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Post, UserSettingUrl)
            {
                Content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string,string>("input",      "UTF-8"),
                    new KeyValuePair<string,string>("output",     "jsonp"),
                    new KeyValuePair<string,string>("action",     "set"),
                    new KeyValuePair<string,string>("simple_bet", "n"),
                })
            };
            req.Headers.Referrer = new Uri($"{ShutubaUrl}?race_id={race12}");
            req.Headers.Add("Origin", "https://orepro.netkeiba.com");
            req.Headers.Add("X-Requested-With", "XMLHttpRequest");
            using var resp = await http.SendAsync(req, ct);
            var txt = await resp.Content.ReadAsStringAsync(ct);
            easyModeOff = resp.IsSuccessStatusCode && txt.Contains("\"status\":\"OK\"", StringComparison.OrdinalIgnoreCase);
            if (!easyModeOff)
                _logger.LogWarning("Custom-bet: simple_bet=n set did not confirm OK for {Race}. Body: {Body}", race12, Trunc(txt, 200));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Custom-bet: failed setting simple_bet=n for {Race} (continuing to add anyway)", race12);
        }

        // ── Fire the add ─────────────────────────────────────────────────────────
        string addResp;
        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Get, addUrl);
            req.Headers.Referrer = new Uri($"{ShutubaUrl}?race_id={race12}");
            req.Headers.Add("Origin", "https://orepro.netkeiba.com");
            using var resp = await http.SendAsync(req, ct);
            addResp = await resp.Content.ReadAsStringAsync(ct);
            if (!resp.IsSuccessStatusCode)
                return Err($"OrePro add returned HTTP {(int)resp.StatusCode}. Body: {Trunc(addResp, 300)}");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Custom-bet: add request failed for {Race}", race12);
            return Err($"Add request failed: {ex.Message}");
        }

        // ── Confirm by reading the cart back ─────────────────────────────────────
        string[] cartBetIds = Array.Empty<string>();
        string cartRaw = "";
        bool confirmed = false;
        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Post, CartUrl)
            {
                Content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string,string>("input",  "UTF-8"),
                    new KeyValuePair<string,string>("output", "json"),
                    new KeyValuePair<string,string>("action", "get"),
                    new KeyValuePair<string,string>("group",  $"orebet_{race12}"),
                })
            };
            req.Headers.Referrer = new Uri($"{ShutubaUrl}?race_id={race12}");
            req.Headers.Add("Origin", "https://orepro.netkeiba.com");
            using var resp = await http.SendAsync(req, ct);
            cartRaw = await resp.Content.ReadAsStringAsync(ct);
            cartBetIds = ExtractCartBetIds(cartRaw);
            // Confirmed = every bet_id we tried to place is present in the cart read-back.
            confirmed = betEntries.All(b => cartBetIds.Contains(b.bet_id));
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Custom-bet: cart read-back failed for {Race} (non-fatal)", race12);
        }

        // ── Submit (commit the cart) when asked ──────────────────────────────────
        // Reuses OrePro's bet-agnostic commit endpoint (api_post_mybet action=update), the SAME
        // one the marks path uses — it commits whatever is in the cart for the race, so it works
        // for a custom orebet_ ticket too. Only fires when the add was confirmed, to avoid
        // committing a half-placed bet.
        string submitStatus = "not-requested";
        string submitMessage = "";
        bool submitted = false;
        if (submitAfter && confirmed)
        {
            (submitStatus, submitMessage) = await TrySubmitAsync(http, race12, ct);
            submitted = submitStatus == "ok";
        }
        else if (submitAfter && !confirmed)
        {
            submitStatus = "skipped";
            submitMessage = "Submit skipped — cart read-back did not confirm the lines.";
        }

        // Record shared apply-state (same key/shape as the marks path) keyed by the JRA 16-char id,
        // so the frontend's apply badges + Auto Bet Day's "already submitted → skip" both see it.
        if (submitAfter)
            await RecordApplyStateAsync(rawRaceId, lines.Count, submitted, submitMessage, lines, ct);

        var finalStatus = !confirmed ? "warn"
                        : (submitAfter && !submitted) ? "warn"
                        : "ok";

        // Carry forward any session rotation from this run — the next OrePro call (another race in
        // the same Apply Day Votes batch) reads the stored cookie fresh and must not miss it.
        await PersistSessionCookieAsync(cookieJar, ct);

        return Json(new
        {
            status     = finalStatus,
            message    = !confirmed
                ? "Add fired, but the cart read-back did not confirm all bet_ids. Check OrePro manually."
                : submitAfter
                    ? (submitted ? "Custom ticket placed AND submitted to OrePro." : $"Custom ticket placed, but submit FAILED: {submitMessage}")
                    : "Custom ticket placed in the OrePro cart and confirmed via cart read-back.",
            raceId12   = race12,
            prefix,
            betIds     = betEntries.Select(b => b.bet_id).ToArray(),
            confirmed,
            easyModeOff,
            submitted,
            submitStatus,
            cartBetIds,
            addResponse = Trunc(addResp, 300),
            cartRaw     = Trunc(cartRaw, 800),
        });
    }

    /// <summary>
    /// Commits the cart for the given netkeiba race_id via api_post_mybet (action=update) — the
    /// same bet-agnostic commit the marks path uses. Returns ("ok", …) only on an explicit OK.
    /// </summary>
    private async Task<(string status, string message)> TrySubmitAsync(HttpClient http, string race12, CancellationToken ct)
    {
        const string url = "https://orepro.netkeiba.com/bet/api_post_mybet.html";
        var form = new[]
        {
            new KeyValuePair<string,string>("input",   "UTF-8"),
            new KeyValuePair<string,string>("output",  "jsonp"),
            new KeyValuePair<string,string>("action",  "update"),
            new KeyValuePair<string,string>("race_id", race12),
        };
        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Post, url) { Content = new FormUrlEncodedContent(form) };
            req.Headers.Referrer = new Uri($"{ShutubaUrl}?race_id={race12}");
            req.Headers.Add("Origin", "https://orepro.netkeiba.com");
            req.Headers.Add("X-Requested-With", "XMLHttpRequest");
            using var resp = await http.SendAsync(req, ct);
            var body = await resp.Content.ReadAsStringAsync(ct);
            if (!resp.IsSuccessStatusCode)
                return ("error", $"Submit HTTP {(int)resp.StatusCode}. Body: {Trunc(body, 200)}");
            if (body.Contains("\"status\":\"OK\"", StringComparison.OrdinalIgnoreCase))
                return ("ok", "Bet submitted to OrePro.");
            return ("warn", $"Submit returned 200 but no OK status. Body: {Trunc(body, 200)}");
        }
        catch (Exception ex)
        {
            return ("error", $"Submit threw: {ex.Message}");
        }
    }

    /// <summary>
    /// Persists OrePro apply-state under the SAME key/shape as OreProVoteApplyService, so the
    /// frontend's "📤 Submitted" badges and Auto Bet Day's skip-already-submitted both work for
    /// custom-path bets. Keyed by the JRA 16-char race id (what the frontend keys on).
    /// </summary>
    private async Task RecordApplyStateAsync(string jraRaceId, int lineCount, bool submitted, string? lastMessage, IReadOnlyList<BetLine> lines, CancellationToken ct)
    {
        try
        {
            var existing = await _appState.GetStringAsync(OreProVoteApplyService.ApplyStateKey) ?? "{}";
            Dictionary<string, JsonElement> dict;
            try { dict = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(existing) ?? new(); }
            catch { dict = new(); }

            var now = DateTime.UtcNow.ToString("O");
            var status = submitted ? "confirmed" : ((lastMessage ?? "").StartsWith("Submit skipped", StringComparison.OrdinalIgnoreCase) ? "unknown" : "failed");
            var attempts = new List<JsonElement>();
            if (dict.TryGetValue(jraRaceId, out var previous) && previous.ValueKind == JsonValueKind.Object &&
                previous.TryGetProperty("attempts", out var previousAttempts) && previousAttempts.ValueKind == JsonValueKind.Array)
                attempts.AddRange(previousAttempts.EnumerateArray().Select(x => x.Clone()));
            attempts.Add(JsonSerializer.SerializeToElement(new
            {
                attemptedAt = now,
                status,
                submitted,
                marksCount = lineCount,
                message = lastMessage,
                via = "custom",
            }));
            var betLines = lines.Select(l => new
            {
                ticket = l.Type switch { 1 => "win", 2 => "place", 5 => "wide", 7 => "trio", 8 => "trifecta", _ => "" },
                method = l.Method == 0 ? "normal" : "box",
                label = l.Type switch { 1 => "単勝", 2 => "複勝", 5 => "ワイド", 7 => "3連複", 8 => "3連単", _ => "" },
                horses = l.Umaban.Split('-', StringSplitOptions.RemoveEmptyEntries).Select(x => new { pp = int.Parse(x) }).ToArray(),
                comboCount = 1,
                stakePerCombo = (double)l.Money,
            }).Where(l => !string.IsNullOrEmpty(l.ticket)).ToArray();
            dict[jraRaceId] = JsonSerializer.SerializeToElement(new
            {
                appliedAt = now,
                submitted,
                submittedAt = submitted ? now : (string?)null,
                marksCount = lineCount,
                lastMessage,
                status,
                attemptCount = attempts.Count,
                attempts,
                betLines,
                via = "custom",
            });
            await _appState.SetStringAsync(OreProVoteApplyService.ApplyStateKey, JsonSerializer.Serialize(dict));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Custom-bet: failed to persist apply state for {RaceId}", jraRaceId);
        }
    }

    /// <summary>Parses the cart "get" JSON and returns the bet_ids currently in data.bet_list._cd.</summary>
    private static string[] ExtractCartBetIds(string cartJson)
    {
        try
        {
            using var doc = JsonDocument.Parse(cartJson);
            if (!doc.RootElement.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Object)
                return Array.Empty<string>();
            if (!data.TryGetProperty("bet_list", out var bl) || bl.ValueKind != JsonValueKind.Object)
                return Array.Empty<string>();
            if (!bl.TryGetProperty("_cd", out var cd) || cd.ValueKind != JsonValueKind.String)
                return Array.Empty<string>();
            var inner = cd.GetString() ?? "";
            if (string.IsNullOrWhiteSpace(inner)) return Array.Empty<string>();
            using var arr = JsonDocument.Parse(inner);
            if (arr.RootElement.ValueKind != JsonValueKind.Array) return Array.Empty<string>();
            var ids = new List<string>();
            foreach (var item in arr.RootElement.EnumerateArray())
                if (item.TryGetProperty("bet_id", out var idEl) && idEl.ValueKind == JsonValueKind.String)
                    ids.Add(idEl.GetString() ?? "");
            return ids.ToArray();
        }
        catch { return Array.Empty<string>(); }
    }

    private static int GetInt(JsonElement obj, string name)
    {
        if (!obj.TryGetProperty(name, out var el)) return int.MinValue;
        return el.ValueKind switch
        {
            JsonValueKind.Number => el.TryGetInt32(out var i) ? i : int.MinValue,
            JsonValueKind.String => int.TryParse(el.GetString(), out var s) ? s : int.MinValue,
            _ => int.MinValue,
        };
    }

    private static string Trunc(string s, int n) => string.IsNullOrEmpty(s) ? "" : (s.Length <= n ? s : s[..n]);

    private static JsonElement Err(string message) => Json(new { status = "error", message });

    private static JsonElement Json(object o)
    {
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(o));
        return doc.RootElement.Clone();
    }
}
