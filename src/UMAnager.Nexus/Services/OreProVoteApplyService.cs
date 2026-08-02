// ============================================================
// FILE: OreProVoteApplyService.cs
// LAYER: Service
// PURPOSE: Applies vote marks to orepro.netkeiba.com over plain HTTP using the user's stored
//          session cookie (no browser automation). Per race: fetch shutuba → build post→seq map →
//          POST /cart/ to set marks → generator/preview → optional submit. Records votes + apply-state.
// KEY DEPENDENCIES: SettingsService (cookie/UA), AppStateService (apply-state), VoteHistoryService.
// CAUTION: Converts JRA-VAN 16-char race id → 12-char OrePro/netkeiba id. Shutuba HTML charset
//          is detected per-response. Mirrors the old companion .ps1 contract — frontend unchanged.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Applies vote marks to OrePro by calling orepro.netkeiba.com directly with the user's
/// stored session cookie. No browser automation, no PowerShell — pure HTTP. Works regardless
/// of which client machine the dashboard is being viewed from.
///
/// Cookie source: the user logs in to OrePro in any browser, copies the Cookie header value
/// from DevTools, and pastes it into the OrePro Settings card. We attach it to every request.
///
/// Shutuba HTML is served as EUC-JP; we decode accordingly.
///
/// Mirrors the contract previously served by orepro_apply_votes_companion.ps1 so the existing
/// frontend code in script.js (applyVotesToOrePro / applySingleRaceVotesToOrePro) works without
/// modification.
/// </summary>
public sealed class OreProVoteApplyService
{
    private const string ShutubaUrl       = "https://orepro.netkeiba.com/bet/shutuba.html";
    private const string CartUrl          = "https://orepro.netkeiba.com/cart/";
    private const string GeneratorApiUrl  = "https://orepro.netkeiba.com/bet/api_post_bet_generator.html";
    private const string BetViewApiUrl    = "https://orepro.netkeiba.com/bet/api_get_bet_view.html";
    private const string BetReferer       = "https://orepro.netkeiba.com/bet/race_list.html";
    private const string UserSettingUrl   = "https://orepro.netkeiba.com/mydata/api_post_user_setting.html";

    private static readonly IReadOnlyDictionary<string, string> SymbolToMarkCode =
        new Dictionary<string, string> { ["◎"] = "1", ["〇"] = "2", ["▲"] = "3", ["△"] = "4" };
    private static readonly IReadOnlyDictionary<string, string> MarkCodeToSymbol =
        new Dictionary<string, string> { ["1"] = "◎", ["2"] = "〇", ["3"] = "▲", ["4"] = "△" };

    // Stores per-race OrePro apply/submit state as a JSON dict in app_state under this key.
    // Shape: { "<jraRaceId>": { appliedAt, submitted, submittedAt, marksCount, lastMessage } }
    public const string ApplyStateKey = "orepro_apply_state";

    private readonly ILogger<OreProVoteApplyService> _logger;
    private readonly SettingsService _settings;
    private readonly AppStateService _appState;
    private readonly VoteHistoryService _voteHistory;

    public OreProVoteApplyService(ILogger<OreProVoteApplyService> logger, SettingsService settings, AppStateService appState, VoteHistoryService voteHistory)
    {
        _logger      = logger;
        _settings    = settings;
        _appState    = appState;
        _voteHistory = voteHistory;
    }

    /// <summary>
    /// Re-reads whatever cookies OrePro set during this call (session rotation, CSRF refresh) and
    /// writes them back to Settings, so the NEXT independent OrePro call — a separate HTTP request
    /// that seeds its own fresh CookieContainer from the stored string — doesn't reuse a cookie OrePro
    /// already rotated past. Found live 2026-07-17: Apply Day Votes' two-step custom-bet flow
    /// (marks-first POST here, then a separate OreProCustomBetService.PlaceAsync POST) intermittently
    /// failed "not login" on the second call even though the session was fine, because the first
    /// call's Set-Cookie response never made it back into the cookie the second call reads.
    ///
    /// BUG FOUND same night: we seed every cookie under TWO domain scopes (wildcard ".netkeiba.com"
    /// AND exact "orepro.netkeiba.com"), so after a real response rotates a cookie, the jar can hold
    /// two DIFFERENT values for the same name — a stale one under the wildcard entry (never touched
    /// by the response) and the fresh one under the exact-host entry (what the response actually
    /// updated). The first cut of this method deduped by "whichever came first," a coin flip that
    /// could persist the STALE copy right before the next call needed the fresh one. Fixed: always
    /// prefer the non-wildcard (exact-domain) cookie when both exist for the same name.
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

    public async Task<JsonElement> ApplyAsync(JsonElement payload, CancellationToken ct)
    {
        var cookie = (await _settings.GetStringAsync(SettingsService.Keys.OreProSessionCookie) ?? "").Trim();
        if (string.IsNullOrEmpty(cookie))
        {
            return ErrorJson("OrePro session cookie is not set. Open Settings → OrePro and paste the Cookie header value from DevTools after logging in to orepro.netkeiba.com.", payload);
        }
        var userAgent = (await _settings.GetStringAsync(SettingsService.Keys.OreProUserAgent) ?? "").Trim();
        if (string.IsNullOrEmpty(userAgent)) userAgent = SettingsService.Defaults.OreProUserAgent;

        var dryRun = payload.TryGetProperty("dry_run", out var dryEl) && dryEl.ValueKind == JsonValueKind.True;
        var submitAfter = payload.TryGetProperty("submit_after_apply", out var subEl) && subEl.ValueKind == JsonValueKind.True;

        // Use a real CookieContainer so Set-Cookie responses from OrePro (CSRF tokens,
        // session rotation, etc.) ride forward into subsequent requests in this flow.
        // We seed it from the user's pasted Cookie header.
        var cookieJar = new CookieContainer();
        var oreproDomain = new Uri("https://orepro.netkeiba.com/");
        var netkeibaDomain = new Uri("https://netkeiba.com/");
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
            catch
            {
                // Names with characters CookieContainer rejects — fall back to the raw header below.
            }
        }

        using var handler = new HttpClientHandler
        {
            UseCookies = true,
            CookieContainer = cookieJar,
            AutomaticDecompression = DecompressionMethods.All,
        };
        using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(25) };
        http.DefaultRequestHeaders.UserAgent.ParseAdd(userAgent);
        http.DefaultRequestHeaders.Add("Accept-Language", "ja,en;q=0.8");

        var results = new List<object>();
        var lastOkRace = "";

        if (!payload.TryGetProperty("races", out var racesEl) || racesEl.ValueKind != JsonValueKind.Array)
        {
            return ErrorJson("Payload had no races array.", payload);
        }

        // Phase 37: the marks path needs easy-mode (簡単投票) ON so OrePro's generator builds the
        // default bet from the applied marks. The custom path flips it OFF; left in that state, a
        // marks apply would silently produce nothing. Self-correct here so the operator never has
        // to toggle 簡単投票 by hand. One-shot per apply, real fires only (skipped on dry runs).
        if (!dryRun)
            await TrySetSimpleBetAsync(http, "y", ct);

        foreach (var race in racesEl.EnumerateArray())
        {
            // raceId keeps the JRA-VAN 16-char form so the frontend can match results by it.
            // oreproRaceId is the 12-char form netkeiba uses (YYYY + TT + KK + DD + RR, MMDD stripped).
            var raceId = (race.TryGetProperty("race_id", out var ridEl) ? ridEl.GetString() ?? "" : "").Trim();
            if (string.IsNullOrEmpty(raceId))
            {
                results.Add(new { raceId = "", status = "error", message = "race_id is required", requested = Array.Empty<object>(), resolved = Array.Empty<object>() });
                continue;
            }
            var oreproRaceId = raceId.Length == 16 ? raceId[..4] + raceId[8..] : raceId;

            var requested = ExtractRequestedMarks(race);
            if (requested.Count == 0)
            {
                results.Add(new { raceId, status = "skipped", message = "No valid main marks (1-4) to apply for this race.", requested, resolved = Array.Empty<object>(), unmatchedPosts = Array.Empty<int>() });
                continue;
            }

            // 1. Fetch shutuba HTML and build the post → seq map. Decode using the
            //    Content-Type charset if present, falling back to UTF-8. (OrePro currently
            //    serves UTF-8; the v1 Python code mis-hinted EUC-JP and got away with it
            //    because BeautifulSoup re-detected. We have to be precise.)
            string shutubaHtml;
            string shutubaUrl = $"{ShutubaUrl}?race_id={Uri.EscapeDataString(oreproRaceId)}";
            int shutubaStatus = 0;
            string shutubaCharset = "?";
            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Get, shutubaUrl);
                req.Headers.Referrer = new Uri(BetReferer);
                using var resp = await http.SendAsync(req, ct);
                shutubaStatus = (int)resp.StatusCode;
                var bytes = await resp.Content.ReadAsByteArrayAsync(ct);
                shutubaCharset = (resp.Content.Headers.ContentType?.CharSet ?? "").Trim().Trim('"');
                Encoding enc;
                try { enc = string.IsNullOrEmpty(shutubaCharset) ? Encoding.UTF8 : Encoding.GetEncoding(shutubaCharset); }
                catch { enc = Encoding.UTF8; }
                shutubaHtml = enc.GetString(bytes);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed fetching shutuba for {RaceId}", raceId);
                results.Add(new { raceId, status = "error", message = $"Failed fetching shutuba page: {ex.Message}", requested, resolved = Array.Empty<object>(), unmatchedPosts = requested.Select(r => r.post).ToArray() });
                continue;
            }

            if (!ContainsLoggedInMarker(shutubaHtml))
            {
                var diag = BuildShutubaDiagnostic(shutubaHtml, raceId);
                _logger.LogWarning("OrePro shutuba returned no HorseList rows for {RaceId}. URL={Url} HTTP={Status} charset={Charset} Diagnostic: {Diag}",
                    raceId, shutubaUrl, shutubaStatus, shutubaCharset, diag);
                results.Add(new
                {
                    raceId,
                    status = "error",
                    message = $"OrePro returned no HorseList rows. URL={shutubaUrl} HTTP={shutubaStatus} charset={shutubaCharset}. Diagnostic: {diag}",
                    requested,
                    resolved = Array.Empty<object>(),
                    unmatchedPosts = requested.Select(r => r.post).ToArray()
                });
                continue;
            }

            var postToSeq = ExtractSeqByPost(shutubaHtml);
            var resolved = new List<dynamic>();
            var unmatched = new List<int>();
            foreach (var r in requested)
            {
                if (postToSeq.TryGetValue(r.post.ToString(), out var seq))
                {
                    resolved.Add(new { symbol = r.symbol, post = r.post, seq = int.Parse(seq), markCode = int.Parse(r.markCode) });
                }
                else
                {
                    unmatched.Add(r.post);
                }
            }

            if (resolved.Count == 0)
            {
                results.Add(new { raceId, status = "error", message = "None of the requested post numbers were found in OrePro shutuba rows.", requested, resolved = Array.Empty<object>(), unmatchedPosts = unmatched });
                continue;
            }

            if (dryRun)
            {
                results.Add(new { raceId, status = "dry-run", message = "Dry run only. No OrePro cart updates were sent.", requested, resolved, unmatchedPosts = unmatched });
                continue;
            }

            // 2. POST to /cart/ with form payload to set marks.
            object cartJson;
            try
            {
                var form = new List<KeyValuePair<string, string>>
                {
                    new("input", "UTF-8"),
                    new("output", "json"),
                    new("action", "replace"),
                    new("group", $"oremark_{oreproRaceId}"),
                };
                foreach (var row in resolved)
                {
                    form.Add(new("item_id[]",     ((int)row.seq).ToString()));
                    form.Add(new("item_value[]",  "1"));
                    form.Add(new("item_price[]",  "0"));
                    form.Add(new("client_data[]", $"_{(int)row.markCode}"));
                }

                using var req = new HttpRequestMessage(HttpMethod.Post, CartUrl)
                {
                    Content = new FormUrlEncodedContent(form)
                };
                req.Headers.Referrer = new Uri($"{ShutubaUrl}?race_id={oreproRaceId}");
                using var resp = await http.SendAsync(req, ct);
                var text = await resp.Content.ReadAsStringAsync(ct);

                try { cartJson = JsonSerializer.Deserialize<object>(text) ?? new { raw = text }; }
                catch { cartJson = new { raw = text.Length > 1000 ? text[..1000] : text }; }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed posting cart for {RaceId}", raceId);
                results.Add(new { raceId, status = "error", message = $"Failed applying marks via OrePro API: {ex.Message}", requested, resolved, unmatchedPosts = unmatched });
                continue;
            }

            // 3. Trigger generator + 4. fetch preview (best-effort — failure here doesn't fail the apply).
            var previewLines = new List<string>();
            try
            {
                using var genReq = new HttpRequestMessage(HttpMethod.Post, GeneratorApiUrl)
                {
                    Content = new FormUrlEncodedContent(new[]
                    {
                        new KeyValuePair<string,string>("input", "UTF-8"),
                        new KeyValuePair<string,string>("output", "jsonp"),
                        new KeyValuePair<string,string>("race_id", oreproRaceId),
                    })
                };
                genReq.Headers.Referrer = new Uri(BetReferer);
                using var _g = await http.SendAsync(genReq, ct);

                using var viewReq = new HttpRequestMessage(HttpMethod.Post, BetViewApiUrl)
                {
                    Content = new FormUrlEncodedContent(new[]
                    {
                        new KeyValuePair<string,string>("input", "UTF-8"),
                        new KeyValuePair<string,string>("output", "jsonp"),
                        new KeyValuePair<string,string>("race_id", oreproRaceId),
                        new KeyValuePair<string,string>("src", "session"),
                    })
                };
                viewReq.Headers.Referrer = new Uri(BetReferer);
                using var viewResp = await http.SendAsync(viewReq, ct);
                var viewText = await viewResp.Content.ReadAsStringAsync(ct);
                previewLines = ExtractPreviewLines(viewText);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Preview fetch failed for {RaceId} (non-fatal)", raceId);
            }

            // 5. submit_after_apply — POST api_post_mybet.html action=update to commit the bet.
            object? submitFlow = null;
            string finalMessage = "Marks applied to OrePro cart.";
            string finalStatus  = "ok";
            if (submitAfter)
            {
                var sub = await TrySubmitAsync(http, oreproRaceId, ct);
                submitFlow = new { submitStatus = sub.status, submitMessage = sub.message, receiptUrl = sub.receiptUrl };
                if (sub.status == "ok")
                {
                    finalMessage = "Marks applied AND bet submitted to OrePro.";
                }
                else
                {
                    finalMessage = $"Marks applied, but submit FAILED: {sub.message}";
                    finalStatus  = "warn";
                }
            }

            results.Add(new
            {
                raceId,
                status = finalStatus,
                message = finalMessage,
                requested,
                resolved,
                unmatchedPosts = unmatched,
                cartResponse = cartJson,
                betPreviewLines = previewLines,
                submitFlow,
            });
            lastOkRace = raceId;

            // Persist apply/submit state so the UI can show a badge after refresh.
            await RecordApplyStateAsync(raceId, resolved.Count, submitFlow, ct);

            // Phase 30: record votes to vote_history so repeat runners show "Voted N×" badge.
            var votesToRecord = ExtractHorseIdsFromMarks(race, resolved);
            if (votesToRecord.Count > 0)
                _ = Task.Run(() => _voteHistory.RecordVotesAsync(raceId, votesToRecord));
        }

        var okCount = results.Count(r => r.GetType().GetProperty("status")?.GetValue(r) as string == "ok");

        var responseObj = new
        {
            status = okCount > 0 ? "ok" : "warn",
            dryRun,
            message = $"Applied marks for {okCount}/{results.Count} race(s). " +
                      (submitAfter
                          ? "Submit-after-apply was attempted — see submitFlow per race in diagnostics."
                          : "Marks are in your OrePro cart; open OrePro in your browser to confirm/submit."),
            results,
            lastOkRace,
        };

        // Carry forward any session rotation from this run before handing control back — the
        // very next OrePro call (e.g. the custom-bet placement that follows marks-first) reads
        // the stored cookie fresh and must not miss whatever OrePro just rotated it to.
        await PersistSessionCookieAsync(cookieJar, ct);

        var json = JsonSerializer.Serialize(responseObj);
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }

    /// <summary>
    /// Lightweight "is this OrePro session cookie actually logged in?" probe — no marks placed,
    /// no cart touched. Apply Day Votes calls this BEFORE the loop so a dead cookie stops at zero
    /// bets instead of failing every race; the Settings "Test OrePro Session" button uses it too.
    ///
    /// When a race_id is supplied (the reliable path — the dashboard always has the loaded card)
    /// we fetch that race's shutuba and reuse the same HorseList-row marker the apply path trusts.
    /// With no race we fall back to the bet race-list page; that's best-effort (an empty card on a
    /// non-race day can read as "not confirmed") so the message says so.
    /// Returns { status: ok|expired|unconfigured|error, loggedIn, message }.
    /// </summary>
    /// <summary>
    /// Verifies the stored OrePro session. If it's not logged in (expired/unconfigured) AND login
    /// credentials are stored, transparently re-logs in once and re-checks — so a stale cookie
    /// self-heals without the operator noticing. <paramref name="allowRelogin"/> guards against
    /// recursion (LoginAsync's own verification passes false).
    /// </summary>
    public async Task<JsonElement> CheckCookieAsync(string? rawRaceId, CancellationToken ct, bool allowRelogin = true)
    {
        var result = await CheckCookieOnceAsync(rawRaceId, ct);
        var loggedIn = result.TryGetProperty("loggedIn", out var li) && li.ValueKind == JsonValueKind.True;
        if (loggedIn || !allowRelogin) return result;

        // Cookie is dead — auto-relogin if we have credentials, then re-verify once.
        if (await HasStoredCredentialsAsync())
        {
            _logger.LogInformation("[OrePro] Session not logged in; attempting server-side auto-relogin.");
            var login = await LoginAsync(null, null, rawRaceId, ct);
            var reLoggedIn = login.TryGetProperty("loggedIn", out var rli) && rli.ValueKind == JsonValueKind.True;
            if (reLoggedIn) return await CheckCookieOnceAsync(rawRaceId, ct);
        }
        return result;
    }

    private async Task<JsonElement> CheckCookieOnceAsync(string? rawRaceId, CancellationToken ct)
    {
        var cookie = (await _settings.GetStringAsync(SettingsService.Keys.OreProSessionCookie) ?? "").Trim();
        if (string.IsNullOrEmpty(cookie))
            return Result("unconfigured", false, "No OrePro session cookie is set. Paste the Cookie header value from a logged-in OrePro session into Settings → OrePro.");

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
            UseCookies = true,
            CookieContainer = cookieJar,
            AutomaticDecompression = DecompressionMethods.All,
        };
        using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(20) };
        http.DefaultRequestHeaders.UserAgent.ParseAdd(userAgent);
        http.DefaultRequestHeaders.Add("Accept-Language", "ja,en;q=0.8");

        var raceId = (rawRaceId ?? "").Trim();
        try
        {
            if (!string.IsNullOrEmpty(raceId))
            {
                var oreproRaceId = raceId.Length == 16 ? raceId[..4] + raceId[8..] : raceId;
                var url = $"{ShutubaUrl}?race_id={Uri.EscapeDataString(oreproRaceId)}";
                using var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.Referrer = new Uri(BetReferer);
                using var resp = await http.SendAsync(req, ct);
                var bytes = await resp.Content.ReadAsByteArrayAsync(ct);
                var charset = (resp.Content.Headers.ContentType?.CharSet ?? "").Trim().Trim('"');
                Encoding enc;
                try { enc = string.IsNullOrEmpty(charset) ? Encoding.UTF8 : Encoding.GetEncoding(charset); }
                catch { enc = Encoding.UTF8; }
                var html = enc.GetString(bytes);

                // Even a read-only check can pick up a rotated session cookie — save it before
                // the caller's next (separate) OrePro call reads the stored string fresh.
                await PersistSessionCookieAsync(cookieJar, ct);

                var isLoggedIn = ContainsLoggedInMarker(html);
                if (!isLoggedIn)
                {
                    // This branch used to be completely silent — a "not logged in" verdict left no
                    // trace of WHY (what page actually came back). Found live 2026-07-17 debugging a
                    // false-negative-shaped failure with zero log evidence either way. Same diagnostic
                    // shape as the marks-apply path's "no HorseList rows" log below.
                    var diag = BuildShutubaDiagnostic(html, raceId);
                    _logger.LogWarning("OrePro cookie check: shutuba page for {RaceId} did not show a logged-in marker. URL={Url} HTTP={Status} {Diag}",
                        raceId, url, (int)resp.StatusCode, diag);
                }
                return isLoggedIn
                    ? Result("ok", true, "OrePro session is logged in — bets can be placed.")
                    : Result("expired", false, "OrePro session is NOT logged in (the cookie looks expired). Re-copy the Cookie header from a fresh OrePro login into Settings → OrePro.");
            }

            // No race supplied — probe the bet race-list page for LOGIN-GATED markers only.
            // NOTE (s61 fix): we used to also treat `shutuba.html?race_id=` links as proof of login,
            // but those race-card links are served to LOGGED-OUT visitors too (the race list is public),
            // so a dead cookie could match them and FALSE-POSITIVE as "logged in — bets can be placed."
            // Only the logout control and the login-gated /mydata/ area actually require a session, so
            // trust just those. Biasing toward a false NEGATIVE here is the safe direction (worst case:
            // "open a race and re-test"); a false positive would tell the operator they can bet when they
            // can't. When a race is available the caller uses the stronger ContainsLoggedInMarker path above.
            using var lreq = new HttpRequestMessage(HttpMethod.Get, BetReferer);
            using var lresp = await http.SendAsync(lreq, ct);
            var body = await lresp.Content.ReadAsStringAsync(ct);
            await PersistSessionCookieAsync(cookieJar, ct);
            var loggedIn = body.Contains("ログアウト", StringComparison.Ordinal)
                           || body.Contains("/mydata/", StringComparison.OrdinalIgnoreCase);
            if (!loggedIn)
            {
                var diag = BuildShutubaDiagnostic(body, "(no race supplied)");
                _logger.LogWarning("OrePro cookie check: race-list page did not show logged-in markers. URL={Url} HTTP={Status} {Diag}",
                    BetReferer, (int)lresp.StatusCode, diag);
            }
            return loggedIn
                ? Result("ok", true, "OrePro session is logged in.")
                : Result("expired", false, "Could not confirm an OrePro login from the race-list page (cookie may be expired, or no race card is open today). Open a race day and test again, or re-paste the cookie.");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "OrePro cookie check failed");
            return Result("error", false, $"Cookie check failed to reach OrePro: {ex.Message}");
        }
    }

    private const string NetkeibaLoginUrl = "https://regist.netkeiba.com/";

    /// <summary>
    /// Logs in to netkeiba server-side with the stored (or supplied) credentials and re-mints
    /// <c>orepro_session_cookie</c> from the Set-Cookie handshake — no browser, works from any
    /// device. Returns { status, loggedIn, message }. On success the fresh cookie is persisted and
    /// then verified against a real shutuba page (the same reliable check the Settings button uses).
    /// </summary>
    public async Task<JsonElement> LoginAsync(string? loginIdOverride, string? passwordOverride, string? verifyRaceId, CancellationToken ct)
    {
        var loginId = (loginIdOverride ?? await _settings.GetStringAsync(SettingsService.Keys.OreProLoginId) ?? "").Trim();
        var password = passwordOverride ?? await _settings.GetStringAsync(SettingsService.Keys.OreProPassword) ?? "";
        if (string.IsNullOrEmpty(loginId) || string.IsNullOrEmpty(password))
            return Result("unconfigured", false, "OrePro login id and password are not set. Enter them in Settings → OrePro, then try again.");

        var userAgent = (await _settings.GetStringAsync(SettingsService.Keys.OreProUserAgent) ?? "").Trim();
        if (string.IsNullOrEmpty(userAgent)) userAgent = SettingsService.Defaults.OreProUserAgent;

        var cookieJar = new CookieContainer();
        using var handler = new HttpClientHandler
        {
            UseCookies = true,
            CookieContainer = cookieJar,
            AllowAutoRedirect = true,
            AutomaticDecompression = DecompressionMethods.All,
        };
        using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(25) };
        http.DefaultRequestHeaders.UserAgent.ParseAdd(userAgent);
        http.DefaultRequestHeaders.Add("Accept-Language", "ja,en;q=0.8");

        try
        {
            // Prime any initial cookies the login page hands out (some flows expect them echoed back).
            using (var priming = new HttpRequestMessage(HttpMethod.Get, "https://regist.netkeiba.com/account/?pid=login"))
                (await http.SendAsync(priming, ct)).Dispose();

            var form = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("pid", "login"),
                new KeyValuePair<string, string>("action", "auth"),
                new KeyValuePair<string, string>("rtn_url", ""),
                new KeyValuePair<string, string>("login_id", loginId),
                new KeyValuePair<string, string>("pswd", password),
            });
            using var loginReq = new HttpRequestMessage(HttpMethod.Post, NetkeibaLoginUrl) { Content = form };
            loginReq.Headers.Referrer = new Uri("https://regist.netkeiba.com/account/?pid=login");
            loginReq.Headers.Add("Origin", "https://regist.netkeiba.com");
            using var loginResp = await http.SendAsync(loginReq, ct);
            var respBytes = await loginResp.Content.ReadAsByteArrayAsync(ct);
            // The regist/login page is served UTF-8 (unlike the EUC-JP betting pages) — honor the
            // response charset, fall back to UTF-8, so any error text decodes readably.
            var respCharset = (loginResp.Content.Headers.ContentType?.CharSet ?? "").Trim().Trim('"');
            string respBody;
            try { respBody = (string.IsNullOrEmpty(respCharset) ? Encoding.UTF8 : Encoding.GetEncoding(respCharset)).GetString(respBytes); }
            catch { respBody = Encoding.UTF8.GetString(respBytes); }

            // Harvest every cookie the handshake set on the netkeiba domains. The auth ones
            // (netkeiba, nkauth) are what OrePro requires; grab the analytics ones too so the
            // header looks like a real browser's.
            var jar = new List<Cookie>();
            foreach (var host in new[] { "https://regist.netkeiba.com/", "https://orepro.netkeiba.com/", "https://www.netkeiba.com/" })
                foreach (Cookie c in cookieJar.GetCookies(new Uri(host)))
                    if (!jar.Any(x => x.Name == c.Name)) jar.Add(c);

            var hasAuth = jar.Any(c => c.Name.Equals("nkauth", StringComparison.OrdinalIgnoreCase))
                       || jar.Any(c => c.Name.Equals("netkeiba", StringComparison.OrdinalIgnoreCase));
            if (!hasAuth)
            {
                // Surface WHY, from netkeiba's own response, so the operator can self-diagnose.
                // Log a redacted diagnostic (final URL, status, page title, cookie names) — never the password.
                var title = Regex.Match(respBody, "<title>(?<t>[^<]*)</title>", RegexOptions.IgnoreCase).Groups["t"].Value.Trim();
                var cookieNames = string.Join(",", jar.Select(c => c.Name));
                _logger.LogWarning("[OrePro] Login got no auth cookie. finalUrl={Url} status={Status} title=\"{Title}\" cookies=[{Cookies}] bodyLen={Len}",
                    loginResp.RequestMessage?.RequestUri, (int)loginResp.StatusCode, title, cookieNames, respBody.Length);

                var badCreds = respBody.Contains("パスワードが間違", StringComparison.Ordinal)
                            || respBody.Contains("メールアドレス）もしくは", StringComparison.Ordinal)
                            || respBody.Contains("間違っています", StringComparison.Ordinal);
                if (badCreds)
                    return Result("bad-credentials", false, "netkeiba says the login ID or password is incorrect. Double-check both — the ID is the EMAIL you log in with (or your netkeiba member ID), and re-type the password to be sure (a trailing space or wrong character will do it).");

                var botCheck = respBody.Contains("captcha", StringComparison.OrdinalIgnoreCase)
                            || respBody.Contains("認証", StringComparison.Ordinal) && respBody.Contains("画像", StringComparison.Ordinal);
                if (botCheck)
                    return Result("bot-check", false, "netkeiba is asking for an extra verification (captcha) that this can't complete automatically. Use the manual cookie paste under Advanced this time.");

                return Result("failed", false, $"netkeiba didn't return a login session (page title: \"{title}\"). Check the credentials, or use the manual cookie paste under Advanced.");
            }

            var cookieHeader = string.Join("; ", jar.Select(c => $"{c.Name}={c.Value}"));
            await _settings.SetStringAsync(SettingsService.Keys.OreProSessionCookie, cookieHeader);

            // Verify the fresh cookie against a real logged-in-only page (reliable check).
            // allowRelogin:false — we're already inside the login flow; don't recurse.
            var verify = await CheckCookieAsync(verifyRaceId, ct, allowRelogin: false);
            var loggedIn = verify.TryGetProperty("loggedIn", out var li) && li.ValueKind == JsonValueKind.True;
            return loggedIn
                ? Result("ok", true, "Logged in to OrePro and refreshed the session. You're ready to bet.")
                : Result("saved-unverified", false, "Login handshake succeeded and a cookie was saved, but it didn't verify as logged in. Try once more; if it persists, use the manual cookie paste.");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "OrePro server-side login failed");
            return Result("error", false, $"Could not reach netkeiba to log in: {ex.Message}");
        }
    }

    /// <summary>True when both stored login credentials are present (enables auto-relogin).</summary>
    public async Task<bool> HasStoredCredentialsAsync()
    {
        var id = (await _settings.GetStringAsync(SettingsService.Keys.OreProLoginId) ?? "").Trim();
        var pw = await _settings.GetStringAsync(SettingsService.Keys.OreProPassword) ?? "";
        return !string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(pw);
    }

    private static JsonElement Result(string status, bool loggedIn, string message)
    {
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(new { status, loggedIn, message }));
        return doc.RootElement.Clone();
    }

    // ─── HTML parsing ──────────────────────────────────────────────────────────

    private static readonly Regex RowRegex = new(
        """<tr\b[^>]*\bid="tr_(?<seq>\d+)"[^>]*>(?<body>.*?)</tr>""",
        RegexOptions.Singleline | RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex WakuCellRegex = new(
        """<td\b[^>]*\bid="act_waku_[^"]*"[^>]*>(?<inner>.*?)</td>""",
        RegexOptions.Singleline | RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex DigitsOnly = new(@"\D+", RegexOptions.Compiled);

    private static Dictionary<string, string> ExtractSeqByPost(string html)
    {
        var map = new Dictionary<string, string>();
        if (string.IsNullOrEmpty(html)) return map;

        foreach (Match row in RowRegex.Matches(html))
        {
            if (!row.Success) continue;
            // Only count rows that look like HorseList rows (defensive).
            if (!row.Value.Contains("HorseList", StringComparison.Ordinal)) continue;

            var seq = row.Groups["seq"].Value;
            var body = row.Groups["body"].Value;
            var cell = WakuCellRegex.Match(body);
            if (!cell.Success) continue;

            var inner = StripTags(cell.Groups["inner"].Value);
            var digits = DigitsOnly.Replace(inner, "");
            if (digits.Length > 0 && !map.ContainsKey(digits))
            {
                map[digits] = seq;
            }
        }
        return map;
    }

    private static string StripTags(string s)
        => Regex.Replace(s ?? "", "<[^>]+>", " ");

    private static bool ContainsLoggedInMarker(string html)
    {
        // If OrePro served a login redirect, the response will lack the shutuba table entirely.
        // We treat presence of any `tr_<digits>` HorseList row as proof the session is valid.
        return RowRegex.IsMatch(html ?? "");
    }

    /// <summary>
    /// Returns a short, redacted summary of what OrePro served when the HorseList rows
    /// were missing — title, response length, login-page markers, "no entries" markers.
    /// Does NOT include any cookie values or credentials.
    /// </summary>
    private static string BuildShutubaDiagnostic(string html, string raceId)
    {
        if (string.IsNullOrEmpty(html)) return "empty response body";

        var title = Regex.Match(html, "<title>(?<t>.*?)</title>", RegexOptions.Singleline | RegexOptions.IgnoreCase);
        var titleText = title.Success ? title.Groups["t"].Value.Trim() : "(no title)";

        // Quick signal flags.
        var hasLoginForm   = Regex.IsMatch(html, @"login|\bsign[\s_-]?in|ログイン", RegexOptions.IgnoreCase);
        var hasNoEntries   = html.Contains("出馬表が公開されていません") || html.Contains("未確定") || html.Contains("出走馬未定");
        var hasShutubaRoot = html.Contains("Shutuba_Table") || html.Contains("HorseList");
        var lengthChars    = html.Length;

        // First 200 chars of stripped body text — gives a hint without leaking cookies.
        var stripped = Regex.Replace(StripTags(html), @"\s+", " ").Trim();
        var preview  = stripped.Length > 200 ? stripped[..200] : stripped;

        return $"raceId={raceId}; title=\"{titleText}\"; len={lengthChars}; hasLoginForm={hasLoginForm}; hasNoEntriesMsg={hasNoEntries}; hasShutubaRoot={hasShutubaRoot}; preview=\"{preview}\"";
    }

    private static List<string> ExtractPreviewLines(string jsonpText)
    {
        var lines = new List<string>();
        if (string.IsNullOrWhiteSpace(jsonpText)) return lines;

        // JSONP shape: foo({...}) — strip to inner object.
        var trimmed = jsonpText.Trim();
        var lp = trimmed.IndexOf('{');
        var rp = trimmed.LastIndexOf('}');
        if (lp < 0 || rp <= lp) return lines;
        var inner = trimmed[lp..(rp + 1)];

        try
        {
            using var doc = JsonDocument.Parse(inner);
            if (!doc.RootElement.TryGetProperty("data", out var dataEl) || dataEl.ValueKind != JsonValueKind.String)
                return lines;

            var html = dataEl.GetString() ?? "";
            var text = Regex.Replace(StripTags(html), @"\s+", " ").Trim();
            foreach (var part in text.Split(new[] { ' ', '\n', '\r', '\t' }, StringSplitOptions.RemoveEmptyEntries))
            {
                if (lines.Count >= 24) break;
                if (part.Length > 0) lines.Add(part);
            }
        }
        catch
        {
            /* ignore */
        }
        return lines;
    }

    // ─── Mark extraction ───────────────────────────────────────────────────────

    private static List<RequestedMark> ExtractRequestedMarks(JsonElement race)
    {
        var singleByCode = new Dictionary<string, int>();
        var extraTriangles = new List<int>();
        var seenPairs = new HashSet<string>();

        if (!race.TryGetProperty("marks", out var marksEl) || marksEl.ValueKind != JsonValueKind.Array)
            return new();

        foreach (var m in marksEl.EnumerateArray())
        {
            string markCode = "";
            if (m.TryGetProperty("mark_code", out var mc))
            {
                markCode = mc.ValueKind == JsonValueKind.String ? mc.GetString() ?? ""
                          : mc.ValueKind == JsonValueKind.Number ? mc.GetInt32().ToString()
                          : "";
            }
            if (string.IsNullOrEmpty(markCode) && m.TryGetProperty("symbol", out var sym) && sym.ValueKind == JsonValueKind.String)
            {
                SymbolToMarkCode.TryGetValue(sym.GetString()?.Trim() ?? "", out var fromSym);
                markCode = fromSym ?? "";
            }
            if (markCode is not ("1" or "2" or "3" or "4")) continue;

            int post = 0;
            if (m.TryGetProperty("post", out var p))
            {
                post = p.ValueKind == JsonValueKind.Number ? p.GetInt32()
                     : p.ValueKind == JsonValueKind.String && int.TryParse(p.GetString(), out var pi) ? pi
                     : 0;
            }
            if (post <= 0) continue;

            var pairKey = $"{markCode}:{post}";
            if (!seenPairs.Add(pairKey)) continue;

            if (markCode == "4") extraTriangles.Add(post);
            else if (!singleByCode.ContainsKey(markCode)) singleByCode[markCode] = post;
        }

        var ordered = new List<RequestedMark>();
        foreach (var kv in singleByCode.OrderBy(k => int.Parse(k.Key)))
            ordered.Add(new RequestedMark(MarkCodeToSymbol[kv.Key], kv.Value, kv.Key));
        foreach (var post in extraTriangles)
            ordered.Add(new RequestedMark(MarkCodeToSymbol["4"], post, "4"));

        return ordered;
    }

    private record RequestedMark(string symbol, int post, string markCode);

    /// <summary>
    /// Matches resolved marks back to horse_ids using the optional horse_id field the
    /// frontend embeds in each mark object (added in Phase 30). Falls back to post-position
    /// lookup in the incoming marks array if horse_id is absent (backwards compat).
    /// Returns a list of (horseId, symbol) pairs for all successfully resolved marks.
    /// </summary>
    private static List<(string horseId, string mark)> ExtractHorseIdsFromMarks(JsonElement race, IEnumerable<dynamic> resolvedMarks)
    {
        // Build post → horse_id map from the marks array (frontend sends horse_id per mark).
        var postToHorse = new Dictionary<int, string>();
        if (race.TryGetProperty("marks", out var marksEl) && marksEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var m in marksEl.EnumerateArray())
            {
                if (!m.TryGetProperty("horse_id", out var hEl) || hEl.ValueKind != JsonValueKind.String) continue;
                var hId = hEl.GetString()?.Trim() ?? "";
                if (string.IsNullOrEmpty(hId)) continue;
                int post = 0;
                if (m.TryGetProperty("post", out var p))
                {
                    post = p.ValueKind == JsonValueKind.Number ? p.GetInt32()
                         : p.ValueKind == JsonValueKind.String && int.TryParse(p.GetString(), out var pi) ? pi : 0;
                }
                if (post > 0) postToHorse[post] = hId;
            }
        }

        var result = new List<(string, string)>();
        foreach (var r in resolvedMarks)
        {
            try
            {
                int post = (int)r.post;
                string symbol = (string)r.symbol;
                if (postToHorse.TryGetValue(post, out var horseId) && !string.IsNullOrEmpty(horseId))
                    result.Add((horseId, symbol));
            }
            catch { /* dynamic access can throw on type mismatch — skip */ }
        }
        return result;
    }

    // ─── Submit (best-effort) ──────────────────────────────────────────────────

    /// <summary>
    /// Submits the bet for the given OrePro race_id via the same endpoint OrePro's own JS
    /// fires on submit click. Captured from a real HAR on 2026-05-16:
    ///   POST /bet/api_post_mybet.html  body: input=UTF-8&output=jsonp&action=update&race_id=...
    ///   Headers: Origin, Referer (shutuba page), X-Requested-With: XMLHttpRequest
    /// raceId here is the 12-char netkeiba form.
    /// </summary>
    /// <summary>
    /// Commits the bet for the given OrePro race_id via /bet/api_post_mybet.html. Captured
    /// from a real OrePro session HAR on 2026-05-16; works for both new and edited bets
    /// (OrePro's free-play model lets you adjust bets in place even after submit).
    ///   POST /bet/api_post_mybet.html  body: input=UTF-8&output=jsonp&action=update&race_id=...
    ///   Headers: Origin, Referer (shutuba page), X-Requested-With: XMLHttpRequest
    /// </summary>
    /// <summary>
    /// Sets the account's 簡単投票 / easy-mode flag (simple_bet = "y" on / "n" off). Best-effort —
    /// a failure here is logged but doesn't fail the apply (OrePro may already be in the right state).
    /// </summary>
    private async Task TrySetSimpleBetAsync(HttpClient http, string value, CancellationToken ct)
    {
        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Post, UserSettingUrl)
            {
                Content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string,string>("input",      "UTF-8"),
                    new KeyValuePair<string,string>("output",     "jsonp"),
                    new KeyValuePair<string,string>("action",     "set"),
                    new KeyValuePair<string,string>("simple_bet", value),
                })
            };
            req.Headers.Referrer = new Uri(BetReferer);
            req.Headers.Add("X-Requested-With", "XMLHttpRequest");
            using var resp = await http.SendAsync(req, ct);
            if (!resp.IsSuccessStatusCode)
                _logger.LogWarning("Set simple_bet={Value} returned HTTP {Status}", value, (int)resp.StatusCode);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed setting simple_bet={Value} (continuing — may already be set)", value);
        }
    }

    private async Task<(string status, string message, string receiptUrl)> TrySubmitAsync(HttpClient http, string raceId, CancellationToken ct)
    {
        var url = "https://orepro.netkeiba.com/bet/api_post_mybet.html";
        var form = new[]
        {
            new KeyValuePair<string,string>("input",   "UTF-8"),
            new KeyValuePair<string,string>("output",  "jsonp"),
            new KeyValuePair<string,string>("action",  "update"),
            new KeyValuePair<string,string>("race_id", raceId),
        };

        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Post, url) { Content = new FormUrlEncodedContent(form) };
            req.Headers.Referrer = new Uri($"https://orepro.netkeiba.com/bet/shutuba.html?race_id={raceId}");
            req.Headers.Add("Origin", "https://orepro.netkeiba.com");
            req.Headers.Add("X-Requested-With", "XMLHttpRequest");

            using var resp = await http.SendAsync(req, ct);
            var body = await resp.Content.ReadAsStringAsync(ct);
            var inner = ExtractJsonpInner(body) ?? body;

            if (!resp.IsSuccessStatusCode)
            {
                return ("error", $"Submit HTTP {(int)resp.StatusCode}. Body: {Trunc(inner, 200)}", "");
            }
            // OrePro returns {"status":"OK", ...} on success. A 200 with any other body
            // means OrePro didn't accept the submit (rare, but worth surfacing distinctly).
            if (inner.Contains("\"status\":\"OK\"", StringComparison.OrdinalIgnoreCase))
            {
                return ("ok", "Bet submitted to OrePro.", url);
            }
            return ("warn", $"Submit returned 200 but no OK status. Body: {Trunc(inner, 200)}", "");
        }
        catch (Exception ex)
        {
            return ("error", $"Submit threw: {ex.Message}", "");
        }
    }

    private async Task RecordApplyStateAsync(string jraRaceId, int marksCount, object? submitFlow, CancellationToken ct)
    {
        try
        {
            var existing = await _appState.GetStringAsync(ApplyStateKey) ?? "{}";
            Dictionary<string, JsonElement> dict;
            try { dict = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(existing) ?? new(); }
            catch { dict = new(); }

            var submitted = false;
            string? lastMsg = null;
            if (submitFlow is not null)
            {
                var sfJson = JsonSerializer.SerializeToElement(submitFlow);
                if (sfJson.TryGetProperty("submitStatus", out var ss) && ss.GetString() == "ok") submitted = true;
                if (sfJson.TryGetProperty("submitMessage", out var sm)) lastMsg = sm.GetString();
            }

            var now = DateTime.UtcNow.ToString("O");
            var status = submitFlow is null ? "applied" : (submitted ? "confirmed" : "failed");
            var attempts = new List<JsonElement>();
            if (dict.TryGetValue(jraRaceId, out var previous) && previous.ValueKind == JsonValueKind.Object &&
                previous.TryGetProperty("attempts", out var previousAttempts) && previousAttempts.ValueKind == JsonValueKind.Array)
                attempts.AddRange(previousAttempts.EnumerateArray().Select(x => x.Clone()));
            attempts.Add(JsonSerializer.SerializeToElement(new
            {
                attemptedAt = now,
                status,
                submitted,
                marksCount,
                message = lastMsg,
                via = "marks",
            }));
            var entry = new
            {
                appliedAt    = now,
                submitted,
                submittedAt  = submitted ? now : (string?)null,
                marksCount,
                lastMessage  = lastMsg,
                status,
                attemptCount = attempts.Count,
                attempts,
            };
            dict[jraRaceId] = JsonSerializer.SerializeToElement(entry);

            await _appState.SetStringAsync(ApplyStateKey, JsonSerializer.Serialize(dict));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to persist OrePro apply state for {RaceId}", jraRaceId);
        }
    }

    private static string Trunc(string s, int n) => string.IsNullOrEmpty(s) ? "" : (s.Length <= n ? s : s[..n]);

    private static string? ExtractJsonpInner(string text)
    {
        if (string.IsNullOrEmpty(text)) return null;
        var lp = text.IndexOf('{');
        var rp = text.LastIndexOf('}');
        return (lp >= 0 && rp > lp) ? text[lp..(rp + 1)] : null;
    }

    // ─── Error helpers ─────────────────────────────────────────────────────────

    private static JsonElement ErrorJson(string message, JsonElement requestPayload)
    {
        var obj = new { status = "error", message, results = Array.Empty<object>(), dryRun = false };
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(obj));
        return doc.RootElement.Clone();
    }
}
