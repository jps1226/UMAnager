using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Services;

/// <summary>
/// User-configurable settings (polling intervals, webhook URLs, etc.).
/// Distinct from AppStateService, which tracks system-managed state.
/// </summary>
public sealed class SettingsService
{
    public static class Keys
    {
        // TimeSpan — how often to check for new race plans when in WAITING_FOR_RACES.
        public const string PopulatePollInterval = "populate_poll_interval";

        // TimeSpan — odds refresh cadence during RACES_POPULATED (Thu/Fri).
        public const string OddsPollIntervalPrelive = "odds_poll_interval_prelive";

        // TimeSpan — TOKURACESNPN refresh cadence during AWAITING_POSTS (race plan ingested
        // but post positions not yet drawn). Polls the race card until any SE record
        // has PostPosition > 0, then transitions to AWAITING_ODDS.
        public const string PostsPollInterval = "posts_poll_interval";

        // TimeSpan — odds refresh cadence during AWAITING_ODDS (races present, JRA has
        // not published any odds yet). Tighter than prelive so first-publish is picked
        // up promptly; falls back to prelive cadence once any upcoming-race entry has odds.
        public const string OddsPollIntervalAwaiting = "odds_poll_interval_awaiting";

        // TimeSpan — odds refresh cadence during LIVE_OPERATIONS. HARD FLOOR 5 minutes (JV-Link rate limit).
        public const string OddsPollIntervalLive = "odds_poll_interval_live";

        // Int — minutes before post time when a race enters the LIVE window (kmy-keiba RB41 gate).
        public const string LiveWindowMinutes = "live_window_minutes";

        // String — Discord webhook URL for phase-change and bet-win notifications. Nullable.
        public const string DiscordWebhookUrl = "discord_webhook_url";

        // String — Raw "Cookie:" header value from the user's logged-in OrePro browser session.
        // User copies this from DevTools after logging in to orepro.netkeiba.com. Nullable.
        public const string OreProSessionCookie = "orepro_session_cookie";

        // String — User-Agent string to match the browser where the cookie was captured.
        // Some sites bind sessions to UA fingerprints; matching it avoids stale-session errors.
        public const string OreProUserAgent = "orepro_user_agent";

        // Bool ("true"/"false") — after a successful Apply+Submit, navigate the popup
        // to /bet/bet_complete.html?race_id=... to show the receipt page. Mirrors v1 UX.
        public const string OreProNavToCompleteAfterSubmit = "orepro_nav_to_complete_after_submit";

        // Bool ("true"/"false") — show race times in the operator's local timezone with
        // AM/PM (e.g. "8:55 PM ET") instead of the default JST 24h ("09:55"). The clean_date
        // and sort_time fields are unchanged; only the display string is converted.
        public const string DisplayLocalTime = "display_local_time";

        // Int (yen) — legacy single-stake fallback. Per-leg keys below override this.
        public const string BetEstimateStakeYen = "bet_estimate_stake_yen";

        // Int (yen) — per-leg stakes matching how the operator actually bets in OrePro.
        // Win = single-ticket stake on the ◎ horse. Quinella/Trio = stake PER COMBO in
        // the box (so a 4-mark Q Box = quinellaStake × 6, T Box = trioStake × 4).
        // Defaults to BetEstimateStakeYen when unset for backward-compat.
        public const string BetStakeWinYen      = "bet_stake_win_yen";
        public const string BetStakeQuinellaYen = "bet_stake_quinella_yen";
        public const string BetStakeTrioYen     = "bet_stake_trio_yen";
    }

    public static readonly TimeSpan LiveOddsHardFloor = TimeSpan.FromMinutes(5);

    public static class Defaults
    {
        public static readonly TimeSpan PopulatePollInterval     = TimeSpan.FromHours(1);
        public static readonly TimeSpan PostsPollInterval        = TimeSpan.FromHours(1);
        public static readonly TimeSpan OddsPollIntervalPrelive  = TimeSpan.FromHours(1);
        public static readonly TimeSpan OddsPollIntervalAwaiting = TimeSpan.FromMinutes(15);
        public static readonly TimeSpan OddsPollIntervalLive     = TimeSpan.FromMinutes(5);
        public const int                LiveWindowMinutes        = 90;
        public const string?            DiscordWebhookUrl        = null;
        public const string?            OreProSessionCookie      = null;
        public const string             OreProUserAgent          = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
        public const string             OreProNavToCompleteAfterSubmit = "false";
        public const string             DisplayLocalTime               = "false";
        public const int                BetEstimateStakeYen            = 100;
    }

    private readonly IDbContextFactory<AppDbContext> _contextFactory;

    public SettingsService(IDbContextFactory<AppDbContext> contextFactory)
        => _contextFactory = contextFactory;

    public async Task<string?> GetStringAsync(string key)
    {
        using var ctx = _contextFactory.CreateDbContext();
        var row = await ctx.AppSettings.AsNoTracking().FirstOrDefaultAsync(s => s.Key == key);
        return row?.Value;
    }

    public async Task SetStringAsync(string key, string? value)
    {
        using var ctx = _contextFactory.CreateDbContext();
        var row = await ctx.AppSettings.FirstOrDefaultAsync(s => s.Key == key);
        if (row == null)
        {
            ctx.AppSettings.Add(new AppSetting { Key = key, Value = value, UpdatedAt = DateTime.UtcNow });
        }
        else
        {
            row.Value = value;
            row.UpdatedAt = DateTime.UtcNow;
        }
        await ctx.SaveChangesAsync();
    }

    public async Task<TimeSpan> GetTimeSpanAsync(string key, TimeSpan fallback)
    {
        var raw = await GetStringAsync(key);
        return TimeSpan.TryParse(raw, out var ts) ? ts : fallback;
    }

    public async Task<int> GetIntAsync(string key, int fallback)
    {
        var raw = await GetStringAsync(key);
        return int.TryParse(raw, out var i) ? i : fallback;
    }

    /// <summary>
    /// Per-leg bet stakes. Each leg falls back to the legacy single-stake setting
    /// then to ¥100 (JRA standard). Used by the voting-tab estimator + DayRecapNotifier
    /// so calculated winnings reflect how the operator actually bets in OrePro
    /// (e.g. ¥5000 single Win ticket, ¥500/combo Q+T Box).
    /// </summary>
    public async Task<(int Win, int Quinella, int Trio)> GetBetStakesAsync()
    {
        var legacy = await GetIntAsync(Keys.BetEstimateStakeYen, Defaults.BetEstimateStakeYen);
        var win    = await GetIntAsync(Keys.BetStakeWinYen,      legacy);
        var quin   = await GetIntAsync(Keys.BetStakeQuinellaYen, legacy);
        var trio   = await GetIntAsync(Keys.BetStakeTrioYen,     legacy);
        return (
            win  > 0 ? win  : Defaults.BetEstimateStakeYen,
            quin > 0 ? quin : Defaults.BetEstimateStakeYen,
            trio > 0 ? trio : Defaults.BetEstimateStakeYen
        );
    }

    /// <summary>
    /// Reads the live odds polling interval, clamping below the hard 5-minute floor.
    /// </summary>
    public async Task<TimeSpan> GetLiveOddsIntervalAsync()
    {
        var raw = await GetTimeSpanAsync(Keys.OddsPollIntervalLive, Defaults.OddsPollIntervalLive);
        return raw < LiveOddsHardFloor ? LiveOddsHardFloor : raw;
    }

    /// <summary>
    /// Inserts default rows for any setting key that doesn't already have a row. Idempotent.
    /// Called once at app startup.
    /// </summary>
    public async Task SeedDefaultsAsync()
    {
        using var ctx = _contextFactory.CreateDbContext();
        var existing = await ctx.AppSettings.Select(s => s.Key).ToListAsync();
        var existingSet = new HashSet<string>(existing);

        void AddIfMissing(string key, string? value)
        {
            if (!existingSet.Contains(key))
                ctx.AppSettings.Add(new AppSetting { Key = key, Value = value, UpdatedAt = DateTime.UtcNow });
        }

        AddIfMissing(Keys.PopulatePollInterval,    Defaults.PopulatePollInterval.ToString());
        AddIfMissing(Keys.PostsPollInterval,       Defaults.PostsPollInterval.ToString());
        AddIfMissing(Keys.OddsPollIntervalPrelive,  Defaults.OddsPollIntervalPrelive.ToString());
        AddIfMissing(Keys.OddsPollIntervalAwaiting, Defaults.OddsPollIntervalAwaiting.ToString());
        AddIfMissing(Keys.OddsPollIntervalLive,    Defaults.OddsPollIntervalLive.ToString());
        AddIfMissing(Keys.LiveWindowMinutes,       Defaults.LiveWindowMinutes.ToString());
        AddIfMissing(Keys.DiscordWebhookUrl,       Defaults.DiscordWebhookUrl);
        AddIfMissing(Keys.OreProSessionCookie,     Defaults.OreProSessionCookie);
        AddIfMissing(Keys.OreProUserAgent,         Defaults.OreProUserAgent);
        AddIfMissing(Keys.OreProNavToCompleteAfterSubmit, Defaults.OreProNavToCompleteAfterSubmit);
        AddIfMissing(Keys.DisplayLocalTime,               Defaults.DisplayLocalTime);
        AddIfMissing(Keys.BetEstimateStakeYen,            Defaults.BetEstimateStakeYen.ToString());

        await ctx.SaveChangesAsync();
    }
}
