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

        // TimeSpan — odds refresh cadence during LIVE_OPERATIONS. HARD FLOOR 5 minutes (JV-Link rate limit).
        public const string OddsPollIntervalLive = "odds_poll_interval_live";

        // Int — minutes before post time when a race enters the LIVE window (kmy-keiba RB41 gate).
        public const string LiveWindowMinutes = "live_window_minutes";

        // String — Discord webhook URL for phase-change and bet-win notifications. Nullable.
        public const string DiscordWebhookUrl = "discord_webhook_url";
    }

    public static readonly TimeSpan LiveOddsHardFloor = TimeSpan.FromMinutes(5);

    public static class Defaults
    {
        public static readonly TimeSpan PopulatePollInterval     = TimeSpan.FromHours(1);
        public static readonly TimeSpan OddsPollIntervalPrelive  = TimeSpan.FromHours(1);
        public static readonly TimeSpan OddsPollIntervalLive     = TimeSpan.FromMinutes(5);
        public const int                LiveWindowMinutes        = 90;
        public const string?            DiscordWebhookUrl        = null;
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
        AddIfMissing(Keys.OddsPollIntervalPrelive, Defaults.OddsPollIntervalPrelive.ToString());
        AddIfMissing(Keys.OddsPollIntervalLive,    Defaults.OddsPollIntervalLive.ToString());
        AddIfMissing(Keys.LiveWindowMinutes,       Defaults.LiveWindowMinutes.ToString());
        AddIfMissing(Keys.DiscordWebhookUrl,       Defaults.DiscordWebhookUrl);

        await ctx.SaveChangesAsync();
    }
}
