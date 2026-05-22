using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Services;

public sealed class AppStateService
{
    public static class Keys
    {
        // DateTime — wall-clock of last refresh trigger (used by the 4-hour throttle).
        public const string LastRacePlanDownload = "last_race_plan_download";
        public const string LastResultsDownload  = "last_results_download";

        // String — JV-Link lastfiletimestamp cursor (yyyyMMddHHmmss). Required for Option=2 fetches.
        public const string TokuFileCursor = "toku_file_cursor";

        // String — current AppPhase (one of WAITING_FOR_RACES / RACES_POPULATED / LIVE_OPERATIONS).
        public const string AppPhase = "app_phase";

        // Bool ("true"/"false") — manual pause flag for the live orchestrator loop.
        public const string LivePollPaused = "live_poll_paused";

        // JSON array of "yyyy-MM-dd" strings — race dates for which "odds are live" has been sent.
        public const string OddsNotifiedDates = "odds_notified_dates";

        // DateTime — wall-clock of last successful UM (horse master) ingest.
        public const string LastUmRefresh = "last_um_refresh";
    }

    private readonly IDbContextFactory<AppDbContext> _contextFactory;

    public AppStateService(IDbContextFactory<AppDbContext> contextFactory)
        => _contextFactory = contextFactory;

    public async Task<DateTime?> GetTimestampAsync(string key)
    {
        using var ctx = _contextFactory.CreateDbContext();
        var row = await ctx.AppState.AsNoTracking()
            .FirstOrDefaultAsync(a => a.Key == key);

        if (row?.Value == null) return null;
        return DateTime.TryParse(row.Value, out var dt) ? dt : null;
    }

    public async Task SetTimestampAsync(string key, DateTime value)
    {
        using var ctx = _contextFactory.CreateDbContext();
        var row = await ctx.AppState.FirstOrDefaultAsync(a => a.Key == key);
        if (row == null)
        {
            ctx.AppState.Add(new AppState { Key = key, Value = value.ToString("O"), UpdatedAt = DateTime.UtcNow });
        }
        else
        {
            row.Value = value.ToString("O");
            row.UpdatedAt = DateTime.UtcNow;
        }
        await ctx.SaveChangesAsync();
    }

    public async Task<string?> GetStringAsync(string key)
    {
        using var ctx = _contextFactory.CreateDbContext();
        var row = await ctx.AppState.AsNoTracking()
            .FirstOrDefaultAsync(a => a.Key == key);
        return row?.Value;
    }

    public async Task SetStringAsync(string key, string value)
    {
        using var ctx = _contextFactory.CreateDbContext();
        var row = await ctx.AppState.FirstOrDefaultAsync(a => a.Key == key);
        if (row == null)
        {
            ctx.AppState.Add(new AppState { Key = key, Value = value, UpdatedAt = DateTime.UtcNow });
        }
        else
        {
            row.Value = value;
            row.UpdatedAt = DateTime.UtcNow;
        }
        await ctx.SaveChangesAsync();
    }
}
