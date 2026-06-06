// ============================================================
// FILE: AppStateService.cs
// LAYER: Service
// PURPOSE: Typed get/set over the app_state key/value table — system-managed state: JV-Link
//          cursors (toku_file_cursor), current phase, notified-dates, last-refresh timestamps,
//          and the marks/config blobs. The nested Keys class holds the canonical key strings.
// KEY DEPENDENCIES: AppDbContext.
// CAUTION: Distinct from SettingsService (user-configurable). last_race_plan_download is a
//          throttle timestamp; toku_file_cursor is the JV-Link resume point — do not conflate.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
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

        // String — current AppPhase (one of WAITING_FOR_RACES / AWAITING_POSTS / AWAITING_ODDS /
        // RACES_POPULATED / LIVE_OPERATIONS). See PhaseService.AppPhase for the authoritative enum.
        public const string AppPhase = "app_phase";

        // Bool ("true"/"false") — manual pause flag for the live orchestrator loop.
        public const string LivePollPaused = "live_poll_paused";

        // JSON array of "yyyy-MM-dd" strings — race dates for which "odds are live" has been sent.
        public const string OddsNotifiedDates = "odds_notified_dates";

        // JSON — the day-level bet COMPOSITION for a JST date: { presetId, lines:[{type,yen}] }.
        // Opaque to the server (the frontend builds + prices it; applied bets freeze explicit lines
        // the server scores separately). Keyed by JST date:
        // DayBetCompositionKey("2026-06-06") → "day_bet_structure_2026-06-06". Temporary per race
        // day (client reads null → falls back to the default preset).
        public static string DayBetStructureKey(string jstDate) => $"day_bet_structure_{jstDate}";

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
