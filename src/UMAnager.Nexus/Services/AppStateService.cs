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
    // Seed VALUE (not a key) for the FIRST option=1 DIFN run, used only until a real
    // lastFileTimestamp is stored under Keys.DifnFileCursor. No cursor was ever captured under the
    // old option=4 scheme, so there is no exact resume point. Deliberately EARLIER than the last
    // successful refresh (2026-07-24 ~11:42 JST): too-early only re-sends master records we already
    // have and they UPSERT harmlessly, whereas too-late would silently skip updates. Overlap is the
    // safe direction; a gap is not.
    public const string DifnCursorBootstrap = "20260720000000";

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

        // DateTime — wall-clock of the last failed/hung weekly UM refresh. Used as a short backoff so
        // a stuck JVOpen(DIFN) does not retrigger every watchdog cycle overnight.
        public const string LastUmRefreshFailedAt = "last_um_refresh_failed_at";

        // String — JV-Link cursor (yyyyMMddHHmmss JST) for the weekly DIFN master refresh: the
        // lastFileTimestamp returned by the previous JVOpen. Passed as fromTime to the next
        // option=1 call for gap-free delta syncing. Same role TokuFileCursor plays for TOKU.
        // Never blank it — an empty cursor means "from the beginning" (see DifnCursorBootstrap).
        public const string DifnFileCursor = "difn_file_cursor";

        // DateTime — wall-clock when JV-Link last reported rc=-504 (JRA-VAN server maintenance).
        // Set by the pipe server on a maintenance completion; cleared (Value="") on the next
        // successful (record_count >= 0) stream. The orchestrator backs off to
        // maintenance_retry_interval while this is set and recent. (Oracle 2026-06-07.)
        public const string MaintenanceDetectedAt = "maintenance_detected_at";

        // DateTime — wall-clock of the last raw_staging dedup/retention pass (T1-2). The
        // orchestrator runs the pass at most once per day to keep duplicate UM/SE/RA rows from
        // re-accumulating (every weekly master pull + weekend refresh re-stages the same records).
        public const string LastRawStagingRetention = "last_raw_staging_retention";
    }

    // JV-Link "server under maintenance" return code (Oracle 2026-06-07). The Sidecar reports this
    // value back as a *_COMPLETE record_count so the Nexus can distinguish it from -1 (generic error).
    public const int MaintenanceRecordCount = -504;

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
