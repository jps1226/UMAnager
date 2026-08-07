// ============================================================
// FILE: Program.cs  (UMAnager.Nexus)
// LAYER: Entry point / host (ASP.NET Core x64)
// PURPOSE: Composition root for the Nexus. Wires DI, runs inline schema DDL
//          (ALTER/CREATE TABLE IF NOT EXISTS — the migration-free pattern),
//          bootstraps the sire_performance MV, configures compression + static
//          hosting, maps controllers and the /hubs/live SignalR hub.
// KEY DEPENDENCIES: AppDbContext + every Service/Parsing class (registered here),
//          NexusPipeServer + LiveOrchestrator + RaceCardRefreshService (hosted).
// CAUTION: Schema changes go HERE as inline IF NOT EXISTS DDL, not EF migrations.
//          Registration order matters for hosted services. app.Run binds 0.0.0.0:5000.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.IO.Compression;
using System.Text.Json;
using Microsoft.AspNetCore.ResponseCompression;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;
using UMAnager.Nexus.Hubs;
using UMAnager.Nexus.Pipes;
using UMAnager.Nexus.Services;
using UMAnager.Nexus.Services.Parsing;

System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory,
});

builder.WebHost.UseStaticWebAssets();

builder.Services.AddMemoryCache();
builder.Services.AddControllers()
    .AddJsonOptions(o => o.JsonSerializerOptions.PropertyNamingPolicy = null);

builder.Services.AddSignalR();

// Phase 14: gzip/brotli for API responses. /api/races is ~1MB+ uncompressed;
// gzip typically gets it under ~150KB. Static files ALSO flow through this
// middleware (it runs before UseStaticFiles) — but .NET serves .js as
// "text/javascript", which was missing from the list, so our 600KB script.js
// shipped UNCOMPRESSED. Include both the text/* and application/* spellings of
// JS (s55 fix — ~600KB → ~120KB on the largest static asset).
builder.Services.AddResponseCompression(opts =>
{
    opts.EnableForHttps = true;
    opts.Providers.Add<BrotliCompressionProvider>();
    opts.Providers.Add<GzipCompressionProvider>();
    opts.MimeTypes = new[] { "application/json", "text/plain", "text/html", "text/css",
        "application/javascript", "text/javascript" };
});
builder.Services.Configure<BrotliCompressionProviderOptions>(o => o.Level = CompressionLevel.Fastest);
builder.Services.Configure<GzipCompressionProviderOptions>(o => o.Level = CompressionLevel.Fastest);

builder.Services.AddDbContextFactory<AppDbContext>(opts =>
    opts.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

builder.Services.AddSingleton<SidecarBridge>();
builder.Services.AddSingleton<PipelineHealthService>();   // T1-1: pipeline health/alerting (singleton).
builder.Services.AddSingleton<AppStateService>();
builder.Services.AddSingleton<SettingsService>();
builder.Services.AddSingleton<BetReminderNotifier>();
builder.Services.AddSingleton<PhaseService>();
builder.Services.AddSingleton<OddsFetchService>();
builder.Services.AddSingleton<ResultsFetchService>();
builder.Services.AddSingleton<LiveBroadcastService>();
builder.Services.AddHttpClient(nameof(DiscordNotifier));
builder.Services.AddHttpClient("UptimeKuma", client => client.Timeout = TimeSpan.FromSeconds(10));
builder.Services.AddSingleton<IDiscordNotifier, DiscordNotifier>();
builder.Services.AddScoped<BetWinNotifier>();
builder.Services.AddScoped<ClaudeRecapWriter>();
builder.Services.AddScoped<DayRecapNotifier>();
builder.Services.AddSingleton<VoteHistoryService>();
builder.Services.AddSingleton<SunkCostService>();
builder.Services.AddSingleton<OreProVoteApplyService>();
builder.Services.AddSingleton<OreProCustomBetService>();
builder.Services.AddSingleton<LiveOrchestrator>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<LiveOrchestrator>());
builder.Services.AddSingleton<RaceCardRefreshService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<RaceCardRefreshService>());
builder.Services.AddSingleton<RaceCardRtFetchService>();
builder.Services.AddScoped<DifnRecordParsingService>();
builder.Services.AddScoped<BreedingHorseBackfillService>();
builder.Services.AddScoped<HnNameBackfillService>();
builder.Services.AddScoped<SurfaceBackfillService>();
builder.Services.AddScoped<GoingBackfillService>();
builder.Services.AddScoped<RaceClassBackfillService>();
builder.Services.AddScoped<JockeyTrainerIngestService>();
builder.Services.AddScoped<SeCodeBackfillService>();
builder.Services.AddScoped<OddsApplyService>();
builder.Services.AddSingleton<SirePerformanceService>();
builder.Services.AddSingleton<JockeyTrainerStatsService>();
builder.Services.AddHostedService<NexusPipeServer>();

var app = builder.Build();

await app.Services.GetRequiredService<SettingsService>().SeedDefaultsAsync();

// Race-class column (Oracle Q20). Inline ALTER matches the NameEn/OddsJson pattern.
// Safe to run on every startup — IF NOT EXISTS makes it a no-op once added.
using (var scope = app.Services.CreateScope())
{
    try
    {
        var factory = scope.ServiceProvider.GetRequiredService<Microsoft.EntityFrameworkCore.IDbContextFactory<UMAnager.Nexus.Data.AppDbContext>>();
        using var db = await factory.CreateDbContextAsync();
        await db.Database.ExecuteSqlRawAsync(
            "ALTER TABLE races ADD COLUMN IF NOT EXISTS \"RaceClass\" VARCHAR(16)");
        // Track condition + weather (Oracle 2026-06-22, RA bytes 888/889/890) — the cold engine's first
        // Group-B factor. Additive/idempotent; populated by GoingBackfillService. No curly braces (the
        // SQL goes through string.Format).
        await db.Database.ExecuteSqlRawAsync(
            "ALTER TABLE races ADD COLUMN IF NOT EXISTS \"Weather\" SMALLINT; " +
            "ALTER TABLE races ADD COLUMN IF NOT EXISTS \"TurfGoing\" SMALLINT; " +
            "ALTER TABLE races ADD COLUMN IF NOT EXISTS \"DirtGoing\" SMALLINT;");
    }
    catch (Exception ex)
    {
        app.Logger.LogError(ex, "[Startup] races.RaceClass/going column ensure failed (non-fatal).");
    }
}

// Phase 8: jockey/trainer code columns on race_entries + rolling-stats columns on jockeys/trainers.
// Inline ALTER matches the RaceClass / NameEn / OddsJson pattern.
using (var scope = app.Services.CreateScope())
{
    try
    {
        var factory = scope.ServiceProvider.GetRequiredService<Microsoft.EntityFrameworkCore.IDbContextFactory<UMAnager.Nexus.Data.AppDbContext>>();
        using var db = await factory.CreateDbContextAsync();
        await db.Database.ExecuteSqlRawAsync(@"
            ALTER TABLE race_entries ADD COLUMN IF NOT EXISTS ""JockeyCode""  VARCHAR(5);
            ALTER TABLE race_entries ADD COLUMN IF NOT EXISTS ""TrainerCode"" VARCHAR(5);
            -- Scratched: SE 異常区分 codes 1/2/3 (取消/除外) = horse removed from betting. Additive,
            -- idempotent; same raw-SQL pattern as the columns above (no curly braces here — the SQL is
            -- passed through string.Format, which would treat them as format items and throw).
            ALTER TABLE race_entries ADD COLUMN IF NOT EXISTS ""Scratched"" BOOLEAN NOT NULL DEFAULT FALSE;
            -- Sex: SE 性別コード (offset 79, len 1, Oracle 2026-06-27). 1=牡 colt · 2=牝 filly/mare ·
            -- 3=セ gelding · 0=unknown. Drives the TV-mode gender sign. Additive/idempotent.
            ALTER TABLE race_entries ADD COLUMN IF NOT EXISTS ""Sex"" SMALLINT;
            CREATE INDEX IF NOT EXISTS ix_race_entries_jockey  ON race_entries (""JockeyCode"")  WHERE ""JockeyCode""  IS NOT NULL;
            CREATE INDEX IF NOT EXISTS ix_race_entries_trainer ON race_entries (""TrainerCode"") WHERE ""TrainerCode"" IS NOT NULL;

            ALTER TABLE jockeys  ADD COLUMN IF NOT EXISTS starts_90d  INT;
            ALTER TABLE jockeys  ADD COLUMN IF NOT EXISTS wins_90d    INT;
            ALTER TABLE jockeys  ADD COLUMN IF NOT EXISTS places_90d  INT;
            ALTER TABLE jockeys  ADD COLUMN IF NOT EXISTS win_pct_90d   NUMERIC(5,4);
            ALTER TABLE jockeys  ADD COLUMN IF NOT EXISTS place_pct_90d NUMERIC(5,4);
            ALTER TABLE jockeys  ADD COLUMN IF NOT EXISTS ae_90d        NUMERIC(6,4);
            ALTER TABLE jockeys  ADD COLUMN IF NOT EXISTS stats_refreshed_at TIMESTAMPTZ;

            ALTER TABLE trainers ADD COLUMN IF NOT EXISTS starts_180d INT;
            ALTER TABLE trainers ADD COLUMN IF NOT EXISTS wins_180d   INT;
            ALTER TABLE trainers ADD COLUMN IF NOT EXISTS places_180d INT;
            ALTER TABLE trainers ADD COLUMN IF NOT EXISTS win_pct_180d   NUMERIC(5,4);
            ALTER TABLE trainers ADD COLUMN IF NOT EXISTS place_pct_180d NUMERIC(5,4);
            ALTER TABLE trainers ADD COLUMN IF NOT EXISTS ae_180d        NUMERIC(6,4);
            ALTER TABLE trainers ADD COLUMN IF NOT EXISTS stats_refreshed_at TIMESTAMPTZ;
        ");
    }
    catch (Exception ex)
    {
        app.Logger.LogError(ex, "[Startup] Phase 8 jockey/trainer column ensure failed (non-fatal).");
    }
}

// Phase 37: odds-history time-series table (idempotent). Created via raw SQL — same
// pattern as breeding_horses — so we never fight EF migrations for this additive table.
using (var scope = app.Services.CreateScope())
{
    try
    {
        var factory = scope.ServiceProvider.GetRequiredService<Microsoft.EntityFrameworkCore.IDbContextFactory<UMAnager.Nexus.Data.AppDbContext>>();
        using var db = await factory.CreateDbContextAsync();
        await db.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS odds_history (
                ""Id""           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                ""RaceId""       VARCHAR(16)   NOT NULL,
                ""HorseId""      VARCHAR(10)   NOT NULL,
                ""PostPosition"" INT,
                ""Odds""         NUMERIC(10,2) NOT NULL,
                ""FavRank""      INT,
                ""CapturedAt""   TIMESTAMPTZ   NOT NULL DEFAULT now()
            );
            ALTER TABLE odds_history ADD COLUMN IF NOT EXISTS ""FavRank"" INT;
            CREATE INDEX IF NOT EXISTS ix_odds_history_race_time ON odds_history (""RaceId"", ""CapturedAt"");
        ");
    }
    catch (Exception ex)
    {
        app.Logger.LogError(ex, "[Startup] Phase 37 odds_history table ensure failed (non-fatal).");
    }
}

// Phase 30: vote_history table (idempotent, raw SQL — same pattern as odds_history).
using (var scope = app.Services.CreateScope())
{
    try
    {
        var factory = scope.ServiceProvider.GetRequiredService<Microsoft.EntityFrameworkCore.IDbContextFactory<UMAnager.Nexus.Data.AppDbContext>>();
        using var db = await factory.CreateDbContextAsync();
        await db.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS vote_history (
                ""Id""       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                ""HorseId""  VARCHAR(10)  NOT NULL,
                ""RaceId""   VARCHAR(16)  NOT NULL,
                ""Mark""     VARCHAR(4)   NOT NULL,
                ""VotedAt""  TIMESTAMPTZ  NOT NULL,
                CONSTRAINT uq_vote_history_race_horse UNIQUE (""RaceId"", ""HorseId"")
            );
            CREATE INDEX IF NOT EXISTS ix_vote_history_horse ON vote_history (""HorseId"");
        ");
    }
    catch (Exception ex)
    {
        app.Logger.LogError(ex, "[Startup] Phase 30 vote_history table ensure failed (non-fatal).");
    }
}

// Phase 9: ensure the sire_performance MV exists (idempotent). First-run also kicks
// off an initial population so /api/races has data immediately; subsequent restarts
// no-op since the MV already holds rows.
try
{
    var sirePerf = app.Services.GetRequiredService<SirePerformanceService>();
    await sirePerf.EnsureSchemaAsync();
    _ = Task.Run(async () => { try { await sirePerf.RefreshAsync(); } catch { /* logged inside */ } });
}
catch (Exception ex)
{
    app.Logger.LogError(ex, "[Startup] sire_performance MV bootstrap failed (non-fatal).");
}

app.UseResponseCompression();
app.UseDefaultFiles();
app.UseStaticFiles();

app.MapControllers();
app.MapHub<LiveHub>("/hubs/live");

app.Run("http://0.0.0.0:5000");
