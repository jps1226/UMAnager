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

builder.Services.AddControllers()
    .AddJsonOptions(o => o.JsonSerializerOptions.PropertyNamingPolicy = null);

builder.Services.AddSignalR();

// Phase 14: gzip/brotli for API responses. /api/races is ~1MB+ uncompressed;
// gzip typically gets it under ~150KB. Static files are pre-compressed by
// UseStaticFiles + the file extensions, so we mainly care about JSON here.
builder.Services.AddResponseCompression(opts =>
{
    opts.EnableForHttps = true;
    opts.Providers.Add<BrotliCompressionProvider>();
    opts.Providers.Add<GzipCompressionProvider>();
    opts.MimeTypes = new[] { "application/json", "text/plain", "text/html", "text/css", "application/javascript" };
});
builder.Services.Configure<BrotliCompressionProviderOptions>(o => o.Level = CompressionLevel.Fastest);
builder.Services.Configure<GzipCompressionProviderOptions>(o => o.Level = CompressionLevel.Fastest);

builder.Services.AddDbContextFactory<AppDbContext>(opts =>
    opts.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

builder.Services.AddSingleton<SidecarBridge>();
builder.Services.AddSingleton<AppStateService>();
builder.Services.AddSingleton<SettingsService>();
builder.Services.AddSingleton<PhaseService>();
builder.Services.AddSingleton<OddsFetchService>();
builder.Services.AddSingleton<ResultsFetchService>();
builder.Services.AddSingleton<LiveBroadcastService>();
builder.Services.AddHttpClient(nameof(DiscordNotifier));
builder.Services.AddSingleton<IDiscordNotifier, DiscordNotifier>();
builder.Services.AddScoped<BetWinNotifier>();
builder.Services.AddScoped<DayRecapNotifier>();
builder.Services.AddSingleton<OreProVoteApplyService>();
builder.Services.AddSingleton<LiveOrchestrator>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<LiveOrchestrator>());
builder.Services.AddSingleton<RaceCardRefreshService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<RaceCardRefreshService>());
builder.Services.AddScoped<DifnRecordParsingService>();
builder.Services.AddScoped<BreedingHorseBackfillService>();
builder.Services.AddScoped<HnNameBackfillService>();
builder.Services.AddScoped<SurfaceBackfillService>();
builder.Services.AddScoped<RaceClassBackfillService>();
builder.Services.AddScoped<OddsApplyService>();
builder.Services.AddSingleton<SirePerformanceService>();
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
    }
    catch (Exception ex)
    {
        app.Logger.LogError(ex, "[Startup] races.RaceClass column ensure failed (non-fatal).");
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
