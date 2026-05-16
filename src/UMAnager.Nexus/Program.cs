using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;
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

builder.Services.AddDbContextFactory<AppDbContext>(opts =>
    opts.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

builder.Services.AddSingleton<SidecarBridge>();
builder.Services.AddSingleton<AppStateService>();
builder.Services.AddSingleton<RaceCardRefreshService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<RaceCardRefreshService>());
builder.Services.AddScoped<DifnRecordParsingService>();
builder.Services.AddScoped<BreedingHorseBackfillService>();
builder.Services.AddScoped<OddsApplyService>();
builder.Services.AddHostedService<NexusPipeServer>();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapControllers();

app.Run("http://0.0.0.0:5000");
