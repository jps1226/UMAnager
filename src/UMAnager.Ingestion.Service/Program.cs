using Serilog;
using UMAnager.Ingestion.Service;

// Configure Serilog logging before building the host
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .Enrich.WithMachineName()
    .WriteTo.File(
        path: @"C:\Logs\UMAnager\ingestion-.log",
        rollingInterval: RollingInterval.Day,
        retainedFileCountLimit: 30,
        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

try
{
    Log.Information("UMAnager Ingestion Service starting");

    var builder = Host.CreateApplicationBuilder(args);

    // Configure Serilog as the logging provider
    builder.Logging.ClearProviders();
    builder.Services.AddSerilog();

    // Register application services
    builder.Services.AddScoped<SyncStateRepository>(sp =>
    {
        var config = sp.GetRequiredService<IConfiguration>();
        var dbConfig = config.GetSection("Database");
        var connectionString = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";
        var logger = sp.GetRequiredService<ILogger<SyncStateRepository>>();
        return new SyncStateRepository(connectionString, logger);
    });
    builder.Services.AddScoped<JVLinkClient>();
    builder.Services.AddHostedService<Worker>();

    var host = builder.Build();
    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Application terminated unexpectedly");
}
finally
{
    Log.CloseAndFlush();
}
