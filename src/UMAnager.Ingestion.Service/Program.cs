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

    // Load local config (appsettings.local.json) if it exists, overriding appsettings.json values
    builder.Configuration.AddJsonFile("appsettings.local.json", optional: true, reloadOnChange: false);

    // Configure Serilog as the logging provider
    builder.Logging.ClearProviders();
    builder.Services.AddSerilog();

    // Register application services
    builder.Services.AddSingleton<SyncStateRepository>(sp =>
    {
        var config = sp.GetRequiredService<IConfiguration>();
        var dbConfig = config.GetSection("Database");

        var host = dbConfig["Host"];
        var port = dbConfig["Port"];
        var database = dbConfig["Database"];
        var username = dbConfig["Username"];
        var password = dbConfig["Password"];

        // Fallback to defaults if not configured
        if (string.IsNullOrWhiteSpace(host)) host = "localhost";
        if (string.IsNullOrWhiteSpace(port)) port = "5432";
        if (string.IsNullOrWhiteSpace(database)) database = "umanager";
        if (string.IsNullOrWhiteSpace(username)) username = "postgres";
        if (string.IsNullOrWhiteSpace(password)) password = "";

        var connectionString = $"Host={host};Port={port};Database={database};Username={username};Password={password}";

        Log.Information("PostgreSQL Connection: Host={Host}, Port={Port}, Database={Database}, Username={Username}",
            host, port, database, username);

        var logger = sp.GetRequiredService<ILogger<SyncStateRepository>>();
        return new SyncStateRepository(connectionString, logger);
    });
    builder.Services.AddSingleton<JVLinkClient>();
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
