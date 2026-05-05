using Npgsql;
using UMAnager.Common;

var builder = WebApplication.CreateBuilder(args);

// Add services
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Serve static files (index.html, CSS, JS)
app.UseDefaultFiles();
app.UseStaticFiles();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// API Endpoints

app.MapGet("/api/status", async (IConfiguration config) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        // Get sync state
        using var syncCmd = new NpgsqlCommand("SELECT id, last_timestamp_um, last_timestamp_races, sync_count FROM sync_state WHERE id = 1", conn);
        using var syncReader = await syncCmd.ExecuteReaderAsync();

        SyncState? syncState = null;
        if (await syncReader.ReadAsync())
        {
            syncState = new SyncState
            {
                Id = syncReader.GetInt32(0),
                LastTimestampUm = syncReader.GetInt64(1),
                LastTimestampRaces = syncReader.GetInt64(2),
                SyncCount = syncReader.GetInt32(3)
            };
        }

        // Get counts
        int horseCount = 0, raceCount = 0, entryCount = 0;

        using (var countCmd = new NpgsqlCommand("SELECT COUNT(*) FROM horses", conn))
            horseCount = (int)(await countCmd.ExecuteScalarAsync() ?? 0);

        using (var countCmd = new NpgsqlCommand("SELECT COUNT(*) FROM races", conn))
            raceCount = (int)(await countCmd.ExecuteScalarAsync() ?? 0);

        using (var countCmd = new NpgsqlCommand("SELECT COUNT(*) FROM race_entries", conn))
            entryCount = (int)(await countCmd.ExecuteScalarAsync() ?? 0);

        return Results.Ok(new { syncState, horseCount, raceCount, entryCount });
    }
    catch (Exception ex)
    {
        return Results.Problem("Database connection failed: " + ex.Message, statusCode: 500);
    }
});

app.MapGet("/api/races", async (IConfiguration config) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        using var cmd = new NpgsqlCommand("SELECT race_id, race_date, track_code, race_number, race_name_japanese, distance, surface, grade FROM races ORDER BY race_date DESC LIMIT 20", conn);
        using var reader = await cmd.ExecuteReaderAsync();

        var races = new List<object>();
        while (await reader.ReadAsync())
        {
            races.Add(new
            {
                raceId = reader.GetString(0),
                raceDate = reader.GetDateTime(1).ToString("yyyy-MM-dd"),
                trackCode = reader.GetString(2),
                raceNumber = reader.GetInt32(3),
                raceName = reader.IsDBNull(4) ? null : reader.GetString(4),
                distance = reader.GetInt32(5),
                surface = reader.IsDBNull(6) ? null : reader.GetString(6),
                grade = reader.IsDBNull(7) ? null : reader.GetString(7)
            });
        }

        return Results.Ok(races);
    }
    catch
    {
        return Results.Ok(new List<object>());
    }
});

app.MapGet("/api/horses", async (IConfiguration config) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        using var cmd = new NpgsqlCommand("SELECT horse_id, horse_name_japanese, horse_name_romaji, birth_year, sire_id, dam_id, broodmare_sire_id FROM horses ORDER BY horse_id LIMIT 50", conn);
        using var reader = await cmd.ExecuteReaderAsync();

        var horses = new List<object>();
        while (await reader.ReadAsync())
        {
            horses.Add(new
            {
                horseId = reader.GetString(0),
                horseName = reader.IsDBNull(1) ? null : reader.GetString(1),
                horseRomaji = reader.IsDBNull(2) ? null : reader.GetString(2),
                birthYear = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3),
                sireId = reader.IsDBNull(4) ? null : reader.GetString(4),
                damId = reader.IsDBNull(5) ? null : reader.GetString(5),
                broodmareSireId = reader.IsDBNull(6) ? null : reader.GetString(6)
            });
        }

        return Results.Ok(horses);
    }
    catch
    {
        return Results.Ok(new List<object>());
    }
});

// Serve index.html for unknown routes (SPA fallback)
app.MapFallback(() => Results.File("wwwroot/index.html", "text/html"));

app.Run();
