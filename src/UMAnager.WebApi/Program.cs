using Npgsql;
using UMAnager.Common;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

// Load local config (appsettings.local.json) if it exists, overriding appsettings.json values
builder.Configuration.AddJsonFile("appsettings.local.json", optional: true, reloadOnChange: false);

// Configure Kestrel to listen on all network interfaces
builder.WebHost.ConfigureKestrel(options =>
{
    options.Listen(System.Net.IPAddress.Any, 5125); // HTTP on port 5125
});

// Add services
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var dbConfig = builder.Configuration.GetSection("Database");
var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

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
        SyncState? syncState = null;
        using (var syncCmd = new NpgsqlCommand("SELECT id, last_timestamp_um, last_timestamp_races, sync_count FROM sync_state WHERE id = 1", conn))
        using (var syncReader = await syncCmd.ExecuteReaderAsync())
        {
            if (await syncReader.ReadAsync())
            {
                syncState = new SyncState
                {
                    Id = syncReader.GetInt32(0),
                    LastTimestampUm = syncReader.GetInt64(1),
                    LastTimestampRaces = syncReader.GetInt64(2),
                    SyncCount = (int)syncReader.GetInt64(3)
                };
            }
        }

        // Get counts
        int horseCount = 0, raceCount = 0, entryCount = 0;

        using (var countCmd = new NpgsqlCommand("SELECT COUNT(*) FROM horses", conn))
            horseCount = (int)((long)(await countCmd.ExecuteScalarAsync() ?? 0L));

        using (var countCmd = new NpgsqlCommand("SELECT COUNT(*) FROM races", conn))
            raceCount = (int)((long)(await countCmd.ExecuteScalarAsync() ?? 0L));

        using (var countCmd = new NpgsqlCommand("SELECT COUNT(*) FROM race_entries", conn))
            entryCount = (int)((long)(await countCmd.ExecuteScalarAsync() ?? 0L));

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

app.MapGet("/api/races/{race_id}", async (string race_id, IConfiguration config) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        const string raceSql = @"
            SELECT race_id, race_key, race_date, track_code, race_number,
                   race_name_japanese, distance, surface, grade,
                   conditions_2yo, conditions_3yo, conditions_4yo, conditions_5plus
            FROM races WHERE race_id = @raceId";

        using var raceCmd = new NpgsqlCommand(raceSql, conn);
        raceCmd.Parameters.AddWithValue("@raceId", race_id);
        using var raceReader = await raceCmd.ExecuteReaderAsync();

        if (!await raceReader.ReadAsync())
            return Results.NotFound(new { error = "Race not found" });

        var race = new
        {
            raceId = raceReader.GetString(0),
            raceKey = raceReader.IsDBNull(1) ? null : raceReader.GetString(1),
            raceDate = raceReader.IsDBNull(2) ? null : raceReader.GetDateTime(2).ToString("yyyy-MM-dd"),
            trackCode = raceReader.IsDBNull(3) ? null : raceReader.GetString(3),
            raceNumber = raceReader.IsDBNull(4) ? (int?)null : raceReader.GetInt32(4),
            raceName = raceReader.IsDBNull(5) ? null : raceReader.GetString(5),
            distance = raceReader.IsDBNull(6) ? (int?)null : raceReader.GetInt32(6),
            surface = raceReader.IsDBNull(7) ? null : raceReader.GetString(7),
            grade = raceReader.IsDBNull(8) ? null : raceReader.GetString(8),
            conditions2yo = raceReader.IsDBNull(9) ? null : raceReader.GetString(9),
            conditions3yo = raceReader.IsDBNull(10) ? null : raceReader.GetString(10),
            conditions4yo = raceReader.IsDBNull(11) ? null : raceReader.GetString(11),
            conditions5plus = raceReader.IsDBNull(12) ? null : raceReader.GetString(12),
            entries = new List<object>()
        };

        raceReader.Close();

        const string entriesSql = @"
            SELECT e.id, e.race_id, e.horse_id, e.post_position, e.frame_number,
                   e.jockey_code, e.trainer_code, e.morning_line_odds,
                   h.horse_name_japanese, h.horse_name_romaji, h.birth_year
            FROM race_entries e
            LEFT JOIN horses h ON e.horse_id = h.horse_id
            WHERE e.race_id = @raceId
            ORDER BY e.post_position ASC";

        using var entriesCmd = new NpgsqlCommand(entriesSql, conn);
        entriesCmd.Parameters.AddWithValue("@raceId", race_id);
        using var entriesReader = await entriesCmd.ExecuteReaderAsync();

        while (await entriesReader.ReadAsync())
        {
            ((List<object>)race.entries).Add(new
            {
                entryId = entriesReader.GetInt32(0),
                raceId = entriesReader.GetString(1),
                horseId = entriesReader.GetString(2),
                postPosition = entriesReader.IsDBNull(3) ? (int?)null : entriesReader.GetInt32(3),
                frameNumber = entriesReader.IsDBNull(4) ? (int?)null : entriesReader.GetInt32(4),
                jockeyCode = entriesReader.IsDBNull(5) ? null : entriesReader.GetString(5),
                trainerCode = entriesReader.IsDBNull(6) ? null : entriesReader.GetString(6),
                morningLineOdds = entriesReader.IsDBNull(7) ? (decimal?)null : entriesReader.GetDecimal(7),
                horseName = entriesReader.IsDBNull(8) ? null : entriesReader.GetString(8),
                horseRomaji = entriesReader.IsDBNull(9) ? null : entriesReader.GetString(9),
                birthYear = entriesReader.IsDBNull(10) ? (int?)null : entriesReader.GetInt32(10)
            });
        }

        return Results.Ok(race);
    }
    catch (Exception ex)
    {
        return Results.Problem("Failed to fetch race: " + ex.Message, statusCode: 500);
    }
});

app.MapGet("/api/horses/{horse_id}", async (string horse_id, IConfiguration config) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        const string horseSql = @"
            SELECT horse_id, horse_name_japanese, horse_name_romaji, birth_year,
                   sire_id, dam_id, broodmare_sire_id
            FROM horses WHERE horse_id = @horseId";

        using var horseCmd = new NpgsqlCommand(horseSql, conn);
        horseCmd.Parameters.AddWithValue("@horseId", horse_id);
        using var horseReader = await horseCmd.ExecuteReaderAsync();

        if (!await horseReader.ReadAsync())
            return Results.NotFound(new { error = "Horse not found" });

        var horseId = horseReader.GetString(0);
        var japaneseName = horseReader.IsDBNull(1) ? null : horseReader.GetString(1);
        var romajiName = horseReader.IsDBNull(2) ? null : horseReader.GetString(2);
        var birthYear = horseReader.IsDBNull(3) ? (int?)null : horseReader.GetInt32(3);
        var sireId = horseReader.IsDBNull(4) ? null : horseReader.GetString(4);
        var damId = horseReader.IsDBNull(5) ? null : horseReader.GetString(5);
        var broodmareSireId = horseReader.IsDBNull(6) ? null : horseReader.GetString(6);

        horseReader.Close();

        // Fetch pedigree
        object? sire = null;
        if (!string.IsNullOrEmpty(sireId))
            sire = await GetHorseBasicAsync(conn, sireId);

        object? dam = null;
        if (!string.IsNullOrEmpty(damId))
            dam = await GetHorseBasicAsync(conn, damId);

        object? bms = null;
        if (!string.IsNullOrEmpty(broodmareSireId))
            bms = await GetHorseBasicAsync(conn, broodmareSireId);

        return Results.Ok(new
        {
            horseId,
            japaneseName,
            romajiName,
            birthYear,
            sireId,
            damId,
            broodmareSireId,
            sire,
            dam,
            broodmareSire = bms
        });
    }
    catch (Exception ex)
    {
        return Results.Problem("Failed to fetch horse: " + ex.Message, statusCode: 500);
    }
});

// Serve index.html for unknown routes (SPA fallback)
app.MapFallback(() =>
{
    var wwwrootPath = Path.Combine(AppContext.BaseDirectory, "wwwroot", "index.html");
    if (!File.Exists(wwwrootPath))
        wwwrootPath = Path.Combine(Directory.GetCurrentDirectory(), "wwwroot", "index.html");

    return Results.File(wwwrootPath, "text/html");
});

app.Run();

async Task<object?> GetHorseBasicAsync(NpgsqlConnection conn, string horseId)
{
    const string sql = "SELECT horse_id, horse_name_japanese, horse_name_romaji, birth_year FROM horses WHERE horse_id = @id";
    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("@id", horseId);
    await using var reader = await cmd.ExecuteReaderAsync();

    if (!await reader.ReadAsync())
        return null;

    return new
    {
        horseId = reader.GetString(0),
        japaneseName = reader.IsDBNull(1) ? null : reader.GetString(1),
        romajiName = reader.IsDBNull(2) ? null : reader.GetString(2),
        birthYear = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3)
    };
}
