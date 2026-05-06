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

// Enable CORS for all origins (needed for frontend access)
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policyBuilder =>
    {
        policyBuilder.AllowAnyOrigin()
            .AllowAnyMethod()
            .AllowAnyHeader();
    });
});

var dbConfig = builder.Configuration.GetSection("Database");
var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

var app = builder.Build();

// Use CORS
app.UseCors();

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

// Note: /api/races endpoint is now in Phase 4 MVP section with date filtering support

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
                   h.horse_name_japanese, h.horse_name_romaji, h.birth_year,
                   h.sire_id, h.dam_id, h.broodmare_sire_id,
                   sire.horse_name_japanese, sire.horse_name_romaji,
                   dam.horse_name_japanese, dam.horse_name_romaji,
                   bms.horse_name_japanese, bms.horse_name_romaji
            FROM race_entries e
            LEFT JOIN horses h ON e.horse_id = h.horse_id
            LEFT JOIN horses sire ON h.sire_id = sire.horse_id
            LEFT JOIN horses dam ON h.dam_id = dam.horse_id
            LEFT JOIN horses bms ON h.broodmare_sire_id = bms.horse_id
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
                horse = new
                {
                    horseId = entriesReader.GetString(2),
                    japaneseeName = entriesReader.IsDBNull(8) ? null : entriesReader.GetString(8),
                    romajiName = entriesReader.IsDBNull(9) ? null : entriesReader.GetString(9),
                    birthYear = entriesReader.IsDBNull(10) ? (int?)null : entriesReader.GetInt32(10),
                    pedigree = new
                    {
                        sireId = entriesReader.IsDBNull(11) ? null : entriesReader.GetString(11),
                        sireName = entriesReader.IsDBNull(14) ? null : entriesReader.GetString(14),
                        sireRomaji = entriesReader.IsDBNull(15) ? null : entriesReader.GetString(15),
                        damId = entriesReader.IsDBNull(12) ? null : entriesReader.GetString(12),
                        damName = entriesReader.IsDBNull(16) ? null : entriesReader.GetString(16),
                        damRomaji = entriesReader.IsDBNull(17) ? null : entriesReader.GetString(17),
                        broodmareSireId = entriesReader.IsDBNull(13) ? null : entriesReader.GetString(13),
                        broodmareSireName = entriesReader.IsDBNull(18) ? null : entriesReader.GetString(18),
                        broodmareSireRomaji = entriesReader.IsDBNull(19) ? null : entriesReader.GetString(19)
                    }
                }
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

// Phase 4 MVP Endpoints

// GET /api/calendar — calendar with race days marked
app.MapGet("/api/calendar", async (IConfiguration config, int year, int month) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        var sql = @"
            SELECT DISTINCT DATE_PART('day', race_date)::int as day,
                   race_date >= CURRENT_DATE as isUpcoming,
                   COUNT(*) as raceCount
            FROM races
            WHERE DATE_PART('year', race_date)::int = @year
              AND DATE_PART('month', race_date)::int = @month
            GROUP BY DATE_PART('day', race_date)::int, isUpcoming
            ORDER BY day ASC";

        using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@year", year);
        cmd.Parameters.AddWithValue("@month", month);
        using var reader = await cmd.ExecuteReaderAsync();

        var days = new List<object>();
        while (await reader.ReadAsync())
        {
            days.Add(new
            {
                day = reader.GetInt32(0),
                isUpcoming = reader.GetBoolean(1),
                raceCount = (int)reader.GetInt64(2)
            });
        }

        return Results.Ok(days);
    }
    catch
    {
        return Results.Ok(new List<object>());
    }
});

// GET /api/races?date=YYYY-MM-DD — races for a specific date, ordered by start time
app.MapGet("/api/races", async (IConfiguration config, string? date) =>
{
    try
    {
        if (!DateTime.TryParse(date ?? "", out var raceDate))
            return Results.Ok(new List<object>());

        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        var sql = @"
            SELECT race_id, race_number, track_code, race_date, race_start_time,
                   distance, surface, grade, race_name_japanese
            FROM races
            WHERE race_date = @raceDate
            ORDER BY race_start_time ASC, track_code ASC, race_number ASC";

        using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@raceDate", raceDate.Date);
        using var reader = await cmd.ExecuteReaderAsync();

        var races = new List<object>();
        while (await reader.ReadAsync())
        {
            races.Add(new
            {
                raceId = reader.GetString(0),
                raceNumber = reader.GetInt32(1),
                trackCode = reader.GetString(2),
                raceDate = reader.IsDBNull(3) ? null : reader.GetDateTime(3).ToString("yyyy-MM-dd"),
                raceStartTime = reader.IsDBNull(4) ? null : reader.GetTimeSpan(4).ToString(@"hh\:mm"),
                distance = reader.GetInt32(5),
                surface = reader.IsDBNull(6) ? null : reader.GetString(6),
                grade = reader.IsDBNull(7) ? null : reader.GetString(7),
                raceName = reader.IsDBNull(8) ? null : reader.GetString(8)
            });
        }

        return Results.Ok(races);
    }
    catch
    {
        return Results.Ok(new List<object>());
    }
});

// GET /api/horses/search — horse search with autocomplete
app.MapGet("/api/horses/search", async (IConfiguration config, string q, int limit = 10) =>
{
    try
    {
        if (string.IsNullOrWhiteSpace(q) || q.Length < 2)
            return Results.Ok(new List<object>());

        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        // Use trigram similarity for fuzzy matching, with limit
        var sql = @"
            SELECT horse_id, horse_name_japanese, horse_name_romaji, birth_year
            FROM horses
            WHERE horse_name_japanese ILIKE @q || '%'
               OR horse_name_romaji ILIKE @q || '%'
            ORDER BY
              CASE
                WHEN horse_name_japanese ILIKE @q || '%' THEN 0
                WHEN horse_name_romaji ILIKE @q || '%' THEN 1
                ELSE 2
              END,
              horse_name_japanese ASC
            LIMIT @limit";

        using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@q", q);
        cmd.Parameters.AddWithValue("@limit", limit);
        using var reader = await cmd.ExecuteReaderAsync();

        var horses = new List<object>();
        while (await reader.ReadAsync())
        {
            horses.Add(new
            {
                horseId = reader.GetString(0),
                japaneseeName = reader.IsDBNull(1) ? null : reader.GetString(1),
                romajiName = reader.IsDBNull(2) ? null : reader.GetString(2),
                birthYear = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3)
            });
        }

        return Results.Ok(horses);
    }
    catch
    {
        return Results.Ok(new List<object>());
    }
});

// GET /api/races/next — next race on a given date
app.MapGet("/api/races/next", async (IConfiguration config, string? date) =>
{
    try
    {
        if (!DateTime.TryParse(date ?? "", out var raceDate))
            return Results.NotFound();

        var now = DateTime.UtcNow.AddHours(9); // JST
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        var sql = @"
            SELECT race_id, race_number, track_code, race_start_time
            FROM races
            WHERE race_date = @raceDate AND race_start_time > @nowTime
            ORDER BY race_start_time ASC
            LIMIT 1";

        using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@raceDate", raceDate.Date);
        cmd.Parameters.AddWithValue("@nowTime", TimeOnly.FromDateTime(now));
        using var reader = await cmd.ExecuteReaderAsync();

        if (!await reader.ReadAsync())
            return Results.NotFound();

        return Results.Ok(new
        {
            raceId = reader.GetString(0),
            raceNumber = reader.GetInt32(1),
            trackCode = reader.GetString(2),
            raceStartTime = reader.IsDBNull(3) ? null : reader.GetTimeSpan(3).ToString(@"hh\:mm")
        });
    }
    catch
    {
        return Results.NotFound();
    }
});

// GET /api/user/lists/favorites — get favorites list
app.MapGet("/api/user/lists/favorites", async (IConfiguration config) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        var sql = @"
            SELECT h.horse_id, h.horse_name_japanese, h.horse_name_romaji, h.birth_year
            FROM user_horse_lists uhl
            JOIN horses h ON uhl.horse_id = h.horse_id
            WHERE uhl.list_type = 'favorites'
            ORDER BY uhl.created_at DESC";

        using var cmd = new NpgsqlCommand(sql, conn);
        using var reader = await cmd.ExecuteReaderAsync();

        var horses = new List<object>();
        while (await reader.ReadAsync())
        {
            horses.Add(new
            {
                horseId = reader.GetString(0),
                japaneseeName = reader.IsDBNull(1) ? null : reader.GetString(1),
                romajiName = reader.IsDBNull(2) ? null : reader.GetString(2),
                birthYear = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3)
            });
        }

        return Results.Ok(horses);
    }
    catch
    {
        return Results.Ok(new List<object>());
    }
});

// POST /api/user/lists/favorites — add horse to favorites
app.MapPost("/api/user/lists/favorites", async (IConfiguration config, PostListRequest req) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        // Verify horse exists
        using var checkCmd = new NpgsqlCommand("SELECT 1 FROM horses WHERE horse_id = @id", conn);
        checkCmd.Parameters.AddWithValue("@id", req.HorseId);
        if (await checkCmd.ExecuteScalarAsync() == null)
            return Results.BadRequest(new { error = "Horse not found" });

        // Insert or ignore if already exists
        var sql = @"
            INSERT INTO user_horse_lists (horse_id, list_type)
            VALUES (@horseId, 'favorites')
            ON CONFLICT (horse_id, list_type) DO NOTHING";

        using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@horseId", req.HorseId);
        await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new { success = true });
    }
    catch (Exception ex)
    {
        return Results.Problem(ex.Message, statusCode: 500);
    }
});

// DELETE /api/user/lists/favorites/{horseId} — remove from favorites
app.MapDelete("/api/user/lists/favorites/{horseId}", async (IConfiguration config, string horseId) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        var sql = "DELETE FROM user_horse_lists WHERE horse_id = @id AND list_type = 'favorites'";
        using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", horseId);
        await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new { success = true });
    }
    catch (Exception ex)
    {
        return Results.Problem(ex.Message, statusCode: 500);
    }
});

// GET /api/user/lists/watchlist — get watchlist
app.MapGet("/api/user/lists/watchlist", async (IConfiguration config) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        var sql = @"
            SELECT h.horse_id, h.horse_name_japanese, h.horse_name_romaji, h.birth_year
            FROM user_horse_lists uhl
            JOIN horses h ON uhl.horse_id = h.horse_id
            WHERE uhl.list_type = 'watchlist'
            ORDER BY uhl.created_at DESC";

        using var cmd = new NpgsqlCommand(sql, conn);
        using var reader = await cmd.ExecuteReaderAsync();

        var horses = new List<object>();
        while (await reader.ReadAsync())
        {
            horses.Add(new
            {
                horseId = reader.GetString(0),
                japaneseeName = reader.IsDBNull(1) ? null : reader.GetString(1),
                romajiName = reader.IsDBNull(2) ? null : reader.GetString(2),
                birthYear = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3)
            });
        }

        return Results.Ok(horses);
    }
    catch
    {
        return Results.Ok(new List<object>());
    }
});

// POST /api/user/lists/watchlist — add horse to watchlist
app.MapPost("/api/user/lists/watchlist", async (IConfiguration config, PostListRequest req) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        // Verify horse exists
        using var checkCmd = new NpgsqlCommand("SELECT 1 FROM horses WHERE horse_id = @id", conn);
        checkCmd.Parameters.AddWithValue("@id", req.HorseId);
        if (await checkCmd.ExecuteScalarAsync() == null)
            return Results.BadRequest(new { error = "Horse not found" });

        // Insert or ignore if already exists
        var sql = @"
            INSERT INTO user_horse_lists (horse_id, list_type)
            VALUES (@horseId, 'watchlist')
            ON CONFLICT (horse_id, list_type) DO NOTHING";

        using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@horseId", req.HorseId);
        await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new { success = true });
    }
    catch (Exception ex)
    {
        return Results.Problem(ex.Message, statusCode: 500);
    }
});

// DELETE /api/user/lists/watchlist/{horseId} — remove from watchlist
app.MapDelete("/api/user/lists/watchlist/{horseId}", async (IConfiguration config, string horseId) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        var sql = "DELETE FROM user_horse_lists WHERE horse_id = @id AND list_type = 'watchlist'";
        using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", horseId);
        await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new { success = true });
    }
    catch (Exception ex)
    {
        return Results.Problem(ex.Message, statusCode: 500);
    }
});

// GET /api/user/lists/watchlist/upcoming — weekend watchlist
app.MapGet("/api/user/lists/watchlist/upcoming", async (IConfiguration config) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        // Get next Saturday and Sunday
        var now = DateTime.UtcNow.AddHours(9); // JST
        var daysUntilSaturday = (DayOfWeek.Saturday - now.DayOfWeek + 7) % 7;
        if (daysUntilSaturday == 0) daysUntilSaturday = 7;
        var saturday = now.AddDays(daysUntilSaturday).Date;
        var sunday = saturday.AddDays(1);

        var sql = @"
            SELECT DISTINCT h.horse_id, h.horse_name_japanese, h.horse_name_romaji, h.birth_year, r.race_date
            FROM user_horse_lists uhl
            JOIN horses h ON uhl.horse_id = h.horse_id
            JOIN race_entries re ON h.horse_id = re.horse_id
            JOIN races r ON re.race_id = r.race_id
            WHERE uhl.list_type = 'watchlist'
              AND r.race_date >= @saturday
              AND r.race_date <= @sunday
            ORDER BY r.race_date ASC, h.horse_name_japanese ASC";

        using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@saturday", saturday);
        cmd.Parameters.AddWithValue("@sunday", sunday);
        using var reader = await cmd.ExecuteReaderAsync();

        var horses = new List<object>();
        while (await reader.ReadAsync())
        {
            horses.Add(new
            {
                horseId = reader.GetString(0),
                japaneseeName = reader.IsDBNull(1) ? null : reader.GetString(1),
                romajiName = reader.IsDBNull(2) ? null : reader.GetString(2),
                birthYear = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3),
                raceDate = reader.GetDateTime(4).ToString("yyyy-MM-dd")
            });
        }

        return Results.Ok(horses);
    }
    catch
    {
        return Results.Ok(new List<object>());
    }
});

// GET /api/user/settings — get user settings
app.MapGet("/api/user/settings", async (IConfiguration config) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        using var cmd = new NpgsqlCommand("SELECT settings_json FROM user_settings WHERE id = 1", conn);
        var result = await cmd.ExecuteScalarAsync();

        if (result == null)
            return Results.Ok(new { });

        // Return as-is; the JSON is already a string from PostgreSQL
        return Results.Ok(result.ToString());
    }
    catch
    {
        return Results.Ok(new { });
    }
});

// PUT /api/user/settings — save user settings
app.MapPut("/api/user/settings", async (IConfiguration config, SettingsRequest req) =>
{
    try
    {
        var dbConfig = config.GetSection("Database");
        var connStr = $"Host={dbConfig["Host"]};Port={dbConfig["Port"]};Database={dbConfig["Database"]};Username={dbConfig["Username"]};Password={dbConfig["Password"]}";

        using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        var sql = @"
            INSERT INTO user_settings (id, settings_json, updated_at)
            VALUES (1, @json, NOW())
            ON CONFLICT (id) DO UPDATE SET
              settings_json = @json,
              updated_at = NOW()";

        using var cmd = new NpgsqlCommand(sql, conn);
        var settingsToSave = req.Settings ?? new Dictionary<string, object>();
        cmd.Parameters.AddWithValue("@json", System.Text.Json.JsonSerializer.Serialize(settingsToSave));
        await cmd.ExecuteNonQueryAsync();

        return Results.Ok(new { success = true });
    }
    catch (Exception ex)
    {
        return Results.Problem(ex.Message, statusCode: 500);
    }
});

// Serve index.html for unknown routes (SPA fallback)
app.MapFallback(() =>
{
    var wwwrootPath = Path.Combine(AppContext.BaseDirectory, "wwwroot", "index.html");
    if (!File.Exists(wwwrootPath))
        wwwrootPath = Path.Combine(Directory.GetCurrentDirectory(), "wwwroot", "index.html");

    if (!File.Exists(wwwrootPath))
        return Results.NotFound(new { error = "index.html not found" });

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

// Helper records for API requests/responses
public record PostListRequest(string HorseId);
public record SettingsRequest(Dictionary<string, object>? Settings);
