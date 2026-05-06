namespace UMAnager.Ingestion.Service;

using Npgsql;
using UMAnager.Common;

/// <summary>
/// Repository for reading and writing sync state to PostgreSQL.
/// Manages timestamp persistence for incremental JV-Link data pulls.
/// </summary>
public sealed class SyncStateRepository
{
    private readonly string _connectionString;
    private readonly ILogger<SyncStateRepository> _logger;

    public SyncStateRepository(string connectionString, ILogger<SyncStateRepository> logger)
    {
        _connectionString = connectionString;
        _logger = logger;
    }

    /// <summary>
    /// Load the current sync state. Returns default (zeros) if not found.
    /// </summary>
    public async Task<SyncState> LoadAsync()
    {
        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            const string sql = "SELECT id, last_timestamp_um, last_timestamp_races, last_sync_at, last_error, sync_count FROM sync_state WHERE id = 1";

            await using var cmd = new NpgsqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync();

            if (await reader.ReadAsync())
            {
                return new SyncState
                {
                    Id = reader.GetInt32(0),
                    LastTimestampUm = reader.GetInt64(1),
                    LastTimestampRaces = reader.GetInt64(2),
                    LastSyncAt = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    LastError = reader.IsDBNull(4) ? null : reader.GetString(4),
                    SyncCount = reader.GetInt32(5)
                };
            }

            _logger.LogWarning("sync_state row not found, returning default (zeros)");
            return new SyncState { Id = 1 };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load sync state");
            throw;
        }
    }

    /// <summary>
    /// Save the last timestamp after successful data pull.
    /// Only call this after parsing and inserting are complete.
    /// </summary>
    public async Task SaveTimestampAsync(string dataSpec, string newTimestamp)
    {
        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            var updateColumn = dataSpec switch
            {
                "DIFN" or "DIFF" => "last_timestamp_um",
                "TOKU" or "RACE" or "SNPN" or "TOKURACESNPN" => "last_timestamp_races",
                _ => throw new ArgumentException($"Unknown dataspec: {dataSpec}")
            };

            // Convert YYYYMMDDhhmmss string to long
            if (!long.TryParse(newTimestamp, out var timestampLong))
                throw new ArgumentException($"Invalid timestamp format: {newTimestamp}");

            var sql = $@"
                UPDATE sync_state
                SET {updateColumn} = @timestamp,
                    last_sync_at = CURRENT_TIMESTAMP,
                    sync_count = sync_count + 1,
                    last_error = NULL
                WHERE id = 1";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@timestamp", timestampLong);

            int affected = await cmd.ExecuteNonQueryAsync();
            _logger.LogInformation("Saved timestamp for {DataSpec}: {Timestamp}", dataSpec, newTimestamp);

            if (affected == 0)
                _logger.LogWarning("No rows updated in sync_state (row may not exist)");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save timestamp");
            throw;
        }
    }

    /// <summary>
    /// Record an error that occurred during sync.
    /// </summary>
    public async Task RecordErrorAsync(string message)
    {
        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            const string sql = "UPDATE sync_state SET last_error = @error, last_sync_at = CURRENT_TIMESTAMP WHERE id = 1";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@error", message ?? "Unknown error");

            await cmd.ExecuteNonQueryAsync();
            _logger.LogWarning("Recorded error in sync_state: {Message}", message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to record error");
        }
    }

    /// <summary>
    /// Determine if a bootstrap (setup) is needed.
    /// Returns true if timestamps are zero or older than 2 weeks.
    /// </summary>
    public bool NeedsBootstrap(SyncState state)
    {
        if (state.LastTimestampUm == 0 && state.LastTimestampRaces == 0)
        {
            _logger.LogInformation("Bootstrap needed: timestamps are zero");
            return true;
        }

        // Check if either timestamp is older than 14 days
        var cutoff = DateTime.UtcNow.AddDays(-14);

        if (state.LastSyncAt.HasValue && state.LastSyncAt < cutoff)
        {
            _logger.LogInformation("Bootstrap needed: last sync was {Days} days ago",
                Math.Round((DateTime.UtcNow - state.LastSyncAt.Value).TotalDays));
            return true;
        }

        return false;
    }

    /// <summary>
    /// Get the correct fromTime for the next JVOpen call.
    /// Returns the saved timestamp, or "00000000000000" if not available.
    /// </summary>
    public string GetFromTime(SyncState state, string dataSpec)
    {
        var timestamp = dataSpec switch
        {
            "DIFN" or "DIFF" => state.LastTimestampUm,
            "TOKU" or "RACE" or "SNPN" or "TOKURACESNPN" => state.LastTimestampRaces,
            _ => 0L
        };

        if (timestamp == 0)
        {
            _logger.LogInformation("No saved timestamp for {DataSpec}, starting from epoch", dataSpec);
            return "00000000000000";
        }

        return timestamp.ToString("D14");
    }

    /// <summary>
    /// Insert or update a horse record (upsert).
    /// If horse_id exists, update with new data. Otherwise, insert.
    /// </summary>
    public async Task InsertOrUpdateHorseAsync(Horse horse)
    {
        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            const string sql = @"
                INSERT INTO horses (horse_id, horse_name_japanese, horse_name_romaji, birth_year, sire_id, dam_id, broodmare_sire_id, data_source, last_updated)
                VALUES (@horseId, @nameJp, @nameRomaji, @birthYear, @sireId, @damId, @bmsId, @dataSource, @lastUpdated)
                ON CONFLICT (horse_id) DO UPDATE SET
                    horse_name_japanese = COALESCE(EXCLUDED.horse_name_japanese, horses.horse_name_japanese),
                    horse_name_romaji = COALESCE(EXCLUDED.horse_name_romaji, horses.horse_name_romaji),
                    birth_year = COALESCE(EXCLUDED.birth_year, horses.birth_year),
                    sire_id = COALESCE(EXCLUDED.sire_id, horses.sire_id),
                    dam_id = COALESCE(EXCLUDED.dam_id, horses.dam_id),
                    broodmare_sire_id = COALESCE(EXCLUDED.broodmare_sire_id, horses.broodmare_sire_id),
                    data_source = EXCLUDED.data_source,
                    last_updated = EXCLUDED.last_updated";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@horseId", horse.HorseId ?? "");
            cmd.Parameters.AddWithValue("@nameJp", (object?)horse.JapaneseName ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@nameRomaji", (object?)horse.RomajiName ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@birthYear", (object?)horse.BirthYear ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@sireId", (object?)horse.SireId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@damId", (object?)horse.DamId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@bmsId", (object?)horse.BroodmareSireId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@dataSource", horse.DataSource ?? "");
            cmd.Parameters.AddWithValue("@lastUpdated", (object?)horse.LastUpdated ?? DateTime.UtcNow);

            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to insert/update horse {HorseId}", horse.HorseId);
            throw;
        }
    }

    /// <summary>
    /// Insert or update a race record (upsert).
    /// If race_id exists, update with new data. Otherwise, insert.
    /// </summary>
    public async Task InsertOrUpdateRaceAsync(Race race)
    {
        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            const string sql = @"
                INSERT INTO races (race_id, race_key, race_year, race_month, race_day, track_code, round, day_of_round, race_number, race_date, distance, surface, grade, conditions_2yo, conditions_3yo, conditions_4yo, conditions_5plus, last_updated)
                VALUES (@raceId, @raceKey, @raceYear, @raceMonth, @raceDay, @trackCode, @round, @dayOfRound, @raceNumber, @raceDate, @distance, @surface, @grade, @conditions2yo, @conditions3yo, @conditions4yo, @conditions5plus, @lastUpdated)
                ON CONFLICT (race_id) DO UPDATE SET
                    race_key = COALESCE(EXCLUDED.race_key, races.race_key),
                    distance = COALESCE(EXCLUDED.distance, races.distance),
                    surface = COALESCE(EXCLUDED.surface, races.surface),
                    grade = COALESCE(EXCLUDED.grade, races.grade),
                    conditions_2yo = COALESCE(EXCLUDED.conditions_2yo, races.conditions_2yo),
                    conditions_3yo = COALESCE(EXCLUDED.conditions_3yo, races.conditions_3yo),
                    conditions_4yo = COALESCE(EXCLUDED.conditions_4yo, races.conditions_4yo),
                    conditions_5plus = COALESCE(EXCLUDED.conditions_5plus, races.conditions_5plus),
                    last_updated = EXCLUDED.last_updated";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@raceId", race.RaceId ?? "");
            cmd.Parameters.AddWithValue("@raceKey", (object?)race.RaceKey ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@raceYear", (object?)race.RaceYear ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@raceMonth", (object?)race.RaceMonth ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@raceDay", (object?)race.RaceDay ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@trackCode", (object?)race.TrackCode ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@round", (object?)race.Round ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@dayOfRound", (object?)race.DayOfRound ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@raceNumber", (object?)race.RaceNumber ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@raceDate", (object?)race.RaceDate ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@distance", (object?)race.Distance ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@surface", (object?)race.Surface ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@grade", (object?)race.Grade ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@conditions2yo", (object?)race.Conditions2yo ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@conditions3yo", (object?)race.Conditions3yo ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@conditions4yo", (object?)race.Conditions4yo ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@conditions5plus", (object?)race.Conditions5plus ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@lastUpdated", (object?)race.LastUpdated ?? DateTime.UtcNow);

            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to insert/update race {RaceId}", race.RaceId);
            throw;
        }
    }

    /// <summary>
    /// Insert or update a race entry record (upsert).
    /// If horse_id and race_id exist, update. Otherwise, insert.
    /// </summary>
    public async Task InsertOrUpdateRaceEntryAsync(RaceEntry entry)
    {
        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            const string sql = @"
                INSERT INTO race_entries (race_id, horse_id, post_position, frame_number, jockey_code, trainer_code, morning_line_odds, updated_at)
                VALUES (@raceId, @horseId, @postPosition, @frameNumber, @jockeyCode, @trainerCode, @morningLineOdds, @updatedAt)
                ON CONFLICT (race_id, horse_id) DO UPDATE SET
                    post_position = COALESCE(EXCLUDED.post_position, race_entries.post_position),
                    frame_number = COALESCE(EXCLUDED.frame_number, race_entries.frame_number),
                    jockey_code = COALESCE(EXCLUDED.jockey_code, race_entries.jockey_code),
                    trainer_code = COALESCE(EXCLUDED.trainer_code, race_entries.trainer_code),
                    morning_line_odds = COALESCE(EXCLUDED.morning_line_odds, race_entries.morning_line_odds),
                    updated_at = EXCLUDED.updated_at";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@raceId", entry.RaceId ?? "");
            cmd.Parameters.AddWithValue("@horseId", entry.HorseId ?? "");
            cmd.Parameters.AddWithValue("@postPosition", (object?)entry.PostPosition ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@frameNumber", (object?)entry.FrameNumber ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@jockeyCode", (object?)entry.JockeyCode ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@trainerCode", (object?)entry.TrainerCode ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@morningLineOdds", (object?)entry.MorningLineOdds ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@updatedAt", (object?)entry.UpdatedAt ?? DateTime.UtcNow);

            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to insert/update race entry {RaceId}/{HorseId}", entry.RaceId, entry.HorseId);
            throw;
        }
    }

    /// <summary>
    /// Create the database schema if it doesn't exist.
    /// Reads from database.sql in the src folder.
    /// </summary>
    public async Task InitializeDatabaseAsync()
    {
        try
        {
            // First, create the database if it doesn't exist
            var dbConfig = _connectionString.Split(';')
                .Select(s => s.Split('='))
                .Where(s => s.Length == 2)
                .ToDictionary(s => s[0].Trim(), s => s[1].Trim());

            string host = dbConfig.ContainsKey("Host") ? dbConfig["Host"] : "localhost";
            string port = dbConfig.ContainsKey("Port") ? dbConfig["Port"] : "5432";
            string database = dbConfig.ContainsKey("Database") ? dbConfig["Database"] : "umanager";
            string username = dbConfig.ContainsKey("Username") ? dbConfig["Username"] : "postgres";
            string password = dbConfig.ContainsKey("Password") ? dbConfig["Password"] : "";

            // Connect to 'postgres' database to create 'umanager' if needed
            var masterConnStr = $"Host={host};Port={port};Database=postgres;Username={username};Password={password}";
            await using (var masterConn = new NpgsqlConnection(masterConnStr))
            {
                await masterConn.OpenAsync();

                // Check if database exists
                using var checkCmd = new NpgsqlCommand(
                    "SELECT 1 FROM pg_database WHERE datname = @dbname",
                    masterConn);
                checkCmd.Parameters.AddWithValue("@dbname", database);
                var exists = await checkCmd.ExecuteScalarAsync();

                if (exists == null)
                {
                    _logger.LogInformation("Creating database '{Database}'", database);
                    using var createCmd = new NpgsqlCommand($"CREATE DATABASE {database}", masterConn);
                    await createCmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Database '{Database}' created successfully", database);
                }
            }

            // Now connect to the actual database and run schema
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            // Check if tables exist
            const string tableCheckSql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'sync_state'";
            using var tableCheckCmd = new NpgsqlCommand(tableCheckSql, conn);
            long tableCountLong = (long)(await tableCheckCmd.ExecuteScalarAsync() ?? 0L);
            int tableCount = (int)tableCountLong;

            if (tableCount == 0)
            {
                _logger.LogInformation("Initializing database schema");

                // Try multiple paths to find database.sql
                string[] schemaPaths = new[]
                {
                    "src/database.sql",
                    Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "database.sql"),
                    Path.Combine(AppContext.BaseDirectory, "database.sql"),
                    @"C:\Users\UMAnager\UMAnager_RE\src\database.sql"
                };

                string schemaPath = schemaPaths.FirstOrDefault(File.Exists);

                if (schemaPath != null)
                {
                    string schema = await File.ReadAllTextAsync(schemaPath);
                    using var schemaCmd = new NpgsqlCommand(schema, conn);
                    await schemaCmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Database schema initialized successfully from {Path}", schemaPath);
                }
                else
                {
                    _logger.LogError("Schema file not found. Tried: {Paths}", string.Join(", ", schemaPaths));
                    throw new FileNotFoundException("database.sql not found");
                }
            }
            else
            {
                _logger.LogInformation("Database schema already exists");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize database");
            throw;
        }
    }

    /// <summary>
    /// Temporarily disable foreign key constraints for bulk import by dropping them.
    /// Call EnableForeignKeyConstraintsAsync() in a finally block to recreate them.
    /// </summary>
    public async Task DisableForeignKeyConstraintsAsync()
    {
        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            // Drop all FK constraints on the horses table
            const string sql = @"
                ALTER TABLE horses DROP CONSTRAINT IF EXISTS horses_sire_id_fkey;
                ALTER TABLE horses DROP CONSTRAINT IF EXISTS horses_dam_id_fkey;
                ALTER TABLE horses DROP CONSTRAINT IF EXISTS horses_broodmare_sire_id_fkey;
            ";
            await using var cmd = new NpgsqlCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync();

            _logger.LogInformation("Dropped foreign key constraints for bulk import");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to drop foreign key constraints");
            throw;
        }
    }

    /// <summary>
    /// Re-create foreign key constraints after bulk import.
    /// Uses NOT VALID to allow incomplete pedigree data (horses may reference sires not yet in database).
    /// </summary>
    public async Task EnableForeignKeyConstraintsAsync()
    {
        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            // Recreate FK constraints as NOT VALID (allows referential violations during initial load)
            const string sql = @"
                ALTER TABLE horses ADD CONSTRAINT horses_sire_id_fkey
                    FOREIGN KEY (sire_id) REFERENCES horses(horse_id) ON DELETE SET NULL NOT VALID;
                ALTER TABLE horses ADD CONSTRAINT horses_dam_id_fkey
                    FOREIGN KEY (dam_id) REFERENCES horses(horse_id) ON DELETE SET NULL NOT VALID;
                ALTER TABLE horses ADD CONSTRAINT horses_broodmare_sire_id_fkey
                    FOREIGN KEY (broodmare_sire_id) REFERENCES horses(horse_id) ON DELETE SET NULL NOT VALID;
            ";
            await using var cmd = new NpgsqlCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync();

            _logger.LogInformation("Recreated foreign key constraints (NOT VALID) after bulk import");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to recreate foreign key constraints");
            throw;
        }
    }
}
