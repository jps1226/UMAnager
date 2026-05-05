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
    /// Create the database schema if it doesn't exist.
    /// Reads from database.sql in the src folder.
    /// </summary>
    public async Task InitializeDatabaseAsync()
    {
        try
        {
            // This would normally read the SQL file and execute it
            // For now, just log that this would run
            _logger.LogInformation("Database initialization should be run manually via: psql -U postgres < src/database.sql");

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            // Verify the tables exist by querying sync_state
            const string sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'sync_state'";
            await using var cmd = new NpgsqlCommand(sql, conn);
            int count = (int)(await cmd.ExecuteScalarAsync() ?? 0);

            if (count == 0)
                _logger.LogWarning("sync_state table not found. Run: psql -U postgres < src/database.sql");
            else
                _logger.LogInformation("Database schema verified");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize database");
            throw;
        }
    }
}
