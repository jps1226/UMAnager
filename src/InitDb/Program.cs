using Npgsql;

// Quick database initialization utility
string connectionString = "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=dude!!;";

try
{
    using var conn = new NpgsqlConnection(connectionString);
    conn.Open();
    Console.WriteLine("✓ Connected to PostgreSQL");

    // Read and execute the schema file
    string schemaPath = @"C:\Users\UMAnager\UMAnager_RE\src\database.sql";
    string schema = File.ReadAllText(schemaPath);

    using var cmd = new NpgsqlCommand(schema, conn);
    cmd.ExecuteNonQuery();
    Console.WriteLine("✓ Database schema initialized");

    // Verify sync_state table was initialized
    using var verifyCmd = new NpgsqlCommand("SELECT id, sync_count FROM sync_state WHERE id = 1", conn);
    using var reader = await verifyCmd.ExecuteReaderAsync();
    if (await reader.ReadAsync())
    {
        int id = reader.GetInt32(0);
        int syncCount = reader.GetInt32(1);
        Console.WriteLine($"✓ sync_state table initialized: id={id}, sync_count={syncCount}");
    }

    Console.WriteLine("\n✅ Database initialization complete!");
}
catch (Exception ex)
{
    Console.WriteLine($"❌ Error: {ex.Message}");
    Environment.Exit(1);
}
