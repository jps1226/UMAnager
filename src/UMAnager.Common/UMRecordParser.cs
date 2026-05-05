namespace UMAnager.Common;

/// <summary>
/// Parser for UM (Horse Master) records from JRA-VAN.
/// UM records contain horse ID, name, birth year, and 3-generation pedigree.
/// Format: Fixed-width fields, CP932-encoded, per JRA-VAN UM specification.
/// </summary>
public static class UMRecordParser
{
    /// <summary>
    /// Parse a single UM record line.
    /// Returns a horse entity with pedigree information, or null if invalid.
    /// </summary>
    public static Horse? ParseUMRecord(string umLine)
    {
        if (string.IsNullOrWhiteSpace(umLine) || umLine.Length < 100)
            return null;

        try
        {
            // UM Record Format (per JRA-VAN spec):
            // Byte offsets and field lengths
            var record = new Dictionary<string, string>
            {
                { "RecordType", JVEncoding.ExtractField(umLine, 0, 2) },          // "UM"
                { "HorseId", JVEncoding.ExtractField(umLine, 2, 10) },            // Horse ID (1-10)
                { "HorseName", JVEncoding.ExtractField(umLine, 12, 64) },         // Horse name Japanese
                { "HorseRomaji", JVEncoding.ExtractField(umLine, 76, 40) },       // Horse name Romaji
                { "BirthYear", JVEncoding.ExtractField(umLine, 116, 4) },         // Birth year (YYYY)
                { "SireId", JVEncoding.ExtractField(umLine, 120, 10) },           // Sire ID
                { "DamId", JVEncoding.ExtractField(umLine, 130, 10) },            // Dam ID
                { "BroodmareSireId", JVEncoding.ExtractField(umLine, 140, 10) }, // Broodmare Sire ID
            };

            // Validate record type
            if (record["RecordType"] != "UM")
                return null;

            // Validate required fields
            if (string.IsNullOrWhiteSpace(record["HorseId"]))
                return null;

            // Parse birth year (if present)
            int birthYear = 0;
            if (!string.IsNullOrWhiteSpace(record["BirthYear"]) &&
                int.TryParse(record["BirthYear"], out var year) &&
                year > 0)
            {
                birthYear = year;
            }

            // Create horse entity with pedigree
            return new Horse
            {
                HorseId = record["HorseId"],
                JapaneseName = string.IsNullOrWhiteSpace(record["HorseName"]) ? null : record["HorseName"],
                RomajiName = string.IsNullOrWhiteSpace(record["HorseRomaji"]) ? null : record["HorseRomaji"],
                BirthYear = birthYear > 0 ? birthYear : null,
                SireId = string.IsNullOrWhiteSpace(record["SireId"]) ? null : record["SireId"],
                DamId = string.IsNullOrWhiteSpace(record["DamId"]) ? null : record["DamId"],
                BroodmareSireId = string.IsNullOrWhiteSpace(record["BroodmareSireId"]) ? null : record["BroodmareSireId"],
                LastUpdated = DateTime.UtcNow,
                DataSource = "UM"
            };
        }
        catch (Exception ex)
        {
            // Log parsing error but don't throw; allows graceful handling of corrupted records
            System.Diagnostics.Debug.WriteLine($"Failed to parse UM record: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Validate that a horse has complete 3-generation pedigree (no nulls).
    /// </summary>
    public static bool HasCompletePedigree(Horse horse)
    {
        return !string.IsNullOrWhiteSpace(horse.HorseId) &&
               !string.IsNullOrWhiteSpace(horse.SireId) &&
               !string.IsNullOrWhiteSpace(horse.DamId) &&
               !string.IsNullOrWhiteSpace(horse.BroodmareSireId);
    }
}
