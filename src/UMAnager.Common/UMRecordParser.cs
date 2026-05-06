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
            // UM Record Format (official JRA-VAN spec, 0-based offsets):
            var record = new Dictionary<string, string>
            {
                { "RecordType",      JVEncoding.ExtractField(umLine,   0,  2) }, // Record type "UM"
                { "HorseId",         JVEncoding.ExtractField(umLine,  11, 10) }, // 血統登録番号
                { "BirthDate",       JVEncoding.ExtractField(umLine,  38,  8) }, // 生年月日 YYYYMMDD
                { "HorseName",       JVEncoding.ExtractField(umLine,  46, 36) }, // 馬名 (Japanese)
                { "HorseRomaji",     JVEncoding.ExtractField(umLine, 118, 60) }, // 馬名欧字 (Romaji)
                // 3-generation pedigree block starts at 204; each entry is 46 bytes (10 ID + 36 name)
                // Order: Sire(0), Dam(1), PGS(2), PGD(3), BMS(4), MGD(5), ...
                { "SireId",          JVEncoding.ExtractField(umLine, 204, 10) }, // 父
                { "DamId",           JVEncoding.ExtractField(umLine, 250, 10) }, // 母
                { "BroodmareSireId", JVEncoding.ExtractField(umLine, 388, 10) }, // 母父 (4th entry: 204 + 4*46)
            };

            // Validate record type
            if (record["RecordType"] != "UM")
                return null;

            // Validate required fields
            if (string.IsNullOrWhiteSpace(record["HorseId"]))
                return null;

            // Parse birth year from YYYYMMDD — take first 4 chars
            int birthYear = 0;
            string birthDate = record["BirthDate"];
            if (birthDate.Length >= 4 && int.TryParse(birthDate[..4], out var year) && year > 1900)
                birthYear = year;

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
