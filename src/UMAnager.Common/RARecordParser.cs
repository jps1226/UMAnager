namespace UMAnager.Common;

/// <summary>
/// Parser for RA (Race Detail) records from JRA-VAN.
/// RA records contain race metadata: distance, surface, grade, conditions.
/// Format: Fixed-width fields, 1272 bytes total, per JRA-VAN specification.
/// Byte positions are 0-based (adjusted from 1-based spec).
/// </summary>
public static class RARecordParser
{
    /// <summary>
    /// Parse a single RA record line.
    /// Returns a Race entity with composite race_id and race_key, or null if invalid.
    /// </summary>
    public static Race? ParseRARecord(string raLine)
    {
        if (string.IsNullOrWhiteSpace(raLine) || raLine.Length < 700)
            return null;

        try
        {
            // RA Record Format (per JRA-VAN spec, converted to 0-based offsets):
            var recordType = JVEncoding.ExtractField(raLine, 0, 2);

            // Validate record type
            if (recordType != "RA")
                return null;

            // Parse composite race_id components (all as strings for now, will construct race_id and race_key)
            string year = JVEncoding.ExtractField(raLine, 11, 4);          // Pos 12 in spec (1-based)
            string monthDay = JVEncoding.ExtractField(raLine, 15, 4);      // Pos 16 in spec
            string trackCode = JVEncoding.ExtractField(raLine, 19, 2);     // Pos 20 in spec
            string round = JVEncoding.ExtractField(raLine, 21, 2);         // Pos 22 in spec
            string dayOfRound = JVEncoding.ExtractField(raLine, 23, 2);    // Pos 24 in spec
            string raceNumber = JVEncoding.ExtractField(raLine, 25, 2);    // Pos 26 in spec

            // Validate required fields
            if (string.IsNullOrWhiteSpace(year) || string.IsNullOrWhiteSpace(trackCode) || string.IsNullOrWhiteSpace(raceNumber))
                return null;

            // Parse other fields
            string distanceStr = JVEncoding.ExtractField(raLine, 697, 4);  // Pos 698 in spec
            string surfaceCode = JVEncoding.ExtractField(raLine, 705, 2);  // Pos 706 in spec
            string gradeCode = JVEncoding.ExtractField(raLine, 614, 1);    // Pos 615 in spec
            string conditions2yo = JVEncoding.ExtractField(raLine, 622, 3); // Pos 623 in spec
            string conditions3yo = JVEncoding.ExtractField(raLine, 625, 3); // Pos 626 in spec
            string conditions4yo = JVEncoding.ExtractField(raLine, 628, 3); // Pos 629 in spec
            string conditions5plus = JVEncoding.ExtractField(raLine, 631, 3); // Pos 632 in spec

            // Parse numeric fields
            int distance = int.TryParse(distanceStr, out var d) ? d : 0;

            // Parse date components
            if (!int.TryParse(year, out var y) || !int.TryParse(monthDay, out var md))
                return null;

            int month = md / 100;
            int day = md % 100;

            // Construct composite race_id: YYYYMMDDTRDR (year+month+day+track+round+day+race)
            string raceId = $"{year}{month:D2}{day:D2}{trackCode}{round}{dayOfRound}{raceNumber}";

            // Construct race_key for SDK calls: YYYYMMDDJJKKHHRR (16 chars exactly)
            string raceKey = $"{year}{month:D2}{day:D2}{trackCode}{round}{dayOfRound}{raceNumber}";

            return new Race
            {
                RaceId = raceId,
                RaceKey = raceKey,
                RaceYear = y,
                RaceMonth = month,
                RaceDay = day,
                TrackCode = trackCode,
                Round = int.TryParse(round, out var r) ? r : 0,
                DayOfRound = int.TryParse(dayOfRound, out var dor) ? dor : 0,
                RaceNumber = int.TryParse(raceNumber, out var rn) ? rn : 0,
                RaceDate = TryConstructDate(y, month, day),
                Distance = distance,
                Surface = surfaceCode,
                Grade = gradeCode,
                Conditions2yo = string.IsNullOrWhiteSpace(conditions2yo) ? null : conditions2yo,
                Conditions3yo = string.IsNullOrWhiteSpace(conditions3yo) ? null : conditions3yo,
                Conditions4yo = string.IsNullOrWhiteSpace(conditions4yo) ? null : conditions4yo,
                Conditions5plus = string.IsNullOrWhiteSpace(conditions5plus) ? null : conditions5plus,
                LastUpdated = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            System.Diagnostics.Debug.WriteLine($"Failed to parse RA record: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Safely construct a DateTime from year, month, day components.
    /// Returns null if the date is invalid.
    /// </summary>
    private static DateTime? TryConstructDate(int year, int month, int day)
    {
        try
        {
            if (year <= 0 || month < 1 || month > 12 || day < 1 || day > 31)
                return null;

            return new DateTime(year, month, day);
        }
        catch
        {
            return null;
        }
    }
}
