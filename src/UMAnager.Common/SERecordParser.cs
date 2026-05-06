namespace UMAnager.Common;

/// <summary>
/// Parser for SE (Entry Detail) records from JRA-VAN.
/// SE records contain entry information: horse, post position, frame, jockey, trainer, odds.
/// Format: Fixed-width fields, 555 bytes total, per JRA-VAN specification.
/// Byte positions are 0-based (adjusted from 1-based spec).
/// </summary>
public static class SERecordParser
{
    /// <summary>
    /// Parse a single SE record line.
    /// Returns a RaceEntry entity with entry details, or null if invalid.
    /// </summary>
    public static RaceEntry? ParseSERecord(string seLine, string raceId)
    {
        if (string.IsNullOrWhiteSpace(seLine) || seLine.Length < 360)
            return null;

        try
        {
            // SE Record Format (per JRA-VAN spec, converted to 0-based offsets):
            var recordType = JVEncoding.ExtractField(seLine, 0, 2);

            // Validate record type
            if (recordType != "SE")
                return null;

            // Parse fields
            string frameNumberStr = JVEncoding.ExtractField(seLine, 27, 1);      // Pos 28 in spec (1-based)
            string postPositionStr = JVEncoding.ExtractField(seLine, 28, 2);    // Pos 29 in spec
            string horseId = JVEncoding.ExtractField(seLine, 30, 10);           // Pos 31 in spec
            string trainerCode = JVEncoding.ExtractField(seLine, 85, 5);        // Pos 86 in spec
            string jockeyCode = JVEncoding.ExtractField(seLine, 296, 5);        // Pos 297 in spec
            string oddsStr = JVEncoding.ExtractField(seLine, 359, 4);           // Pos 360 in spec

            // Validate required fields
            if (string.IsNullOrWhiteSpace(horseId) || string.IsNullOrWhiteSpace(raceId))
                return null;

            // Parse numeric fields
            int frameNumber = int.TryParse(frameNumberStr, out var f) ? f : 0;
            int postPosition = int.TryParse(postPositionStr, out var p) ? p : 0;

            // Parse odds: "999.9" format × 10, "0000" = no votes, "----" = scratched
            decimal? odds = ParseOdds(oddsStr);

            return new RaceEntry
            {
                RaceId = raceId,
                HorseId = horseId,
                FrameNumber = frameNumber > 0 ? frameNumber : null,
                PostPosition = postPosition > 0 ? postPosition : null,
                JockeyCode = string.IsNullOrWhiteSpace(jockeyCode) ? null : jockeyCode,
                TrainerCode = string.IsNullOrWhiteSpace(trainerCode) ? null : trainerCode,
                MorningLineOdds = odds,
                UpdatedAt = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            System.Diagnostics.Debug.WriteLine($"Failed to parse SE record: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Parse JRA-VAN odds format.
    /// "0000" = no votes, "----" = scratched, otherwise numeric "999.9" × 10.
    /// </summary>
    private static decimal? ParseOdds(string oddsStr)
    {
        if (string.IsNullOrWhiteSpace(oddsStr))
            return null;

        // "0000" means no votes
        if (oddsStr == "0000")
            return null;

        // "----" means scratched
        if (oddsStr == "----")
            return null;

        // Otherwise, it's numeric: divide by 10 to get actual odds
        if (decimal.TryParse(oddsStr, out var oddValue))
            return oddValue / 10m;

        return null;
    }
}
