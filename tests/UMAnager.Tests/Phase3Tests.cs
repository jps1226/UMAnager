namespace UMAnager.Tests;

using System.Text;
using UMAnager.Common;

/// <summary>
/// Unit tests for Phase 3: Weekly Data Fetch (RA/SE/UM records)
/// Tests RA and SE record parsing, race_key formatting, and entry parsing.
/// Does NOT require JRA-VAN connection or real data.
/// </summary>
[Collection("Phase2 Tests")]
public class Phase3Tests
{
    /// <summary>
    /// Test RA record parsing with valid race metadata.
    /// </summary>
    [Fact]
    public void ParseRARecord_ValidRecord_ReturnsRaceWithMetadata()
    {
        // Arrange: Mock RA record (1272 bytes total)
        string raRecord = CreateMockRARecord(
            year: "2026",
            monthDay: "0504",
            trackCode: "02",
            round: "01",
            dayOfRound: "01",
            raceNumber: "01",
            distance: "1600",
            surfaceCode: "01",
            gradeCode: "1",
            conditions2yo: "条件",
            conditions3yo: "条件",
            conditions4yo: "条件",
            conditions5plus: "条件"
        );

        // Act
        var race = RARecordParser.ParseRARecord(raRecord);

        // Assert
        Assert.NotNull(race);
        Assert.Equal("2026050402010101", race.RaceId);
        Assert.Equal("2026050402010101", race.RaceKey);
        Assert.Equal(2026, race.RaceYear);
        Assert.Equal(5, race.RaceMonth);
        Assert.Equal(4, race.RaceDay);
        Assert.Equal("02", race.TrackCode);
        Assert.Equal(1, race.Round);
        Assert.Equal(1, race.DayOfRound);
        Assert.Equal(1, race.RaceNumber);
        Assert.Equal(1600, race.Distance);
        Assert.Equal("01", race.Surface);
        Assert.Equal("1", race.Grade);
    }

    /// <summary>
    /// Test RA record parsing with wrong record type.
    /// </summary>
    [Fact]
    public void ParseRARecord_WrongRecordType_ReturnsNull()
    {
        // Arrange: Record with "SE" instead of "RA"
        string seRecord = "SE" + new string(' ', 1270);

        // Act
        var race = RARecordParser.ParseRARecord(seRecord);

        // Assert
        Assert.Null(race);
    }

    /// <summary>
    /// Test RA record parsing with too-short record.
    /// </summary>
    [Fact]
    public void ParseRARecord_TooShort_ReturnsNull()
    {
        // Arrange: Record shorter than minimum required
        string shortRecord = "RA00000001";

        // Act
        var race = RARecordParser.ParseRARecord(shortRecord);

        // Assert
        Assert.Null(race);
    }

    /// <summary>
    /// Test SE record parsing with valid entry data.
    /// </summary>
    [Fact]
    public void ParseSERecord_ValidRecord_ReturnsEntryWithMetadata()
    {
        // Arrange: Mock SE record (555 bytes)
        string seRecord = CreateMockSERecord(
            frameNumber: "1",
            postPosition: "01",
            horseId: "0000000001",
            trainerCode: "00001",
            jockeyCode: "00002",
            oddsStr: "0500"
        );

        // Act
        var entry = SERecordParser.ParseSERecord(seRecord, "2026050402010101");

        // Assert
        Assert.NotNull(entry);
        Assert.Equal("2026050402010101", entry.RaceId);
        Assert.Equal("0000000001", entry.HorseId);
        Assert.Equal(1, entry.FrameNumber);
        Assert.Equal(1, entry.PostPosition);
        Assert.Equal("00001", entry.TrainerCode);
        Assert.Equal("00002", entry.JockeyCode);
        Assert.Equal(50.0m, entry.MorningLineOdds); // 0500 / 10 = 50.0
    }

    /// <summary>
    /// Test SE record parsing with no-votes odds.
    /// </summary>
    [Fact]
    public void ParseSERecord_NoVotesOdds_ReturnsNullOdds()
    {
        // Arrange: SE record with odds = "0000" (no votes)
        string seRecord = CreateMockSERecord(
            frameNumber: "2",
            postPosition: "02",
            horseId: "0000000002",
            trainerCode: "00001",
            jockeyCode: "00003",
            oddsStr: "0000"
        );

        // Act
        var entry = SERecordParser.ParseSERecord(seRecord, "2026050402010101");

        // Assert
        Assert.NotNull(entry);
        Assert.Null(entry.MorningLineOdds);
    }

    /// <summary>
    /// Test SE record parsing with scratched horse.
    /// </summary>
    [Fact]
    public void ParseSERecord_ScratcedHorse_ReturnsNullOdds()
    {
        // Arrange: SE record with odds = "----" (scratched)
        string seRecord = CreateMockSERecord(
            frameNumber: "3",
            postPosition: "03",
            horseId: "0000000003",
            trainerCode: "00001",
            jockeyCode: "00004",
            oddsStr: "----"
        );

        // Act
        var entry = SERecordParser.ParseSERecord(seRecord, "2026050402010101");

        // Assert
        Assert.NotNull(entry);
        Assert.Null(entry.MorningLineOdds);
    }

    /// <summary>
    /// Test race_key format consistency: YYYYMMDDJJKKHHRR (16 chars exactly).
    /// </summary>
    [Fact]
    public void RaceKey_Format_Is16CharactersExactly()
    {
        // Arrange: Parse a valid RA record
        string raRecord = CreateMockRARecord(
            year: "2026",
            monthDay: "0504",
            trackCode: "02",
            round: "01",
            dayOfRound: "01",
            raceNumber: "01",
            distance: "1600",
            surfaceCode: "01",
            gradeCode: "1",
            conditions2yo: "条件",
            conditions3yo: "条件",
            conditions4yo: "条件",
            conditions5plus: "条件"
        );

        // Act
        var race = RARecordParser.ParseRARecord(raRecord);

        // Assert
        Assert.NotNull(race);
        Assert.NotNull(race.RaceKey);
        Assert.Equal(16, race.RaceKey.Length);
        Assert.Equal("2026", race.RaceKey.Substring(0, 4)); // Year
        Assert.Equal("05", race.RaceKey.Substring(4, 2));   // Month
        Assert.Equal("04", race.RaceKey.Substring(6, 2));   // Day
        Assert.Equal("02", race.RaceKey.Substring(8, 2));   // Track
        Assert.Equal("01", race.RaceKey.Substring(10, 2));  // Round
        Assert.Equal("01", race.RaceKey.Substring(12, 2));  // Day of round
        Assert.Equal("01", race.RaceKey.Substring(14, 2));  // Race number
    }

    // ──── Helper Methods ────

    /// <summary>
    /// Create a mock RA record for testing.
    /// </summary>
    private static string CreateMockRARecord(
        string year, string monthDay, string trackCode, string round, string dayOfRound, string raceNumber,
        string distance, string surfaceCode, string gradeCode,
        string conditions2yo, string conditions3yo, string conditions4yo, string conditions5plus)
    {
        // Build a fixed-width record matching RARecordParser expectations
        var sb = new StringBuilder(1272);

        // Pad to position 11 (year starts at 0-based index 11)
        sb.Append("RA".PadRight(11));           // 0-10
        sb.Append(year);                        // 11-14
        sb.Append(monthDay);                    // 15-18
        sb.Append(trackCode);                   // 19-20
        sb.Append(round);                       // 21-22
        sb.Append(dayOfRound);                  // 23-24
        sb.Append(raceNumber);                  // 25-26

        // Pad to grade position (614)
        int currentPos = 27;
        sb.Append(new string(' ', 614 - currentPos));

        // Add grade (pos 614)
        sb.Append(gradeCode.PadRight(1));      // 614

        // Pad to conditions positions
        currentPos = 615;
        sb.Append(new string(' ', 622 - currentPos));

        // Add conditions (3 bytes each)
        sb.Append(conditions2yo.PadRight(3));  // 622-624
        sb.Append(conditions3yo.PadRight(3));  // 625-627
        sb.Append(conditions4yo.PadRight(3));  // 628-630
        sb.Append(conditions5plus.PadRight(3)); // 631-633

        // Pad to distance position (697)
        currentPos = 634;
        sb.Append(new string(' ', 697 - currentPos));

        // Add distance (4 bytes)
        sb.Append(distance.PadRight(4));       // 697-700

        // Pad to surface position (705)
        currentPos = 701;
        sb.Append(new string(' ', 705 - currentPos));

        // Add surface code (2 bytes)
        sb.Append(surfaceCode.PadRight(2));    // 705-706

        // Pad to end (1272 total)
        currentPos = 707;
        if (currentPos < 1272)
            sb.Append(new string(' ', 1272 - currentPos));

        return sb.ToString();
    }

    /// <summary>
    /// Create a mock SE record for testing.
    /// </summary>
    private static string CreateMockSERecord(
        string frameNumber, string postPosition, string horseId,
        string trainerCode, string jockeyCode, string oddsStr)
    {
        var sb = new StringBuilder(555);

        // Record type (0-1)
        sb.Append("SE");                        // 0-1

        // Pad to frame position (27)
        sb.Append(new string(' ', 25));         // 2-26

        // Frame number (27)
        sb.Append(frameNumber.PadRight(1));     // 27

        // Post position (28)
        sb.Append(postPosition.PadRight(2));    // 28-29

        // Horse ID (30-39)
        sb.Append(horseId.PadRight(10));        // 30-39

        // Pad to trainer code (85)
        int currentPos = 40;
        sb.Append(new string(' ', 85 - currentPos));

        // Trainer code (85-89)
        sb.Append(trainerCode.PadRight(5));     // 85-89

        // Pad to jockey code (296)
        currentPos = 90;
        sb.Append(new string(' ', 296 - currentPos));

        // Jockey code (296-300)
        sb.Append(jockeyCode.PadRight(5));      // 296-300

        // Pad to odds (359)
        currentPos = 301;
        sb.Append(new string(' ', 359 - currentPos));

        // Odds (359-362)
        sb.Append(oddsStr.PadRight(4));         // 359-362

        // Pad to end (555 total)
        currentPos = 363;
        if (currentPos < 555)
            sb.Append(new string(' ', 555 - currentPos));

        return sb.ToString();
    }
}
