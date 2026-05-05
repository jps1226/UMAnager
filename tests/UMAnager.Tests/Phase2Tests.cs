namespace UMAnager.Tests;

using System.Text;
using UMAnager.Common;
using UMAnager.Ingestion.Service;

/// <summary>
/// Unit tests for Phase 2: Master Data Bootstrap (UM Records)
/// Tests UM record parsing, Shift-JIS decoding, and pedigree validation.
/// Does NOT require JRA-VAN connection or real data.
/// </summary>
public class Phase2Tests
{
    /// <summary>
    /// Test Shift-JIS (CP932) decoding of horse names.
    /// </summary>
    [Fact]
    public void DecodeRecord_ValidCP932Bytes_DecodesSuccessfully()
    {
        // Arrange: Japanese characters encoded as CP932
        // "競走" (racehorse) in CP932: 0x8AB88E9F
        byte[] cp932Bytes = { 0x8A, 0xB8, 0x8E, 0x9F };

        // Act
        string result = JVEncoding.DecodeRecord(cp932Bytes);

        // Assert
        Assert.Equal("競走", result);
    }

    /// <summary>
    /// Test that DecodeRecord trims null bytes and ideographic spaces.
    /// </summary>
    [Fact]
    public void DecodeRecord_WithPadding_TrimsPadding()
    {
        // Arrange: CP932 encoded "馬" (horse) with null byte padding
        byte[] cp932Bytes = Encoding.GetEncoding(932).GetBytes("馬\0\0\0");

        // Act
        string result = JVEncoding.DecodeRecord(cp932Bytes);

        // Assert
        Assert.Equal("馬", result);
    }

    /// <summary>
    /// Test DecodeRecord with empty bytes.
    /// </summary>
    [Fact]
    public void DecodeRecord_EmptyBytes_ReturnsEmptyString()
    {
        // Act
        string result = JVEncoding.DecodeRecord([]);

        // Assert
        Assert.Empty(result);
    }

    /// <summary>
    /// Test UM record parsing with valid data.
    /// </summary>
    [Fact]
    public void ParseUMRecord_ValidRecord_ReturnsHorseWithPedigree()
    {
        // Arrange: Mock UM record line (fixed-width format)
        // This simulates a real UM record structure
        string umRecord = CreateMockUMRecord(
            horseId: "0000000001",
            horseName: "テスト馬    ",
            horseRomaji: "Test Horse  ",
            birthYear: "2020",
            sireId: "0000000010  ",
            damId: "0000000011  ",
            bmsId: "0000000012  "
        );

        // Act
        var horse = UMRecordParser.ParseUMRecord(umRecord);

        // Assert
        Assert.NotNull(horse);
        Assert.Equal("0000000001", horse.HorseId);
        Assert.Equal("テスト馬", horse.HorseNameJapanese);
        Assert.Equal("Test Horse", horse.HorseNameRomaji);
        Assert.Equal(2020, horse.BirthYear);
        Assert.Equal("0000000010", horse.SireId);
        Assert.Equal("0000000011", horse.DamId);
        Assert.Equal("0000000012", horse.BroodmareSireId);
        Assert.Equal("UM", horse.DataSource);
    }

    /// <summary>
    /// Test UM record parsing with missing optional fields.
    /// </summary>
    [Fact]
    public void ParseUMRecord_WithNullFields_HandlesNullsGracefully()
    {
        // Arrange: UM record with empty pedigree fields
        string umRecord = CreateMockUMRecord(
            horseId: "0000000002",
            horseName: "新馬        ",
            horseRomaji: "Debut Horse ",
            birthYear: "2024",
            sireId: "          ",  // Empty
            damId: "          ",   // Empty
            bmsId: "          "    // Empty
        );

        // Act
        var horse = UMRecordParser.ParseUMRecord(umRecord);

        // Assert
        Assert.NotNull(horse);
        Assert.Equal("0000000002", horse.HorseId);
        Assert.Null(horse.SireId);
        Assert.Null(horse.DamId);
        Assert.Null(horse.BroodmareSireId);
    }

    /// <summary>
    /// Test pedigree validation - complete pedigree.
    /// </summary>
    [Fact]
    public void HasCompletePedigree_WithAllThreeGenerations_ReturnsTrue()
    {
        // Arrange
        var horse = new Horse
        {
            HorseId = "0000000001",
            SireId = "0000000010",
            DamId = "0000000011",
            BroodmareSireId = "0000000012"
        };

        // Act
        bool result = UMRecordParser.HasCompletePedigree(horse);

        // Assert
        Assert.True(result);
    }

    /// <summary>
    /// Test pedigree validation - missing sire.
    /// </summary>
    [Fact]
    public void HasCompletePedigree_MissingSire_ReturnsFalse()
    {
        // Arrange
        var horse = new Horse
        {
            HorseId = "0000000002",
            SireId = null,  // Missing
            DamId = "0000000011",
            BroodmareSireId = "0000000012"
        };

        // Act
        bool result = UMRecordParser.HasCompletePedigree(horse);

        // Assert
        Assert.False(result);
    }

    /// <summary>
    /// Test pedigree validation - missing dam.
    /// </summary>
    [Fact]
    public void HasCompletePedigree_MissingDam_ReturnsFalse()
    {
        // Arrange
        var horse = new Horse
        {
            HorseId = "0000000003",
            SireId = "0000000010",
            DamId = null,  // Missing
            BroodmareSireId = "0000000012"
        };

        // Act
        bool result = UMRecordParser.HasCompletePedigree(horse);

        // Assert
        Assert.False(result);
    }

    /// <summary>
    /// Test pedigree validation - missing broodmare sire.
    /// </summary>
    [Fact]
    public void HasCompletePedigree_MissingBroodmareSire_ReturnsFalse()
    {
        // Arrange
        var horse = new Horse
        {
            HorseId = "0000000004",
            SireId = "0000000010",
            DamId = "0000000011",
            BroodmareSireId = null  // Missing
        };

        // Act
        bool result = UMRecordParser.HasCompletePedigree(horse);

        // Assert
        Assert.False(result);
    }

    /// <summary>
    /// Test UM record parsing with invalid record type (not "UM").
    /// </summary>
    [Fact]
    public void ParseUMRecord_WrongRecordType_ReturnsNull()
    {
        // Arrange: Record with "RA" instead of "UM"
        string raRecord = "RA" + new string(' ', 200);

        // Act
        var horse = UMRecordParser.ParseUMRecord(raRecord);

        // Assert
        Assert.Null(horse);
    }

    /// <summary>
    /// Test UM record parsing with too-short record.
    /// </summary>
    [Fact]
    public void ParseUMRecord_TooShort_ReturnsNull()
    {
        // Arrange: Record shorter than expected
        string shortRecord = "UM00000001";

        // Act
        var horse = UMRecordParser.ParseUMRecord(shortRecord);

        // Assert
        Assert.Null(horse);
    }

    /// <summary>
    /// Test UM record parsing with null input.
    /// </summary>
    [Fact]
    public void ParseUMRecord_NullInput_ReturnsNull()
    {
        // Act
        var horse = UMRecordParser.ParseUMRecord(null);

        // Assert
        Assert.Null(horse);
    }

    /// <summary>
    /// Test JVTimestamp parsing.
    /// </summary>
    [Fact]
    public void ParseJVTimestamp_ValidFormat_ParsesCorrectly()
    {
        // Arrange
        string timestamp = "20260504143000";  // 2026-05-04 14:30:00 UTC

        // Act
        var dt = JVEncoding.ParseJVTimestamp(timestamp);

        // Assert
        Assert.Equal(2026, dt.Year);
        Assert.Equal(5, dt.Month);
        Assert.Equal(4, dt.Day);
        Assert.Equal(14, dt.Hour);
        Assert.Equal(30, dt.Minute);
        Assert.Equal(0, dt.Second);
        Assert.Equal(DateTimeKind.Utc, dt.Kind);
    }

    /// <summary>
    /// Test JVTimestamp formatting (reverse of parsing).
    /// </summary>
    [Fact]
    public void FormatJVTimestamp_ValidDateTime_FormatsCorrectly()
    {
        // Arrange
        var dt = new DateTime(2026, 5, 4, 14, 30, 0, DateTimeKind.Utc);

        // Act
        string result = JVEncoding.FormatJVTimestamp(dt);

        // Assert
        Assert.Equal("20260504143000", result);
    }

    // ──── Helper Methods ────

    /// <summary>
    /// Create a mock UM record line for testing.
    /// Note: This is simplified for testing; actual UM records have more fields.
    /// </summary>
    private static string CreateMockUMRecord(
        string horseId,
        string horseName,
        string horseRomaji,
        string birthYear,
        string sireId,
        string damId,
        string bmsId)
    {
        // Build a fixed-width record matching UMRecordParser expectations
        // Record Type (0-2): "UM"
        // Horse ID (2-12): 10 chars
        // Padding (12): empty
        // Horse Name JP (12-76): 64 chars
        // Horse Name Romaji (76-116): 40 chars
        // Birth Year (116-120): 4 chars
        // Sire ID (120-130): 10 chars
        // Dam ID (130-140): 10 chars
        // BMS ID (140-150): 10 chars

        var sb = new StringBuilder();
        sb.Append("UM");                           // 0-2
        sb.Append(horseId.PadRight(10));          // 2-12
        sb.Append(horseName.PadRight(64));        // 12-76
        sb.Append(horseRomaji.PadRight(40));      // 76-116
        sb.Append(birthYear.PadRight(4));         // 116-120
        sb.Append(sireId.PadRight(10));           // 120-130
        sb.Append(damId.PadRight(10));            // 130-140
        sb.Append(bmsId.PadRight(10));            // 140-150

        return sb.ToString();
    }
}
