namespace UMAnager.Tests;

using System.Text;
using UMAnager.Common;

// Static constructor to register encoding provider (needed for CP932 on some systems)
[CollectionDefinition("Phase2 Tests", DisableParallelization = true)]
public class Phase2TestCollection
{
    static Phase2TestCollection()
    {
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
    }
}

/// <summary>
/// Unit tests for Phase 2: Master Data Bootstrap (UM Records)
/// Tests UM record parsing, Shift-JIS decoding, and pedigree validation.
/// Does NOT require JRA-VAN connection or real data.
/// </summary>
[Collection("Phase2 Tests")]
public class Phase2Tests
{
    /// <summary>
    /// Test Shift-JIS (CP932) decoding of horse names.
    /// </summary>
    [Fact]
    public void DecodeRecord_ValidCP932Bytes_DecodesSuccessfully()
    {
        // Arrange: Japanese characters encoded as CP932
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
        // "競走" (racehorse) - generate bytes to ensure they're correct
        byte[] cp932Bytes = Encoding.GetEncoding(932).GetBytes("競走");

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
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
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
            horseName: "テスト馬",
            horseRomaji: "Test Horse",
            birthYear: "2020",
            sireId: "0000000010",
            damId: "0000000011",
            bmsId: "0000000012"
        );

        // Act
        var horse = UMRecordParser.ParseUMRecord(umRecord);

        // Assert
        Assert.NotNull(horse);
        Assert.Equal("0000000001", horse.HorseId);
        Assert.Equal("テスト馬", horse.JapaneseName);
        Assert.Equal("Test Horse", horse.RomajiName);
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
            horseName: "新馬",
            horseRomaji: "Debut Horse",
            birthYear: "2024",
            sireId: "",  // Empty
            damId: "",   // Empty
            bmsId: ""    // Empty
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
        // Build a fixed-width record matching official JRA-VAN UM spec (0-based offsets):
        //   0  -  2 : Record type "UM"
        //   2  - 11 : Header padding (9 bytes)
        //  11  - 21 : HorseId (血統登録番号, 10 bytes)
        //  21  - 38 : Padding (17 bytes)
        //  38  - 46 : BirthDate YYYYMMDD (生年月日, 8 bytes)
        //  46  - 82 : HorseName Japanese (馬名, 36 bytes)
        //  82  -118 : Padding (36 bytes)
        // 118  -178 : HorseRomaji (馬名欧字, 60 bytes)
        // 178  -204 : Padding (26 bytes)
        // 204  -214 : SireId (父, 10 bytes)
        // 214  -250 : Sire name (36 bytes)
        // 250  -260 : DamId (母, 10 bytes)
        // 260  -296 : Dam name (36 bytes)
        // 296  -342 : PGS block (46 bytes)
        // 342  -388 : PGD block (46 bytes)
        // 388  -398 : BroodmareSireId (母父, 10 bytes)

        var buf = new char[500];
        Array.Fill(buf, ' ');

        void Place(int offset, string value, int length)
        {
            for (int i = 0; i < length && i < value.Length; i++)
                buf[offset + i] = value[i];
        }

        Place(0,   "UM",                          2);
        Place(11,  horseId,                       10);
        Place(38,  (birthYear + "0101").PadRight(8), 8); // YYYYMMDD
        Place(46,  horseName,                     36);
        Place(118, horseRomaji,                   60);
        Place(204, sireId,                        10);
        Place(250, damId,                         10);
        Place(388, bmsId,                         10);

        return new string(buf);
    }
}
