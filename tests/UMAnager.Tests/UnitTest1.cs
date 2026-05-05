namespace UMAnager.Tests;

using UMAnager.Common;

public class ModelTests
{
    [Fact]
    public void SyncState_CreateWithDefaults_ShouldHaveZeroTimestamps()
    {
        // Arrange & Act
        var state = new SyncState { Id = 1 };

        // Assert
        Assert.Equal(1, state.Id);
        Assert.Equal(0, state.LastTimestampUm);
        Assert.Equal(0, state.LastTimestampRaces);
        Assert.Equal(0, state.SyncCount);
        Assert.Null(state.LastError);
    }

    [Fact]
    public void Horse_CreateWithData_ShouldStoreValues()
    {
        // Arrange & Act
        var horse = new Horse
        {
            HorseId = "000001",
            JapaneseName = "テスト馬",
            RomajiName = "Test Horse",
            BirthYear = 2020,
            DataSource = "UM"
        };

        // Assert
        Assert.Equal("000001", horse.HorseId);
        Assert.Equal("テスト馬", horse.JapaneseName);
        Assert.Equal("Test Horse", horse.RomajiName);
        Assert.Equal(2020, horse.BirthYear);
        Assert.Equal("UM", horse.DataSource);
    }

    [Fact]
    public void JVLinkException_CreateWithCode_ShouldStoreErrorCode()
    {
        // Arrange & Act
        var ex = new JVLinkException(-303, "Auth failed");

        // Assert
        Assert.Equal("Auth failed", ex.Message);
        Assert.Equal(JVLinkErrorCode.AuthenticationFailed, ex.ErrorCode);
    }

    [Fact]
    public void Race_CreateWithMetadata_ShouldStoreDateAndDistance()
    {
        // Arrange & Act
        var race = new Race
        {
            RaceId = "202605040101",
            RaceDate = new DateOnly(2026, 5, 4),
            Distance = 2000,
            Surface = "turf",
            Grade = "G1"
        };

        // Assert
        Assert.Equal("202605040101", race.RaceId);
        Assert.Equal(new DateOnly(2026, 5, 4), race.RaceDate);
        Assert.Equal(2000, race.Distance);
        Assert.Equal("turf", race.Surface);
        Assert.Equal("G1", race.Grade);
    }
}