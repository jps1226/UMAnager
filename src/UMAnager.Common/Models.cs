namespace UMAnager.Common;

public record Horse
{
    public string HorseId { get; init; } = string.Empty;
    public string? JapaneseName { get; init; }
    public string? RomajiName { get; init; }
    public int? BirthYear { get; init; }
    public string? SireId { get; init; }
    public string? DamId { get; init; }
    public string? BroodmareSireId { get; init; }
    public DateTime? LastUpdated { get; init; }
    public string DataSource { get; init; } = "UM";
}

public record Race
{
    public string RaceId { get; init; } = string.Empty;
    public string? RaceKey { get; init; }  // 16-char SDK key: YYYYMMDDJJKKHHRR
    public int RaceYear { get; init; }
    public int RaceMonth { get; init; }
    public int RaceDay { get; init; }
    public string? TrackCode { get; init; }
    public int Round { get; init; }
    public int DayOfRound { get; init; }
    public int RaceNumber { get; init; }
    public DateTime? RaceDate { get; init; }
    public TimeOnly? RaceStartTime { get; init; }
    public string? JapaneseName { get; init; }
    public int Distance { get; init; }
    public string? Surface { get; init; }
    public string? Grade { get; init; }
    public string? Conditions2yo { get; init; }
    public string? Conditions3yo { get; init; }
    public string? Conditions4yo { get; init; }
    public string? Conditions5plus { get; init; }
    public DateTime? LastUpdated { get; init; }
}

public record RaceEntry
{
    public int Id { get; init; }
    public string RaceId { get; init; } = string.Empty;
    public string HorseId { get; init; } = string.Empty;
    public int? PostPosition { get; init; }
    public int? FrameNumber { get; init; }
    public int? HorseWeight { get; init; }
    public string? JockeyCode { get; init; }
    public string? JockeyName { get; init; }
    public string? TrainerCode { get; init; }
    public string? TrainerName { get; init; }
    public decimal? MorningLineOdds { get; init; }
    public decimal? LatestOdds { get; init; }
    public int? FinishPosition { get; init; }
    public int? FinishTimeHundredths { get; init; }
    public decimal? PayoffWin { get; init; }
    public decimal? PayoffPlace { get; init; }
    public decimal? PayoffShow { get; init; }
    public DateTime? UpdatedAt { get; init; }
}

public record SyncState
{
    public int Id { get; init; } = 1;
    public long LastTimestampUm { get; init; }
    public long LastTimestampRaces { get; init; }
    public DateTime? LastSyncAt { get; init; }
    public string? LastError { get; init; }
    public int SyncCount { get; init; }
}

public record BetSlip
{
    public int Id { get; init; }
    public string RaceId { get; init; } = string.Empty;
    public string BetType { get; init; } = string.Empty;
    public string HorsesJson { get; init; } = "[]";
    public string OddsJson { get; init; } = "{}";
    public DateTime CreatedAt { get; init; }
    public DateTime? ExportedAt { get; init; }
}

public enum JVLinkErrorCode
{
    Success = 0,
    NoData = -1,
    FileChange = -1,
    InvalidParameter = -101,
    RegistryError = -211,
    AuthenticationFailed = -303,
    NotAuthorized = -501,
    DownloadError = -401,
    ParseError = -402,
    CommunicationError = -502,
}

public class JVLinkException : Exception
{
    public JVLinkErrorCode ErrorCode { get; }

    public JVLinkException(int code, string message) : base(message)
    {
        ErrorCode = (JVLinkErrorCode)code;
    }

    public JVLinkException(int code, string message, Exception? innerException) : base(message, innerException)
    {
        ErrorCode = (JVLinkErrorCode)code;
    }
}
