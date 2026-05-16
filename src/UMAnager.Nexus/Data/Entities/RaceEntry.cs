namespace UMAnager.Nexus.Data.Entities;

public class RaceEntry
{
    public long Id { get; set; }               // SERIAL (auto-increment)
    public string RaceId { get; set; } = "";   // FK to races.race_id
    public string HorseId { get; set; } = "";  // FK to horses.horse_id
    public int? PostPosition { get; set; }     // Gate number (1-18)
    public int? Bracket { get; set; }          // Internal bracket number (0-7)
    public int? Weight { get; set; }           // Combined burden weight (jockey + saddle) in kg
    public int? HorseWeight { get; set; }      // Horse weight in kg
    public string? JockeyName { get; set; }    // Jockey name (Shift-JIS decoded)
    public decimal? Odds { get; set; }         // Win odds (converted from raw)
    public int? FavRank { get; set; }          // Favorite rank (1-18, where 1 is favorite)
    public int? FinishPos { get; set; }        // Finishing position (0 = withdrew/disqualified)
    public string? PerformanceJson { get; set; } // Finish time, margins, etc. (JSONB)
    public short DataStatus { get; set; }        // JRA-VAN データ区分 (offset 3, len 1)
    public DateOnly? LastModified { get; set; }  // データ作成年月日 (offset 4, len 8, YYYYMMDD)
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}
