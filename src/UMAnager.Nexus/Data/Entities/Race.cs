namespace UMAnager.Nexus.Data.Entities;

public class Race
{
    public string RaceId { get; set; } = "";  // YYYYMMDDPPNNNNNN (16 chars)
    public DateTime RaceDate { get; set; }    // Extract from RaceId YYYYMMDD
    public string? TrackCode { get; set; }    // 01-10 (JRA track code)
    public int? RaceNumber { get; set; }      // Race number in the day (1-12)
    public string? NameJa { get; set; }       // Race name (Shift-JIS decoded, Hondai)
    public int? Distance { get; set; }        // Distance in meters (1000-3600)
    public string? Surface { get; set; }      // 'turf' or 'dirt' (from 1=turf, 2=dirt)
    public DateTime? SortTime { get; set; }   // Combined date + scheduled race time
    public string? ResultsJson { get; set; }  // Payoffs and finish positions (JSONB)
    public string? OddsJson { get; set; }     // Phase 11 forward: O2/O5 quinella+trio odds (JSONB)
    public short DataStatus { get; set; }       // JRA-VAN データ区分 (offset 3, len 1)
    public DateOnly? LastModified { get; set; } // データ作成年月日 (offset 4, len 8, YYYYMMDD)
    public bool HistoryRefreshed { get; set; } = false;
    public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
}
