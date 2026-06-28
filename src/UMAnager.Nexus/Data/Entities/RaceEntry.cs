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
    public string? JockeyName { get; set; }    // Jockey name (Shift-JIS short form, SE offset 307 len 8)
    public string? JockeyCode { get; set; }    // 5-digit ASCII KisyuCode, SE offset 297 len 5 (Oracle Q21)
    public string? TrainerCode { get; set; }   // 5-digit ASCII ChokyosiCode, SE offset 86 len 5 (Oracle Q21)
    public decimal? Odds { get; set; }         // Win odds (converted from raw)
    public decimal? PrevOdds { get; set; }    // Win odds from the previous fetch cycle (for ↑↓ delta indicator)
    public int? FavRank { get; set; }          // Favorite rank (1-18, where 1 is favorite)
    public int? FinishPos { get; set; }        // Finishing position (0 = withdrew/disqualified)
    public bool Scratched { get; set; }        // SE 異常区分 ∈ {1 出走取消, 2 発走除外, 3 競走除外} = removed from betting/refunded.
                                               // 中止 (code 4) does NOT set this — that bet stands and loses. Only populates
                                               // once results settle (DataKubun 3-7); blank on the confirmed card / live window.
    public string? PerformanceJson { get; set; } // Finish time, margins, etc. (JSONB)
    public short? Sex { get; set; }              // SE 性別コード (offset 79, len 1): 1=牡 colt, 2=牝 filly/mare, 3=セ gelding, 0=unknown
    public short DataStatus { get; set; }        // JRA-VAN データ区分 (offset 3, len 1)
    public DateOnly? LastModified { get; set; }  // データ作成年月日 (offset 4, len 8, YYYYMMDD)
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}
