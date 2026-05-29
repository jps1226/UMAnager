namespace UMAnager.Nexus.Data.Entities;

/// <summary>
/// Phase 37: a time-series snapshot of a runner's win odds, appended every time the
/// odds value actually changes (see OddsApplyService). Lets the UI draw an odds-over-time
/// trend graph instead of just the single ↑↓ delta arrow. Rows for past race days are
/// purged when the next weekly race plan is ingested (see NexusPipeServer TOKU handler).
/// </summary>
public class OddsHistory
{
    public long Id { get; set; }
    public string RaceId { get; set; } = "";   // FK to races.race_id
    public string HorseId { get; set; } = "";  // FK to horses.horse_id
    public int? PostPosition { get; set; }      // Umaban at capture time (for display/coloring)
    public decimal Odds { get; set; }           // Win odds at this instant
    public int? FavRank { get; set; }           // Popularity rank (1=favorite) at this instant
    public DateTime CapturedAt { get; set; }    // UTC timestamp of the snapshot
}
