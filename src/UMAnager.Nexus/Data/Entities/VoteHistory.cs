namespace UMAnager.Nexus.Data.Entities;

/// <summary>
/// Phase 30: records every horse the user sends to OrePro via Apply Votes.
/// One row per (race_id, horse_id) — re-applying the same race updates the mark/timestamp
/// rather than duplicating. The count of rows per horse_id drives the "Voted N×" badge.
/// </summary>
public class VoteHistory
{
    public long Id { get; set; }
    public string HorseId { get; set; } = "";   // FK to horses.horse_id
    public string RaceId { get; set; } = "";    // FK to races.race_id
    public string Mark { get; set; } = "";      // ◎〇▲△
    public DateTime VotedAt { get; set; }       // UTC
}
