namespace UMAnager.Nexus.Data.Entities;

public class RawStagingRecord
{
    public long Id { get; set; }
    public string RecordType { get; set; } = "";  // 2-char ASCII: "UM", "RA", "SE", "KS", "CH", "RC"
    public byte[] RawBytes { get; set; } = [];    // full record as BYTEA (e.g. 1609 bytes for UM)
    public DateTime ReceivedAt { get; set; }
    public bool IsProcessed { get; set; }
}
