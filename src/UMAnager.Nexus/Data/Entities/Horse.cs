namespace UMAnager.Nexus.Data.Entities;

public class Horse
{
    public string HorseId { get; set; } = "";  // KettoNum, 10 chars
    public string NameJa { get; set; } = "";   // Japanese name (Shift-JIS decoded)
    public string? NameEn { get; set; }        // English/Romanized name
    public int? BirthYear { get; set; }        // Year of birth (YYYY)
    public string? SireId { get; set; }        // Sire's KettoNum
    public string? DamId { get; set; }         // Dam's KettoNum
    public string? BmsId { get; set; }         // Brood Mare Sire's KettoNum
    public string? PedigreeJson { get; set; }  // Cached recursive pedigree tree (JSONB)
    public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
}
