namespace UMAnager.Nexus.Data.Entities;

// Pedigree-ancestor master sourced from the KETTO3_INFO slots embedded in every UM record.
// Keyed by HansyokuNum (繁殖登録番号 / breeding-registration-number) — a separate ID space
// from the KettoNum used in the horses table. Foreign sires/dams live here.
public class BreedingHorse
{
    public string HansyokuNum { get; set; } = ""; // 10 chars, distinct from KettoNum
    public string NameJa { get; set; } = "";      // Shift-JIS decoded Bamei (from UM KETTO3_INFO slot, len 36)
    public string? NameEn { get; set; }           // Romaji name (from HN record offset 117, len 80, via BLDN spec)
    public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
}
