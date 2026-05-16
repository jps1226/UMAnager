namespace UMAnager.Nexus.Data.Entities;

// Pedigree-ancestor master sourced from the KETTO3_INFO slots embedded in every UM record.
// Keyed by HansyokuNum (繁殖登録番号 / breeding-registration-number) — a separate ID space
// from the KettoNum used in the horses table. Foreign sires/dams live here.
public class BreedingHorse
{
    public string HansyokuNum { get; set; } = ""; // 10 chars, distinct from KettoNum
    public string NameJa { get; set; } = "";      // Shift-JIS decoded Bamei
    public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
}
