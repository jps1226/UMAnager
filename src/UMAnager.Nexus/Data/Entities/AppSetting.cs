namespace UMAnager.Nexus.Data.Entities;

public class AppSetting
{
    public string Key { get; set; } = "";
    public string? Value { get; set; }
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}
