using System.Text;

namespace UMAnager.Nexus.Services.Parsing;

/// <summary>
/// Parses HN (繁殖馬マスタ / Breeding Horse Master) records from the BLDN DataSpec.
/// Provides the romaji name for HansyokuNum-keyed breeding horses — these never appear
/// as KettoNum in the UM stream, so this is the only path to fill breeding_horses.NameEn.
///
/// Layout per JV_HN (Oracle Q19, 1-indexed):
///   RecordTypeId  offset 1   len 2   "HN"
///   HansyokuNum   offset 12  len 10  ASCII digits
///   Bamei         offset 22  len 36  Shift-JIS (we ignore — already populated from UM)
///   BameiEng      offset 117 len 80  ASCII, padded with spaces, may carry "(USA)" suffix
/// </summary>
public static class HnRecordParser
{
    public sealed record HnRow(string HansyokuNum, string? NameEn);

    public static HnRow? Parse(byte[] raw)
    {
        if (raw.Length < 197) return null;
        var span = raw.AsSpan();

        if (!(span[0] == (byte)'H' && span[1] == (byte)'N')) return null;

        var hansyokuNum = Encoding.ASCII.GetString(span.Slice(11, 10)).Trim();
        if (string.IsNullOrEmpty(hansyokuNum) || hansyokuNum == "0000000000") return null;

        var rawEn = Encoding.ASCII.GetString(span.Slice(116, 80)).Trim();
        var nameEn = StripCountrySuffix(rawEn);
        if (string.IsNullOrWhiteSpace(nameEn)) nameEn = null;

        return new HnRow(hansyokuNum, nameEn);
    }

    // Strip trailing "(USA)" / "(JPN)" / "(GB)" — same convention as UM romaji field.
    private static string StripCountrySuffix(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        int open = name.LastIndexOf('(');
        if (open <= 0) return name;
        int close = name.LastIndexOf(')');
        if (close != name.Length - 1) return name;
        return name.Substring(0, open).TrimEnd();
    }
}
