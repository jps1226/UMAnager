using System.Text;

namespace UMAnager.Nexus.Services.Parsing;

/// <summary>
/// Parses KS (騎手マスタ / Jockey Master) records from the DIFN DataSpec.
///
/// Layout per JV_KS_KISYU (Oracle Q21, 1-indexed, record length 4173 bytes):
///   RecordTypeId   offset 1   len 2   "KS"
///   KisyuCode      offset 12  len 5   5-digit ASCII jockey ID, PK
///   IssueFlag      offset 17  len 1   0=Active, 1=Retired
///   MenkyoNenMD    offset 18  len 8   license start YYYYMMDD
///   BirthDate      offset 34  len 8   YYYYMMDD
///   KisyuName      offset 42  len 34  Shift-JIS full kanji
///   KisyuRyakusyo  offset 140 len 8   Shift-JIS short kanji
///   KisyuNameEng   offset 148 len 80  ASCII romaji
/// </summary>
public static class KsRecordParser
{
    private static readonly Encoding ShiftJis = Encoding.GetEncoding(932);

    public sealed record KsRow(
        string JockeyCode,
        string? NameJa,        // KisyuRyakusyo (short) preferred; falls back to KisyuName
        string? NameEn,
        int? IssueFlag);

    public static KsRow? Parse(byte[] raw)
    {
        if (raw.Length < 228) return null;
        var span = raw.AsSpan();
        if (!(span[0] == (byte)'K' && span[1] == (byte)'S')) return null;

        var code = Encoding.ASCII.GetString(span.Slice(11, 5)).Trim();
        if (string.IsNullOrEmpty(code) || code == "00000") return null;

        int? issueFlag = null;
        var flagStr = Encoding.ASCII.GetString(span.Slice(16, 1)).Trim();
        if (int.TryParse(flagStr, out var f)) issueFlag = f;

        var fullKanji = ShiftJis.GetString(span.Slice(41, 34)).Trim();
        var shortKanji = ShiftJis.GetString(span.Slice(139, 8)).Trim();
        var nameJa = !string.IsNullOrWhiteSpace(shortKanji)
            ? shortKanji
            : (!string.IsNullOrWhiteSpace(fullKanji) ? fullKanji : null);

        string? nameEn = Encoding.ASCII.GetString(span.Slice(147, 80)).Trim();
        if (string.IsNullOrWhiteSpace(nameEn)) nameEn = null;

        return new KsRow(code, nameJa, nameEn, issueFlag);
    }
}
