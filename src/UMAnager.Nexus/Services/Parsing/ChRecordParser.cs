using System.Text;

namespace UMAnager.Nexus.Services.Parsing;

/// <summary>
/// Parses CH (調教師マスタ / Trainer Master) records from the DIFN DataSpec.
///
/// Layout per JV_CH_CHOKYOSI (Oracle Q21, 1-indexed, record length 3862 bytes):
///   RecordTypeId      offset 1   len 2   "CH"
///   ChokyosiCode      offset 12  len 5   5-digit ASCII trainer ID, PK
///   IssueFlag         offset 17  len 1   0=Active, 1=Retired
///   MenkyoNenMD       offset 18  len 8   license start YYYYMMDD
///   BirthDate         offset 34  len 8   YYYYMMDD
///   ChokyosiName      offset 42  len 34  Shift-JIS full kanji
///   ChokyosiRyakusyo  offset 106 len 8   Shift-JIS short kanji
///   ChokyosiNameEng   offset 114 len 80  ASCII romaji
///   Syozoku           offset 195 len 1   1=East, 2=West
/// </summary>
public static class ChRecordParser
{
    private static readonly Encoding ShiftJis = Encoding.GetEncoding(932);

    public sealed record ChRow(
        string TrainerCode,
        string? NameJa,
        string? NameEn,
        int? IssueFlag,
        string? Syozoku);

    public static ChRow? Parse(byte[] raw)
    {
        if (raw.Length < 195) return null;
        var span = raw.AsSpan();
        if (!(span[0] == (byte)'C' && span[1] == (byte)'H')) return null;

        var code = Encoding.ASCII.GetString(span.Slice(11, 5)).Trim();
        if (string.IsNullOrEmpty(code) || code == "00000") return null;

        int? issueFlag = null;
        var flagStr = Encoding.ASCII.GetString(span.Slice(16, 1)).Trim();
        if (int.TryParse(flagStr, out var f)) issueFlag = f;

        var fullKanji = ShiftJis.GetString(span.Slice(41, 34)).Trim();
        var shortKanji = ShiftJis.GetString(span.Slice(105, 8)).Trim();
        var nameJa = !string.IsNullOrWhiteSpace(shortKanji)
            ? shortKanji
            : (!string.IsNullOrWhiteSpace(fullKanji) ? fullKanji : null);

        string? nameEn = Encoding.ASCII.GetString(span.Slice(113, 80)).Trim();
        if (string.IsNullOrWhiteSpace(nameEn)) nameEn = null;

        var syozoku = Encoding.ASCII.GetString(span.Slice(194, 1)).Trim();
        if (string.IsNullOrEmpty(syozoku)) syozoku = null;

        return new ChRow(code, nameJa, nameEn, issueFlag, syozoku);
    }
}
