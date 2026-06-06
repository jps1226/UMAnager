// ============================================================
// FILE: OddsRecordParser.cs
// LAYER: Parsing (static, zero-alloc span parser)
// PURPOSE: O2 (quinella) + O5 (trio) odds bytes → slot arrays merged into races.OddsJson under
//          "quinella"/"trio". Records are dynamically sized; the slot loop breaks at end-of-data.
// CAUTION: Skips the "999999" sentinel (sold-but-no-odds / impossible combo).
// LAST DOCUMENTED: 2026-06-02
// ============================================================
namespace UMAnager.Nexus.Services.Parsing;

/// <summary>
/// Parser for JV-Link O2 (Quinella / 馬連) and O5 (Trio / 3連複) odds records.
/// Byte offsets per Oracle Q19 (2026-05-17).
///
/// Output is consumed by DifnRecordParsingService and merged into Race.OddsJson
/// with this shape:
/// {
///   "quinella": [{ "combo": [a, b],    "odds": 12.5,  "rank": 2 }, ...],
///   "trio":     [{ "combo": [a, b, c], "odds": 85.2,  "rank": 5 }, ...]
/// }
/// </summary>
public static class OddsRecordParser
{
    private static readonly System.Text.Encoding Ascii = System.Text.Encoding.ASCII;

    public sealed record OddsSlot(int[] Combo, decimal Odds, int Rank);
    public sealed record OddsParseResult(string RaceId, List<OddsSlot> Slots);

    /// <summary>
    /// Parse an O2 (Quinella) record. 153 slots × 13 bytes starting at offset 41
    /// (1-indexed). Combo is "aabb" (4 ASCII digits), odds is 6 bytes (÷10),
    /// rank is 3 bytes. Total record: 2042 bytes.
    /// </summary>
    public static OddsParseResult? ParseO2(byte[] rawRecord)
        => ParseGeneric(rawRecord, comboBytes: 2 * 2, slots: 153, stride: 13, oddsBytes: 6, rankBytes: 3,
                        expectedTypeLabel: "O2");

    /// <summary>
    /// Parse an O5 (Trio) record. 816 slots × 15 bytes starting at offset 41
    /// (1-indexed). Combo is "aabbcc" (6 ASCII digits), odds is 6 bytes (÷10),
    /// rank is 3 bytes. Total record: 12293 bytes.
    /// </summary>
    public static OddsParseResult? ParseO5(byte[] rawRecord)
        => ParseGeneric(rawRecord, comboBytes: 3 * 2, slots: 816, stride: 15, oddsBytes: 6, rankBytes: 3,
                        expectedTypeLabel: "O5");

    private static OddsParseResult? ParseGeneric(
        byte[] rawRecord,
        int comboBytes,
        int slots,
        int stride,
        int oddsBytes,
        int rankBytes,
        string expectedTypeLabel)
    {
        var data = rawRecord.AsSpan();
        // Oracle's `slots` value (153 for O2, 816 for O5) is the MAX combination count
        // for an 18-horse race. Real records are dynamically sized to actual race
        // entries — a 16-horse race emits a much shorter O5 (180 slots ≈ 2750 bytes).
        // We just need the 41-byte header; the slot loop breaks when data runs out.
        if (data.Length < 41 + stride)
        {
            throw new InvalidOperationException(
                $"{expectedTypeLabel} record missing header: {data.Length} bytes (need ≥{41 + stride})");
        }

        // Race ID: bytes 12-27 (16 chars), 0-indexed start 11.
        var raceId = ExtractAscii(data, 11, 16).Trim();
        if (string.IsNullOrEmpty(raceId) || raceId.Length < 16) return null;

        const int blockStart0Idx = 40;  // 41 (1-indexed) → 40 (0-indexed)
        var horsesPerSlot = comboBytes / 2;
        var result = new List<OddsSlot>(Math.Min(slots, 64));

        for (int i = 0; i < slots; i++)
        {
            var slotStart = blockStart0Idx + (i * stride);
            if (slotStart + stride > data.Length) break;

            var comboStr  = ExtractAscii(data, slotStart, comboBytes).Trim();
            var oddsStr   = ExtractAscii(data, slotStart + comboBytes, oddsBytes).Trim();
            var rankStr   = ExtractAscii(data, slotStart + comboBytes + oddsBytes, rankBytes).Trim();

            // Empty / all-zero combo = unused slot (Quinella has 153 capacity but
            // a typical 16-horse race only uses C(16,2)=120; smaller fields use less).
            if (string.IsNullOrEmpty(comboStr) || comboStr.All(c => c == '0')) continue;

            var combo = new int[horsesPerSlot];
            var validCombo = true;
            for (int h = 0; h < horsesPerSlot; h++)
            {
                if (!int.TryParse(comboStr.AsSpan(h * 2, 2), out combo[h]) || combo[h] <= 0)
                {
                    validCombo = false;
                    break;
                }
            }
            if (!validCombo) continue;

            if (!int.TryParse(oddsStr, out var rawOdds) || rawOdds <= 0) continue;
            int.TryParse(rankStr, out var rank);

            // Special JV-Link sentinel: odds value "999999" (= 99,999.9) means the
            // combination is sold but no odds yet, or is mathematically impossible
            // (e.g. scratched horse). Skip — caller treats missing slot as "no odds".
            if (rawOdds >= 999999) continue;

            var odds = rawOdds / 10m;
            result.Add(new OddsSlot(combo, odds, rank));
        }

        return new OddsParseResult(raceId, result);
    }

    private static string ExtractAscii(ReadOnlySpan<byte> data, int start, int length)
    {
        if (start + length > data.Length)
            throw new ArgumentException($"Record too short: need {start + length} bytes, got {data.Length}");
        return Ascii.GetString(data.Slice(start, length));
    }
}
