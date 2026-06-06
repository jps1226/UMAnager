// ============================================================
// FILE: HrRecordParser.cs
// LAYER: Parsing (static, zero-alloc span parser)
// PURPOSE: HR (払戻 / payout) bytes → races.ResultsJson with all 8 bet types (win/place/bracketQ/
//          quinella/wide/exacta/trio/trifecta). Payouts are per-¥100; empty/zero slots skipped.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Text.Json;

namespace UMAnager.Nexus.Services.Parsing;

/// <summary>
/// Parser for JV-Link HR (Haray / 払戻) race-payout records.
/// Byte offsets per Oracle Q18 (2026-05-17). All offsets stored 1-indexed
/// to match the spec; converted to 0-indexed via Slice() calls.
///
/// Output is JSON serialized into Race.ResultsJson with this shape:
/// {
///   "win":      [{ "combo": [n], "payout": 190,  "rank": 1 }, ...],
///   "place":    [{ "combo": [n], "payout": 110              }, ...],
///   "bracketQ": [{ "combo": [a,b], "payout": 350, "rank": 2 }, ...],
///   "quinella": [{ "combo": [a,b], "payout": 420, "rank": 2 }, ...],
///   "wide":     [{ "combo": [a,b], "payout": 180             }, ...],
///   "exacta":   [{ "combo": [a,b], "payout": 850, "rank": 4 }, ...],
///   "trio":     [{ "combo": [a,b,c], "payout": 1430, "rank": 5 }, ...],
///   "trifecta": [{ "combo": [a,b,c], "payout": 8470, "rank":12 }, ...]
/// }
/// Payouts are per-¥100 ticket (JRA standard). Empty slots (all zeros) are skipped.
/// </summary>
public static class HrRecordParser
{
    private static readonly System.Text.Encoding Ascii = System.Text.Encoding.ASCII;

    public sealed record HrParseResult(string RaceId, string ResultsJson);

    public static HrParseResult? Parse(byte[] rawRecord)
    {
        var data = rawRecord.AsSpan();
        if (data.Length < 600)
        {
            throw new InvalidOperationException($"HR record too short: {data.Length} bytes (expected ~719)");
        }

        // Race ID: bytes 12-27 (16 chars, YYYYMMDDPPKKNNRR). 0-indexed start = 11.
        var raceId = ExtractAscii(data, 11, 16).Trim();
        if (string.IsNullOrEmpty(raceId) || raceId.Length < 16)
        {
            return null;
        }

        var payload = new
        {
            win      = ParseSlots(data, 102, slots: 3, comboBytes: 2, payoutBytes: 9, rankBytes: 2, stride: 13),
            place    = ParseSlots(data, 141, slots: 5, comboBytes: 2, payoutBytes: 9, rankBytes: 2, stride: 13),
            bracketQ = ParseSlots(data, 206, slots: 3, comboBytes: 2, payoutBytes: 9, rankBytes: 2, stride: 13),
            quinella = ParseSlots(data, 245, slots: 3, comboBytes: 4, payoutBytes: 9, rankBytes: 3, stride: 16),
            wide     = ParseSlots(data, 293, slots: 7, comboBytes: 4, payoutBytes: 9, rankBytes: 3, stride: 16),
            exacta   = ParseSlots(data, 453, slots: 6, comboBytes: 4, payoutBytes: 9, rankBytes: 3, stride: 16),
            trio     = ParseSlots(data, 549, slots: 3, comboBytes: 6, payoutBytes: 9, rankBytes: 3, stride: 18),
            trifecta = ParseSlots(data, 603, slots: 6, comboBytes: 6, payoutBytes: 9, rankBytes: 4, stride: 19),
        };

        return new HrParseResult(raceId, JsonSerializer.Serialize(payload));
    }

    /// <summary>
    /// Parse a repeating slot block. Each slot = combination + payout amount + popularity rank.
    /// Combinations are 2/4/6 ASCII digits encoding 1/2/3 horse (or bracket) numbers.
    /// Slots with all-zero combinations are skipped (unused / void).
    /// </summary>
    private static List<object> ParseSlots(
        ReadOnlySpan<byte> data,
        int blockStartZeroIdx,
        int slots,
        int comboBytes,
        int payoutBytes,
        int rankBytes,
        int stride)
    {
        var horsesPerSlot = comboBytes / 2;
        var result = new List<object>(slots);

        for (int i = 0; i < slots; i++)
        {
            var slotStart = blockStartZeroIdx + i * stride;
            if (slotStart + stride > data.Length) break;

            var comboStr = ExtractAscii(data, slotStart, comboBytes).Trim();
            var payoutStr = ExtractAscii(data, slotStart + comboBytes, payoutBytes).Trim();
            var rankStr = ExtractAscii(data, slotStart + comboBytes + payoutBytes, rankBytes).Trim();

            // Combination of all zeros (e.g. "00", "0000", "000000") = unused slot.
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

            if (!int.TryParse(payoutStr, out var payout)) payout = 0;
            int.TryParse(rankStr, out var rank);

            // Skip rows with zero payout (the slot was published but no payout was made,
            // e.g. all horses scratched or non-finishers).
            if (payout <= 0) continue;

            result.Add(new { combo, payout, rank });
        }

        return result;
    }

    private static string ExtractAscii(ReadOnlySpan<byte> data, int startIndex, int length)
    {
        if (startIndex + length > data.Length)
            throw new ArgumentException($"HR record too short: need {startIndex + length} bytes, got {data.Length}");
        return Ascii.GetString(data.Slice(startIndex, length));
    }
}
