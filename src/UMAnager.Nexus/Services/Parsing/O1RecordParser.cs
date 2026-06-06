// ============================================================
// FILE: O1RecordParser.cs
// LAYER: Parsing (static, zero-alloc span parser)
// PURPOSE: O1 (real-time win/place/bracket odds) bytes → (raceId, [HorseOdds]) — umaban, win
//          odds (raw÷10), fav rank. Consumed by OddsApplyService.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
namespace UMAnager.Nexus.Services.Parsing;

public static class O1RecordParser
{
    // O1 (JV_O1_ODDS_TANFUKUWAKU) byte layout — all offsets 0-indexed for Span.Slice.
    // Source: Oracle Q1 answer + jra_van_offsets.md cross-reference.
    //
    // [0-1]   Record type "O1"
    // [2]     DataStatus
    // [3-10]  Creation date (YYYYMMDD)
    // [11-26] RaceId (16 bytes, YYYYMMDDPPNNNNNN)
    // [27-34] HappyoTime (announcement date+time, 8 bytes)
    // [35-36] TorokuTosu (registered horse count, 2 bytes)
    // [37-38] ShutsumaTousu (entered horse count, 2 bytes)
    // [39]    HatsubaiFlagTansho (win betting open flag)
    // [40]    HatsubaiFlagFukusho (place betting open flag)
    // [41]    HatsubaiFlagWakuren (bracket quinella flag)
    // [42]    FukushoAtaraibaraKey
    // [43+]   OddsTansyoInfo array — 28 slots × 8 bytes
    //
    // Per slot (0-indexed within slot):
    //   [0-1] Umaban (horse number / 馬番)
    //   [2-5] Win odds raw (÷10 = actual)
    //   [6-7] Ninki (favorite rank / 単勝人気順)

    private const int SlotArrayStart = 43;
    private const int SlotStride     = 8;
    private const int SlotCount      = 28;

    public record HorseOdds(int HorseNumber, decimal WinOdds, int FavRank);

    public static (string RaceId, List<HorseOdds> Slots) Parse(ReadOnlySpan<byte> data)
    {
        var raceId = ExtractAscii(data, 11, 16).Trim();
        var slots  = new List<HorseOdds>(18);

        for (int i = 0; i < SlotCount; i++)
        {
            int slotBase = SlotArrayStart + (i * SlotStride);
            if (slotBase + SlotStride > data.Length) break;

            var umabanStr = ExtractAscii(data, slotBase,     2).Trim();
            var oddsStr   = ExtractAscii(data, slotBase + 2, 4).Trim();
            var ninkiStr  = ExtractAscii(data, slotBase + 6, 2).Trim();

            if (!int.TryParse(umabanStr, out int umaban) || umaban <= 0) continue;

            decimal winOdds = 0;
            if (int.TryParse(oddsStr, out int rawOdds) && rawOdds > 0)
                winOdds = rawOdds / 10m;

            int.TryParse(ninkiStr, out int ninki);

            slots.Add(new HorseOdds(umaban, winOdds, ninki));
        }

        return (raceId, slots);
    }

    private static string ExtractAscii(ReadOnlySpan<byte> data, int start, int length)
        => System.Text.Encoding.ASCII.GetString(data.Slice(start, length));
}
