using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Services.Parsing;

public static class SeRecordParser
{
    private static readonly System.Text.Encoding ShiftJis = System.Text.Encoding.GetEncoding(932);

    /// <summary>
    /// Parse a SE (race entry) record from raw bytes.
    /// All byte offsets are 1-indexed per JRA-VAN standard; converted to 0-indexed for Span.Slice().
    /// </summary>
    public static RaceEntry Parse(byte[] rawRecord)
    {
        try
        {
            var data = rawRecord.AsSpan();

            // DataStatus: 3 (1 byte, データ区分)
            var dataStatusStr = ExtractString(data, 2, 1).Trim();
            short.TryParse(dataStatusStr, out var dataStatus);

            // LastModified: 4-11 (8 bytes, YYYYMMDD, データ作成年月日)
            var lastModified = ParseDateOnly(ExtractString(data, 3, 8).Trim());

            // RaceId: 12-27 (16 bytes, YYYYMMDDPPNNNNNN)
            var raceId = ExtractString(data, 11, 16).Trim();

            // HorseId: 31-40 (10 bytes)
            var horseId = ExtractString(data, 30, 10).Trim();

            // PostPosition: 29-30 (2 bytes, gate number 1-18)
            var postPositionStr = ExtractString(data, 28, 2).Trim();
            int? postPosition = null;
            if (!string.IsNullOrEmpty(postPositionStr) && int.TryParse(postPositionStr, out var pos))
            {
                postPosition = pos;
            }

            // Bracket: 28-28 (1 byte, 0-7)
            var bracketStr = ExtractString(data, 27, 1).Trim();
            int? bracket = null;
            if (!string.IsNullOrEmpty(bracketStr) && int.TryParse(bracketStr, out var br))
            {
                bracket = br;
            }

            // BurdenWeight: 289-291 (3 bytes, jockey weight in kg)
            var burdenWeightStr = ExtractString(data, 288, 3).Trim();
            int? burdenWeight = null;
            if (!string.IsNullOrEmpty(burdenWeightStr) && int.TryParse(burdenWeightStr, out var bw))
            {
                burdenWeight = bw;
            }

            // HorseWeight: 325-327 (3 bytes, horse weight in kg)
            var horseWeightStr = ExtractString(data, 324, 3).Trim();
            int? horseWeight = null;
            if (!string.IsNullOrEmpty(horseWeightStr) && int.TryParse(horseWeightStr, out var hw))
            {
                horseWeight = hw;
            }

            // JockeyName: 307-314 (8 bytes, Shift-JIS)
            var jockeyName = ExtractString(data, 306, 8).Trim();

            // Odds: 360-363 (4 bytes, raw odds ÷ 10 = actual). Oracle-confirmed offset.
            var oddsStr = ExtractString(data, 359, 4).Trim();
            decimal? odds = null;
            if (!string.IsNullOrEmpty(oddsStr) && int.TryParse(oddsStr, out var rawOdds) && rawOdds > 0)
            {
                odds = rawOdds / 10m;
            }

            // FavRank: 364-365 (2 bytes, 1=favorite). Oracle-confirmed offset.
            var favRankStr = ExtractString(data, 363, 2).Trim();
            int? favRank = null;
            if (!string.IsNullOrEmpty(favRankStr) && int.TryParse(favRankStr, out var fav))
            {
                favRank = fav;
            }

            // FinishPos: 335-336 (2 bytes, finishing position). JV-Link emits "00" both pre-race
            // and for DNF/disqualified — treat 0 as NULL so the UI shows blank for upcoming races.
            // (We lose DNF distinction; no UI currently surfaces it.)
            var finishPosStr = ExtractString(data, 334, 2).Trim();
            int? finishPos = null;
            if (!string.IsNullOrEmpty(finishPosStr) && int.TryParse(finishPosStr, out var fp) && fp > 0)
            {
                finishPos = fp;
            }

            return new RaceEntry
            {
                RaceId = raceId,
                HorseId = horseId,
                PostPosition = postPosition,
                Bracket = bracket,
                Weight = burdenWeight,
                HorseWeight = horseWeight,
                JockeyName = string.IsNullOrEmpty(jockeyName) ? null : jockeyName,
                Odds = odds,
                FavRank = favRank,
                FinishPos = finishPos,
                DataStatus = dataStatus,
                LastModified = lastModified,
                UpdatedAt = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to parse SE record: {ex.Message}", ex);
        }
    }

    private static string ExtractString(ReadOnlySpan<byte> data, int startIndex, int length)
    {
        if (startIndex + length > data.Length)
            throw new ArgumentException($"Record too short: need {startIndex + length} bytes, got {data.Length}");

        var slice = data.Slice(startIndex, length);
        return ShiftJis.GetString(slice);
    }

    private static DateOnly? ParseDateOnly(string yyyymmddStr)
    {
        if (yyyymmddStr.Length < 8) return null;
        if (int.TryParse(yyyymmddStr.AsSpan(0, 4), out var y) &&
            int.TryParse(yyyymmddStr.AsSpan(4, 2), out var mo) &&
            int.TryParse(yyyymmddStr.AsSpan(6, 2), out var d) &&
            mo >= 1 && mo <= 12 && d >= 1 && d <= 31)
        {
            try { return new DateOnly(y, mo, d); } catch { return null; }
        }
        return null;
    }
}
