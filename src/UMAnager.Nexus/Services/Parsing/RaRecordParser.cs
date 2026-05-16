using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Services.Parsing;

public static class RaRecordParser
{
    private static readonly System.Text.Encoding ShiftJis = System.Text.Encoding.GetEncoding(932);

    /// <summary>
    /// Parse a RA (race info) record from raw bytes.
    /// All byte offsets are 1-indexed per JRA-VAN standard; converted to 0-indexed for Span.Slice().
    /// </summary>
    public static Race Parse(byte[] rawRecord)
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

            // RaceDate: 12-19 (8 bytes, YYYYMMDD)
            var raceDateStr = ExtractString(data, 11, 8).Trim();
            var raceDate = ParseRaceDate(raceDateStr);

            // TrackCode: 20-21 (2 bytes)
            var trackCode = ExtractString(data, 19, 2).Trim();
            if (string.IsNullOrEmpty(trackCode) || trackCode == "00")
                trackCode = null;

            // RaceNumber: 26-27 (2 bytes)
            var raceNumberStr = ExtractString(data, 25, 2).Trim();
            int? raceNumber = null;
            if (!string.IsNullOrEmpty(raceNumberStr) && int.TryParse(raceNumberStr, out var num))
            {
                raceNumber = num;
            }

            // NameJa: 33-92 (60 bytes, Shift-JIS - Hondai/race name)
            var nameJa = ExtractString(data, 32, 60).Trim();

            // Distance: 698-701 (4 bytes, meters)
            var distanceStr = ExtractString(data, 697, 4).Trim();
            int? distance = null;
            if (!string.IsNullOrEmpty(distanceStr) && int.TryParse(distanceStr, out var d))
            {
                distance = d;
            }

            // Surface: 706-707 (2 bytes, 1=turf, 2=dirt)
            var surfaceStr = ExtractString(data, 705, 2).Trim();
            string? surface = null;
            if (surfaceStr == "1")
                surface = "turf";
            else if (surfaceStr == "2")
                surface = "dirt";

            // Start Time: 874-877 (4 bytes, HHMM format)
            var startTimeStr = ExtractString(data, 873, 4).Trim();
            var sortTime = CombineDateAndTime(raceDate, startTimeStr);

            return new Race
            {
                RaceId = raceId,
                RaceDate = raceDate,
                TrackCode = trackCode,
                RaceNumber = raceNumber,
                NameJa = string.IsNullOrEmpty(nameJa) ? null : nameJa,
                Distance = distance,
                Surface = surface,
                DataStatus = dataStatus,
                LastModified = lastModified,
                SortTime = sortTime,
                LastUpdated = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to parse RA record: {ex.Message}", ex);
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

    private static DateTime ParseRaceDate(string yyyymmddStr)
    {
        if (string.IsNullOrEmpty(yyyymmddStr) || yyyymmddStr.Length < 8)
            return DateTime.UtcNow;

        if (int.TryParse(yyyymmddStr.Substring(0, 4), out var year) &&
            int.TryParse(yyyymmddStr.Substring(4, 2), out var month) &&
            int.TryParse(yyyymmddStr.Substring(6, 2), out var day))
        {
            try
            {
                return new DateTime(year, month, day, 0, 0, 0, DateTimeKind.Utc);
            }
            catch
            {
                return DateTime.UtcNow;
            }
        }

        return DateTime.UtcNow;
    }

    private static DateTime CombineDateAndTime(DateTime date, string hhmmStr)
    {
        if (string.IsNullOrEmpty(hhmmStr) || hhmmStr.Length < 4)
            return date;

        if (int.TryParse(hhmmStr.Substring(0, 2), out var hour) &&
            int.TryParse(hhmmStr.Substring(2, 2), out var minute))
        {
            if (hour >= 0 && hour < 24 && minute >= 0 && minute < 60)
            {
                try
                {
                    return new DateTime(date.Year, date.Month, date.Day, hour, minute, 0, DateTimeKind.Utc);
                }
                catch { }
            }
        }

        return date;
    }
}
