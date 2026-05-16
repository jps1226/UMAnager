using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Services.Parsing;

public static class UmRecordParser
{
    private static readonly System.Text.Encoding ShiftJis = System.Text.Encoding.GetEncoding(932);

    /// <summary>
    /// Parse a UM (horse master) record from raw bytes.
    /// All byte offsets are 1-indexed per JRA-VAN standard; converted to 0-indexed for Span.Slice().
    /// </summary>
    public static Horse Parse(byte[] rawRecord)
    {
        try
        {
            var data = rawRecord.AsSpan();

            // HorseId: 12-21 (10 bytes)
            var horseId = ExtractString(data, 11, 10).Trim();

            // NameJa: 47-82 (36 bytes, Shift-JIS)
            var nameJa = ExtractString(data, 46, 36).Trim();

            // NameEn: 119-178 (60 bytes). JRA-VAN appends a country suffix like "(JPN)" / "(USA)".
            // Strip the trailing parenthesized country code — the column should be the pure name.
            var nameEnRaw = ExtractString(data, 118, 60).Trim();
            var nameEn = StripCountrySuffix(nameEnRaw);

            // BirthYear: 39-42 (4 bytes, YYYY)
            var birthYearStr = ExtractString(data, 38, 4).Trim();
            int? birthYear = null;
            if (!string.IsNullOrEmpty(birthYearStr) && int.TryParse(birthYearStr, out var year))
            {
                birthYear = year;
            }

            // SireId: 205-214 (10 bytes)
            var sireId = ExtractString(data, 204, 10).Trim();
            if (string.IsNullOrEmpty(sireId) || sireId == "0000000000")
                sireId = null;

            // DamId: 251-260 (10 bytes)
            var damId = ExtractString(data, 250, 10).Trim();
            if (string.IsNullOrEmpty(damId) || damId == "0000000000")
                damId = null;

            // BmsId: 389-398 (10 bytes)
            var bmsId = ExtractString(data, 388, 10).Trim();
            if (string.IsNullOrEmpty(bmsId) || bmsId == "0000000000")
                bmsId = null;

            return new Horse
            {
                HorseId = horseId,
                NameJa = nameJa,
                NameEn = string.IsNullOrEmpty(nameEn) ? null : nameEn,
                BirthYear = birthYear,
                SireId = sireId,
                DamId = damId,
                BmsId = bmsId,
                LastUpdated = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to parse UM record: {ex.Message}", ex);
        }
    }

    // Strip a trailing country-code parenthetical like "(JPN)" / "(USA)" / "(GB)" from a romanized horse name.
    private static string StripCountrySuffix(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        int open = name.LastIndexOf('(');
        if (open <= 0) return name;
        int close = name.LastIndexOf(')');
        if (close != name.Length - 1) return name;
        return name.Substring(0, open).TrimEnd();
    }

    private static string ExtractString(ReadOnlySpan<byte> data, int startIndex, int length)
    {
        if (startIndex + length > data.Length)
            throw new ArgumentException($"Record too short: need {startIndex + length} bytes, got {data.Length}");

        var slice = data.Slice(startIndex, length);
        return ShiftJis.GetString(slice);
    }
}
