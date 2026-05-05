namespace UMAnager.Common;

using System.Text;

public static class JVEncoding
{
    private static Encoding? _cp932;

    private static Encoding Cp932
    {
        get
        {
            if (_cp932 == null)
            {
                System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
                _cp932 = Encoding.GetEncoding(932);
            }
            return _cp932;
        }
    }

    /// <summary>
    /// Decode raw Shift-JIS (CP932) bytes from JV-Link to a readable string.
    /// Trims null bytes and ideographic spaces.
    /// </summary>
    public static string DecodeRecord(byte[] raw)
    {
        if (raw == null || raw.Length == 0)
            return string.Empty;

        var decoded = Cp932.GetString(raw);
        return decoded.TrimEnd('\0', '　');
    }

    /// <summary>
    /// Parse a fixed-width field from a JV-Link record line.
    /// </summary>
    public static string ExtractField(string record, int startIndex, int length)
    {
        if (string.IsNullOrEmpty(record) || startIndex >= record.Length)
            return string.Empty;

        var end = Math.Min(startIndex + length, record.Length);
        return record[startIndex..end].Trim();
    }

    /// <summary>
    /// Convert a 14-character YYYYMMDDhhmmss timestamp string to DateTime.
    /// </summary>
    public static DateTime ParseJVTimestamp(string timestamp)
    {
        if (string.IsNullOrEmpty(timestamp) || timestamp.Length < 14)
            return DateTime.MinValue;

        var year = int.Parse(timestamp[..4]);
        var month = int.Parse(timestamp[4..6]);
        var day = int.Parse(timestamp[6..8]);
        var hour = int.Parse(timestamp[8..10]);
        var minute = int.Parse(timestamp[10..12]);
        var second = int.Parse(timestamp[12..14]);

        return new DateTime(year, month, day, hour, minute, second, DateTimeKind.Utc);
    }

    /// <summary>
    /// Convert DateTime to JV-Link 14-character YYYYMMDDhhmmss format.
    /// </summary>
    public static string FormatJVTimestamp(DateTime dt)
    {
        return dt.ToString("yyyyMMddHHmmss");
    }
}
