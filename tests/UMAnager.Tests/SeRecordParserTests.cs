using System.Text;
using System.Text.Json;
using UMAnager.Nexus.Services.Parsing;
using Xunit;

namespace UMAnager.Tests;

public sealed class SeRecordParserTests
{
    static SeRecordParserTests()
        => Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

    [Fact]
    public void Settled_record_captures_each_available_corner_and_final_3f()
    {
        var raw = Enumerable.Repeat((byte)' ', 555).ToArray();
        Put(raw, 1, "SE");
        Put(raw, 3, "6");
        Put(raw, 4, "20261027");
        Put(raw, 12, "2026102705050101");
        Put(raw, 29, "07");
        Put(raw, 31, "2020100001");
        Put(raw, 335, "02");
        Put(raw, 352, "03");
        Put(raw, 354, "02");
        Put(raw, 356, "01");
        Put(raw, 358, "01");
        Put(raw, 391, "355");

        var entry = SeRecordParser.Parse(raw);

        Assert.Equal(2, entry.FinishPos);
        using var performance = JsonDocument.Parse(entry.PerformanceJson!);
        Assert.Equal(new[] { 3, 2, 1, 1 }, performance.RootElement.GetProperty("corners")
            .EnumerateArray().Select(x => x.GetInt32()).ToArray());
        Assert.Equal(355, performance.RootElement.GetProperty("l3f").GetInt32());
    }

    [Fact]
    public void Pre_race_record_does_not_create_performance_payload()
    {
        var raw = Enumerable.Repeat((byte)' ', 555).ToArray();
        Put(raw, 1, "SE");
        Put(raw, 3, "2");
        Put(raw, 4, "20261027");
        Put(raw, 12, "2026102705050101");
        Put(raw, 29, "07");
        Put(raw, 31, "2020100001");

        var entry = SeRecordParser.Parse(raw);

        Assert.Null(entry.PerformanceJson);
    }

    private static void Put(byte[] raw, int oneBasedOffset, string value)
        => Encoding.ASCII.GetBytes(value).CopyTo(raw, oneBasedOffset - 1);
}
