using UMAnager.Nexus.Services;
using Xunit;

namespace UMAnager.Tests;

public sealed class OreProDailyVerificationTests
{
    [Fact]
    public void Parses_public_jsonp_daily_totals()
    {
        const string body = "({\"status\":\"OK\",\"data\":{\"20260927\":{\"mydata\":{\"bet_num\":\"24\",\"hit_num\":\"8\",\"price\":\"240,000\",\"payback\":\"190,000\",\"diff\":\"-50,000\"}}}})";

        var ok = OreProDailyVerificationService.TryParseDailyTotals(body, "20260927", out var totals, out var error);

        Assert.True(ok, error);
        Assert.Equal(new DailyBetTotals(24, 8, 240000, 190000, -50000), totals);
    }

    [Fact]
    public void Reports_unavailable_when_requested_day_is_missing()
    {
        const string body = "({\"status\":\"OK\",\"data\":{}})";

        var ok = OreProDailyVerificationService.TryParseDailyTotals(body, "20260927", out _, out var error);

        Assert.False(ok);
        Assert.Contains("has not published", error);
    }
}
