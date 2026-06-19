// Golden-vector parity tests — C# side (tech-debt T2-1).
//
// Asserts TemplateBetEvaluator against the SHARED bet_vectors.json. The `scoring` cases use the
// exact same expected values the JS runner (tests/parity/run-js-vectors.mjs) asserts — pass on both
// = C# and JS score frozen bet lines identically (the path every APPLIED bet takes). `csBuilding`
// locks the C# structure→lines builder. See bet_vectors.json "_doc" for the full rationale.

using System.Text.Json;
using UMAnager.Nexus.Services;
using Xunit;

namespace UMAnager.Tests;

public sealed class BetParityTests
{
    private static readonly JsonDocument Vectors = JsonDocument.Parse(
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "bet_vectors.json")));

    private static IEnumerable<object[]> Cases(string section)
    {
        foreach (var c in Vectors.RootElement.GetProperty(section).EnumerateArray())
            yield return new object[] { c.GetProperty("name").GetString()!, c };
    }

    public static IEnumerable<object[]> ScoringCases() => Cases("scoring");
    public static IEnumerable<object[]> CsBuildingCases() => Cases("csBuilding");

    [Theory]
    [MemberData(nameof(ScoringCases))]
    public void Scoring_matches_golden(string name, JsonElement c)
    {
        var lines = TemplateBetEvaluator.ParseFrozenLines(c.GetProperty("lines"));
        var result = c.GetProperty("result");
        var payouts = c.GetProperty("payouts");

        var outcome = TemplateBetEvaluator.EvaluateLines(
            lines, lines.Count,
            result.GetProperty("pp1").GetInt32(),
            result.GetProperty("pp2").GetInt32(),
            result.GetProperty("pp3").GetInt32(),
            payouts);

        var expect = c.GetProperty("expect");
        Assert.Equal(expect.GetProperty("won").GetInt32(), outcome.Won);
        Assert.Equal(ExpectedLabels(expect), Sorted(outcome.HitLabels));
    }

    [Theory]
    [MemberData(nameof(CsBuildingCases))]
    public void CsBuilding_matches_golden(string name, JsonElement c)
    {
        var runners = c.GetProperty("runners").EnumerateArray()
            .Select(r => new TemplateBetEvaluator.MarkedRunner(
                r.GetProperty("symbol").GetString()!,
                r.TryGetProperty("pp", out var pp) ? pp.GetInt32() : null))
            .ToList();

        var (lines, staked) = TemplateBetEvaluator.BuildLines(
            runners,
            c.GetProperty("betStructure").GetString()!,
            c.GetProperty("stake").GetInt32());

        var actualLines = Sorted(lines.Select(l => $"{l.Label}|{(int)Math.Round(l.StakePerCombo)}|{l.ComboCount}"));

        var expect = c.GetProperty("expect");
        Assert.Equal(expect.GetProperty("staked").GetInt32(), staked);
        Assert.Equal(ExpectedLines(expect), actualLines);
    }

    private static List<string> Sorted(IEnumerable<string> xs) => xs.OrderBy(x => x, StringComparer.Ordinal).ToList();
    private static List<string> ExpectedLabels(JsonElement expect) =>
        Sorted(expect.GetProperty("labels").EnumerateArray().Select(e => e.GetString()!));
    private static List<string> ExpectedLines(JsonElement expect) =>
        Sorted(expect.GetProperty("lines").EnumerateArray().Select(e => e.GetString()!));
}
