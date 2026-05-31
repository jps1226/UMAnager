using System.Text.Json;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Evaluates what a race's placed bets actually staked + won. Mirrors the frontend
/// <c>buildRaceBetLines</c> / <c>evaluateTemplateOutcome</c> — keep both in sync.
///
/// FUTURE-PROOF for dynamic bets: a race's bets are modelled as a list of self-describing
/// <see cref="BetLine"/>s (ticket, method, selections, per-combo stake). <see cref="BuildLines"/>
/// is the ONE place the bet shape is decided — today it derives from the user's mark-count
/// ladder (see OREPRO_CAPABILITIES.md), but a future source (OrePro easy-mode-off custom bets
/// read back via the customize API, or an explicit per-race bet record) can return the same
/// shape and staked/won/labels all flow unchanged. Staked = Σ ComboCount·StakePerCombo, so any
/// ticket type / method / stake is priced consistently with no "mark count" assumption beyond
/// how the ladder happens to build the lines today.
/// </summary>
public static class TemplateBetEvaluator
{
    public const int BetUnitYen = 100; // JRA ticket unit; the ladder's default per-combo stake

    public sealed record MarkedRunner(string Symbol, int? Pp);

    public sealed class BetLine
    {
        public string Ticket { get; init; } = "";        // place / wide / trio / quinella / exacta / trifecta
        public string Method { get; init; } = "normal";  // normal / box / nagashi1 / nagashi2 / formation
        public string Label  { get; init; } = "";         // display (e.g. 3連複)
        public IReadOnlyList<int> Pps { get; init; } = Array.Empty<int>(); // selections (opponents only, for nagashi)
        public int? AxisPp { get; init; }                 // axis post position, for nagashi
        public int ComboCount { get; init; }
        public double StakePerCombo { get; set; }
    }

    public sealed record BetOutcome(int MarkCount, bool HasResults, int Staked, int Won, List<string> HitLabels)
    {
        public bool AnyHit => Won > 0;
    }

    private static int NCk(int n, int k)
    {
        if (k < 0 || k > n) return 0;
        long r = 1;
        for (int i = 0; i < k; i++) r = r * (n - i) / (i + 1);
        return (int)r;
    }

    /// <summary>The bet-plan seam — build a race's bet lines + total staked from the marked runners.</summary>
    public static (List<BetLine> Lines, int Staked) BuildLines(IReadOnlyList<MarkedRunner> runners, int[] ladder)
    {
        var n = runners.Count;
        var lines = new List<BetLine>();
        if (n == 0) return (lines, 0);

        var honmei = runners.FirstOrDefault(r => r.Symbol == "◎") ?? runners[0];
        var allPps = runners.Where(r => r.Pp.HasValue).Select(r => r.Pp!.Value).ToList();

        if (n == 1)
        {
            var pps = honmei.Pp.HasValue ? new[] { honmei.Pp.Value } : Array.Empty<int>();
            lines.Add(new BetLine { Ticket = "place", Method = "normal", Label = "複勝", Pps = pps, ComboCount = 1 });
        }
        else if (n == 2)
        {
            lines.Add(new BetLine { Ticket = "wide", Method = "box", Label = "ワイド", Pps = allPps, ComboCount = NCk(n, 2) });
        }
        else if (n == 3)
        {
            lines.Add(new BetLine { Ticket = "trio", Method = "box", Label = "3連複", Pps = allPps, ComboCount = NCk(n, 3) });
            lines.Add(new BetLine { Ticket = "wide", Method = "box", Label = "ワイド", Pps = allPps, ComboCount = NCk(n, 2) });
        }
        else if (n == 4)
        {
            lines.Add(new BetLine { Ticket = "trio", Method = "box", Label = "3連複", Pps = allPps, ComboCount = NCk(n, 3) });
        }
        else // n >= 5: 3連複 軸1頭ながし — ◎ axis, the rest opponents
        {
            var oppPps = runners.Where(r => !ReferenceEquals(r, honmei) && r.Pp.HasValue).Select(r => r.Pp!.Value).ToList();
            lines.Add(new BetLine { Ticket = "trio", Method = "nagashi1", Label = "3連複ながし", Pps = oppPps, AxisPp = honmei.Pp, ComboCount = NCk(n - 1, 2) });
        }

        var totalCombos = lines.Sum(l => l.ComboCount);
        if (totalCombos == 0) totalCombos = 1;
        var totalStake = SettingsService.StakeForMarkCount(n, ladder);
        var perCombo = (double)totalStake / totalCombos;
        foreach (var l in lines) l.StakePerCombo = perCombo;
        return (lines, totalStake);
    }

    public static BetOutcome Evaluate(
        IReadOnlyList<MarkedRunner> markedRunners,
        int? pp1, int? pp2, int? pp3,
        JsonElement? payouts,
        int[] ladder)
    {
        var n = markedRunners.Count;
        var (lines, staked) = BuildLines(markedRunners, ladder);
        var labels = new List<string>();
        if (n == 0) return new BetOutcome(0, false, 0, 0, labels);
        if (pp1 is null || pp2 is null || pp3 is null || payouts is null)
            return new BetOutcome(n, false, staked, 0, labels);

        var root = payouts.Value;
        var t3 = new[] { pp1.Value, pp2.Value, pp3.Value };
        var t3set = new HashSet<int>(t3);
        int won = 0;
        foreach (var line in lines)
        {
            var w = ScoreLine(line, t3, t3set, root);
            if (w > 0) { won += w; labels.Add(line.Label); }
        }
        return new BetOutcome(n, true, staked, won, labels);
    }

    // Score one line against the result. Extend here for new (ticket, method) combos.
    private static int ScoreLine(BetLine line, int[] t3, HashSet<int> t3set, JsonElement root)
    {
        var f = line.StakePerCombo / 100.0; // payouts are per-¥100
        var pps = line.Pps;
        double won = 0;
        if (line.Ticket == "place")
        {
            if (pps.Count > 0 && t3set.Contains(pps[0])) won += FindPayout(root, "place", new[] { pps[0] }) * f;
        }
        else if (line.Ticket == "wide" && line.Method == "box")
        {
            for (int i = 0; i < pps.Count; i++)
                for (int j = i + 1; j < pps.Count; j++)
                    if (t3set.Contains(pps[i]) && t3set.Contains(pps[j]))
                        won += FindPayout(root, "wide", new[] { pps[i], pps[j] }) * f;
        }
        else if (line.Ticket == "trio" && line.Method == "box")
        {
            var set = new HashSet<int>(pps);
            if (t3.All(set.Contains)) won += FindPayout(root, "trio", t3) * f;
        }
        else if (line.Ticket == "trio" && line.Method == "nagashi1")
        {
            if (line.AxisPp is int ax && t3set.Contains(ax))
            {
                var opp = new HashSet<int>(pps);
                var others = t3.Where(p => p != ax).ToList();
                if (others.Count == 2 && others.All(opp.Contains)) won += FindPayout(root, "trio", t3) * f;
            }
        }
        // Unknown (ticket, method) → 0 for now; add a branch when dynamic bets introduce it.
        return (int)Math.Round(won);
    }

    public static int FindPayout(JsonElement root, string betType, int[] combo)
    {
        if (!root.TryGetProperty(betType, out var arr) || arr.ValueKind != JsonValueKind.Array) return 0;
        var target = combo.OrderBy(x => x).ToArray();
        foreach (var slot in arr.EnumerateArray())
        {
            if (!slot.TryGetProperty("combo", out var slotCombo) || slotCombo.ValueKind != JsonValueKind.Array) continue;
            if (slotCombo.GetArrayLength() != target.Length) continue;
            var slotArr = slotCombo.EnumerateArray().Select(e => e.GetInt32()).OrderBy(x => x).ToArray();
            if (slotArr.SequenceEqual(target))
                return slot.TryGetProperty("payout", out var p) ? p.GetInt32() : 0;
        }
        return 0;
    }

    /// <summary>Build the marked-runner list for one race from the marks dict + a horseId→PP map.</summary>
    public static List<MarkedRunner> BuildRunners(
        string raceId,
        IReadOnlyDictionary<string, string> marks,
        IReadOnlyDictionary<string, int?> ppByHorse)
    {
        var prefix = raceId + "_";
        var list = new List<MarkedRunner>();
        foreach (var kv in marks)
        {
            if (!kv.Key.StartsWith(prefix)) continue;
            var horseId = kv.Key.Substring(prefix.Length);
            ppByHorse.TryGetValue(horseId, out var pp);
            list.Add(new MarkedRunner(kv.Value, pp));
        }
        return list;
    }
}
