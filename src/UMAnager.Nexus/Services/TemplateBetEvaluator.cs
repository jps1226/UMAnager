// ============================================================
// FILE: TemplateBetEvaluator.cs
// LAYER: Service (static) — the bet-pricing seam
// PURPOSE: Builds a race's bet lines (ticket/method/selections/per-combo stake) from the marked
//          runners + bet structure choice, and scores them against the top-3 + payout JSON. The ONE
//          place bet shape is decided (BuildLines); Evaluate returns staked/won/hit-labels.
// KEY DEPENDENCIES: none (standalone static).
// CAUTION: MIRROR of the frontend buildRaceBetLines / evaluateTemplateOutcome — keep both in sync.
//          Used by BetWinNotifier, DayRecapNotifier, and SunkCostService.
// BET STRUCTURES: "default" (単勝+馬連BOX+3連複BOX 50/30/20), "trio_box", "trio_nagashi",
//                 "quinella_box", "wide_box", "win_only". See BetStructures for the catalog.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Text.Json;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Evaluates what a race's placed bets actually staked + won. Mirrors the frontend
/// <c>buildRaceBetLines</c> / <c>evaluateTemplateOutcome</c> — keep both in sync.
///
/// FUTURE-PROOF for dynamic bets: a race's bets are modelled as a list of self-describing
/// <see cref="BetLine"/>s (ticket, method, selections, per-combo stake). <see cref="BuildLines"/>
/// is the ONE place the bet shape is decided. A future source (OrePro easy-mode-off custom bets
/// read back via the customize API) can return the same shape and staked/won/labels all flow
/// unchanged. Staked = Σ ComboCount·StakePerCombo consistently across all structures.
/// </summary>
public static class TemplateBetEvaluator
{
    public const int DefaultStakeYen = 10_000;

    /// <summary>
    /// Valid bet structure identifiers. The frontend and server must agree on these strings.
    /// </summary>
    public static class BetStructures
    {
        public const string Default      = "default";       // 単勝+馬連BOX+3連複BOX 50/30/20
        public const string TrioBox      = "trio_box";      // 3連複BOX only
        public const string TrioNagashi  = "trio_nagashi";  // 3連複ながし — ◎ banker
        public const string QuinellaBox  = "quinella_box";  // 馬連BOX only
        public const string WideBox      = "wide_box";      // ワイドBOX only
        public const string WinOnly      = "win_only";      // 単勝 on ◎ only
    }

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
        // "spine" = the Discipline / main bet (drives the honest recovery number); "side" = an explicit,
        // additive loyalty bet that must NOT pollute the spine recovery %. Absent in any stored bet to
        // date, so it defaults to "spine" and every historical figure is unchanged.
        public string Kind { get; init; } = SpineKind;
    }

    public const string SpineKind = "spine";
    public const string SideKind  = "side";

    /// <summary>The result of scoring a race's bets. Staked/Won are the SPINE (Discipline) bucket — the
    /// numbers that feed the honest recovery %. SideStaked/SideWon are the loyalty side bets, tracked
    /// distinctly so they never move the spine recovery. HitLabels are the spine ticket labels that hit.</summary>
    public sealed record BetOutcome(
        int MarkCount, bool HasResults, int Staked, int Won, List<string> HitLabels,
        int SideStaked = 0, int SideWon = 0)
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

    // Round to the nearest ¥100, floored at ¥100. Uses round-half-UP (Math.Floor(x+0.5)) to
    // match JavaScript's Math.round exactly — the JS buildRaceBetLines mirror must agree to the yen.
    private static int Round100(double v) => (int)Math.Max(100, Math.Floor(v / 100 + 0.5) * 100);

    /// <summary>
    /// The bet-plan seam — build a race's bet lines + total staked from the marked runners
    /// and the chosen bet structure. betStructure defaults to "default" (OrePro 50/30/20 template).
    /// oreProStake is the total ¥ for the race (default ¥10,000).
    /// </summary>
    public static (List<BetLine> Lines, int Staked) BuildLines(
        IReadOnlyList<MarkedRunner> runners,
        string betStructure = BetStructures.Default,
        int oreProStake = DefaultStakeYen)
    {
        var n = runners.Count;
        var lines = new List<BetLine>();
        if (n == 0) return (lines, 0);

        var honmei = runners.FirstOrDefault(r => r.Symbol == "◎") ?? runners[0];
        var allPps = runners.Where(r => r.Pp.HasValue).Select(r => r.Pp!.Value).ToList();
        var stake  = oreProStake > 0 ? oreProStake : DefaultStakeYen;

        if (string.IsNullOrEmpty(betStructure) || betStructure == BetStructures.Default)
        {
            // OrePro default: Win(◎) 50% + Quinella BOX 30% + Trio BOX 20%.
            // n=2: Win 50% + Quinella 50% (trio not possible with 2 horses).
            var hasTrio = n >= 3;
            if (honmei.Pp.HasValue)
                lines.Add(new BetLine { Ticket = "win", Method = "normal", Label = "単勝", Pps = new[] { honmei.Pp.Value }, ComboCount = 1, StakePerCombo = 0 }); // filled below
            if (n >= 2)
            {
                var c = NCk(n, 2);
                var per = c > 0 ? Round100(stake * (hasTrio ? 0.3 : 0.5) / c) : 0;
                lines.Add(new BetLine { Ticket = "quinella", Method = "box", Label = "馬連", Pps = allPps, ComboCount = c, StakePerCombo = per });
            }
            if (n >= 3)
            {
                var c = NCk(n, 3);
                var per = c > 0 ? Round100(stake * 0.2 / c) : 0;
                lines.Add(new BetLine { Ticket = "trio", Method = "box", Label = "3連複", Pps = allPps, ComboCount = c, StakePerCombo = per });
            }
            // Win takes the remainder so total = exactly stake.
            var winLine = lines.FirstOrDefault(l => l.Ticket == "win");
            if (winLine is not null)
                winLine.StakePerCombo = stake - lines.Where(l => l.Ticket != "win").Sum(l => l.StakePerCombo * l.ComboCount);
            return (lines, (int)Math.Round(lines.Sum(l => l.StakePerCombo * l.ComboCount)));
        }

        // Single-ticket structures: spread stake evenly across all combos.
        switch (betStructure)
        {
            case BetStructures.TrioBox when n >= 3:
                lines.Add(new BetLine { Ticket = "trio", Method = "box", Label = "3連複", Pps = allPps, ComboCount = NCk(n, 3) });
                break;
            case BetStructures.TrioNagashi when n >= 3:
            {
                var oppPps = runners.Where(r => !ReferenceEquals(r, honmei) && r.Pp.HasValue).Select(r => r.Pp!.Value).ToList();
                lines.Add(new BetLine { Ticket = "trio", Method = "nagashi1", Label = "3連複ながし", Pps = oppPps, AxisPp = honmei.Pp, ComboCount = NCk(n - 1, 2) });
                break;
            }
            case BetStructures.QuinellaBox when n >= 2:
                lines.Add(new BetLine { Ticket = "quinella", Method = "box", Label = "馬連", Pps = allPps, ComboCount = NCk(n, 2) });
                break;
            case BetStructures.WideBox when n >= 2:
                lines.Add(new BetLine { Ticket = "wide", Method = "box", Label = "ワイド", Pps = allPps, ComboCount = NCk(n, 2) });
                break;
            case BetStructures.WinOnly:
                if (honmei.Pp.HasValue)
                    lines.Add(new BetLine { Ticket = "win", Method = "normal", Label = "単勝", Pps = new[] { honmei.Pp.Value }, ComboCount = 1 });
                break;
        }

        if (lines.Count == 0) return (lines, 0);
        var totalCombos = lines.Sum(l => l.ComboCount);
        if (totalCombos == 0) totalCombos = 1;
        var perCombo = (double)stake / totalCombos;
        foreach (var l in lines) l.StakePerCombo = perCombo;
        return (lines, stake);
    }

    public static BetOutcome Evaluate(
        IReadOnlyList<MarkedRunner> markedRunners,
        int? pp1, int? pp2, int? pp3,
        JsonElement? payouts,
        string betStructure = BetStructures.Default,
        int oreProStake = DefaultStakeYen)
    {
        var n = markedRunners.Count;
        var (lines, staked) = BuildLines(markedRunners, betStructure, oreProStake);
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

    /// <summary>
    /// Score an EXPLICIT, already-built line list (the frozen betLines a race recorded at apply).
    /// This is the authoritative path for applied bets — the frontend froze these exact lines, so
    /// scoring them here keeps the server and UI identical no matter how custom the composition was.
    /// </summary>
    public static BetOutcome EvaluateLines(
        IReadOnlyList<BetLine> lines, int markCount,
        int? pp1, int? pp2, int? pp3, JsonElement? payouts)
    {
        // Partition into the spine (Discipline) bet and any additive side bets, so the side stake/return
        // is tallied separately and never moves the spine recovery number.
        var spineLines = lines.Where(l => l.Kind != SideKind).ToList();
        var sideLines  = lines.Where(l => l.Kind == SideKind).ToList();
        var staked     = (int)Math.Round(spineLines.Sum(l => l.StakePerCombo * l.ComboCount));
        var sideStaked = (int)Math.Round(sideLines.Sum(l => l.StakePerCombo * l.ComboCount));
        var labels = new List<string>();
        if (lines.Count == 0) return new BetOutcome(markCount, false, 0, 0, labels);
        if (pp1 is null || pp2 is null || pp3 is null || payouts is null)
            return new BetOutcome(markCount, false, staked, 0, labels, sideStaked, 0);

        var root = payouts.Value;
        var t3 = new[] { pp1.Value, pp2.Value, pp3.Value };
        var t3set = new HashSet<int>(t3);
        int won = 0, sideWon = 0;
        foreach (var line in spineLines)
        {
            var w = ScoreLine(line, t3, t3set, root);
            if (w > 0) { won += w; labels.Add(line.Label); }
        }
        foreach (var line in sideLines)
        {
            var w = ScoreLine(line, t3, t3set, root);
            if (w > 0) sideWon += w;
        }
        return new BetOutcome(markCount, true, staked, won, labels, sideStaked, sideWon);
    }

    /// <summary>
    /// Parse a frozen betLines JSON array (raceMeta.betProfile.betLines) into BetLines. Each
    /// element: { ticket, method, label, horses:[{pp}], axisPp?, comboCount, stakePerCombo }.
    /// Mirrors the frontend's normalizeBetProfile line shape.
    /// </summary>
    public static List<BetLine> ParseFrozenLines(JsonElement arr)
    {
        var list = new List<BetLine>();
        if (arr.ValueKind != JsonValueKind.Array) return list;
        foreach (var l in arr.EnumerateArray())
        {
            if (l.ValueKind != JsonValueKind.Object) continue;
            var ticket = l.TryGetProperty("ticket", out var tk) && tk.ValueKind == JsonValueKind.String ? tk.GetString() ?? "" : "";
            if (string.IsNullOrEmpty(ticket)) continue;
            var method = l.TryGetProperty("method", out var m) && m.ValueKind == JsonValueKind.String ? m.GetString() ?? "normal" : "normal";
            var label  = l.TryGetProperty("label",  out var lb) && lb.ValueKind == JsonValueKind.String ? lb.GetString() ?? "" : "";
            var combo  = l.TryGetProperty("comboCount", out var cc) && cc.TryGetInt32(out var cv) ? cv : 0;
            var per    = l.TryGetProperty("stakePerCombo", out var sp) && sp.ValueKind == JsonValueKind.Number ? sp.GetDouble() : 0;
            var pps = new List<int>();
            if (l.TryGetProperty("horses", out var hs) && hs.ValueKind == JsonValueKind.Array)
                foreach (var h in hs.EnumerateArray())
                    if (h.ValueKind == JsonValueKind.Object && h.TryGetProperty("pp", out var pp) && pp.TryGetInt32(out var ppv) && ppv > 0)
                        pps.Add(ppv);
            int? axis = l.TryGetProperty("axisPp", out var ax) && ax.TryGetInt32(out var axv) && axv > 0 ? axv : null;
            var kind = l.TryGetProperty("kind", out var k) && k.ValueKind == JsonValueKind.String && k.GetString() == SideKind ? SideKind : SpineKind;
            list.Add(new BetLine { Ticket = ticket, Method = method, Label = label, Pps = pps, AxisPp = axis, ComboCount = combo, StakePerCombo = per, Kind = kind });
        }
        return list;
    }

    // Score one line against the result. Extend here for new (ticket, method) combos.
    private static int ScoreLine(BetLine line, int[] t3, HashSet<int> t3set, JsonElement root)
    {
        var f = line.StakePerCombo / 100.0; // payouts are per-¥100
        var pps = line.Pps;
        double won = 0;
        if (line.Ticket == "win")
        {
            if (pps.Count > 0 && t3.Length > 0 && t3[0] == pps[0]) won += FindPayout(root, "win", new[] { pps[0] }) * f;
        }
        else if (line.Ticket == "quinella" && line.Method == "box")
        {
            var top2 = new HashSet<int> { t3[0], t3[1] };
            for (int i = 0; i < pps.Count; i++)
                for (int j = i + 1; j < pps.Count; j++)
                    if (top2.Contains(pps[i]) && top2.Contains(pps[j]))
                        won += FindPayout(root, "quinella", new[] { pps[i], pps[j] }) * f;
        }
        else if (line.Ticket == "place")
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
