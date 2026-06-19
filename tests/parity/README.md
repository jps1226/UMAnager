# Bet-math golden-vector parity (tech-debt T2-1)

The bet **pricing** and **scoring** math is implemented twice — in C#
(`TemplateBetEvaluator`) and in JS (`script.js`: `buildLinesFromComposition`, `scoreBetLine`).
If they drift, the dashboard/TV show one number while the Discord recap and OrePro reconciliation
show another. These tests freeze a set of **golden vectors** both sides must reproduce, so any drift
fails a test instead of going silent.

## What it guards

| Section in `bet_vectors.json` | Asserted by | What it locks |
|---|---|---|
| **`scoring`** (shared) | **C# `EvaluateLines` AND JS `scoreBetLine`** against the **same** expected | The two languages score a frozen bet-line list **identically** — the path EVERY applied bet takes (the freeze-at-apply lines C# scores verbatim). This is the real T2-1 guard. |
| **`jsBuilding`** | JS `buildLinesFromComposition` | The JS-only composition→lines builder, incl. the **uniform-¥10k redistribution** (e.g. a 2-mark balanced card collapsing the trio line into win+quinella) that shipped JS-only. |
| **`csBuilding`** | C# `BuildLines` | The C# structure→lines builder (default 50/30/20, the n=2 default, single-structure presets). |

> The two **builders take different inputs by design** (JS = composition + per-line ¥; C# = a
> structure name + even spread), so they are **not** compared to each other — each locks its own
> output. The cross-language guarantee lives entirely in the shared **`scoring`** section.

## Run it

```powershell
# both sides (recommended):
pwsh tests/parity/run-parity.ps1

# or individually:
dotnet test tests/UMAnager.Tests/UMAnager.Tests.csproj
node tests/parity/run-js-vectors.mjs
```

Safe to run anytime: the C# side builds **Debug** (separate output from the live **Release**
binary), and the JS side loads `script.js` in a sandboxed `vm` — neither touches the running app.

## When a test fails

A failure means a bet-math change moved a number away from the frozen contract. Either:
1. **It was a bug** → fix the code so it matches the vector again; or
2. **It was an intentional change** → update the expected value in `bet_vectors.json` (and make the
   matching change on the OTHER side if it's a `scoring` vector, so they stay in lockstep).

The expected values were generated from the live code and frozen as the contract. The JS runner
loads the **real** `script.js` (not a copy), so it always tests the shipping code.
