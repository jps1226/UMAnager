# Counterfactual backtest (project #2)

Replays a **settled** race weekend through the **real engine** at different settings and
settles the resulting tickets against the **real stored dividends + finishing order** — so we
can ask "what would a different setting have paid?" against history, instead of waiting weeks
for a hypothesis to play out.

It loads the shipping `wwwroot/static/script.js` in a sandbox and calls the same functions the
live app uses (`getEngineMarkPlanForRace` → `buildLinesFromComposition` → `scoreBetLine`), so
the numbers can't drift from what the app would actually have done. **Read-only. Places nothing,
touches no live state.**

## Run

```sh
# 1. Capture a weekend's data from the running Nexus (read-only GET). One-time / refresh as needed.
curl -s -o tools/backtest/fixtures/races_raw.json "http://localhost:5000/api/races"

# 2. Risk sweep — net P&L at every risk level, bet preset held fixed (default: balanced).
node tools/backtest/risk-sweep.mjs 2026-06-20,2026-06-21 balanced

# Per-race detail (marks → tickets → settlement) for the first 3 races, to eyeball correctness:
BT_DEBUG=1 node tools/backtest/risk-sweep.mjs 2026-06-20 balanced

# Which bet TYPE pays — preset recovery at a fixed risk (default 30):
node tools/backtest/preset-sweep.mjs 2026-06-20,2026-06-21 30

# Many weekends at once → per-weekend verdict on the risk + preset hypotheses (reads fixtures/history.json):
node tools/backtest/backfill-report.mjs

# Faithfulness check — replay a day in the live Auto-per-race mode vs the known reconciled actual:
node tools/backtest/validate-actual.mjs 2026-06-21 30
```

`history.json` is a merged multi-weekend snapshot: fetch each day with `curl .../api/races?date=YYYY-MM-DD`
and merge their `past_races_by_date` into one file (gitignored, like all fixtures).

- **args:** `[dates,comma,sep] [presetId]` — preset is held fixed so RISK is the only variable.
  Valid presets: `win_place`, `balanced`, `quinella_wide`, `trio_chase`, `nagashi_chase`, `wide_safe`.
- The fixture (`fixtures/races_raw.json`) carries each race's entries (odds + form), finishing
  order, and `results_json` (the actual win/quinella/wide/trio dividends). It's the only input.

## Reading the output

- **net** is the headline. The **Read** at the bottom uses the **median** of the safe half
  (risk 0–40) vs the bold half (risk 60–100), so one lucky longshot weekend can't flip the verdict.
- A **⚠ variance spike** flag marks any risk level that posted a strong net on a very low hit rate —
  i.e. the money came from rare big longshots, not from picking more winners. Don't read it as an edge.

## Caveats

- Stored odds are the **last poll before each race**, not the exact bet-time tick — fine for a
  retro trend, not penny-perfect.
- Scratched horses (取消/除外) are dropped from the engine's pool (they aren't bettable).
- **Discipline:** one weekend is one data point. Do not change engine math off a single read —
  log the result and only act on a signal that holds ≥3 weekends (see `tuning_hypotheses.md`).
