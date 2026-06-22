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

# Upset autopsy — of the races we lost to a surprise winner, how many were CATCHABLE (strong on
# stats, market missed it) vs genuine FREAKS (weak on everything)? Reads fixtures/history.json:
node tools/backtest/upset-autopsy.mjs
```

`history.json` is a merged multi-weekend snapshot: fetch each day with `curl .../api/races?date=YYYY-MM-DD`
and merge their `past_races_by_date` into one file (gitignored, like all fixtures).

### Point-in-time stats (`BT_PIT=1`) + the `finish_history.json` export

The historical `/api/races` payload computes a horse's `Record` / `Surface_Win_Pct` / `Dist_Win_Pct` /
`Sire_Fit` / `Jockey_AE` / `Trainer_AE` **as-of-NOW**, so replaying a past race leaks its own outcome.
`BT_PIT=1` makes the harness rebuild every one of those fields **point-in-time** (strictly before each
race) via `point-in-time.mjs` — own-record splits (Step 1) + jockey/trainer A/E and sire-fit (Step 1.5).
**Always run the analysis tools with `BT_PIT=1`.** (The live app is unaffected: an upcoming race has no
future data, so as-of-now == as-of-race-date.)

This needs `fixtures/finish_history.json` — one row per finished start, with the fields the
reconstructions read (date keyed off the **RaceId prefix** to match how the backtest keys race dates).
Regenerate it from the read-only DB:

```sh
PGPASSWORD='<pw>' psql -h localhost -U postgres -d umanager -t -A -o tools/backtest/fixtures/finish_history.json -c "
SELECT json_agg(json_build_object(
  'h',   re.\"HorseId\",
  'ymd', substring(re.\"RaceId\",1,8),   -- RaceId prefix, NOT RaceDate (they can differ by a day)
  's',   r.\"Surface\", 'd', r.\"Distance\", 'f', re.\"FinishPos\",
  'fav', re.\"FavRank\",                  -- for the A/E P(win|favRank) baseline
  'j',   re.\"JockeyCode\", 't', re.\"TrainerCode\", 'sire', h.\"SireId\"))
FROM race_entries re
JOIN races  r ON r.\"RaceId\"  = re.\"RaceId\"
LEFT JOIN horses h ON h.\"HorseId\" = re.\"HorseId\"   -- LEFT: keep finishes for horses we lack master rows for
WHERE re.\"FinishPos\" IS NOT NULL AND re.\"FinishPos\" > 0;"
```

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
