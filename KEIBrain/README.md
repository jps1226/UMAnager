# KEIBrain 🧠🐎 — machine-learning roadmap (MOCK-UP)

> **Status (s52): Phases 0-2 BUILT + run. Verdict so far: no broad edge; confirms the longshot thesis;
> NOT productionized.** This started as a design mock-up for the next evolution of the cold "value" engine
> (TODO §0 / CLAUDE.md North Star): instead of testing race factors one at a time by hand, train a model
> to weigh many factors *and their combinations* at once — graded by the same brutal honesty as everything
> else (point-in-time, net-of-takeout, walk-forward).
>
> Read this with the session-51/52 findings in mind: **we proved the market is efficient at picking
> winners.** KEIBrain does NOT get a pass on that. Its only job is to find *mispricing*, not winners.
>
> **What's built:** `build_features.mjs` (Phase 0 — 64k point-in-time feature rows), `train_stage1.py`
> (Phase 1 — calibrated odds-blind place model), `money_test.py` (Phase 2 — longshot money test). Result
> logged as **H11** in `tuning_hypotheses.md`: model is well-calibrated and independently rediscovered our
> hand-found factors (layoff/pace), but does NOT beat the market broadly and does NOT clearly beat the
> H7/H8 hand rules — so it stays an offline tool, Phase 4 (feeding the app) remains gated. `pipeline.py` is
> the original annotated skeleton (kept for the Phase 3/4 shape we haven't built).

---

## 1. The goal — say it out loud so we never forget it

**KEIBrain is NOT trying to predict winners. It is trying to beat the closing odds.**

A model trained to predict winners will just re-learn the public odds (the crowd is already a
near-perfect winner-predictor). That model is worthless — you can't profit by agreeing with the market.
The model only earns its keep where it **disagrees with the market price and is right.** Every design
choice below flows from that one sentence.

Realistic bar (unchanged from the North Star): **beat the average bettor / lose less / occasionally find
a genuinely underpriced horse.** NOT guaranteed profit — the ~20-25% track takeout makes reliable profit
a stretch even for pros. Success = "a disciplined, honest model that finds real overlays," not "riches."

---

## 2. Why a model at all (what it adds over the by-hand sweep)

Sessions 51-52 tested factors **one at a time**: layoff, surface switch, closing kick, draw, etc. That
found two leads (H7 fresh longshots, H8 avoid switch-to-dirt) and killed the rest. The ceiling of that
method is low: we can only eyeball a handful of factors and maybe one interaction at a time.

A model's real superpower is **combinations and weighting**: "a fresh longshot, by *this kind of sire*,
under *this jockey*, cutting back in distance, not on dirt, in a big field" — blending dozens of weak
hints into one probability. That is genuinely beyond what we can do by hand, and it's the legitimate
reason to do this. (It's how Bill Benter's Hong Kong model worked: combine many weak factors, then blend
with the public odds.)

---

## 3. Tool choice — and the big "do NOT do the obvious thing"

**DO NOT use a neural network.** For tabular data of this size (~87k rows, dozens of columns), neural
nets overfit, need more data than we have, and are hard to interpret. The right tool is
**gradient-boosted decision trees** (LightGBM or XGBoost): faster, less data-hungry, more honest about
what they learned, and the consistent winner on this exact shape of problem.

- **Language: Python** (this is where the ML ecosystem lives). KEIBrain is a *separate offline analysis
  tool*, like `tools/backtest/` — it never runs inside the live Nexus/Sidecar. It reads the same
  read-only data exports and writes findings; the live app stays untouched until/unless a model earns its
  way in (and even then, likely only as exported probabilities, see Phase 4).
- **No GPU, no cloud needed.** GBMs train in seconds-to-minutes on a laptop CPU for this data size.

---

## 4. Architecture — two stages (the Benter pattern)

```
                    point-in-time features (form/class/fit/situation/pace/connections)
                                          │
                          ┌───────────────▼────────────────┐
            STAGE 1       │  "fundamental" model            │   → P_model(place)  per horse
         (no odds!)       │  GBM, predicts top-3 finish     │     (the model's OWN opinion,
                          └───────────────┬────────────────┘      deliberately blind to the market)
                                          │
                          ┌───────────────▼────────────────┐
            STAGE 2       │  "blend" model                  │   → P_final(place)  per horse
       (adds the odds)    │  inputs: P_model  +  market     │     (how much to trust the model
                          │  implied prob (from the odds)   │      vs the crowd, learned from data)
                          └───────────────┬────────────────┘
                                          │
                          ┌───────────────▼────────────────┐
            BET RULE       │  bet to PLACE only where        │   → the overlay picks
                          │  P_final  >  market implied  +  │
                          │  a margin (the "edge" threshold)│
                          └─────────────────────────────────┘
```

**Why two stages?** Stage 1 is kept *blind to the odds on purpose* so it forms an independent opinion.
Stage 2 then learns, from history, exactly how much that independent opinion should override the market —
which is the whole game. If we let odds into Stage 1, the model just parrots them and Stage 2 has nothing
to correct. (This separation is also our main defense against "it just re-learned the odds.")

We target **place (top-3)**, not win, because every honest finding so far says place bets recover better
and our edges (H7/H8) live there. A win model can come later.

---

## 5. Features (all point-in-time — reuse what we already built)

Everything below is already computed honestly in `tools/backtest/point-in-time.mjs` + the enriched
`finish_history.json`. KEIBrain's first job is just to export these per-run into a training table. **No
feature may use information from on/after the race date** — same rule that caught the s50 leak.

| Group | Features | Source today |
|---|---|---|
| **Market** (Stage 2 only) | final odds, favourite rank, odds drift* | bench payload |
| **Form** | form score, last-3 finishes & field sizes, career W/S | `point-in-time.mjs` |
| **Fit** | surface win%, distance-bucket win%, sire surface×distance fit | `statsAsOf`, `sireFitAsOf` |
| **Situation** | layoff days (H7), surface switch (H8), distance move, field size, draw/post | `finish_history` |
| **Connections** | jockey A/E, trainer A/E (point-in-time) | `jtAEAsOf` |
| **Pace** | prior top-3 last-3F rate (closing kick), running style (corner positions) | `finish_history` l3f/c1-c4 |
| **Going** | track condition + the horse's wet-record edge | `wetRecordAsOf` |

\* odds drift only has ~35% coverage today — include it but let the model handle missingness; don't depend
on it.

---

## 6. Data inventory + the binding constraint

| Asset | Size | Use |
|---|---|---|
| `finish_history.json` | **87,332 runs, 1999-2026** | training the probability model — *plenty* |
| pace/closing features (l3f) | ~55k of those runs | richer features on recent runs only |
| dividend bench (`history.json`) | **22 weekends / ~755 races, Apr-Jun 2026** | the *money* test — **TINY** |

**The binding constraint is the money data.** We can train a probability model on 87k runs, but we can
only check whether it actually *makes money* on 22 weekends, because payouts only exist for the recent
window. That's a small final exam. Consequences:
- Profit numbers from KEIBrain will have **wide error bars** until the dividend bench grows (it grows one
  weekend at a time — the weekly learning loop feeds it).
- We must lean hard on **win/place *accuracy* and *calibration*** over the big sample (which we *can*
  measure on 87k runs) as the primary signal, and treat the 22-weekend profit number as corroborating,
  not decisive, at first.

---

## 7. Honesty & validation (this is the whole ballgame)

A model with thousands of internal knobs will invent fake patterns *constantly* and look brilliant doing
it. The s50 leak ("proved" a fake edge from hindsight) would be 100× easier to commit here. So:

- **Walk-forward only.** Train on runs strictly before a cutoff date; test on races after it; roll the
  cutoff forward through time. NEVER test on data the model trained on. NEVER a random train/test split
  (that leaks the future into the past).
- **Point-in-time features, enforced.** Every feature gated `< race_date`. Re-use the exact discipline +
  the "same value in April & June?" leak check from `point-in-time.mjs`.
- **Grade on net-of-takeout recovery,** the same metric as `tools/backtest/*-recovery.mjs`, not on
  accuracy or log-loss alone. Reuse the place-dividend settlement code.
- **The market baseline is the bar.** A model only counts if its overlay bets beat *just betting the
  field / betting the favourites* on the same races. Beating "predict winners" accuracy is meaningless if
  it doesn't beat the *price*.
- **Hold out the most recent weekends entirely** as a final untouched test, and confirm forward on live
  weekends before trusting anything (same ≥3-weekend rule as H7/H8).

---

## 8. Success & kill criteria (decide these BEFORE training, so we can't fool ourselves)

**Trust it / promote it if ALL of:**
1. Stage-1 model is **well-calibrated** on the big out-of-sample history (when it says 30% place, ~30%
   place) — proves it learned real signal, not noise.
2. Stage-2 overlay bets **out-recover the field baseline** across walk-forward folds, net of takeout.
3. The edge **holds on the fully-held-out recent weekends** and then across **≥3 live weekends**.
4. Its picks are **explainable** (which features drove each overlay) — North Star teaching requirement.

**Kill it / shelve it if ANY of:**
- It only matches the odds (Stage 2 learns to ignore Stage 1) → no edge exists to find; stop.
- Profit comes from a handful of freak payouts (same concentration check we ran on H7).
- It needs constant re-tuning to stay good → it's overfitting, not learning.

A clean *negative* result here is a real, valuable outcome (it tells us the market is too sharp even for
a model) — not a failure. Honest measurement is the product.

---

## 9. Phased plan (small steps, each one cheap to abandon)

- **Phase 0 — Data prep.** Export the point-in-time feature table (one row per past run) from the same
  sources `point-in-time.mjs` uses. Pure plumbing, no modelling. *Deliverable:* `features.parquet` + a
  documented schema.
- **Phase 1 — Baseline probability model.** Stage-1 GBM predicting top-3, walk-forward, measure
  calibration + accuracy vs the market on the big sample. *Question answered:* does the model see anything
  the odds don't?
- **Phase 2 — Market blend (the real test).** Add Stage-2; measure overlay recovery vs field baseline on
  the dividend bench, walk-forward. *Question answered:* does any disagreement with the market make money?
- **Phase 3 — Betting simulation.** Plug Stage-2 probabilities into the existing recovery harness; bet to
  place only above the edge threshold; full honesty pass + concentration/time-split checks. *Question:*
  would it have made/lost money, honestly?
- **Phase 4 — (Only if Phases 1-3 + ≥3 live weekends pass) productionize.** Export the model's per-horse
  place probabilities into the app as another cold-engine signal (a 🧠 chip beside the 💧/🚫 chips). The
  model still runs *offline*; the app just consumes its exported numbers. No live Python in Nexus.

We stop at the first phase that fails its question. Most likely stopping point, honestly: Phase 2.

---

## 10. Honest risk summary (for the operator)

- **Most likely outcome: a near-miss.** The market is sharp (we proved it). A model probably gets *close*
  to break-even and finds a few real overlays — valuable for learning, not a money fountain.
- **Biggest danger: fooling ourselves.** Overfitting + thin profit-data could make a useless model look
  great. The validation discipline above is non-negotiable; if we cut corners there, we learn nothing.
- **Effort: moderate, not huge.** Phases 0-3 are a few focused sessions of offline Python, reusing our
  existing honest-backtest plumbing. Low risk to the live app (it's a separate offline tool).
- **It changes nothing about the live app** until Phase 4, which is gated behind real, confirmed results.

**Bottom line:** worth doing as the cold engine's next chapter and a genuine learning exercise — *if* we
hold the same honesty bar that made the by-hand sweep trustworthy. Go in expecting to *learn whether an
edge exists*, not to find a jackpot.

---

## 11. Relationship to existing work

- Reuses: `tools/backtest/point-in-time.mjs` (features + leak discipline), `finish_history.json`,
  `history.json` (dividends), and the place-settlement logic in the `*-recovery.mjs` tools.
- Extends: H7 (fresh longshots) and H8 (avoid switch-to-dirt) become *features* the model can combine,
  not standalone rules.
- Evidence lands in `tuning_hypotheses.md` like every other hypothesis, under a new KEIBrain section.

See `pipeline.py` in this folder for an annotated skeleton of the Phase 3/4 flow not yet built.

---

## 12. What this is / when to run it (operational role — read before "running it weekly")

**KEIBrain is an OFFLINE lab tool, not a part of the live app and NOT an auto-tuner.** It does not place
bets, does not change any engine setting, and is not wired into Nexus/Sidecar. It reads data exports and
prints findings. Same category as `tools/backtest/` — a measuring instrument.

**It is a judge that re-judges, not a dial you spin.** You *may* re-run it as new weekends accumulate, but
only to ask *"do the candidate edges (H7/H8, the model's longshot sort) STILL hold on more data?"* — never
to retrain-and-tweak until the numbers look good. That latter habit is the overfitting trap the whole
project exists to avoid. **Re-run to confirm, not to chase performance.** Most re-runs should change nothing.

**Weekly loop (if/when run):**
1. A weekend settles → regenerate `tools/backtest/fixtures/finish_history.json` (currently a manual DB
   export — see dev_log s51 for the query; a one-command refresh script is a TODO).
2. `node KEIBrain/build_features.mjs` → rebuild the feature table.
3. `python KEIBrain/money_test.py` (and `train_stage1.py`) → re-read the verdict; re-run the hand-rule
   `tools/backtest/*-recovery.mjs` alongside.
4. Log the result in `tuning_hypotheses.md`. Adopt a change ONLY if it holds across **≥3 weekends**.

**It does not need to run weekly.** It already answered its first question. Re-running is optional, for
watching whether the edge survives as the bench grows.

**Graduation (Phase 4) is gated:** only if the model's longshot sense proves consistent across many
weekends AND beats the H7/H8 hand rules does it feed the app — and even then OFFLINE (it exports per-horse
place probabilities the app reads as a 🧠 chip; no live Python in Nexus). Not there yet.

> **Relationship to the LIVE betting engine:** that engine (the marks/auto-pick scorer in `script.js` +
> the RISK slider / presets / `SHAPE_TO_PRESET`) follows the SAME discipline — see `tuning_hypotheses.md`.
> The weekly recap pipeline *feeds it evidence* every weekend, but its knobs are changed RARELY and only on
> multi-weekend evidence, never weekly. Neither engine is auto-tuned. KEIBrain just studies; the live engine
> bets.
