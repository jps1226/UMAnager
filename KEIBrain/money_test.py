"""
KEIBrain Phase 2/3 — the MONEY test (the decisive one).

Phase 1 showed the model is well-calibrated but, ACROSS ALL RACES, the market already knows everything it
knows (model+market did not beat market alone on overall accuracy). But our by-hand sweep found the edge
was never broad — it lived in a NARROW pocket (longshots: fresh, not-to-dirt). Overall AUC can't see a
small pocket; only a net-of-takeout betting simulation can. So:

  QUESTION: among LONGSHOTS (the soft band), does betting the ones the MODEL likes — to place — beat just
  betting all longshots, net of the ~20-25% takeout?

Leak-safe: the Stage-1 model is trained ONLY on runs strictly before the betting bench's first date, and
every feature is point-in-time. We then bet bench races the model never saw, settling against the stored
place dividends (same settlement as tools/backtest/*-recovery.mjs).

Run:  python KEIBrain/money_test.py
"""
import json
import numpy as np
import pandas as pd
from pathlib import Path
from lightgbm import LGBMClassifier

HERE = Path(__file__).parent
CSV = HERE / "data" / "features.csv"
BENCH = HERE.parent / "tools" / "backtest" / "fixtures" / "history.json"
STAKE = 100

# ── load features + bench ─────────────────────────────────────────────────────
df = pd.read_csv(CSV)
df["surface_code"] = df["surface"].map({"turf": 0, "dirt": 1, "jump": 2}).fillna(-1)
bench = json.loads(BENCH.read_text(encoding="utf-8"))["past_races_by_date"]
bench_dates = sorted(bench.keys())
bench_min_ymd = int(bench_dates[0].replace("-", ""))   # e.g. 20260411
print(f"\n── KEIBrain money test ── bench: {bench_dates[0]}…{bench_dates[-1]}  ({sum(len(v) for v in bench.values())} races)")

META = ["rid", "horse_id", "ymd"]
MARKET = ["market_fav_rank"]
TARGET = "placed"
feat_cols = [c for c in df.columns if c not in META + MARKET + [TARGET, "surface"]]

# ── train Stage-1 ONLY on runs strictly before the bench (no bench race in training) ──
train = df[df.ymd < bench_min_ymd]
print(f"train (odds-blind, ymd < {bench_min_ymd}): {len(train):,} runs")
model = LGBMClassifier(n_estimators=400, learning_rate=0.03, num_leaves=31,
                       min_child_samples=200, subsample=0.8, colsample_bytree=0.8,
                       random_state=42, verbose=-1)
model.fit(train[feat_cols], train[TARGET])

# model place-prob for every feature row → dict keyed by (rid, horse_id)
df["p_model"] = model.predict_proba(df[feat_cols])[:, 1]
pmap = {(str(r.rid), str(r.horse_id)): r.p_model for r in df.itertuples()}

# ── collect every longshot bet on the bench with its model prob + place outcome ──
bets = []  # (date, p_model, ret)
for date in bench_dates:
    half = "H1" if date < bench_dates[len(bench_dates) // 2] else "H2"
    for race in bench[date]:
        rid = str(race["info"]["race_id"])
        try:
            place_divs = json.loads(race["info"]["results_json"]).get("place")
        except Exception:
            place_divs = None
        if not place_divs:
            continue
        for e in race.get("entries", []):
            if e.get("Scratched") is True:
                continue
            try:
                fav = int(e.get("Fav"))
                pp = int(e.get("PP"))
            except (TypeError, ValueError):
                continue
            if fav < 9:                      # LONGSHOT band only
                continue
            p = pmap.get((rid, str(e.get("Horse_ID")).split(".")[0]))
            if p is None:                    # no point-in-time features (debut) → skip
                continue
            won = next((d["payout"] for d in place_divs if d.get("combo", [None])[0] == pp), 0)
            bets.append((half, p, won * STAKE / 100))

bdf = pd.DataFrame(bets, columns=["half", "p_model", "ret"])
print(f"longshot bets with model prob: {len(bdf):,}")

def report(name, sub):
    if not len(sub):
        print(f"  {name:32s} —"); return
    rec = 100 * sub.ret.sum() / (len(sub) * STAKE)
    hit = 100 * (sub.ret > 0).mean()
    h1 = sub[sub.half == "H1"]; h2 = sub[sub.half == "H2"]
    r1 = 100 * h1.ret.sum() / (len(h1) * STAKE) if len(h1) else 0
    r2 = 100 * h2.ret.sum() / (len(h2) * STAKE) if len(h2) else 0
    print(f"  {name:32s} {rec:5.1f}%  (hit {hit:4.1f}%, n={len(sub):4d})   [H1 {r1:.0f}% / H2 {r2:.0f}%]")

print("\n── recovery to PLACE, longshots, model-liked vs baseline ──")
report("ALL longshots (baseline)", bdf)
# "model-liked" = top tercile / quartile by the model's place probability, within the longshot band
q67 = bdf.p_model.quantile(0.67)
q75 = bdf.p_model.quantile(0.75)
q90 = bdf.p_model.quantile(0.90)
report("model top third (p>=p67)", bdf[bdf.p_model >= q67])
report("model top quartile (p>=p75)", bdf[bdf.p_model >= q75])
report("model top decile (p>=p90)", bdf[bdf.p_model >= q90])
report("model BOTTOM third (p< p33)", bdf[bdf.p_model < bdf.p_model.quantile(0.33)])

print("\nRead: if 'model top third/quartile' recovers meaningfully ABOVE the longshot baseline (and holds")
print("in both halves), the model found which longshots are underpriced — a learned, holistic version of")
print("H7/H8. If it's flat, even the model can't beat the market in the soft band.\n")
