"""
KEIBrain Phase 1 — Stage-1 "fundamental" model (BLIND to the market).

Trains a gradient-boosted tree (LightGBM) to predict P(place = top-3) from the horse's OWN merit +
situation features only — deliberately NOT given the odds/favourite rank (README §4). Evaluated on a
time-ordered holdout (most recent runs), never a random split.

The Phase-1 question (README §8.1): does the model learn REAL signal (is it well-calibrated), and how
close does its accuracy get to the market's own ranking? It does NOT need to beat the market yet — that's
Stage 2. We also peek at whether model + market together beats market alone (a preview of Stage-2 hope).

Run:  python KEIBrain/train_stage1.py
"""
import numpy as np
import pandas as pd
from pathlib import Path
from lightgbm import LGBMClassifier
from sklearn.metrics import roc_auc_score, log_loss, brier_score_loss
from sklearn.linear_model import LogisticRegression

HERE = Path(__file__).parent
CSV = HERE / "data" / "features.csv"

# Columns that are NOT Stage-1 features: identifiers, the target, and the market (Stage-2 only).
META = ["rid", "horse_id", "ymd"]
MARKET = ["market_fav_rank"]
TARGET = "placed"

df = pd.read_csv(CSV)

# surface → integer code (a legit pre-race context feature; turf/dirt/jump).
df["surface_code"] = df["surface"].map({"turf": 0, "dirt": 1, "jump": 2}).fillna(-1)

# ── data sanity (print before modelling) ──────────────────────────────────────
print(f"\n── KEIBrain Phase 1 ── rows: {len(df):,}  date range: {df.ymd.min()}–{df.ymd.max()}")
print(f"place rate (target mean): {df.placed.mean():.3f}   (≈ field place share, sanity ~0.20)")
print(f"NaN share by a few features: "
      f"layoff_days {df.layoff_days.isna().mean():.0%}, "
      f"closing_top3_rate {df.closing_top3_rate.isna().mean():.0%}, "
      f"surface_place_pct {df.surface_place_pct.isna().mean():.0%}")

# Stage-1 feature set = everything except meta / market / target / raw surface string.
feat_cols = [c for c in df.columns
             if c not in META + MARKET + [TARGET, "surface"]]
print(f"Stage-1 features ({len(feat_cols)}): {', '.join(feat_cols)}")

# ── time-ordered split: most-recent 20% of runs = test (no future leaks into train) ──
df = df.sort_values("ymd").reset_index(drop=True)
cut = int(len(df) * 0.80)
cut_ymd = df.ymd.iloc[cut]
train, test = df.iloc[:cut], df.iloc[cut:]
print(f"\ntrain: {len(train):,} runs (< {cut_ymd})   test: {len(test):,} runs (>= {cut_ymd})")

Xtr, ytr = train[feat_cols], train[TARGET]
Xte, yte = test[feat_cols], test[TARGET]

# ── Stage-1 model (odds-blind) ────────────────────────────────────────────────
model = LGBMClassifier(
    n_estimators=400, learning_rate=0.03, num_leaves=31,
    min_child_samples=200, subsample=0.8, colsample_bytree=0.8,
    random_state=42, verbose=-1,
)
model.fit(Xtr, ytr)
p_model = model.predict_proba(Xte)[:, 1]

# ── the market's own ranking as the BAR (lower fav rank = more likely to place) ──
mkt_rank = test[MARKET[0]].to_numpy(dtype=float)
mkt_score = -mkt_rank  # higher = more favoured
mask = ~np.isnan(mkt_rank)

auc_model = roc_auc_score(yte, p_model)
auc_market = roc_auc_score(yte[mask], mkt_score[mask])

print("\n── ACCURACY (AUC: 0.5 = coin flip, 1.0 = perfect; higher = ranks placers better) ──")
print(f"  Stage-1 model (odds-blind): {auc_model:.4f}")
print(f"  Market favourite-rank only: {auc_market:.4f}   <- the bar the crowd already clears")

# ── calibration: when the model says X%, does ~X% actually place? ──────────────
print("\n── CALIBRATION (Stage-1) — predicted vs actual place rate by probability decile ──")
dec = pd.qcut(p_model, 10, labels=False, duplicates="drop")
cal = pd.DataFrame({"pred": p_model, "actual": yte.to_numpy(), "dec": dec})
tbl = cal.groupby("dec").agg(pred=("pred", "mean"), actual=("actual", "mean"), n=("pred", "size"))
for _, r in tbl.iterrows():
    bar = "█" * round(r.actual * 40)
    print(f"  pred {r.pred:5.1%}  actual {r.actual:5.1%}  (n={int(r.n):4d})  {bar}")
print(f"  Brier score (lower=better): {brier_score_loss(yte, p_model):.4f}")

# ── Stage-2 PREVIEW: does model + market beat market alone? (a quick logistic blend) ──
blend_tr = pd.DataFrame({"p_model": model.predict_proba(Xtr)[:, 1],
                         "mkt": -train[MARKET[0]].to_numpy(dtype=float)}).fillna(0)
blend_te = pd.DataFrame({"p_model": p_model, "mkt": mkt_score}).fillna(0)
blender = LogisticRegression(max_iter=1000).fit(blend_tr, ytr)
p_blend = blender.predict_proba(blend_te)[:, 1]
auc_blend = roc_auc_score(yte, p_blend)
print("\n── STAGE-2 PREVIEW (model + market vs market alone) ──")
print(f"  market alone AUC:    {auc_market:.4f}")
print(f"  model + market AUC:  {auc_blend:.4f}   ({'+' if auc_blend>=auc_market else ''}{auc_blend-auc_market:+.4f})")
print("  (>0 means the fundamentals add something the odds miss — the only path to an edge.)\n")

# ── what the model leaned on ──────────────────────────────────────────────────
imp = pd.Series(model.feature_importances_, index=feat_cols).sort_values(ascending=False)
print("── top Stage-1 features by importance ──")
for name, val in imp.head(12).items():
    print(f"  {name:22s} {int(val)}")
print()
