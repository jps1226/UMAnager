"""
KEIBrain — pipeline skeleton (MOCK-UP, NOT IMPLEMENTED).

This file is a *map* of the intended flow described in README.md, written so the structure + the honesty
guardrails are concrete before any modelling starts. Every stage raises NotImplementedError on purpose —
nothing here trains, predicts, or bets. Do not wire this into anything until Phase 0/1 actually build it.

Design rules baked into the structure (see README §1, §4, §7):
  • The TARGET is "beat the market price," not "predict winners."
  • Stage 1 is BLIND to the odds; only Stage 2 sees them. (Defends against re-learning the odds.)
  • Features are POINT-IN-TIME (gated strictly < race_date). No random splits — walk-forward only.
  • Grade on net-of-takeout PLACE recovery vs the field baseline, not on accuracy alone.

Intended tool: gradient-boosted trees (LightGBM/XGBoost) — NOT a neural net (README §3). Python, offline,
separate from the live Nexus/Sidecar (mirrors tools/backtest/).
"""
from __future__ import annotations
from dataclasses import dataclass, field


# ─────────────────────────────────────────────────────────────────────────────
# Phase 0 — DATA PREP: build the point-in-time feature table (one row per past run)
# ─────────────────────────────────────────────────────────────────────────────
# Mirror tools/backtest/point-in-time.mjs exactly. Each feature must use ONLY the horse's runs strictly
# before that run's date. Reuse finish_history.json (87k runs) for fundamentals; history.json for the
# dividend bench used later in evaluation. Apply the "same value in April & June?" leak check here.

FEATURE_GROUPS = {
    # group           : columns (Stage-1 = fundamental, market goes to Stage-2 only)
    "market_stage2":   ["odds", "fav_rank", "odds_drift"],            # ~35% drift coverage — allow missing
    "form":            ["form_score", "last3_finishes", "career_win_pct", "starts"],
    "fit":             ["surface_win_pct", "dist_win_pct", "sire_surface_dist_fit"],
    "situation":       ["layoff_days", "switch_to_dirt", "distance_move", "field_size", "draw_frac"],
    "connections":     ["jockey_ae", "trainer_ae"],
    "pace":            ["closing_top3_rate", "running_style"],         # l3f / corner positions (~55k runs)
    "going":           ["track_condition", "wet_edge"],
}


def build_feature_table(finish_history_path: str, out_path: str) -> None:
    """Phase 0: emit features.parquet — one point-in-time row per past run + the place outcome (top-3)."""
    raise NotImplementedError("Phase 0: port point-in-time.mjs feature math to Python; enforce date gate.")


# ─────────────────────────────────────────────────────────────────────────────
# Walk-forward splitter — the ONLY allowed validation (README §7)
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class WalkForwardFold:
    train_end: str            # 'YYYYMMDD' — train on runs strictly before this
    test_start: str           # == train_end
    test_end: str             # roll-forward window end
    holdout: bool = False     # the final most-recent weekends, touched once, at the very end


def walk_forward_folds(min_train_end: str, step_days: int, window_days: int) -> list[WalkForwardFold]:
    """Roll a cutoff forward through time. NEVER a random split (that leaks the future into the past)."""
    raise NotImplementedError("Build time-ordered folds; reserve the most recent N weekends as holdout.")


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1 — STAGE 1: fundamental model (BLIND to odds)
# ─────────────────────────────────────────────────────────────────────────────
def train_stage1(train_df, features_without_market) -> "Model":
    """GBM predicting P(top-3). MUST NOT receive any market/odds column. Output a calibrated probability."""
    raise NotImplementedError("Phase 1: LightGBM binary place model; isotonic/Platt calibration on a fold.")


def report_calibration(model, test_df) -> dict:
    """Primary Phase-1 signal: when it says 30% place, does ~30% place? Measured on the BIG sample."""
    raise NotImplementedError("Reliability curve + Brier; compare accuracy vs the market on the same races.")


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — STAGE 2: market-blend model (the real test)
# ─────────────────────────────────────────────────────────────────────────────
def market_implied_prob(odds_or_payout) -> float:
    """De-vig the place market into an implied probability (mirror value-lab.mjs's de-vig)."""
    raise NotImplementedError("Convert odds/place dividend → implied prob; remove the takeout/overround.")


def train_stage2(train_df, p_model_col: str) -> "Model":
    """Inputs = Stage-1 P_model + market implied prob (+ maybe a few context cols). Learns how much to
    trust the model OVER the crowd. If it learns to ignore P_model → no edge exists (kill criterion)."""
    raise NotImplementedError("Phase 2: small GBM/logistic blending P_model with market implied prob.")


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3 — BETTING SIMULATION: honest, net-of-takeout (reuse the backtest harness logic)
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class BetRule:
    edge_margin: float = 0.05     # bet to place only where P_final > implied + margin
    band: str = "longshot"        # where every honest edge has lived so far (H7/H8)


def simulate_place_betting(p_final_df, dividend_bench, rule: BetRule) -> dict:
    """Settle flat place bets against the stored `place` dividends — SAME settlement as
    tools/backtest/*-recovery.mjs. Report recovery vs the FIELD baseline, per walk-forward fold, plus the
    concentration + time-split checks (top-5 payout share; does it hold in both halves?)."""
    raise NotImplementedError("Phase 3: reuse place-dividend settlement; recovery, baseline delta, robustness.")


# ─────────────────────────────────────────────────────────────────────────────
# Success / kill gate (README §8) — decided up front so results can't be rationalised
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class PromotionGate:
    calibrated_oos: bool = False           # §8.1
    beats_field_walkforward: bool = False  # §8.2
    holds_on_holdout: bool = False         # §8.3
    explainable: bool = False              # §8.4 (feature attributions per pick)
    not_freak_driven: bool = False         # concentration check passed
    adds_value_over_odds: bool = False     # Stage-2 did NOT just ignore Stage-1

    def promote(self) -> bool:
        """All must hold before KEIBrain graduates to a live 🧠 chip (Phase 4). Else: shelve + log honestly."""
        return all(vars(self).values())


# ─────────────────────────────────────────────────────────────────────────────
# Phase 4 — productionize (ONLY if the gate passes + ≥3 live weekends confirm)
# ─────────────────────────────────────────────────────────────────────────────
def export_probabilities_for_app(model, upcoming_races, out_json: str) -> None:
    """Write per-horse place probabilities the LIVE app can read as another cold-engine signal (a 🧠 chip
    beside 💧/🚫). The model stays OFFLINE; the app only consumes exported numbers. No Python in Nexus."""
    raise NotImplementedError("Phase 4 only after Phases 1-3 + live confirmation; export-only integration.")


if __name__ == "__main__":
    raise SystemExit(
        "KEIBrain is a MOCK-UP (plan only). See README.md. No phase is implemented — nothing to run yet."
    )
