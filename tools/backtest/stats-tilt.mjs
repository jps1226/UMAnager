// ============================================================
// ✅ HONEST UNDER BT_PIT=1 (since 2026-06-22, Step 1.5). Run as: BT_PIT=1 node stats-tilt.mjs
// History: this tool once "proved" a +¥millions stats edge that was pure LOOK-AHEAD LEAKAGE — the
// historical /api/races computes Record/Surface/Dist/Sire_Fit/Jockey_AE/Trainer_AE as-of-NOW, so past
// races were fed stats encoding their own outcome. That is FIXED in the backtest: with BT_PIT=1 the
// harness rebuilds every one of those fields point-in-time (own-record in point-in-time.mjs Step 1;
// jockey/trainer A/E + sire-fit Step 1.5), strictly before each race. Without BT_PIT=1 the leak returns —
// always run this with BT_PIT=1. (The LIVE app was always fine: an upcoming race has no future data.)
// Honest verdict (11 weekends): tilting toward stats does NOT beat the market — flat-to-worse past ~1.25×.
// ============================================================
// Counterfactual backtest — STATS-TILT TEST (the payoff test).
//
// The upset autopsy showed most surprise winners were stats-contenders the market underrated.
// But "could've caught it" ≠ "would've made money" (those are winners only; most stats-strong
// longshots still lose). This test settles it on real money: replay every weekend with the picker
// LEANING HARDER ON STATS (form + breeding) and LESS on odds, and see if NET P&L actually improves.
//
// The lever is the live formula weights (the real Settings knob, frontend-only/reversible). Tilt T
// multiplies the merit-side weights (form/pedigree/sire/surface/distance/jockey/trainer) by T while
// holding the odds weight — so the picker trusts stats T× more relative to the market. T=1.0 is today.
// RISK is held at 30 (the operator's default), where the longshot-value term is dormant, so this is a
// clean "stats vs odds" test, not longshot-chasing. Bet preset held at Balanced.
//
// READ-ONLY; reuses the shipping engine via harness.mjs. Input: fixtures/history.json.
// Run:  node tools/backtest/stats-tilt.mjs
// ============================================================
import path from 'node:path';
import { loadEngine, loadWeekend, runRace, fmt, pad, fixturesDir } from './harness.mjs';

const HIST = path.join(fixturesDir, 'history.json');
const RISK = 30;
const TILTS = [1.0, 1.25, 1.5, 2.0, 3.0, 5.0];
const WEEKENDS = [
  ['2026-06-20', '2026-06-21'], ['2026-06-13', '2026-06-14'], ['2026-06-06', '2026-06-07'],
  ['2026-05-30', '2026-05-31'], ['2026-05-23', '2026-05-24'], ['2026-05-16', '2026-05-17'],
  ['2026-05-09', '2026-05-10'], ['2026-05-02', '2026-05-03'], ['2026-04-25', '2026-04-26'],
  ['2026-04-18', '2026-04-19'], ['2026-04-11', '2026-04-12'],
];

// Engine default formula weights (see getFormulaWeights). MERIT_KEYS are the stats side we scale.
const BASE = { oddsCap: 100, formMultiplier: 100, freshnessBonus: 3, freshnessBreakeven: 10,
  pedigreeMultiplier: 30, formWeight: 80, sireFitWeight: 10, surfaceFitWeight: 8,
  distanceFitWeight: 8, jockeyWeight: 20, trainerWeight: 20 };
const MERIT_KEYS = ['formMultiplier', 'formWeight', 'pedigreeMultiplier', 'sireFitWeight',
  'surfaceFitWeight', 'distanceFitWeight', 'jockeyWeight', 'trainerWeight'];
const tilted = T => { const w = { ...BASE }; for (const k of MERIT_KEYS) w[k] = BASE[k] * T; return w; };

const BT = loadEngine();

function runTilt(T) {
  BT.setFormulaWeights(tilted(T));
  const comp = BT.comp('balanced');
  const perWk = [];
  let staked = 0, won = 0, bet = 0, hits = 0;
  for (const wk of WEEKENDS) {
    const { races } = loadWeekend(BT, wk, HIST);
    let s = 0, w = 0;
    for (const m of races) {
      if (!m.settled) continue;
      const r = runRace(BT, m, comp, RISK);
      if (r.status !== 'bet') continue;
      s += r.staked; w += r.won; bet++; if (r.won > 0) hits++;
    }
    perWk.push(w - s);
    staked += s; won += w;
  }
  return { T, perWk, staked, won, net: won - staked, roi: staked ? won / staked * 100 : 0, hitRate: bet ? hits / bet * 100 : 0 };
}

const results = TILTS.map(runTilt);
const baseline = results[0];

console.log(`\nStats-tilt test — RISK ${RISK}, Balanced, ${WEEKENDS.length} weekends`);
console.log('Tilt = how much MORE the picker trusts form/breeding vs odds (1.0 = today).\n');
console.log('tilt   net           recovery   hit%    vs today        weekends better');
console.log('────   ──────────    ────────   ─────   ───────────     ───────────────');
for (const r of results) {
  const better = r === baseline ? '—' : `${r.perWk.filter((n, i) => n > baseline.perWk[i]).length}/${WEEKENDS.length}`;
  const delta = r === baseline ? '(baseline)' : `${r.net - baseline.net >= 0 ? '+' : ''}${fmt(r.net - baseline.net)}`;
  console.log(`${pad(r.T.toFixed(2), 4)}   ${pad(fmt(r.net), 11)}   ${pad(r.roi.toFixed(1), 6)}%   ${pad(r.hitRate.toFixed(1), 5)}   ${pad(delta, 11)}     ${better}`);
}

// ── Read ── best tilt by net, and whether it's a real + consistent improvement.
const best = results.reduce((a, b) => (b.net > a.net ? b : a), results[0]);
const bestBetter = best.perWk.filter((n, i) => n > baseline.perWk[i]).length;
console.log('\n── Read ──');
if (best === baseline || best.net - baseline.net <= 0) {
  console.log('→ No stats-tilt beat today\'s settings on net. Trusting stats MORE does NOT make money here —');
  console.log('  the extra losing bets eat the extra winners. The "catchable" upsets are a hindsight mirage.');
  console.log('  Keep weights as-is; the capital-preservation read (simpler/cheaper bets) stands.');
} else {
  console.log(`→ Best tilt ${best.T.toFixed(2)}× improves net by ${fmt(best.net - baseline.net)} (recovery ${baseline.roi.toFixed(0)}%→${best.roi.toFixed(0)}%),`);
  console.log(`  and beats today in ${bestBetter}/${WEEKENDS.length} weekends. ${bestBetter >= WEEKENDS.length * 0.7 ? 'CONSISTENT — a real candidate to test live.' : 'But mixed across weekends — not yet firm.'}`);
  console.log('  NOTE: still all-losing money on these chaotic cards; "better" = lost less. Confirm live before changing.');
}
console.log('\n(Replayed history, final-poll odds, Balanced @ risk 30 — a strong prior, not a guarantee.)\n');
