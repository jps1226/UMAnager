// ============================================================
// VALUE LAB — the measurement bench for the cold "value" engine (TODO §0 Step 2, broad scan).
//
// Question it answers, honestly: do we have ANY edge over the win market, and where? It does NOT
// place bets or touch the live app. It:
//   1. turns the engine's sentiment-free, market-free MERIT (form + breeding, odds stripped) into a
//      real per-race win probability via a calibrated softmax (one fitted parameter β);
//   2. DE-VIGS the win odds (strips the ~takeout overround) → the crowd's honest probability;
//   3. flags OVERLAYS (our prob > market prob by a ratio) and flat-bets them to win at real odds,
//      across a threshold sweep, vs two baselines (bet-everything ≈ the takeout floor; bet-favourite).
//
// HONESTY RAILS:
//   • Runs under point-in-time stats, FORCED on (pointInTime:true) — merit can't see the future.
//   • β is fit on a TRAIN half of weekends and the overlay table is reported on the held-out TEST
//     half, so the headline recovery isn't hindsight. (In-sample is printed too, flagged, for contrast.)
//   • Calibration is printed so we can SEE whether "20%" horses actually win ~20%.
//
// Read-only. Run:  BT_PIT=1 node tools/backtest/value-lab.mjs   (BT_PIT not required — forced internally)
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { loadEngine, loadWeekend, fmt, pad, fixturesDir } from './harness.mjs';

const HIST = path.join(fixturesDir, 'history.json');
const STAKE = 100; // flat ¥ per overlay bet — recovery is stake-independent, this just scales totals.

// ── build per-race rows: {pp, merit, odds, won} over runners with valid odds + finite merit ──
function buildRaces(BT) {
  const raw = JSON.parse(fs.readFileSync(HIST, 'utf8'));
  const dates = Object.keys(raw.past_races_by_date).sort();
  const { races } = loadWeekend(BT, dates, HIST, { pointInTime: true });
  const out = [];
  for (const r of races) {
    if (!r.settled) continue;
    const winPp = r.t3[0];
    const pool = [];
    for (const row of BT.entries(r.rid)) {
      const odds = parseFloat(row.Odds);
      const merit = BT.merit(row);
      const pp = parseInt(row.PP, 10);
      if (!Number.isFinite(odds) || odds <= 1.0 || !Number.isFinite(merit) || !Number.isFinite(pp)) continue;
      pool.push({ pp, merit, odds, won: pp === winPp });
    }
    if (pool.length < 2) continue;
    if (!pool.some(h => h.won)) continue; // winner must be in the priced pool
    // standardize merit within the race (stabilizes β; softmax is shift-invariant anyway)
    const mean = pool.reduce((a, h) => a + h.merit, 0) / pool.length;
    const sd = Math.sqrt(pool.reduce((a, h) => a + (h.merit - mean) ** 2, 0) / pool.length) || 1;
    for (const h of pool) h.z = (h.merit - mean) / sd;
    // de-vig market: market_i = (1/odds_i) / Σ(1/odds)
    const inv = pool.map(h => 1 / h.odds);
    const S = inv.reduce((a, b) => a + b, 0);
    pool.forEach((h, i) => { h.market = inv[i] / S; });
    out.push({ ymd: String(r.rid).slice(0, 8), surface: r.surface, pool });
  }
  return out;
}

// softmax win-prob over a race's pool at parameter β (operates on standardized z).
function applyProbs(race, beta) {
  const ex = race.pool.map(h => Math.exp(beta * h.z));
  const S = ex.reduce((a, b) => a + b, 0);
  race.pool.forEach((h, i) => { h.p = ex[i] / S; });
}

// conditional log-likelihood of the actual winners at β (concave in β for one standardized covariate).
function logLik(races, beta) {
  let ll = 0;
  for (const race of races) {
    const ex = race.pool.map(h => Math.exp(beta * h.z));
    const S = ex.reduce((a, b) => a + b, 0);
    const w = race.pool.find(h => h.won);
    ll += Math.log(Math.exp(beta * w.z) / S);
  }
  return ll;
}

// fit β by golden-section search on [0, hi] (LL concave → unimodal).
function fitBeta(races, hi = 4) {
  const gr = (Math.sqrt(5) - 1) / 2;
  let a = 0, b = hi;
  let c = b - gr * (b - a), d = a + gr * (b - a);
  let fc = logLik(races, c), fd = logLik(races, d);
  for (let i = 0; i < 60; i++) {
    if (fc > fd) { b = d; d = c; fd = fc; c = b - gr * (b - a); fc = logLik(races, c); }
    else { a = c; c = d; fc = fd; d = a + gr * (b - a); fd = logLik(races, d); }
  }
  return (a + b) / 2;
}

// ── reporting ──
function calibration(races) {
  const all = [];
  for (const race of races) for (const h of race.pool) all.push(h);
  all.sort((x, y) => x.p - y.p);
  const B = 10, n = all.length, rows = [];
  for (let i = 0; i < B; i++) {
    const slice = all.slice(Math.floor(i * n / B), Math.floor((i + 1) * n / B));
    const pred = slice.reduce((a, h) => a + h.p, 0) / slice.length;
    const act = slice.reduce((a, h) => a + (h.won ? 1 : 0), 0) / slice.length;
    rows.push({ pred, act, n: slice.length });
  }
  return rows;
}

function overlaySweep(races, thresholds) {
  const rows = [];
  for (const T of thresholds) {
    let staked = 0, won = 0, bets = 0, hits = 0, oddsSum = 0;
    for (const race of races) for (const h of race.pool) {
      if (h.p / h.market >= T) {
        bets++; staked += STAKE; oddsSum += h.odds;
        if (h.won) { hits++; won += STAKE * h.odds; }
      }
    }
    rows.push({ T, bets, hits, staked, won, rec: staked ? won / staked : 0, avgOdds: bets ? oddsSum / bets : 0 });
  }
  return rows;
}

function baseline(races, pick) {
  let staked = 0, won = 0, bets = 0, hits = 0;
  for (const race of races) {
    const sel = pick(race.pool);
    for (const h of sel) { bets++; staked += STAKE; if (h.won) { hits++; won += STAKE * h.odds; } }
  }
  return { bets, hits, rec: staked ? won / staked : 0 };
}

function splitByWeekendParity(races) {
  const dates = [...new Set(races.map(r => r.ymd))].sort();
  const idxOf = new Map(dates.map((d, i) => [d, i]));
  const train = [], test = [];
  for (const r of races) (idxOf.get(r.ymd) % 2 === 0 ? train : test).push(r);
  return { train, test };
}

// ── main ──
const BT = loadEngine();
const races = buildRaces(BT);
const { train, test } = splitByWeekendParity(races);

const betaAll = fitBeta(races);
const betaTrain = fitBeta(train);

// probabilities: in-sample uses betaAll; out-of-sample TEST uses betaTrain (fit without it).
for (const r of races) applyProbs(r, betaAll);
const calRows = calibration(races);
const THRESH = [1.0, 1.05, 1.1, 1.2, 1.3, 1.5, 2.0];
const inSample = overlaySweep(races, THRESH);

for (const r of test) applyProbs(r, betaTrain);
const outSample = overlaySweep(test, THRESH);
const baseAllT = baseline(test, pool => pool);
const baseFavT = baseline(test, pool => [pool.reduce((a, b) => (a.odds < b.odds ? a : b))]);

const pct = x => (100 * x).toFixed(1) + '%';
console.log(`\nVALUE LAB — ${races.length} settled races (${train.length} train / ${test.length} test weekends), point-in-time merit`);
console.log(`Fitted β: all=${betaAll.toFixed(3)}  train=${betaTrain.toFixed(3)}   (β>0 ⇒ higher merit ⇒ higher win prob)`);

console.log(`\n── Calibration (all races, β=${betaAll.toFixed(3)}) — do our probabilities match reality? ──`);
console.log(' decile   predicted   actual    n');
calRows.forEach((r, i) => console.log(`  ${pad(i + 1, 2)}      ${pad(pct(r.pred), 7)}    ${pad(pct(r.act), 7)}  ${pad(r.n, 4)}`));

console.log(`\n── Baselines (TEST half) ── bet-all recovery ${pct(baseAllT.rec)} (≈ the takeout floor) · bet-favourite ${pct(baseFavT.rec)}`);

const printSweep = (rows, label) => {
  console.log(`\n── Overlay sweep (${label}) — flat-bet every horse priced longer than fair × T ──`);
  console.log(' T      bets   hit%    avgOdds   recovery');
  for (const r of rows)
    console.log(`  ${r.T.toFixed(2)}  ${pad(r.bets, 5)}  ${pad((100 * (r.hits / (r.bets || 1))).toFixed(1), 5)}  ${pad(r.avgOdds.toFixed(1), 7)}    ${pad(pct(r.rec), 7)}`);
};
printSweep(outSample, 'TEST half — held out, β from train — THIS is the honest read');
printSweep(inSample, 'all races, β=all — in-sample, optimistic — for contrast only');

const best = outSample.filter(r => r.bets >= 50).sort((a, b) => b.rec - a.rec)[0];
console.log(`\n── Read ──`);
console.log(`Bet-all recovers ${pct(baseAllT.rec)} (the takeout drag). An edge = an overlay row that beats that on a meaningful sample.`);
if (best)
  console.log(`Best held-out overlay (≥50 bets): T=${best.T} → ${pct(best.rec)} on ${best.bets} bets vs bet-all ${pct(baseAllT.rec)} → ${best.rec > baseAllT.rec ? 'BEATS the floor (investigate, then confirm live)' : 'does NOT beat the floor (no edge in raw merit-vs-market — narrow the hunt next)'}.`);
console.log(`(Replayed history, final-poll win odds, flat stakes. One bench, not live proof — discipline still applies.)\n`);
