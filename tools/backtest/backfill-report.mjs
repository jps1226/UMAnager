// ============================================================
// Counterfactual backtest — MULTI-WEEKEND BACKFILL REPORT.
//
// Replays many past weekends and tabulates, per weekend, the two open hypotheses:
//   • H4 (risk sweet spot): median recovery in the LOW (0-30) / MID (40-60) / HIGH (70-100)
//     risk zones, holding Balanced — which zone recovered best?
//   • H5 (simpler bets win): recovery of each preset @ RISK 30 — does Win+Place beat Trio chase?
// Then counts how often each holds across all weekends, so a "watch" hypothesis can graduate to
// CONFIRMED (or get rejected) against history NOW instead of waiting 3 live weekends.
//
// Input: fixtures/history.json — a merged /api/races snapshot of all the weekends (see README).
// READ-ONLY; reuses the shipping engine via harness.mjs.
//
// Run:  node tools/backtest/backfill-report.mjs
// ============================================================
import path from 'node:path';
import { loadEngine, loadWeekend, runRace, pad, median, fixturesDir } from './harness.mjs';

const HIST = path.join(fixturesDir, 'history.json');
// Newest first. Each entry is one JST race weekend [Saturday, Sunday].
const WEEKENDS = [
  ['2026-06-20', '2026-06-21'], ['2026-06-13', '2026-06-14'], ['2026-06-06', '2026-06-07'],
  ['2026-05-30', '2026-05-31'], ['2026-05-23', '2026-05-24'], ['2026-05-16', '2026-05-17'],
  ['2026-05-09', '2026-05-10'], ['2026-05-02', '2026-05-03'], ['2026-04-25', '2026-04-26'],
  ['2026-04-18', '2026-04-19'], ['2026-04-11', '2026-04-12'],
];
const PRESETS = ['win_place', 'balanced', 'quinella_wide', 'trio_chase', 'nagashi_chase', 'wide_safe'];

const BT = loadEngine();

// Recovery (won/staked) over the currently-loaded races for a given composition + risk.
function recovery(races, comp, risk) {
  let staked = 0, won = 0, bet = 0;
  for (const m of races) {
    if (!m.settled) continue;
    const r = runRace(BT, m, comp, risk);
    if (r.status !== 'bet') continue;
    staked += r.staked; won += r.won; bet++;
  }
  return staked ? (won / staked * 100) : 0;
}

console.log(`\nMulti-weekend backfill — ${WEEKENDS.length} weekends`);
console.log('H4 = which RISK zone recovers best (Balanced)   ·   H5 = preset recovery @ RISK 30\n');
console.log('weekend          races   risk lo/mid/hi   zone    winP / bal / trio   best preset');
console.log('───────────────  ─────  ────────────────  ─────   ─────────────────   ───────────');

const zoneTally = { LOW: 0, MID: 0, HIGH: 0 };
const bestPresetTally = {};
let winPbeatsTrio = 0, weekendsScored = 0;

for (const wk of WEEKENDS) {
  const { races } = loadWeekend(BT, wk, HIST);
  const settledN = races.filter(r => r.settled).length;
  if (settledN === 0) { console.log(`${pad(wk[0].slice(5) + '..' + wk[1].slice(8), 15)}  (no settled races)`); continue; }
  weekendsScored++;

  // H4: median recovery per risk zone, Balanced held.
  const bal = BT.comp('balanced');
  const roiAt = risk => recovery(races, bal, risk);
  const lo = median([0, 10, 20, 30].map(roiAt));
  const mid = median([40, 50, 60].map(roiAt));
  const hi = median([70, 80, 90, 100].map(roiAt));
  const zone = mid >= lo && mid >= hi ? 'MID' : lo >= hi ? 'LOW' : 'HIGH';
  zoneTally[zone]++;

  // H5: preset recovery @ risk 30.
  const presetRoi = {};
  for (const p of PRESETS) presetRoi[p] = recovery(races, BT.comp(p), 30);
  const bestPreset = [...PRESETS].sort((a, b) => presetRoi[b] - presetRoi[a])[0];
  bestPresetTally[bestPreset] = (bestPresetTally[bestPreset] || 0) + 1;
  if (presetRoi.win_place > presetRoi.trio_chase) winPbeatsTrio++;

  const label = wk[0].slice(5) + '..' + wk[1].slice(8);
  console.log(
    `${pad(label, 15)}  ${pad(settledN, 5)}  ${pad(lo.toFixed(0), 4)}/${pad(mid.toFixed(0), 3)}/${pad(hi.toFixed(0), 3)}      ${pad(zone, 5)}   ${pad(presetRoi.win_place.toFixed(0), 4)} / ${pad(presetRoi.balanced.toFixed(0), 3)} / ${pad(presetRoi.trio_chase.toFixed(0), 3)}    ${bestPreset}`
  );
}

console.log('\n── Verdict across weekends ──');
console.log(`H4 (risk sweet spot): best zone — MID ${zoneTally.MID}/${weekendsScored}  ·  LOW ${zoneTally.LOW}/${weekendsScored}  ·  HIGH ${zoneTally.HIGH}/${weekendsScored}.`);
const h4winner = Object.entries(zoneTally).sort((a, b) => b[1] - a[1])[0];
console.log(`   → ${h4winner[0]} zone most common (${h4winner[1]}/${weekendsScored}). ${h4winner[1] >= weekendsScored * 0.7 ? 'CONSISTENT — candidate to act on.' : 'Mixed — not a firm signal yet.'}`);
console.log(`H5 (simpler bets win): Win+Place out-recovered Trio chase in ${winPbeatsTrio}/${weekendsScored} weekends.`);
console.log(`   Best-recovering preset by weekend: ${Object.entries(bestPresetTally).sort((a, b) => b[1] - a[1]).map(([p, n]) => `${p} ${n}`).join(' · ')}.`);
console.log(`   → ${winPbeatsTrio >= weekendsScored * 0.7 ? 'CONSISTENT — simpler bets reliably preserve more capital.' : 'Mixed — not a firm signal yet.'}`);
console.log('\n(Replayed history, final-poll odds, fixed strategy per run — a strong prior, not a guarantee. Confirm on live weekends too.)\n');
