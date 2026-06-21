// ============================================================
// Counterfactual backtest — PRESET SWEEP.
//
// Replays a SETTLED weekend through the REAL engine with RISK held fixed (default 30 = the
// operator's usual setting) and the BET PRESET varied, to answer: "which bet TYPE actually
// pays on this kind of card?"  Reuses the shipping pick/price/settle code via harness.mjs.
//
// Each preset both PICKS differently (its count band + tilt) and STRUCTURES differently (its
// tickets), so this compares whole strategies, not just ticket shapes. Presets stake different
// totals and bet different numbers of races (nagashi abstains when there's no axis), so the fair
// comparison is RECOVERY (ROI %), with net ¥ alongside.
// CAVEAT: stored odds are the last poll before each race, not the exact bet-time tick.
//
// Run:  node tools/backtest/preset-sweep.mjs [date,date,...] [risk]
//   e.g. node tools/backtest/preset-sweep.mjs 2026-06-20,2026-06-21 30
// ============================================================
import { loadEngine, loadWeekend, runRace, fmt, pad } from './harness.mjs';

const argDates = (process.argv[2] || '2026-06-20,2026-06-21').split(',').map(s => s.trim()).filter(Boolean);
const RISK = Number.isFinite(parseInt(process.argv[3], 10)) ? parseInt(process.argv[3], 10) : 30;
// The named day presets (small_token is auto-only; the "🧪 Auto per race" sentinel isn't a fixed type).
const PRESETS = ['win_place', 'balanced', 'quinella_wide', 'trio_chase', 'nagashi_chase', 'wide_safe'];

const BT = loadEngine();
const { races, totalRaces, unsettled } = loadWeekend(BT, argDates);

console.log(`\nCounterfactual preset sweep — RISK held at ${RISK}`);
console.log(`Weekend: ${argDates.join(' + ')}  |  ${totalRaces} races (${unsettled} unsettled, skipped)\n`);
console.log('preset           bet  abst   staked        won          net        ROI    hit%');
console.log('───────────────  ───  ────  ──────────   ──────────   ──────────   ─────  ─────');

const rows = [];
for (const preset of PRESETS) {
  const comp = BT.comp(preset);
  let staked = 0, won = 0, bet = 0, hits = 0, abstain = 0;
  for (const m of races) {
    if (!m.settled) continue;
    const r = runRace(BT, m, comp, RISK);
    if (r.status !== 'bet') { abstain++; continue; }
    staked += r.staked; won += r.won; bet++; if (r.won > 0) hits++;
  }
  const net = won - staked;
  const roi = staked ? (won / staked * 100) : 0;
  const hitRate = bet ? (hits / bet * 100) : 0;
  rows.push({ preset, bet, abstain, staked, won, net, roi, hitRate });
  console.log(
    `${pad(preset, 15)}  ${pad(bet, 3)}  ${pad(abstain, 4)}  ${pad(fmt(staked), 10)}   ${pad(fmt(won), 10)}   ${pad(fmt(net), 10)}   ${pad(roi.toFixed(1), 5)}  ${pad(hitRate.toFixed(1), 5)}`
  );
}

// ── Read ── compare by RECOVERY (ROI), since stakes + bet counts differ across presets.
const byRoi = [...rows].sort((a, b) => b.roi - a.roi);
const byNet = [...rows].sort((a, b) => b.net - a.net);
console.log('\n── Read ──');
console.log(`Best recovery: ${byRoi[0].preset} (${byRoi[0].roi.toFixed(0)}%)   worst: ${byRoi[byRoi.length - 1].preset} (${byRoi[byRoi.length - 1].roi.toFixed(0)}%).`);
console.log(`Best net: ${byNet[0].preset} (${fmt(byNet[0].net)})   worst: ${byNet[byNet.length - 1].preset} (${fmt(byNet[byNet.length - 1].net)}).`);
console.log('Note: presets that bet fewer races (e.g. nagashi abstains with no axis) risk less but skip action.');
console.log('\n(One weekend only — do not change engine math off a single read. Log + watch ≥3 weekends.)\n');
