// ============================================================
// ✅ HONEST UNDER BT_PIT=1 (since 2026-06-22, Step 1.5). Run as: BT_PIT=1 node upset-autopsy.mjs
// The stats grade (BT.merit = form+breeding) leans on Surface_Win_Pct / Dist_Win_Pct / Record / Sire_Fit,
// which the historical /api/races computes as-of-NOW. With BT_PIT=1 the harness rebuilds them all
// point-in-time (strictly before each race), so a winner no longer looks "stats-strong" because its own
// win is baked in. The earlier leak-inflated ~52% "catchable" corrected to ~31% catchable / ~38% freak.
// WITHOUT BT_PIT=1 the leak returns — always run with BT_PIT=1. (The live app was always unaffected.)
// ============================================================
// Counterfactual backtest — UPSET AUTOPSY.
//
// Answers the strategic question: when a surprise horse wins (a race we likely lost), was it a
// horse we COULD have caught, or a genuine freak nobody could have called?
//
// For every settled race it looks at the ACTUAL winner and grades it two ways:
//   • MARKET rank  — where the betting public placed it (favourite rank).
//   • STATS rank   — where the engine's own form + breeding merit places it, with the ODDS
//                    factored out (BT.merit = explainPowerScore form.subtotal + pedigree.subtotal).
// Then it buckets each race:
//   CHALK     — a top-3 favourite won (no upset; if we lost here it's bet STRUCTURE, not the pick).
//   CATCHABLE — an upset winner that ranked in the TOP third of the field on stats: strong horse the
//               market underrated → tuning the picker to trust stats more could catch these.
//   SEMI      — an upset winner mid-pack on stats.
//   FREAK     — an upset winner in the BOTTOM third on stats: weak on everything → unforecastable;
//               no picker tuning helps, the right response is bet cheaper and accept the variance.
//
// READ-ONLY; reuses the shipping engine via harness.mjs. Input: fixtures/history.json.
//
// Run:  node tools/backtest/upset-autopsy.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { loadEngine, loadWeekend, pad, fixturesDir } from './harness.mjs';

const HIST = path.join(fixturesDir, 'history.json');
const dates = Object.keys(JSON.parse(fs.readFileSync(HIST, 'utf8')).past_races_by_date || {}).sort();

const BT = loadEngine();
const { races } = loadWeekend(BT, dates, HIST);

const UPSET_FAV = 4;   // winner favourite-rank ≥ 4 = an upset (matches the recap's definition)
const dateOf = rid => `${rid.slice(0, 4)}-${rid.slice(4, 6)}-${rid.slice(6, 8)}`;

const tally = { chalk: 0, catchable: 0, semi: 0, freak: 0, upsets: 0, races: 0, noWinner: 0 };
const oddsBucket = { catchable: [], semi: [], freak: [] };
const byDate = {}; // date -> { upsets, catchable, freak }

for (const m of races) {
  if (!m.settled) continue;
  const entries = BT.entries(m.rid);
  const winner = entries.find(e => String(e.Finish).trim() === '1');
  if (!winner) { tally.noWinner++; continue; }
  tally.races++;
  const d = dateOf(m.rid);
  byDate[d] = byDate[d] || { upsets: 0, catchable: 0, freak: 0 };

  const favRank = parseInt(winner.Fav, 10) || 999;
  if (favRank <= 3) { tally.chalk++; continue; } // favourite-ish won — not an upset

  tally.upsets++; byDate[d].upsets++;
  // STATS rank of the winner among the field (1 = best on stats).
  const ranked = entries
    .map(e => ({ hid: String(e.Horse_ID).split('.')[0], merit: BT.merit(e) }))
    .sort((a, b) => b.merit - a.merit);
  const winHid = String(winner.Horse_ID).split('.')[0];
  const statsRank = ranked.findIndex(r => r.hid === winHid) + 1;
  const field = entries.length;
  const odds = parseFloat(winner.Odds);

  let bucket;
  if (statsRank <= field / 3) bucket = 'catchable';
  else if (statsRank > field * 2 / 3) bucket = 'freak';
  else bucket = 'semi';
  tally[bucket]++;
  if (bucket === 'catchable') byDate[d].catchable++;
  if (bucket === 'freak') byDate[d].freak++;
  if (Number.isFinite(odds)) oddsBucket[bucket].push(odds);
}

const pct = (n, d) => d ? (n / d * 100).toFixed(0) + '%' : '—';
const avg = a => a.length ? '¥' + (a.reduce((s, x) => s + x, 0) / a.length).toFixed(1) : '—';

console.log(`\nUpset autopsy — ${dates.length} race days, ${tally.races} settled races\n`);
console.log(`Favourite-ish (top-3 fav) winners ........ ${tally.chalk}  (${pct(tally.chalk, tally.races)} of races — predictable)`);
console.log(`Upset winners (fav rank ≥ ${UPSET_FAV}) ............. ${tally.upsets}  (${pct(tally.upsets, tally.races)} of races)\n`);
console.log('Of those upset winners — graded on STATS ONLY (odds ignored):');
console.log(`  CATCHABLE (strong on stats, market missed it) .. ${pad(tally.catchable, 3)}  ${pad(pct(tally.catchable, tally.upsets), 4)}   avg win odds ${avg(oddsBucket.catchable)}`);
console.log(`  SEMI      (middling on stats) .................. ${pad(tally.semi, 3)}  ${pad(pct(tally.semi, tally.upsets), 4)}   avg win odds ${avg(oddsBucket.semi)}`);
console.log(`  FREAK     (weak on everything, unforecastable) . ${pad(tally.freak, 3)}  ${pad(pct(tally.freak, tally.upsets), 4)}   avg win odds ${avg(oddsBucket.freak)}`);

console.log('\nBy race day (upsets · catchable · freak):');
for (const d of Object.keys(byDate).sort())
  console.log(`  ${d}   ${pad(byDate[d].upsets, 2)} upsets   ${pad(byDate[d].catchable, 2)} catchable   ${pad(byDate[d].freak, 2)} freak`);

console.log('\n── Read ──');
const catchPct = tally.upsets ? tally.catchable / tally.upsets * 100 : 0;
const freakPct = tally.upsets ? tally.freak / tally.upsets * 100 : 0;
console.log(`When a surprise horse won, it was a horse the STATS rated a contender (catchable) ${catchPct.toFixed(0)}% of`);
console.log(`the time, and a genuine freak ${freakPct.toFixed(0)}% of the time (the rest middling).`);
console.log(freakPct >= 50
  ? '→ Mostly FREAKS: we can\'t out-pick these. Protect capital (simpler/cheaper bets) — don\'t chase.'
  : '→ A real share were CATCHABLE: tuning the picker to trust the stats more is worth testing.');
console.log('\n(Stats grade = the engine\'s own form+breeding merit with odds removed. Replayed history; a prior, not proof.)\n');
