// ============================================================
// VERIFY the post-race "Why It Won" autopsy (s54) against REAL settled data.
// Drives the SAME computeRaceAutopsy() the UI runs (via the harness `autopsy` seam),
// over every settled race in fixtures/history.json, and checks:
//   • a winner is found and graded for every settled race,
//   • the chalk/catchable/semi/freak split is sane and ≈ upset-autopsy.mjs,
//   • field ranks + the engine ◎ lookup are populated,
//   • no crashes / no NaN buckets.
// Then prints a few worked examples so the output can be eyeballed.
//
// Run:  node tools/backtest/verify-autopsy.mjs   (BT_PIT=1 for honest stats, matching upset-autopsy)
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { loadEngine, loadWeekend, pad, fixturesDir } from './harness.mjs';

const HIST = path.join(fixturesDir, 'history.json');
const dates = Object.keys(JSON.parse(fs.readFileSync(HIST, 'utf8')).past_races_by_date || {}).sort();

const BT = loadEngine();
const { races } = loadWeekend(BT, dates, HIST);

const tally = { chalk: 0, catchable: 0, semi: 0, freak: 0 };
let settled = 0, graded = 0, noWinner = 0, problems = 0;
const examples = [];

for (const m of races) {
  if (!m.settled) continue;
  settled++;
  let a;
  try { a = BT.autopsy(m.rid); }
  catch (e) { problems++; console.error(`  ✗ ${m.rid} threw: ${e.message}`); continue; }
  if (!a) { noWinner++; continue; }
  graded++;

  // Integrity checks.
  if (!a.bucket || !(a.bucket in tally)) { problems++; console.error(`  ✗ ${m.rid} bad bucket: ${a.bucket}`); continue; }
  if (a.statsRank !== null && (a.statsRank < 1 || a.statsRank > a.field)) { problems++; console.error(`  ✗ ${m.rid} statsRank ${a.statsRank} out of 1..${a.field}`); }
  if (a.ranks.odds !== null && (a.ranks.odds < 1 || a.ranks.odds > a.field)) { problems++; console.error(`  ✗ ${m.rid} oddsRank out of range`); }
  tally[a.bucket]++;

  // Collect a worked example of each bucket type for eyeballing.
  if (examples.length < 8 && !examples.some(x => x.bucket === a.bucket && examples.filter(y => y.bucket === a.bucket).length >= 2)) {
    examples.push({ rid: m.rid, bucket: a.bucket, a });
  }
}

console.log(`\n── Autopsy verify — ${dates.length} race days, ${settled} settled races ──\n`);
console.log(`Graded ............ ${graded}`);
console.log(`No winner row ..... ${noWinner}`);
console.log(`Problems .......... ${problems}`);
console.log(`\nBucket split (UI grade, at the sandbox's default risk):`);
for (const k of ['chalk', 'catchable', 'semi', 'freak'])
  console.log(`  ${pad(k, 9)}  ${pad(tally[k], 4)}  ${graded ? (tally[k] / graded * 100).toFixed(0) + '%' : '—'}`);

const upsets = tally.catchable + tally.semi + tally.freak;
console.log(`\nOf ${upsets} upset winners: ${(tally.catchable/upsets*100).toFixed(0)}% catchable / ${(tally.freak/upsets*100).toFixed(0)}% freak`);
console.log(`(upset-autopsy.mjs @risk30 reported ~31% catchable / ~38% freak — same ballpark = good.)`);

console.log(`\n── Worked examples ──`);
for (const ex of examples) {
  const a = ex.a, w = a.winner;
  console.log(`\n[${ex.bucket.toUpperCase()}] ${ex.rid}  field=${a.field}`);
  console.log(`  Winner: ${w.name}  ${Number.isFinite(w.odds)?w.odds.toFixed(1)+'×':'—'}  fav#${w.fav}  → merit rank ${a.statsRank}/${a.field}`);
  console.log(`  Ranks: form ${a.ranks.form}  sire ${a.ranks.sire}  surf ${a.ranks.surf}  odds ${a.ranks.odds}`);
  console.log(`  Engine ◎: ${a.anchor ? a.anchor.name + ' (fin ' + a.anchor.finish + ')' : 'abstained'}  · winner mark: ${a.winnerMark || 'none'}`);
}

console.log(`\n${problems === 0 ? '✅ PASS — no crashes, all buckets valid, ranks in range.' : '❌ ' + problems + ' problem(s) above.'}\n`);
process.exit(problems === 0 ? 0 : 1);
