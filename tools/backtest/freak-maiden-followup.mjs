// ============================================================
// "Are freak maiden/debut winners worth watching, or flukes?" (s54 probe)
//
// QUESTION (operator): when a longshot wins a DEBUT (first career start) or breaks its
// MAIDEN (first career win) at a surprising price, does it go on to be a useful horse,
// or was it a one-off fluke?
//
// METHOD: read the full clean finish history. For every horse, walk its runs in date
// order and tag each with priorRuns / priorWins. Then bucket the WINNERS:
//   • DEBUT win   = won (f==1) with priorRuns == 0.
//   • MAIDEN break= won with priorWins == 0 and priorRuns >= 1.
// Split each by the market's read at the time (fav rank): FAVORED (1-3), SURPRISE (4-6),
// BOMB (7+). For each cohort, measure the horse's FORWARD record (all later starts):
//   ran again? · won again? · future win% · future place% · next-start place%.
// If the BOMB/SURPRISE cohorts go on similarly to FAVORED winners → worth watching.
// If they crater → flukes. (finish_history.json lacks the scoring fields, so this uses
// fav-rank as the surprise proxy, not the exact autopsy merit grade — directional read.)
//
// READ-ONLY. Run:  node tools/backtest/freak-maiden-followup.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fixturesDir, pad } from './harness.mjs';

const rows = JSON.parse(fs.readFileSync(path.join(fixturesDir, 'finish_history.json'), 'utf8'));

// Group by horse, sort by date (ymd), then race id for same-day stability.
const byHorse = new Map();
for (const r of rows) {
  const f = parseInt(r.f, 10);
  const fav = parseInt(r.fav, 10);
  if (!byHorse.has(r.h)) byHorse.set(r.h, []);
  byHorse.get(r.h).push({ ymd: String(r.ymd), rid: String(r.rid), f: Number.isFinite(f) ? f : null, fav: Number.isFinite(fav) ? fav : null });
}
for (const runs of byHorse.values())
  runs.sort((a, b) => a.ymd === b.ymd ? a.rid.localeCompare(b.rid) : a.ymd.localeCompare(b.ymd));

const band = (fav) => fav == null ? null : fav <= 3 ? 'favored' : fav <= 6 ? 'surprise' : 'bomb';

// cohorts[type][band] = array of forward-record objects
const cohorts = { debut: { favored: [], surprise: [], bomb: [] }, maiden: { favored: [], surprise: [], bomb: [] } };

for (const runs of byHorse.values()) {
  let priorRuns = 0, priorWins = 0;
  for (let i = 0; i < runs.length; i++) {
    const cur = runs[i];
    const won = cur.f === 1;
    let type = null;
    if (won && priorRuns === 0) type = 'debut';
    else if (won && priorWins === 0 && priorRuns >= 1) type = 'maiden';

    if (type) {
      const b = band(cur.fav);
      if (b) {
        const future = runs.slice(i + 1).filter(x => x.f != null && x.f >= 1); // genuine subsequent starts
        const fStarts = future.length;
        const fWins = future.filter(x => x.f === 1).length;
        const fPlace = future.filter(x => x.f <= 3).length;
        const nextPlace = fStarts > 0 ? (future[0].f <= 3 ? 1 : 0) : null;
        cohorts[type][b].push({ fStarts, fWins, fPlace, nextPlace, ranAgain: fStarts > 0 });
      }
    }
    priorRuns++;
    if (won) priorWins++;
  }
}

function summarize(arr) {
  const n = arr.length;
  if (!n) return null;
  const ranAgain = arr.filter(x => x.ranAgain).length;
  const withFuture = arr.filter(x => x.fStarts > 0);
  const wonAgain = withFuture.filter(x => x.fWins > 0).length;
  const totFStarts = withFuture.reduce((s, x) => s + x.fStarts, 0);
  const totFWins = withFuture.reduce((s, x) => s + x.fWins, 0);
  const totFPlace = withFuture.reduce((s, x) => s + x.fPlace, 0);
  const nextPlaceN = withFuture.filter(x => x.nextPlace === 1).length;
  return {
    n,
    ranAgainPct: ranAgain / n * 100,
    avgFutureStarts: withFuture.length ? totFStarts / withFuture.length : 0,
    wonAgainPct: withFuture.length ? wonAgain / withFuture.length * 100 : 0,
    futureWinPct: totFStarts ? totFWins / totFStarts * 100 : 0,
    futurePlacePct: totFStarts ? totFPlace / totFStarts * 100 : 0,
    nextPlacePct: withFuture.length ? nextPlaceN / withFuture.length * 100 : 0,
  };
}

const p1 = (x) => x == null ? '   —' : pad(x.toFixed(0) + '%', 5);
const f1 = (x) => x == null ? '  —' : pad(x.toFixed(1), 4);

function printType(type, title) {
  console.log(`\n## ${title}`);
  console.log(`  ${pad('cohort', 9)}  ${pad('n', 5)}  ranAgain  avgFut  wonAgain  futWin%  futPlace%  nextPlace%`);
  for (const b of ['favored', 'surprise', 'bomb']) {
    const s = summarize(cohorts[type][b]);
    if (!s) { console.log(`  ${pad(b, 9)}   (none)`); continue; }
    console.log(`  ${pad(b, 9)}  ${pad(s.n, 5)}   ${p1(s.ranAgainPct)}   ${f1(s.avgFutureStarts)}    ${p1(s.wonAgainPct)}   ${p1(s.futureWinPct)}    ${p1(s.futurePlacePct)}     ${p1(s.nextPlacePct)}`);
  }
}

console.log(`\nFreak/longshot maiden & debut winners — do they go on? (${rows.length} runs, ${byHorse.size} horses)`);
console.log(`FAVORED = won at fav rank 1-3 · SURPRISE = 4-6 · BOMB = 7+ (the "freak" proxy)`);
console.log(`'wonAgain%/futWin%/futPlace%/nextPlace%' are computed only over horses that DID start again.`);

printType('debut', 'DEBUT winners (won first career start)');
printType('maiden', 'MAIDEN-break winners (first career win, had run before)');

console.log(`\n── Read ──`);
console.log(`Compare the BOMB/SURPRISE rows to FAVORED within each block. If future win/place hold up,`);
console.log(`the surprise winners were hidden ability worth watching; if they fall off a cliff, flukes.\n`);
