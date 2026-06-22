// ============================================================
// GOING-EDGE TEST — does the market underprice WET-track form? (§0 Step 2, Group-B factor #1)
//
// The honest question: on a WET track, do horses with a strong prior WET record place MORE than the
// market (their favourite-rank) implies — and, the crucial control, do they NOT show that same edge on
// DRY tracks (which would mean they're just good horses, not wet specialists)?
//
// Pure point-in-time stats from finish_history (now carries `going` per run). No dividends needed —
// this measures PREDICTIVENESS (placing beyond market rank), the precondition for a betting edge.
//   wetEdge = priorWetPlace% − priorOverallPlace%  (>+10 = "wet-lover", <−10 = "wet-hater")
// Within each market favourite-band we compare wet-lovers vs wet-haters' actual place rate, on wet
// races and (control) on dry races. A real edge = lovers beat haters on WET but not on DRY.
//
// Read-only. Run:  node tools/backtest/going-edge.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const rows = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'finish_history.json'), 'utf8'));

// index by horse, chronological
const byHorse = new Map();
for (const r of rows) { let a = byHorse.get(r.h); if (!a) { a = []; byHorse.set(r.h, a); } a.push(r); }
for (const a of byHorse.values()) a.sort((x, y) => (x.ymd < y.ymd ? -1 : 1));

const MIN_OVERALL = 5, MIN_WET = 3, EDGE = 10;
const favBand = f => (f == null ? null : f <= 3 ? '1-3 (chalk)' : f <= 8 ? '4-8 (mid)' : '9+ (longshot)');
const placed = r => r.f >= 1 && r.f <= 3;

// cells[wetDry][band][lover/hater] = { n, placed }
const cells = {};
const bump = (cond, band, grp, did) => {
  ((cells[cond] ??= {})[band] ??= {})[grp] ??= { n: 0, placed: 0 };
  cells[cond][band][grp].n++; if (did) cells[cond][band][grp].placed++;
};

for (const arr of byHorse.values()) {
  let ovS = 0, ovP = 0, wS = 0, wP = 0;  // running PRIOR tallies (strictly before the current run)
  for (const r of arr) {
    // classify this run using ONLY prior tallies
    if (r.going != null && ovS >= MIN_OVERALL && wS >= MIN_WET && r.fav != null) {
      const wetPlacePct = 100 * wP / wS, ovPlacePct = 100 * ovP / ovS;
      const wetEdge = wetPlacePct - ovPlacePct;
      const grp = wetEdge > EDGE ? 'wet-lover' : wetEdge < -EDGE ? 'wet-hater' : null;
      const band = favBand(r.fav);
      if (grp && band) bump(r.going >= 3 ? 'WET race' : 'DRY race', band, grp, placed(r));
    }
    // now fold this run into the tallies for subsequent runs
    ovS++; if (placed(r)) ovP++;
    if (r.going != null && r.going >= 3) { wS++; if (placed(r)) wP++; }
  }
}

const pct = (p, n) => n ? (100 * p / n).toFixed(1) + '%' : '—';
console.log('\nGOING-EDGE TEST — does the market underprice WET form? (point-in-time, finish_history)\n');
console.log('Place rate (top-3) by market favourite-band × wet-form group.');
console.log('EDGE = a wet-lover beats a wet-hater on WET races but NOT on DRY (same market band).\n');
for (const cond of ['WET race', 'DRY race']) {
  console.log(`── ${cond} ──`);
  console.log('  fav band         wet-lover place%   wet-hater place%   lover−hater');
  for (const band of ['1-3 (chalk)', '4-8 (mid)', '9+ (longshot)']) {
    const lo = cells[cond]?.[band]?.['wet-lover'] || { n: 0, placed: 0 };
    const ha = cells[cond]?.[band]?.['wet-hater'] || { n: 0, placed: 0 };
    const diff = (lo.n && ha.n) ? (100 * lo.placed / lo.n - 100 * ha.placed / ha.n).toFixed(1) + ' pts' : '—';
    console.log(`  ${band.padEnd(15)}  ${(pct(lo.placed, lo.n) + ` (n=${lo.n})`).padEnd(18)} ${(pct(ha.placed, ha.n) + ` (n=${ha.n})`).padEnd(18)} ${diff}`);
  }
  console.log('');
}
console.log('Read: a positive lover−hater gap on WET that vanishes (or reverses) on DRY = a real wet-form');
console.log('signal the favourite-rank misses → a candidate factor. A similar gap on DRY = just good horses.\n');
