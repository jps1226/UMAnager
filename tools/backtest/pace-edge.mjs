// ============================================================
// PACE / CLOSING-KICK EDGE TEST — Group-B factor #2 (§0 Step 2)
//
// Two run-shape signals from the SE results data (corner positions + last-3F), tested point-in-time:
//   • RUNNING STYLE — a horse's prior average early position (first corner ÷ field size): front-runner
//     (low) vs closer (high). Does a style beat its market favourite-rank?
//   • CLOSING KICK — a horse's prior rate of posting a TOP-3 last-3F in its races (a proven finisher).
//     Within each favourite-band, do strong closers out-place weak ones (same market rank → mispricing)?
//
// Pure point-in-time stats from finish_history (now carries rid, c1-c4, l3f, n). The within-favourite-
// band comparison is the control for "just good horses" (same market rank, different run-shape).
//
// Read-only. Run:  node tools/backtest/pace-edge.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const rows = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'finish_history.json'), 'utf8'));

const corner = s => { const v = parseInt(s, 10); return Number.isFinite(v) && v > 0 ? v : null; };
const l3f = s => { const v = parseInt(s, 10); return Number.isFinite(v) && v >= 250 && v <= 500 ? v : null; }; // 25.0-50.0s
const placed = r => r.f >= 1 && r.f <= 3;

// 1) per-run run-shape: early position fraction + closing rank within the race
const byRace = new Map();
for (const r of rows) { let a = byRace.get(r.rid); if (!a) { a = []; byRace.set(r.rid, a); } a.push(r); }
for (const r of rows) {
  const first = corner(r.c1) ?? corner(r.c2) ?? corner(r.c3) ?? corner(r.c4);
  r._style = (first != null && r.n > 1) ? (first - 1) / (r.n - 1) : null; // 0 = led, 1 = last early
  r._l3f = l3f(r.l3f);
}
for (const field of byRace.values()) {
  const valid = field.filter(r => r._l3f != null).sort((a, b) => a._l3f - b._l3f);
  valid.forEach((r, i) => { r._closeRank = i + 1; });
}

// 2) per-horse chronological → prior style + prior fast-closer rate (strictly before each run)
const byHorse = new Map();
for (const r of rows) { let a = byHorse.get(r.h); if (!a) { a = []; byHorse.set(r.h, a); } a.push(r); }
for (const a of byHorse.values()) a.sort((x, y) => (x.ymd < y.ymd ? -1 : 1));

const MIN = 3;
const favBand = f => (f == null ? null : f <= 3 ? '1-3 (chalk)' : f <= 8 ? '4-8 (mid)' : '9+ (longshot)');
const cell = {}; // cell[test][band][group] = {n, placed}
const bump = (test, band, grp, did) => { (((cell[test] ??= {})[band] ??= {})[grp] ??= { n: 0, placed: 0 }); cell[test][band][grp].n++; if (did) cell[test][band][grp].placed++; };

for (const arr of byHorse.values()) {
  const styles = [], closes = [];
  for (const r of arr) {
    const band = favBand(r.fav);
    if (band && styles.length >= MIN) {
      const ps = styles.reduce((a, b) => a + b, 0) / styles.length;
      const grp = ps < 0.35 ? 'front-runner' : ps > 0.60 ? 'closer' : null;
      if (grp) bump('RUNNING STYLE', band, grp, placed(r));
    }
    if (band && closes.length >= MIN) {
      const pc = closes.reduce((a, b) => a + b, 0) / closes.length;
      const grp = pc >= 0.50 ? 'strong-closer' : pc <= 0.25 ? 'weak-closer' : null;
      if (grp) bump('CLOSING KICK', band, grp, placed(r));
    }
    // fold this run into the priors for later runs
    if (r._style != null) styles.push(r._style);
    if (r._closeRank != null) closes.push(r._closeRank <= 3 ? 1 : 0);
  }
}

const pct = (p, n) => n ? (100 * p / n).toFixed(1) + '%' : '—';
console.log(`\nPACE / CLOSING-KICK EDGE TEST — point-in-time, ${rows.length} runs\n`);
const show = (test, gA, gB) => {
  console.log(`── ${test}: ${gA} vs ${gB} (place rate by market band) ──`);
  console.log('  fav band          ' + gA.padEnd(20) + gB.padEnd(20) + 'A−B');
  for (const band of ['1-3 (chalk)', '4-8 (mid)', '9+ (longshot)']) {
    const a = cell[test]?.[band]?.[gA] || { n: 0, placed: 0 };
    const b = cell[test]?.[band]?.[gB] || { n: 0, placed: 0 };
    const diff = (a.n && b.n) ? ((100 * a.placed / a.n - 100 * b.placed / b.n).toFixed(1) + ' pts') : '—';
    console.log(`  ${band.padEnd(16)}  ${(pct(a.placed, a.n) + ` (n=${a.n})`).padEnd(20)}${(pct(b.placed, b.n) + ` (n=${b.n})`).padEnd(20)}${diff}`);
  }
  console.log('');
};
show('RUNNING STYLE', 'front-runner', 'closer');
show('CLOSING KICK', 'strong-closer', 'weak-closer');
console.log('Read: a large place-rate gap WITHIN a market band = the favourite-rank isn\'t pricing that');
console.log('run-shape → candidate edge. A gap only in chalk (low fav) is likely just better horses.\n');
