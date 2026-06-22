// ============================================================
// TRANSITION RECOVERY (MONEY) TEST — Group-A factors (§0 Step 1.6 / Step 2).
//
// Two classic handicapping angles, tested for a MONEY edge the same way layoff was:
//   • SURFACE SWITCH — is the horse on a different surface than its last start? (first-time/back-to dirt,
//     or dirt→turf). The crowd often mis-judges surface switches → possible value.
//   • DISTANCE MOVE — is today much shorter (cut-back) or longer (stretch-out) than its last start?
//
// Both are derived point-in-time at FULL coverage from finish_history (each prior run carries surface `s`
// and distance `d`), comparing ONLY to the horse's previous run — no look-ahead. Settlement = flat ¥100
// PLACE bet from stored dividends; recovery = ¥ back ÷ ¥ staked (takeout already in the dividends). The
// test is whether a transition bucket out-recovers the FIELD baseline WITHIN its market band.
//
// Read-only. Run:  node tools/backtest/transition-recovery.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const hist = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'finish_history.json'), 'utf8'));
const bench = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'history.json'), 'utf8'));

const STAKE = 100;
const CUT = +(process.env.TR_CUT ?? 200);   // metres that count as a real distance move
const favBand = f => (f == null ? null : f <= 3 ? 'chalk (1-3)' : f <= 8 ? 'mid (4-8)' : 'longshot (9+)');
const BANDS = ['chalk (1-3)', 'mid (4-8)', 'longshot (9+)'];

// per-horse chronological → PRIOR run's surface + distance, keyed by `rid|h`
const byHorse = new Map();
for (const r of hist) { let a = byHorse.get(r.h); if (!a) { a = []; byHorse.set(r.h, a); } a.push(r); }
for (const a of byHorse.values()) a.sort((x, y) => (x.ymd < y.ymd ? -1 : 1));
const prior = new Map(); // `${rid}|${h}` → { s, d } of the previous run (null if debut)
for (const arr of byHorse.values()) {
  let prev = null;
  for (const r of arr) { prior.set(`${r.rid}|${r.h}`, prev ? { s: prev.s, d: prev.d } : null); prev = r; }
}

const surfaceBucket = (prevS, todayS) => {
  if (!prevS || !todayS || prevS === 'jump' || todayS === 'jump') return null;
  if (prevS === todayS) return 'same surface';
  if (todayS === 'dirt') return 'switch → dirt';
  if (todayS === 'turf') return 'switch → turf';
  return null;
};
const distBucket = (prevD, todayD) => {
  if (!prevD || !todayD) return null;
  const delta = todayD - prevD;
  if (delta <= -CUT) return 'cut-back (shorter)';
  if (delta >= CUT) return 'stretch-out (longer)';
  return 'same trip';
};

// cell[dim][bucket][band] = {staked, returned, bets, hits}
const cell = {};
const grp = (dim, bk, b) => ((((cell[dim] ??= {})[bk] ??= {})[b] ??= { staked: 0, returned: 0, bets: 0, hits: 0 }));
const place = (dim, bk, b, pp, placeDivs) => {
  if (!bk) return;
  const c = grp(dim, bk, b); c.staked += STAKE; c.bets++;
  const win = placeDivs.find(d => d.combo && d.combo[0] === pp);
  if (win) { c.returned += win.payout * (STAKE / 100); c.hits++; }
};

const byDate = bench.past_races_by_date || {};
let settledRaces = 0;
for (const date of Object.keys(byDate)) {
  for (const race of byDate[date]) {
    let payouts = null; try { payouts = JSON.parse(race.info.results_json); } catch (_) {}
    const placeDivs = payouts && payouts.place;
    if (!placeDivs || !placeDivs.length) continue;
    settledRaces++;
    const todayS = race.info.surface, todayD = parseInt(race.info.distance, 10);
    const rid = race.info.race_id;
    for (const e of (race.entries || [])) {
      if (e.Scratched === true) continue;
      const pp = parseInt(e.PP, 10); if (!Number.isFinite(pp)) continue;
      const band = favBand(parseInt(e.Fav, 10)); if (!band) continue;
      const hid = String(e.Horse_ID).split('.')[0];
      place('FIELD', 'FIELD (all)', band, pp, placeDivs);
      const p = prior.get(`${rid}|${hid}`);
      if (!p) continue; // debut / not indexed
      place('SURFACE', surfaceBucket(p.s, todayS), band, pp, placeDivs);
      place('DISTANCE', distBucket(p.d, todayD), band, pp, placeDivs);
    }
  }
}

const rec = c => (c.staked ? (100 * c.returned / c.staked).toFixed(1) + '%' : '—');
const hr = c => (c.bets ? (100 * c.hits / c.bets).toFixed(0) + '%' : '—');
const field = b => cell.FIELD['FIELD (all)'][b];

function table(dim, buckets) {
  console.log(`\n── ${dim} : place recovery (hit, n) and Δ vs FIELD, by market band ──`);
  console.log('bucket'.padEnd(20) + BANDS.map(b => b.padStart(22)).join(''));
  // FIELD reference row
  let f = 'FIELD baseline'.padEnd(20);
  for (const b of BANDS) f += `${rec(field(b))} (${hr(field(b))}, n=${field(b).bets})`.padStart(22);
  console.log(f);
  for (const bk of buckets) {
    let line = bk.padEnd(20);
    for (const b of BANDS) {
      const c = cell[dim]?.[bk]?.[b];
      if (!c || !c.staked) { line += '—'.padStart(22); continue; }
      const d = (100 * c.returned / c.staked) - (100 * field(b).returned / field(b).staked);
      line += `${rec(c)} (${(d >= 0 ? '+' : '') + d.toFixed(0)}, n=${c.bets})`.padStart(22);
    }
    console.log(line);
  }
}

console.log(`\nTRANSITION RECOVERY (PLACE / 複勝) — point-in-time, ${settledRaces} settled races, ¥${STAKE}/bet, move=${CUT}m`);
console.log('Δ = recovery minus the FIELD baseline in the SAME band. Big + with healthy n = candidate edge.');
table('SURFACE', ['same surface', 'switch → dirt', 'switch → turf']);
table('DISTANCE', ['same trip', 'cut-back (shorter)', 'stretch-out (longer)']);
console.log('');
