// ============================================================
// LONGSHOT COMBO — are the longshot edges independent, or one effect seen from different sides?
//
// Every money edge so far lives in the LONGSHOT band (FavRank 9+). This tool combines them to ask:
//   • Does removing the switch-to-dirt TRAP (H8) lift the general longshot baseline?
//   • Is a "clean" positive angle (fresh H7 / cut-back H9) that is NOT also switching to dirt better
//     than the angle alone?
//   • How much do fresh (H7) and cut-back (H9) OVERLAP — i.e. is H9 a separate edge or the same horses?
//
// All point-in-time from finish_history (layoff + prior surface/distance vs the previous run only).
// Longshot band only. Flat ¥100 PLACE bet; recovery = ¥ back ÷ ¥ staked. Read-only.
// Run:  node tools/backtest/longshot-combo.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const hist = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'finish_history.json'), 'utf8'));
const bench = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'history.json'), 'utf8'));

const STAKE = 100;
const dayDiff = (a, b) => { const t = s => Date.UTC(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8)); return Math.round((t(a) - t(b)) / 86400000); };

// per-horse chronological → { layoff, priorS, priorD } as of each run
const byHorse = new Map();
for (const r of hist) { let a = byHorse.get(r.h); if (!a) { a = []; byHorse.set(r.h, a); } a.push(r); }
for (const a of byHorse.values()) a.sort((x, y) => (x.ymd < y.ymd ? -1 : 1));
const asof = new Map(); // `${rid}|${h}` → {layoff, priorS, priorD}
for (const arr of byHorse.values()) {
  let prev = null;
  for (const r of arr) { asof.set(`${r.rid}|${r.h}`, prev ? { layoff: dayDiff(r.ymd, prev.ymd), priorS: prev.s, priorD: prev.d } : null); prev = r; }
}

// Collect every longshot (FavRank 9+) bet with its angle flags + place outcome.
const bets = []; // {ret, fresh, cutback, toDirt}
const byDate = bench.past_races_by_date || {};
const dates = Object.keys(byDate).sort();
const mid = dates[Math.floor(dates.length / 2)];
for (const date of dates) {
  for (const race of byDate[date]) {
    let payouts = null; try { payouts = JSON.parse(race.info.results_json); } catch (_) {}
    const placeDivs = payouts && payouts.place;
    if (!placeDivs || !placeDivs.length) continue;
    const tS = race.info.surface, tD = parseInt(race.info.distance, 10), rid = race.info.race_id;
    for (const e of (race.entries || [])) {
      if (e.Scratched === true) continue;
      const fav = parseInt(e.Fav, 10);
      if (!(fav >= 9)) continue;                 // longshot band only
      const pp = parseInt(e.PP, 10); if (!Number.isFinite(pp)) continue;
      const a = asof.get(`${rid}|${String(e.Horse_ID).split('.')[0]}`);
      if (!a) continue;                          // debut / not indexed
      const win = placeDivs.find(d => d.combo && d.combo[0] === pp);
      bets.push({
        ret: win ? win.payout * (STAKE / 100) : 0,
        half: date < mid ? 'H1' : 'H2',
        fresh: a.layoff != null && a.layoff >= 61 && a.layoff <= 120,
        cutback: a.priorD && tD && (tD - a.priorD) <= -200,
        toDirt: a.priorS && a.priorS !== 'jump' && tS === 'dirt' && a.priorS !== 'dirt',
      });
    }
  }
}

const tally = filter => {
  const sel = bets.filter(filter);
  const staked = sel.length * STAKE, ret = sel.reduce((s, b) => s + b.ret, 0);
  const hits = sel.filter(b => b.ret > 0).length;
  return { rec: staked ? (100 * ret / staked) : 0, n: sel.length, hits };
};
const line = (label, filter) => {
  const t = tally(filter);
  const h1 = tally(b => filter(b) && b.half === 'H1'), h2 = tally(b => filter(b) && b.half === 'H2');
  const split = (t.n >= 50) ? `   [H1 ${h1.rec.toFixed(0)}% / H2 ${h2.rec.toFixed(0)}%]` : '';
  console.log('  ' + label.padEnd(34) + `${t.rec.toFixed(1)}%`.padStart(8) + `  (hit ${(100 * t.hits / Math.max(t.n,1)).toFixed(0)}%, n=${t.n})`.padEnd(20) + split);
};

console.log(`\nLONGSHOT COMBO (FavRank 9+, PLACE) — point-in-time, ${bets.length} longshot bets, ¥${STAKE} each\n`);
console.log('── baselines ──');
line('ALL longshots', () => true);
line('  minus switch-to-dirt (H8 removed)', b => !b.toDirt);
line('  switch-to-dirt only (the trap)', b => b.toDirt);

console.log('\n── positive angles, raw vs "clean" (not switching to dirt) ──');
line('fresh (H7)', b => b.fresh);
line('  fresh & NOT to-dirt', b => b.fresh && !b.toDirt);
line('cut-back (H9)', b => b.cutback);
line('  cut-back & NOT to-dirt', b => b.cutback && !b.toDirt);
line('fresh OR cut-back', b => b.fresh || b.cutback);
line('  (fresh OR cut-back) & NOT to-dirt', b => (b.fresh || b.cutback) && !b.toDirt);

console.log('\n── does the dirt trap poison the positive angles? ──');
line('fresh & to-dirt', b => b.fresh && b.toDirt);
line('cut-back & to-dirt', b => b.cutback && b.toDirt);

console.log('\n── overlap (are H7 and H9 the same horses?) ──');
line('fresh & cut-back (both)', b => b.fresh && b.cutback);
line('fresh & NOT cut-back', b => b.fresh && !b.cutback);
line('cut-back & NOT fresh', b => b.cutback && !b.fresh);
const f = bets.filter(b => b.fresh).length, c = bets.filter(b => b.cutback).length, both = bets.filter(b => b.fresh && b.cutback).length;
console.log(`\n  overlap: ${both} bets are BOTH fresh & cut-back  (= ${(100*both/Math.max(f,1)).toFixed(0)}% of fresh, ${(100*both/Math.max(c,1)).toFixed(0)}% of cut-back)`);
console.log('');
