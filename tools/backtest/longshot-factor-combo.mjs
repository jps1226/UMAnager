// ============================================================
// LONGSHOT FACTOR COMBO — do the four longshot factors STACK into one filter, or re-find the same horses?
//
// The session's robust longshot signals, all in the FavRank-9+ band:
//   FADES  → H8 switch-to-dirt · H13 late odds-DRIFT · H14 BIG weight-swing (±10kg)
//   BACKS  → H7 fresh (61-120d layoff) · H14 STEADY weight (±3kg)
// This asks the independence question (the H9 lesson — test before crediting an "edge"):
//   • Does each FADE still under-perform after removing the OTHER fades? (is it its own effect?)
//   • Does an AVOIDANCE STACK (skip all three fades) lift the longshot baseline more than any one alone?
//   • Is the clean OVERLAY (fresh/steady AND none-of-the-fades) better than the raw angles?
//   • How much do the three fades OVERLAP (are dirt-switchers also drifters / big-swingers)?
//
// Point-in-time: layoff/prior-surface from finish_history; weight-change from weights.json (raw SE @325/329);
// drift from the bench's last-poll→final odds. Longshot band only, flat ¥100 PLACE.
// ⚠ Coverage: drift needs prev-odds (~35% of entries); "not drifting" can't see invisible drift, so the
// stack UNDER-states drift's contribution off the prev-odds subset. Read-only.
// Run:  node tools/backtest/longshot-factor-combo.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const hist = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'finish_history.json'), 'utf8'));
const bench = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'history.json'), 'utf8'));
const weights = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'weights.json'), 'utf8'));

const STAKE = 100;
const DRIFT = +(process.env.LC_DRIFT ?? 0.15);   // ±15% odds move
const dayDiff = (a, b) => { const t = s => Date.UTC(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8)); return Math.round((t(a) - t(b)) / 86400000); };

// per-horse chronological → { layoff, priorS, priorD } as of each run (mirrors longshot-combo)
const byHorse = new Map();
for (const r of hist) { let a = byHorse.get(r.h); if (!a) { a = []; byHorse.set(r.h, a); } a.push(r); }
for (const a of byHorse.values()) a.sort((x, y) => (x.ymd < y.ymd ? -1 : 1));
const asof = new Map();
for (const arr of byHorse.values()) { let prev = null; for (const r of arr) { asof.set(`${r.rid}|${r.h}`, prev ? { layoff: dayDiff(r.ymd, prev.ymd), priorS: prev.s, priorD: prev.d } : null); prev = r; } }

// weight change keyed rid|h
const wgByKey = new Map();
for (const w of weights) { const mag = /^[0-9]{3}$/.test(w.cg) ? parseInt(w.cg, 10) : null; let chg = null; if (mag != null && mag !== 999) chg = w.sg === '+' ? mag : w.sg === '-' ? -mag : (mag === 0 ? 0 : null); wgByKey.set(`${w.rid}|${w.h}`, chg); }

const bets = []; // {ret, half, fresh, toDirt, drift, hasPrev, bigSwing, steady}
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
      if (!(parseInt(e.Fav, 10) >= 9)) continue;          // longshot band only
      const pp = parseInt(e.PP, 10); if (!Number.isFinite(pp)) continue;
      const hid = String(e.Horse_ID).split('.')[0];
      const a = asof.get(`${rid}|${hid}`);
      if (!a) continue;                                    // debut / not indexed
      const win = placeDivs.find(d => d.combo && d.combo[0] === pp);
      const o = parseFloat(e.Odds), p = parseFloat(e.Prev_Odds);
      const hasPrev = o > 0 && p > 0;
      const chg = wgByKey.get(`${rid}|${hid}`);
      bets.push({
        ret: win ? win.payout * (STAKE / 100) : 0,
        half: date < mid ? 'H1' : 'H2',
        fresh: a.layoff != null && a.layoff >= 61 && a.layoff <= 120,
        toDirt: a.priorS && a.priorS !== 'jump' && tS === 'dirt' && a.priorS !== 'dirt',
        drift: hasPrev && (o / p) >= 1 + DRIFT,
        hasPrev,
        bigSwing: chg != null && Math.abs(chg) >= 10,
        steady: chg != null && Math.abs(chg) <= 3,
      });
    }
  }
}

const tally = filter => { const sel = bets.filter(filter); const ret = sel.reduce((s, b) => s + b.ret, 0); const hits = sel.filter(b => b.ret > 0).length; return { rec: sel.length ? 100 * ret / (sel.length * STAKE) : 0, n: sel.length, hits }; };
const line = (label, filter) => {
  const t = tally(filter), h1 = tally(b => filter(b) && b.half === 'H1'), h2 = tally(b => filter(b) && b.half === 'H2');
  const split = (t.n >= 50) ? `   [H1 ${h1.rec.toFixed(0)}% / H2 ${h2.rec.toFixed(0)}%]` : '   [thin]';
  console.log('  ' + label.padEnd(40) + `${t.rec.toFixed(1)}%`.padStart(8) + `  (hit ${(100 * t.hits / Math.max(t.n, 1)).toFixed(0)}%, n=${t.n})`.padEnd(20) + split);
};

console.log(`\nLONGSHOT FACTOR COMBO (FavRank 9+, PLACE) — point-in-time, ${bets.length} longshot bets, ¥${STAKE} each`);
const base = tally(() => true);
console.log(`baseline ALL longshots = ${base.rec.toFixed(1)}%  (n=${base.n})\n`);

console.log('── each FADE alone, then with the OTHER fades removed (is it its own effect?) ──');
line('switch-to-dirt (H8)', b => b.toDirt);
line('  H8 & not drift & not big-swing', b => b.toDirt && !b.drift && !b.bigSwing);
line('big weight-swing ±10 (H14)', b => b.bigSwing);
line('  H14swing & not to-dirt & not drift', b => b.bigSwing && !b.toDirt && !b.drift);
line('late drift (H13, prev-odds only)', b => b.drift);
line('  H13drift & not to-dirt & not big-swing', b => b.drift && !b.toDirt && !b.bigSwing);

console.log('\n── the AVOIDANCE STACK (skip the fades) vs baseline ──');
line('ALL longshots (baseline)', () => true);
line('skip switch-to-dirt', b => !b.toDirt);
line('skip dirt + big-swing', b => !b.toDirt && !b.bigSwing);
line('skip dirt + big-swing + drift', b => !b.toDirt && !b.bigSwing && !b.drift);

console.log('\n── the clean OVERLAY (a BACK among the survivors) ──');
line('fresh (H7)', b => b.fresh);
line('steady weight (H14)', b => b.steady);
line('fresh OR steady', b => b.fresh || b.steady);
line('  (fresh OR steady) & none-of-fades', b => (b.fresh || b.steady) && !b.toDirt && !b.bigSwing && !b.drift);
line('  fresh & steady & none-of-fades', b => b.fresh && b.steady && !b.toDirt && !b.bigSwing && !b.drift);

console.log('\n── fade OVERLAP (are they the same horses?) ──');
const N = bets.length, cnt = f => bets.filter(f).length;
const td = cnt(b => b.toDirt), bs = cnt(b => b.bigSwing), dr = cnt(b => b.drift);
console.log(`  to-dirt n=${td} · big-swing n=${bs} · drift n=${dr} (of ${N})`);
console.log(`  dirt∩swing=${cnt(b => b.toDirt && b.bigSwing)} · dirt∩drift=${cnt(b => b.toDirt && b.drift)} · swing∩drift=${cnt(b => b.bigSwing && b.drift)} · all3=${cnt(b => b.toDirt && b.bigSwing && b.drift)}`);
console.log(`  fresh∩steady=${cnt(b => b.fresh && b.steady)} (fresh n=${cnt(b => b.fresh)}, steady n=${cnt(b => b.steady)}) — do the BACKS overlap?`);
// tail-robustness on the headline overlay (fresh & steady & none-of-fades) and the avoidance stack
const dropTop = (filter, k) => { const s = bets.filter(filter).map(b => b.ret).sort((a, b) => b - a).slice(k); return s.length ? (100 * s.reduce((a, b) => a + b, 0) / (s.length * STAKE)).toFixed(1) + '%' : '—'; };
const combo = b => b.fresh && b.steady && !b.toDirt && !b.bigSwing && !b.drift;
const stack = b => !b.toDirt && !b.bigSwing && !b.drift;
console.log('\n── tail-robustness (drop the biggest place payouts) ──');
console.log(`  fresh & steady & none-of-fades:  full ${tally(combo).rec.toFixed(1)}% · drop-top3 ${dropTop(combo, 3)} · drop-top5 ${dropTop(combo, 5)}  (n=${tally(combo).n})`);
console.log(`  avoidance stack (skip all fades): full ${tally(stack).rec.toFixed(1)}% · drop-top3 ${dropTop(stack, 3)} · drop-top5 ${dropTop(stack, 5)}  (n=${tally(stack).n})`);
console.log(`  ALL longshots (baseline):        full ${base.rec.toFixed(1)}% · drop-top3 ${dropTop(() => true, 3)} · drop-top5 ${dropTop(() => true, 5)}  (n=${base.n})`);
console.log('\nRead: a fade that stays << baseline after removing the other fades = independent. The avoidance');
console.log('stack should beat baseline & each single skip if the fades are additive. Watch H1/H2 on the stack.\n');
