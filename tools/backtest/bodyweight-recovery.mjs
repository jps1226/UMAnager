// ============================================================
// BODY-WEIGHT-CHANGE RECOVERY (MONEY) TEST — TARGET's 馬体重増減 angle (TARGET scope, Oracle s53).
//
// TARGET surfaces each horse's body weight + change vs last start (馬体重 / 増減差). Handicapping lore:
// big swings (heavy gain = unfit / over-weight, big loss = illness / over-trained) precede poor runs, while
// steady weight = reliable condition; debutants/young horses are a different story. The change is announced
// ~1h pre-post, so it is genuinely a FORECAST factor (point-in-time clean — known before the race).
//
// North-Star question (s51-52): the market sees the weight too. Does the swing predict place recovery WITHIN
// the same odds band, beyond what the price already knows? Test = bucket each runner by its signed change,
// settle a flat PLACE bet at the real dividend, stratify by FavRank band, compare to the field.
//
// Data: fixtures/weights.json (raw SE @325 weight / @328 sign / @329 change, DataKubun 7). 000=no change,
// space=first-start/unknown (dropped from the swing buckets). Read-only.
// Run:  node tools/backtest/bodyweight-recovery.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const bench = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'history.json'), 'utf8'));
const weights = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'weights.json'), 'utf8'));

const STAKE = 100;
// signed change in kg, or null when unknown (first start / unweighable)
const wgByKey = new Map();
for (const w of weights) {
  const mag = /^[0-9]{3}$/.test(w.cg) ? parseInt(w.cg, 10) : null;
  let chg = null;
  if (mag != null && mag !== 999) chg = w.sg === '+' ? mag : w.sg === '-' ? -mag : (mag === 0 ? 0 : null);
  wgByKey.set(`${w.rid}|${w.h}`, chg);
}

// swing bucket
const bucketOf = c => c == null ? null
  : c <= -10 ? 'big minus (≤-10)'
  : c <= -4 ? 'minus (-9..-4)'
  : c <= 3 ? 'steady (-3..+3)'
  : c <= 9 ? 'plus (+4..+9)'
  : 'big plus (≥+10)';
const BUCKETS = ['big minus (≤-10)', 'minus (-9..-4)', 'steady (-3..+3)', 'plus (+4..+9)', 'big plus (≥+10)'];
const favBand = f => (f == null ? null : f <= 3 ? 'chalk' : f <= 8 ? 'mid' : 'long');
const BANDS = ['chalk', 'mid', 'long'];

// cell[bucket][band] = recovery accumulator
const cell = {};
const grp = (bk, bd) => (((cell[bk] ??= {})[bd] ??= { staked: 0, returned: 0, bets: 0, hits: 0 }));
const field = {}; const fgrp = bd => (field[bd] ??= { staked: 0, returned: 0, bets: 0, hits: 0 });

const byDate = bench.past_races_by_date || {};
const diag = { STEADY: [], BIGSWING: [], FIELD: [] };   // longshot-band per-bet {date, ret}
let settledRaces = 0, withWt = 0;
for (const date of Object.keys(byDate)) {
  for (const race of byDate[date]) {
    const rid = race.info.race_id;
    let payouts = null; try { payouts = JSON.parse(race.info.results_json); } catch (_) {}
    const placeDivs = payouts && payouts.place;
    if (!placeDivs || !placeDivs.length) continue;
    settledRaces++;
    const retOf = pp => { const w = placeDivs.find(d => d.combo && d.combo[0] === pp); return w ? w.payout : 0; };
    for (const e of (race.entries || [])) {
      if (e.Scratched === true) continue;
      const pp = parseInt(e.PP, 10); if (!Number.isFinite(pp)) continue;
      const band = favBand(parseInt(e.Fav, 10)); if (!band) continue;
      const hid = String(e.Horse_ID).split('.')[0];
      const chg = wgByKey.get(`${rid}|${hid}`);
      const bk = bucketOf(chg);
      const ret = retOf(pp);
      const fb = fgrp(band); fb.staked += STAKE; fb.bets++; if (ret) { fb.returned += ret; fb.hits++; }
      if (band === 'long') diag.FIELD.push({ date, ret });
      if (!bk) continue;
      withWt++;
      const c = grp(bk, band); c.staked += STAKE; c.bets++; if (ret) { c.returned += ret; c.hits++; }
      if (band === 'long') {
        if (bk === 'steady (-3..+3)') diag.STEADY.push({ date, ret });
        else if (bk === 'big minus (≤-10)' || bk === 'big plus (≥+10)') diag.BIGSWING.push({ date, ret });
      }
    }
  }
}

const rec = c => (c && c.staked ? (100 * c.returned / c.staked).toFixed(1) + '%' : '—');
const hr = c => (c && c.bets ? (100 * c.hits / c.bets).toFixed(0) + '%' : '—');
const roll = bk => { const t = { staked: 0, returned: 0, bets: 0, hits: 0 }; for (const b of BANDS) { const c = cell[bk]?.[b]; if (c) { t.staked += c.staked; t.returned += c.returned; t.bets += c.bets; t.hits += c.hits; } } return t; };

console.log(`\nBODY-WEIGHT-CHANGE RECOVERY (PLACE / 複勝) — ${settledRaces} settled races, ${withWt} runners with weight, ¥${STAKE}/bet`);
console.log('signed change vs last start (announced ~1h pre-post → point-in-time)\n');
const W = 18;
console.log('bucket'.padEnd(W) + BANDS.map(b => b.padStart(10)).join('') + 'ALL'.padStart(12) + 'bets'.padStart(8));
console.log('FIELD (all)'.padEnd(W) + BANDS.map(b => rec(field[b]).padStart(10)).join('') +
  rec(['chalk', 'mid', 'long'].reduce((t, b) => { const c = field[b]; t.staked += c.staked; t.returned += c.returned; return t; }, { staked: 0, returned: 0 })).padStart(12) +
  String(BANDS.reduce((n, b) => n + (field[b]?.bets || 0), 0)).padStart(8));
console.log('');
for (const bk of BUCKETS) {
  const line = bk.padEnd(W) + BANDS.map(b => rec(cell[bk]?.[b]).padStart(10)).join('');
  const t = roll(bk);
  console.log(line + rec(t).padStart(12) + String(t.bets).padStart(8));
}
console.log('\nhit% (place) by bucket × band:');
for (const bk of BUCKETS) console.log(bk.padEnd(W) + BANDS.map(b => hr(cell[bk]?.[b]).padStart(10)).join(''));

// robustness on the longshot band (the H12/H13 disciplines)
const recOf = a => a.length ? (a.reduce((s, x) => s + x.ret, 0) / a.length).toFixed(1) + '%' : '—';
const dropTop = (a, k) => { const s = [...a].sort((x, y) => y.ret - x.ret).slice(k); return s.length ? (s.reduce((t, x) => t + x.ret, 0) / s.length).toFixed(1) + '%' : '—'; };
const dates = [...new Set([].concat(...Object.values(diag)).map(x => x.date))].sort();
const mid = dates[Math.floor(dates.length / 2)];
const half = (a, lo, hi) => a.filter(x => x.date >= lo && x.date <= hi);
console.log('\n── ROBUSTNESS (longshot band) ─────────────────────────────────────');
console.log('group        H1 recov   H2 recov   full   drop-top3   drop-top5     bets');
for (const g of ['STEADY', 'BIGSWING', 'FIELD']) {
  const a = diag[g]; const h1 = half(a, dates[0], mid), h2 = half(a, mid, dates[dates.length - 1]);
  console.log(g.padEnd(12) + recOf(h1).padStart(8) + recOf(h2).padStart(11) + recOf(a).padStart(9) + dropTop(a, 3).padStart(12) + dropTop(a, 5).padStart(12) + String(a.length).padStart(9));
}
console.log('\nRead: STEADY beats FIELD in BOTH halves + survives a tail-drop = a usable overlay; BIGSWING << FIELD');
console.log('in both halves = a usable fade. A swing bucket >> FIELD in the same band = weight the market underprices.\n');
