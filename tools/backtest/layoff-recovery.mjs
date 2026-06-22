// ============================================================
// LAYOFF RECOVERY (MONEY) TEST — Group-A factor (§0 Step 1.6 / Step 2).
//
// Does a horse's TIME SINCE ITS LAST RACE move money? "Layoff" angles (a fresh horse off a break, or a
// horse running back quick) are classic spots where the casual crowd may mis-bet — over-trusting recent
// runners, or shying off / piling onto returners. If the market mis-prices any layoff bucket, betting
// that bucket to PLACE should out-recover the field baseline WITHIN the same market band.
//
// Fully point-in-time and full-coverage: layoff = (race date − prior run date) from finish_history, which
// only ever looks at the PREVIOUS run, so there is no look-ahead and (unlike the l3f/closing data) every
// horse with a prior start has it. Same settlement as closing-kick-recovery: flat ¥100 place bet from the
// stored `place` dividends, recovery = ¥ back ÷ ¥ staked (takeout already in the dividends).
//
// Read-only. Run:  node tools/backtest/layoff-recovery.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const hist = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'finish_history.json'), 'utf8'));
const bench = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'history.json'), 'utf8'));

const STAKE = 100;
const favBand = f => (f == null ? null : f <= 3 ? 'chalk (1-3)' : f <= 8 ? 'mid (4-8)' : 'longshot (9+)');
const BANDS = ['chalk (1-3)', 'mid (4-8)', 'longshot (9+)'];

// Layoff buckets (days since prior run). Ordered; reported in this order.
const BUCKETS = ['debut', 'quick (≤21)', 'normal (22-60)', 'fresh (61-120)', 'layoff (121-180)', 'long (181+)'];
const bucketize = days => {
  if (days == null) return 'debut';
  if (days <= 21) return 'quick (≤21)';
  if (days <= 60) return 'normal (22-60)';
  if (days <= 120) return 'fresh (61-120)';
  if (days <= 180) return 'layoff (121-180)';
  return 'long (181+)';
};

// 'YYYYMMDD' difference in days (UTC, date-only → no tz drift).
const dayDiff = (a, b) => {
  const t = s => Date.UTC(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8));
  return Math.round((t(a) - t(b)) / 86400000);
};

// ── per-horse chronological → layoff AS OF each run, keyed by `rid|h` ──────────
const byHorse = new Map();
for (const r of hist) { let a = byHorse.get(r.h); if (!a) { a = []; byHorse.set(r.h, a); } a.push(r); }
for (const a of byHorse.values()) a.sort((x, y) => (x.ymd < y.ymd ? -1 : 1));

const layoff = new Map(); // `${rid}|${h}` → days since prior run (null = debut)
for (const arr of byHorse.values()) {
  let prev = null;
  for (const r of arr) {
    layoff.set(`${r.rid}|${r.h}`, prev ? dayDiff(r.ymd, prev) : null);
    prev = r.ymd;
  }
}

// ── settle place bets by bucket × band ────────────────────────────────────────
const cell = {}; // cell[bucket][band] = {staked, returned, bets, hits}
const grp = (g, b) => (((cell[g] ??= {})[b] ??= { staked: 0, returned: 0, bets: 0, hits: 0 }));
const place = (g, b, pp, placeDivs) => {
  const c = grp(g, b); c.staked += STAKE; c.bets++;
  const win = placeDivs.find(d => d.combo && d.combo[0] === pp);
  if (win) { c.returned += win.payout * (STAKE / 100); c.hits++; }
};

const byDate = bench.past_races_by_date || {};
let settledRaces = 0;
for (const date of Object.keys(byDate)) {
  for (const race of byDate[date]) {
    const rid = race.info.race_id;
    let payouts = null; try { payouts = JSON.parse(race.info.results_json); } catch (_) {}
    const placeDivs = payouts && payouts.place;
    if (!placeDivs || !placeDivs.length) continue;
    settledRaces++;
    for (const e of (race.entries || [])) {
      if (e.Scratched === true) continue;
      const pp = parseInt(e.PP, 10); if (!Number.isFinite(pp)) continue;
      const band = favBand(parseInt(e.Fav, 10)); if (!band) continue;
      const hid = String(e.Horse_ID).split('.')[0];
      place('FIELD (all)', band, pp, placeDivs);
      const key = `${rid}|${hid}`;
      if (!layoff.has(key)) continue; // horse not in history index (rare)
      place(bucketize(layoff.get(key)), band, pp, placeDivs);
    }
  }
}

// ── report ────────────────────────────────────────────────────────────────────
const rec = c => (c.staked ? (100 * c.returned / c.staked).toFixed(1) + '%' : '—');
const hr = c => (c.bets ? (100 * c.hits / c.bets).toFixed(0) + '%' : '—');
const roll = g => { const t = { staked: 0, returned: 0, bets: 0, hits: 0 }; for (const b of BANDS) { const c = cell[g]?.[b]; if (c) { t.staked += c.staked; t.returned += c.returned; t.bets += c.bets; t.hits += c.hits; } } return t; };

console.log(`\nLAYOFF RECOVERY (PLACE / 複勝) — point-in-time, ${settledRaces} settled races, ¥${STAKE}/bet\n`);
const w = 18;
console.log('bucket'.padEnd(w) + 'band'.padEnd(16) + 'recovery'.padStart(10) + 'hit'.padStart(7) + 'bets'.padStart(9) + '   vs FIELD');
for (const g of ['FIELD (all)', ...BUCKETS]) {
  for (const b of BANDS) {
    const c = cell[g]?.[b] || { staked: 0, returned: 0, bets: 0, hits: 0 };
    const f = cell['FIELD (all)']?.[b];
    const delta = (g !== 'FIELD (all)' && c.staked && f && f.staked)
      ? (((100 * c.returned / c.staked) - (100 * f.returned / f.staked)).toFixed(1) + ' pts') : '';
    console.log(g.padEnd(w) + b.padEnd(16) + rec(c).padStart(10) + hr(c).padStart(7) + String(c.bets).padStart(9) + '   ' + delta);
  }
  const t = roll(g), ft = roll('FIELD (all)');
  const d = (g !== 'FIELD (all)' && t.staked) ? (((100 * t.returned / t.staked) - (100 * ft.returned / ft.staked)).toFixed(1) + ' pts') : '';
  console.log(''.padEnd(w) + 'ALL'.padEnd(16) + rec(t).padStart(10) + hr(t).padStart(7) + String(t.bets).padStart(9) + '   ' + d);
  console.log('');
}
console.log('Read: a bucket recovering well ABOVE the FIELD baseline in the SAME band (positive "vs FIELD",');
console.log('with healthy n) = the market mis-prices that layoff = candidate edge. Watch for tiny-n buckets.\n');
