// ============================================================
// SPEED-FIGURE RECOVERY (MONEY) TEST — TARGET frontier JV's headline metric (§0 Step 1.6 / TARGET scope).
//
// TARGET's marquee feature is a SPEED FIGURE: each run's finish time normalized against a "par" time for
// that track+surface+distance (and adjusted for the day's going / track variant), so a fast run on a heavy
// track shows through even if the horse only finished mid-pack. The betting thesis we're testing is the
// North-Star one (s51-52 lesson): the market prices off finishing POSITION + odds; a horse with a strong
// going-adjusted speed figure but a weak recent finishing record may be a genuine OVERLAY the crowd missed.
//
// This is the MONEY test (like closing-kick-recovery): if you actually BET high-speed-figure horses to
// PLACE (複勝) across the settled bench, how much comes back net of the ~20-25% takeout, and does it beat
// betting the field / betting every profiled horse in the SAME market band?
//
// Method (point-in-time, no look-ahead into the target race):
//   • Par time per (track, surface, distance[, going]) = mean+std of finish-seconds across the timed bench
//     (a track/distance population constant — NOT any single horse's outcome). Going bucket used when it has
//     >= GMIN samples (credits fast times on tough tracks = the track-variant adjustment), else coarser.
//   • Speed figure per run = (par.mean - seconds) / par.std  → standardized, positive = faster than typical.
//   • Profile per (race, horse) = the horse's prior speed figures using ONLY runs strictly before that race
//     (>= MIN_SAMPLES). SF_MODE=mean (default, recent form) or best (peak ability, more TARGET-like).
//   • Settle: flat ¥100 PLACE bet on each qualifying horse from stored `place` dividends (per ¥100).
//     Stratified by market band (chalk/mid/longshot) — the overlay, if any, should live in the longshots.
//
// Caveat: par times use the whole window (incl. dates after some early runs). Par is a population constant,
// not the horse's own result, so it does not leak the TARGET race outcome — but it is a mild impurity noted
// for honesty. Read-only. Run:  node tools/backtest/speed-figure-recovery.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const hist = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'finish_history.json'), 'utf8'));
const bench = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'history.json'), 'utf8'));
const times = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'speed_times.json'), 'utf8'));

const MIN_SAMPLES = +(process.env.SF_MIN ?? 3);    // min prior timed runs before a horse has a profile
const STRONG = +(process.env.SF_STRONG ?? 0.50);   // SD units; env-overridable to stress-test the conclusion
const WEAK   = +(process.env.SF_WEAK ?? -0.50);
const GMIN   = +(process.env.SF_GMIN ?? 40);        // min samples to trust a going-specific par bucket
const MODE   = (process.env.SF_MODE ?? 'mean');     // 'mean' = recent form, 'best' = peak ability
const STAKE  = 100;

// ── parse finish time "MSSt" (1273 = 1:27.3) → seconds ────────────────────────
const ftSec = s => {
  if (!/^[0-9]{4}$/.test(s)) return null;
  const m = +s[0], sec = +s.slice(1, 3), t = +s[3];
  const v = m * 60 + sec + t / 10;
  return v > 40 && v < 280 ? v : null;          // sanity: 40s..4:40 covers all JRA distances
};
const ftByKey = new Map();                       // `rid|h` → seconds
for (const r of times) { const v = ftSec(r.ft); if (v != null) ftByKey.set(`${r.rid}|${r.h}`, v); }

const trackOf = rid => rid.slice(8, 10);         // YYYYMMDD[PP]kkddrr
const favBand = f => (f == null ? null : f <= 3 ? 'chalk (1-3)' : f <= 8 ? 'mid (4-8)' : 'longshot (9+)');
const BANDS = ['chalk (1-3)', 'mid (4-8)', 'longshot (9+)'];

// ── 1) attach seconds + bucket key to each historical run ─────────────────────
for (const r of hist) {
  r._sec = ftByKey.get(`${r.rid}|${r.h}`) ?? null;
  r._trk = trackOf(r.rid);
  r._gok = (r.going && r.going > 0) ? r.going : 0;   // 0 = unknown going
}

// ── 2) build par tables: fine (with going) + coarse (without) ─────────────────
const acc = (map, key, sec) => { let a = map.get(key); if (!a) { a = []; map.set(key, a); } a.push(sec); };
const fine = new Map(), coarse = new Map();
for (const r of hist) {
  if (r._sec == null) continue;
  acc(coarse, `${r._trk}|${r.s}|${r.d}`, r._sec);
  if (r._gok) acc(fine, `${r._trk}|${r.s}|${r.d}|${r._gok}`, r._sec);
}
const stat = arr => {
  const n = arr.length, mean = arr.reduce((a, b) => a + b, 0) / n;
  const std = Math.sqrt(arr.reduce((a, b) => a + (b - mean) ** 2, 0) / n) || 1;
  return { n, mean, std };
};
const fineP = new Map(); for (const [k, a] of fine) fineP.set(k, stat(a));
const coarseP = new Map(); for (const [k, a] of coarse) coarseP.set(k, stat(a));

const figureOf = r => {
  if (r._sec == null) return null;
  let p = r._gok ? fineP.get(`${r._trk}|${r.s}|${r.d}|${r._gok}`) : null;
  if (!p || p.n < GMIN) p = coarseP.get(`${r._trk}|${r.s}|${r.d}`);
  if (!p || p.n < 8) return null;               // bucket too thin to trust
  return (p.mean - r._sec) / p.std;             // +ve = faster than par
};
for (const r of hist) r._fig = figureOf(r);

// ── 3) per-horse chronological → prior speed-figure profile, keyed `rid|h` ────
const byHorse = new Map();
for (const r of hist) { let a = byHorse.get(r.h); if (!a) { a = []; byHorse.set(r.h, a); } a.push(r); }
for (const a of byHorse.values()) a.sort((x, y) => (x.ymd < y.ymd ? -1 : 1));

const profile = new Map(); // `rid|h` → { val, samples }
for (const arr of byHorse.values()) {
  const figs = [];
  for (const r of arr) {
    if (figs.length >= MIN_SAMPLES) {
      const val = MODE === 'best' ? Math.max(...figs) : figs.reduce((a, b) => a + b, 0) / figs.length;
      profile.set(`${r.rid}|${r.h}`, { val, samples: figs.length });
    }
    if (r._fig != null) figs.push(r._fig);
  }
}

// ── 4) settle place bets across the bench (mirrors closing-kick-recovery) ─────
const cell = {};
const grp = (g, b) => (((cell[g] ??= {})[b] ??= { staked: 0, returned: 0, bets: 0, hits: 0 }));
const place = (g, b, pp, placeDivs) => {
  const c = grp(g, b); c.staked += STAKE; c.bets++;
  const win = placeDivs.find(d => d.combo && d.combo[0] === pp);
  if (win) { c.returned += win.payout * (STAKE / 100); c.hits++; }
};

const byDate = bench.past_races_by_date || {};
const diag = { FAST: [], FIELD: [] };   // longshot-band per-bet {date, ret} for robustness checks
let settledRaces = 0, profiledHorses = 0;
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

      place('FIELD (all runners)', band, pp, placeDivs);
      if (band === 'longshot (9+)') diag.FIELD.push({ date, ret: retOf(pp) });
      const prof = profile.get(`${rid}|${hid}`);
      if (!prof) continue;
      profiledHorses++;
      place('PROFILED (has SF data)', band, pp, placeDivs);
      if (prof.val >= STRONG) { place('FAST (high SF)', band, pp, placeDivs); if (band === 'longshot (9+)') diag.FAST.push({ date, ret: retOf(pp) }); }
      else if (prof.val <= WEAK) place('SLOW (low SF)', band, pp, placeDivs);
    }
  }
}

// ── 5) report ─────────────────────────────────────────────────────────────────
const rec = c => (c.staked ? (100 * c.returned / c.staked).toFixed(1) + '%' : '—');
const hr = c => (c.bets ? (100 * c.hits / c.bets).toFixed(0) + '%' : '—');
const roll = g => { const t = { staked: 0, returned: 0, bets: 0, hits: 0 }; for (const b of BANDS) { const c = cell[g]?.[b]; if (c) { t.staked += c.staked; t.returned += c.returned; t.bets += c.bets; t.hits += c.hits; } } return t; };

console.log(`\nSPEED-FIGURE RECOVERY (PLACE / 複勝) — point-in-time, ${settledRaces} settled races, ¥${STAKE}/bet`);
console.log(`SF = (par.mean - finishSec)/par.std, going-adjusted · profile ${MODE} of prior figs, >=${MIN_SAMPLES} samples · fast>=${STRONG} · slow<=${WEAK} SD`);
console.log(`par buckets: ${fineP.size} fine(+going) / ${coarseP.size} coarse · timed runs: ${[...ftByKey.values()].length}\n`);

const GROUPS = ['FIELD (all runners)', 'PROFILED (has SF data)', 'FAST (high SF)', 'SLOW (low SF)'];
const w = 26;
console.log('group'.padEnd(w) + 'band'.padEnd(16) + 'recovery'.padStart(10) + 'hit'.padStart(7) + 'bets'.padStart(9));
for (const g of GROUPS) {
  for (const b of BANDS) {
    const c = cell[g]?.[b] || { staked: 0, returned: 0, bets: 0, hits: 0 };
    console.log(g.padEnd(w) + b.padEnd(16) + rec(c).padStart(10) + hr(c).padStart(7) + String(c.bets).padStart(9));
  }
  const t = roll(g);
  console.log(''.padEnd(w) + 'ALL'.padEnd(16) + rec(t).padStart(10) + hr(t).padStart(7) + String(t.bets).padStart(9));
  console.log('');
}

console.log('Read: FAST-SF recovery vs FIELD/PROFILED in the SAME band = the money edge (or its absence).');
console.log('The North-Star question is the LONGSHOT band: if a strong speed figure recovers >> field there,');
console.log('the market underpriced proven time merit = an overlay. If FAST ~= FIELD, the figure is priced-in.\n');

// ── 6) robustness: the two checks that killed H6 / KEIBrain's longshot signal ──
const recOf = arr => arr.length ? (arr.reduce((a, b) => a + b.ret, 0) / arr.length).toFixed(1) + '%' : '—';
const half = (arr, lo, hi) => arr.filter(x => x.date >= lo && x.date <= hi);
const dates = [...new Set([...diag.FAST, ...diag.FIELD].map(x => x.date))].sort();
const mid = dates[Math.floor(dates.length / 2)];
const dropTop = (arr, k) => { const s = [...arr].sort((a, b) => b.ret - a.ret); const cut = s.slice(k); return cut.length ? (cut.reduce((a, b) => a + b.ret, 0) / cut.length).toFixed(1) + '%' : '—'; };

console.log('── ROBUSTNESS (longshot band only) ──────────────────────────────────────');
console.log(`H1 = ${dates[0]}..${mid} · H2 = ${mid}..${dates[dates.length - 1]}`);
console.log('group       H1 recov   H2 recov   full   drop-top1   drop-top3   drop-top5     bets');
for (const g of ['FAST', 'FIELD']) {
  const a = diag[g];
  const h1 = half(a, dates[0], mid), h2 = half(a, mid, dates[dates.length - 1]);
  console.log(g.padEnd(12) + recOf(h1).padStart(8) + recOf(h2).padStart(11) +
    recOf(a).padStart(9) + dropTop(a, 1).padStart(11) + dropTop(a, 3).padStart(12) +
    dropTop(a, 5).padStart(12) + String(a.length).padStart(9));
}
console.log('\nPasses if: FAST beats FIELD in BOTH halves (time-consistent) AND FAST still beats FIELD');
console.log('after dropping its top 3-5 payouts (not a few-freak-dividend mirage).\n');
