// ============================================================
// LONGSHOT INTERACTION MATRIX — do any factor COMBINATIONS hold up differently than their parts predict?
//
// Single-factor reads keep getting reshaped by context (drift collapses once dirt/swing are removed;
// fresh gets poisoned by to-dirt). This tool tests every PAIR of point-in-time longshot factors for an
// INTERACTION: in the 2x2 of {A on/off} x {B on/off}, additivity predicts
//     rec(A&B) ≈ rec(A&¬B) + rec(¬A&B) − rec(¬A&¬B).
// The gap (actual − predicted) is the interaction: positive = synergy (combo better than the sum),
// negative = antagonism (one factor poisons the other). Auto-surfaced, sorted by |interaction|, with
// n-guards on all four cells + a both-halves sanity flag (one-season bench → treat thin/half-split as noise).
//
// Factors (longshot band, FavRank 9+): fresh H7 · to-dirt H8 · cut-back H9 · big-swing H14 · steady H14 ·
// fast-SF H12 · slow-SF H12 · closer H6.  (drift H13 excluded — 35% prev-odds coverage breaks the 2x2.)
// Flat ¥100 PLACE, point-in-time. Read-only.  Run:  node tools/backtest/longshot-interaction-matrix.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const hist = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'finish_history.json'), 'utf8'));
const bench = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'history.json'), 'utf8'));
const weights = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'weights.json'), 'utf8'));
const times = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'speed_times.json'), 'utf8'));

const STAKE = 100;
const MIN_SAMPLES = 3;        // prior runs before a horse has an SF / closer profile
const GMIN = 40;              // going-bucket par threshold
const MIN_CELL = +(process.env.IM_MINCELL ?? 20);   // n-guard per 2x2 cell
const dayDiff = (a, b) => { const t = s => Date.UTC(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8)); return Math.round((t(a) - t(b)) / 86400000); };

// ── speed-figure par tables + per-run figure (from speed-figure-recovery) ─────
const ftSec = s => { if (!/^[0-9]{4}$/.test(s)) return null; const v = +s[0] * 60 + +s.slice(1, 3) + +s[3] / 10; return v > 40 && v < 280 ? v : null; };
const ftByKey = new Map(); for (const r of times) { const v = ftSec(r.ft); if (v != null) ftByKey.set(`${r.rid}|${r.h}`, v); }
const trackOf = rid => rid.slice(8, 10);
for (const r of hist) { r._sec = ftByKey.get(`${r.rid}|${r.h}`) ?? null; r._trk = trackOf(r.rid); r._gok = (r.going && r.going > 0) ? r.going : 0; r._l3f = /^[0-9]{3}$/.test(r.l3f) && +r.l3f >= 250 && +r.l3f <= 500 ? +r.l3f : null; }
const fine = new Map(), coarse = new Map(); const push = (m, k, v) => { let a = m.get(k); if (!a) { a = []; m.set(k, a); } a.push(v); };
for (const r of hist) { if (r._sec == null) continue; push(coarse, `${r._trk}|${r.s}|${r.d}`, r._sec); if (r._gok) push(fine, `${r._trk}|${r.s}|${r.d}|${r._gok}`, r._sec); }
const stat = a => { const n = a.length, m = a.reduce((x, y) => x + y, 0) / n; const sd = Math.sqrt(a.reduce((x, y) => x + (y - m) ** 2, 0) / n) || 1; return { n, m, sd }; };
const fineP = new Map(); for (const [k, a] of fine) fineP.set(k, stat(a)); const coarseP = new Map(); for (const [k, a] of coarse) coarseP.set(k, stat(a));
for (const r of hist) { if (r._sec == null) { r._fig = null; continue; } let p = r._gok ? fineP.get(`${r._trk}|${r.s}|${r.d}|${r._gok}`) : null; if (!p || p.n < GMIN) p = coarseP.get(`${r._trk}|${r.s}|${r.d}`); r._fig = (p && p.n >= 8) ? (p.m - r._sec) / p.sd : null; }

// ── closing top-3 rank within each race (from closing-kick-recovery) ──────────
const byRace = new Map(); for (const r of hist) { let a = byRace.get(r.rid); if (!a) { a = []; byRace.set(r.rid, a); } a.push(r); }
for (const field of byRace.values()) { const v = field.filter(r => r._l3f != null).sort((a, b) => a._l3f - b._l3f); v.forEach((r, i) => { r._top3 = (i + 1) <= 3 ? 1 : 0; }); }

// ── per-horse chronological → as-of profiles (layoff, priorS/D, avgSF, closeRate) ──
const byHorse = new Map(); for (const r of hist) { let a = byHorse.get(r.h); if (!a) { a = []; byHorse.set(r.h, a); } a.push(r); }
for (const a of byHorse.values()) a.sort((x, y) => (x.ymd < y.ymd ? -1 : 1));
const asof = new Map();
for (const arr of byHorse.values()) {
  let prev = null; const figs = [], closes = [];
  for (const r of arr) {
    asof.set(`${r.rid}|${r.h}`, {
      layoff: prev ? dayDiff(r.ymd, prev.ymd) : null, priorS: prev ? prev.s : null, priorD: prev ? prev.d : null,
      avgSF: figs.length >= MIN_SAMPLES ? figs.reduce((a, b) => a + b, 0) / figs.length : null,
      closeRate: closes.length >= MIN_SAMPLES ? closes.reduce((a, b) => a + b, 0) / closes.length : null,
    });
    if (r._fig != null) figs.push(r._fig);
    if (r._top3 != null) closes.push(r._top3);
    prev = r;
  }
}
const wgByKey = new Map(); for (const w of weights) { const mag = /^[0-9]{3}$/.test(w.cg) ? +w.cg : null; let c = null; if (mag != null && mag !== 999) c = w.sg === '+' ? mag : w.sg === '-' ? -mag : (mag === 0 ? 0 : null); wgByKey.set(`${w.rid}|${w.h}`, c); }

// ── assemble longshot bets with all flags ─────────────────────────────────────
const FACTORS = ['fresh', 'toDirt', 'cutback', 'bigSwing', 'steady', 'fastSF', 'slowSF', 'closer'];
const bets = [];
const byDate = bench.past_races_by_date || {}; const dates = Object.keys(byDate).sort(); const mid = dates[Math.floor(dates.length / 2)];
for (const date of dates) for (const race of byDate[date]) {
  let pay = null; try { pay = JSON.parse(race.info.results_json); } catch (_) {}
  const pd = pay && pay.place; if (!pd || !pd.length) continue;
  const tS = race.info.surface, tD = parseInt(race.info.distance, 10), rid = race.info.race_id;
  for (const e of (race.entries || [])) {
    if (e.Scratched === true) continue;
    if (!(parseInt(e.Fav, 10) >= 9)) continue;
    const pp = parseInt(e.PP, 10); if (!Number.isFinite(pp)) continue;
    const hid = String(e.Horse_ID).split('.')[0];
    const a = asof.get(`${rid}|${hid}`); if (!a) continue;
    const w = wgByKey.get(`${rid}|${hid}`);
    const win = pd.find(d => d.combo && d.combo[0] === pp);
    bets.push({
      ret: win ? win.payout * (STAKE / 100) : 0, half: date < mid ? 'H1' : 'H2',
      fresh: a.layoff != null && a.layoff >= 61 && a.layoff <= 120,
      toDirt: a.priorS && a.priorS !== 'jump' && tS === 'dirt' && a.priorS !== 'dirt',
      cutback: a.priorD && tD && (tD - a.priorD) <= -200,
      bigSwing: w != null && Math.abs(w) >= 10,
      steady: w != null && Math.abs(w) <= 3,
      fastSF: a.avgSF != null && a.avgSF >= 0.5,
      slowSF: a.avgSF != null && a.avgSF <= -0.5,
      closer: a.closeRate != null && a.closeRate >= 0.5,
    });
  }
}

const rec = sel => sel.length ? 100 * sel.reduce((s, b) => s + b.ret, 0) / (sel.length * STAKE) : null;
const sub = f => bets.filter(f);
const base = rec(bets);
console.log(`\nLONGSHOT INTERACTION MATRIX (FavRank 9+, PLACE) — ${bets.length} bets · baseline ${base.toFixed(1)}% · cell n-guard ≥${MIN_CELL}\n`);

// ── single-factor reference ───────────────────────────────────────────────────
console.log('── single factors (vs baseline ' + base.toFixed(1) + '%) ──');
console.log('factor'.padEnd(12) + 'recov'.padStart(8) + 'n'.padStart(7) + '   [H1/H2]');
for (const f of FACTORS) {
  const s = sub(b => b[f]); const r = rec(s);
  const h1 = rec(s.filter(b => b.half === 'H1')), h2 = rec(s.filter(b => b.half === 'H2'));
  console.log(f.padEnd(12) + (r == null ? '—' : r.toFixed(1) + '%').padStart(8) + String(s.length).padStart(7) +
    (s.length >= 50 ? `   [${h1 == null ? '—' : h1.toFixed(0)} / ${h2 == null ? '—' : h2.toFixed(0)}]` : '   [thin]'));
}

// ── pairwise interaction (2x2 additivity gap) ─────────────────────────────────
const EXCLUDE = new Set(['bigSwing|steady', 'fastSF|slowSF']);  // mutually exclusive families
const rows = [];
for (let i = 0; i < FACTORS.length; i++) for (let j = i + 1; j < FACTORS.length; j++) {
  const A = FACTORS[i], B = FACTORS[j];
  if (EXCLUDE.has(`${A}|${B}`) || EXCLUDE.has(`${B}|${A}`)) continue;
  const c11 = sub(b => b[A] && b[B]), c10 = sub(b => b[A] && !b[B]), c01 = sub(b => !b[A] && b[B]), c00 = sub(b => !b[A] && !b[B]);
  if ([c11, c10, c01, c00].some(c => c.length < MIN_CELL)) continue;
  const r11 = rec(c11), r10 = rec(c10), r01 = rec(c01), r00 = rec(c00);
  const predicted = r10 + r01 - r00;       // additive expectation for the A&B cell
  const inter = r11 - predicted;           // interaction
  const h1 = rec(c11.filter(b => b.half === 'H1')), h2 = rec(c11.filter(b => b.half === 'H2'));
  const consistent = h1 != null && h2 != null && Math.sign(h1 - predicted) === Math.sign(h2 - predicted);
  rows.push({ A, B, r11, n11: c11.length, r10, r01, r00, predicted, inter, h1, h2, consistent });
}
rows.sort((a, b) => Math.abs(b.inter) - Math.abs(a.inter));

console.log('\n── pairwise interactions (combo vs additive prediction), |gap| desc ──');
console.log('A + B'.padEnd(22) + 'combo'.padStart(8) + 'pred'.padStart(8) + 'gap'.padStart(8) + 'n'.padStart(6) + '  both-halves   solo A/B');
for (const r of rows) {
  const tag = Math.abs(r.inter) >= 15 ? (r.inter > 0 ? ' ▲syn' : ' ▼anti') : '';
  const hh = (r.h1 == null || r.h2 == null) ? '  [thin]' : `  [${r.h1.toFixed(0)}/${r.h2.toFixed(0)}]${r.consistent ? '✓' : '✗'}`;
  console.log(`${r.A} + ${r.B}`.padEnd(22) + `${r.r11.toFixed(0)}%`.padStart(8) + `${r.predicted.toFixed(0)}%`.padStart(8) +
    `${r.inter >= 0 ? '+' : ''}${r.inter.toFixed(0)}`.padStart(8) + String(r.n11).padStart(6) + hh.padEnd(14) +
    `${r.r10.toFixed(0)}/${r.r01.toFixed(0)}` + tag);
}
console.log('\n▲syn = combo beats the sum of its parts · ▼anti = one factor poisons the other (|gap|≥15).');
console.log('both-halves ✓ = the combo deviates from prediction the SAME way in both halves (trust); ✗ = one-half artifact.');
console.log('combo = rec(A&B) · pred = rec(A&¬B)+rec(¬A&B)−rec(¬A&¬B) · solo A/B = rec(A&¬B)/rec(¬A&B). One season — n-guarded, not gospel.\n');

// ── tail-robustness on the strongest TRUSTWORTHY interactions (|gap|≥20, ✓, n≥100) ──
const dropTop = (sel, k) => { const s = sel.map(b => b.ret).sort((a, b) => b - a).slice(k); return s.length ? (100 * s.reduce((a, b) => a + b, 0) / (s.length * STAKE)).toFixed(0) + '%' : '—'; };
const strong = rows.filter(r => Math.abs(r.inter) >= 20 && r.consistent && r.n11 >= 100);
console.log('── tail-robustness on the strong, both-halves-consistent interactions (drop biggest payouts) ──');
console.log('A & B'.padEnd(22) + 'full'.padStart(7) + 'drop3'.padStart(7) + 'drop5'.padStart(7) + 'drop10'.padStart(8) + '   n');
for (const r of strong) {
  const sel = sub(b => b[r.A] && b[r.B]);
  console.log(`${r.A} & ${r.B}`.padEnd(22) + `${r.r11.toFixed(0)}%`.padStart(7) + dropTop(sel, 3).padStart(7) + dropTop(sel, 5).padStart(7) + dropTop(sel, 10).padStart(8) + `   ${sel.length}`);
}
console.log('\nIf a synergy stays well above baseline (' + base.toFixed(0) + '%) after drop10 → repeatable. If it collapses to');
console.log('~baseline → the combo was a few big tickets, not an edge (the fresh&steady lesson).\n');
