// ============================================================
// RUNNING STYLE × DISTANCE × SURFACE — pace-bias study (s54 probe)
//
// QUESTION (operator): does a horse's running style (Lead/Press/Close/Deep) correlate with how it
// finishes, and does that depend on track length + surface (turf/dirt)? i.e. is there a pace bias —
// do front-runners win more in dirt sprints, closers more in turf routes, etc.?
//
// METHOD: read the full finish history. For each run with corner data, classify the style exactly
// like the app (early corner position ÷ field size: ≤1 Lead, ≤.30 Press, ≤.66 Close, else Deep).
// Bucket by surface (turf/dirt) × distance band, and report each style's WIN% and PLACE% vs the
// bucket's own baseline. This is DESCRIPTIVE (how a horse ran → how it finished in the same race) —
// it reveals pace/positional bias by condition. It is NOT a betting edge (no odds/takeout here; the
// market likely prices known biases) and not point-in-time prediction — read it as "what shape of
// race rewards what running style."
//
// READ-ONLY. Run:  node tools/backtest/runstyle-bias.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fixturesDir, pad } from './harness.mjs';

const rows = JSON.parse(fs.readFileSync(path.join(fixturesDir, 'finish_history.json'), 'utf8'));

// Style from corners + field size — mirrors predictedStylePill / horseRunStyle.
function styleOf(r) {
  const corners = [r.c1, r.c2, r.c3, r.c4];
  const pos = corners.map(c => parseInt(c, 10)).filter(n => Number.isFinite(n) && n > 0);
  if (!pos.length) return null;
  const early = pos[0];
  const n = (r.n && r.n > 1) ? r.n : Math.max(...pos);
  const ratio = early / n;
  if (early <= 1) return 'Lead';
  if (ratio <= 0.30) return 'Press';
  if (ratio <= 0.66) return 'Close';
  return 'Deep';
}

function distBand(d) {
  if (!Number.isFinite(d)) return null;
  if (d < 1400) return 'Sprint (<1400)';
  if (d < 1800) return 'Mile (1400-1799)';
  if (d < 2200) return 'Middle (1800-2199)';
  return 'Long (2200+)';
}

const BANDS = ['Sprint (<1400)', 'Mile (1400-1799)', 'Middle (1800-2199)', 'Long (2200+)'];
const STYLES = ['Lead', 'Press', 'Close', 'Deep'];

// cells[surface][band][style] = {n, win, place}
const cells = {};
let total = 0, classified = 0;

for (const r of rows) {
  total++;
  const surf = String(r.s || '').toLowerCase();
  if (surf !== 'turf' && surf !== 'dirt') continue;
  const band = distBand(+r.d);
  if (!band) continue;
  const st = styleOf(r);
  if (!st) continue;
  const f = parseInt(r.f, 10);
  if (!Number.isFinite(f) || f < 1) continue;
  classified++;
  cells[surf] ??= {};
  cells[surf][band] ??= {};
  cells[surf][band][st] ??= { n: 0, win: 0, place: 0 };
  const c = cells[surf][band][st];
  c.n++; if (f === 1) c.win++; if (f <= 3) c.place++;
}

const pctOf = (a, b) => b ? (a / b * 100) : 0;
const p1 = (x) => pad(x.toFixed(1) + '%', 6);

function printSurface(surf) {
  console.log(`\n══════════ ${surf.toUpperCase()} ══════════`);
  for (const band of BANDS) {
    const byStyle = cells[surf]?.[band];
    if (!byStyle) continue;
    // bucket baseline (all styles pooled)
    let bn = 0, bw = 0, bp = 0;
    for (const st of STYLES) { const c = byStyle[st]; if (c) { bn += c.n; bw += c.win; bp += c.place; } }
    if (bn < 200) continue; // skip thin buckets
    console.log(`\n  ${band}   (n=${bn})   baseline win ${pctOf(bw, bn).toFixed(1)}% · place ${pctOf(bp, bn).toFixed(1)}%`);
    console.log(`    ${pad('style', 6)}  ${pad('n', 6)}  ${pad('win%', 6)}  ${pad('place%', 6)}   win vs base   place vs base`);
    for (const st of STYLES) {
      const c = byStyle[st];
      if (!c || c.n < 50) { console.log(`    ${pad(st, 6)}  ${pad(c ? c.n : 0, 6)}   (thin)`); continue; }
      const w = pctOf(c.win, c.n), p = pctOf(c.place, c.n);
      const dw = w - pctOf(bw, bn), dp = p - pctOf(bp, bn);
      const sg = (x) => (x >= 0 ? '+' : '') + x.toFixed(1);
      console.log(`    ${pad(st, 6)}  ${pad(c.n, 6)}  ${p1(w)}  ${p1(p)}   ${pad(sg(dw), 7)}      ${pad(sg(dp), 7)}`);
    }
  }
}

console.log(`Running-style pace bias — ${total} runs, ${classified} classified (have corners + turf/dirt + dist + finish)`);
console.log(`Style = early corner position ÷ field size (Lead ≤1 / Press ≤.30 / Close ≤.66 / Deep else).`);
console.log(`"vs base" = this style's rate minus the bucket's pooled baseline (so it controls for field-size mix).`);

for (const surf of ['turf', 'dirt']) printSurface(surf);

console.log(`\n── Read ──`);
console.log(`Within each distance×surface bucket, a positive "win vs base" / "place vs base" means that`);
console.log(`running style finishes BETTER than the field there — a positional/pace bias for that shape.`);
console.log(`Descriptive only (same-race style→finish); the market likely already prices obvious biases.\n`);
