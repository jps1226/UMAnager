// ============================================================
// KEIBrain Phase 0 — build the point-in-time FEATURE TABLE (one row per past run).
//
// Reads tools/backtest/fixtures/finish_history.json (the same honest history the backtest uses) and, for
// every run, computes features from that horse's runs STRICTLY BEFORE that run only — so there is zero
// look-ahead (the rule that caught the s50 leak). Writes a flat CSV that KEIBrain's Python modelling
// reads. This file does NO modelling; it is pure, leak-checked plumbing.
//
// Feature philosophy (README §4/§5): Stage-1 features are the horse's OWN merit + situation, deliberately
// BLIND to the market. The market columns (fav rank) are emitted too but tagged market_* so the modelling
// keeps them out of Stage 1 and only feeds them to Stage 2.
//
// NOT in this v1 (no data in the current export): jockey/trainer/sire — those need a richer history
// export (j/t/sire columns) → KEIBrain v2. Pace/going are included where present (l3f 63%, corners 27%).
//
// Run:  node KEIBrain/build_features.mjs   →   KEIBrain/data/features.csv
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const HIST = path.join(here, '..', 'tools', 'backtest', 'fixtures', 'finish_history.json');
const OUT_DIR = path.join(here, 'data');
const OUT = path.join(OUT_DIR, 'features.csv');

const rows = JSON.parse(fs.readFileSync(HIST, 'utf8'));

// ── helpers (mirror point-in-time.mjs / pace-edge.mjs) ──
const distanceBucket = d => (d == null || d <= 0) ? '' : d <= 1400 ? 'sprint' : d <= 1800 ? 'mile' : d <= 2200 ? 'middle' : 'long';
const l3fVal = s => { const v = parseInt(s, 10); return Number.isFinite(v) && v >= 250 && v <= 500 ? v : null; };
const cornerVal = s => { const v = parseInt(s, 10); return Number.isFinite(v) && v > 0 ? v : null; };
const placed = f => f >= 1 && f <= 3;
const dayDiff = (a, b) => { const t = s => Date.UTC(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8)); return Math.round((t(a) - t(b)) / 86400000); };
const mean = a => a.length ? a.reduce((x, y) => x + y, 0) / a.length : null;
const round = (x, n = 4) => x == null ? null : Math.round(x * 10 ** n) / 10 ** n;

// ── 1) closing rank within each race (fastest last-3F = top-3 kick) + early position fraction ──
const byRace = new Map();
for (const r of rows) { let a = byRace.get(r.rid); if (!a) { a = []; byRace.set(r.rid, a); } a.push(r); }
for (const r of rows) {
  r._l3f = l3fVal(r.l3f);
  const first = cornerVal(r.c1) ?? cornerVal(r.c2) ?? cornerVal(r.c3) ?? cornerVal(r.c4);
  r._earlyFrac = (first != null && r.n > 1) ? (first - 1) / (r.n - 1) : null; // 0 = led early, 1 = last early
}
for (const field of byRace.values()) {
  const valid = field.filter(r => r._l3f != null).sort((a, b) => a._l3f - b._l3f);
  valid.forEach((r, i) => { r._closeTop3 = (i + 1) <= 3 ? 1 : 0; });
}

// ── 2) per-horse chronological pass → features from strictly-prior runs ──
const byHorse = new Map();
for (const r of rows) { let a = byHorse.get(r.h); if (!a) { a = []; byHorse.set(r.h, a); } a.push(r); }
for (const a of byHorse.values()) a.sort((x, y) => (x.ymd < y.ymd ? -1 : 1));

const COLS = [
  // ── metadata (NOT model features) ──
  'rid', 'horse_id', 'ymd', 'surface', 'distance', 'field_size',
  // ── market (Stage-2 ONLY) ──
  'market_fav_rank',
  // ── Stage-1 features: record / form ──
  'starts_prior', 'win_pct_prior', 'place_pct_prior', 'avg_finish_prior',
  'last_finish', 'avg_finish_last3', 'best_finish_last3',
  // ── Stage-1 features: fit ──
  'surface_place_pct', 'surface_win_pct', 'surface_starts',
  'dist_place_pct', 'dist_win_pct', 'dist_starts',
  // ── Stage-1 features: situation (H7/H8 etc.) ──
  'layoff_days', 'layoff_fresh', 'surface_changed', 'switch_to_dirt', 'distance_move', 'cutback',
  // ── Stage-1 features: pace (sparse) ──
  'closing_top3_rate', 'closing_samples', 'running_style',
  // ── Stage-1 features: going ──
  'wet_place_edge', 'wet_starts',
  // ── TARGET ──
  'placed',
];

const csvCell = v => (v == null ? '' : String(v));
const lines = [COLS.join(',')];
let emitted = 0;

for (const arr of byHorse.values()) {
  // running accumulators of PRIOR runs
  const finishes = [];            // all prior finish positions (most recent pushed last)
  const surf = {};                // surface → {starts, wins, places}
  const dist = {};                // bucket  → {starts, wins, places}
  const closes = [];              // prior _closeTop3 (1/0), where defined
  const styles = [];              // prior _earlyFrac, where defined
  let wetStarts = 0, wetPlaces = 0, allStarts = 0, allPlaces = 0;
  let prevYmd = null, prevSurface = null, prevDist = null;

  for (const r of arr) {
    // ----- emit features from PRIOR state (before folding r in) -----
    if (finishes.length >= 1) {
      const bucket = distanceBucket(r.d);
      const sf = surf[r.s];
      const ds = dist[bucket];
      const overallPlacePct = allStarts ? allPlaces / allStarts : null;
      const wetPlacePct = wetStarts ? wetPlaces / wetStarts : null;
      const last3 = finishes.slice(-3);
      const layoff = prevYmd ? dayDiff(r.ymd, prevYmd) : null;
      const surfaceChanged = (prevSurface && prevSurface !== 'jump' && r.s && prevSurface !== r.s) ? 1 : 0;
      const toDirt = (prevSurface && prevSurface !== 'jump' && r.s === 'dirt' && prevSurface !== 'dirt') ? 1 : 0;
      const distMove = (prevDist && r.d) ? (r.d - prevDist) : null;

      const row = {
        rid: r.rid, horse_id: r.h, ymd: r.ymd, surface: r.s, distance: r.d, field_size: r.n,
        market_fav_rank: r.fav ?? '',
        starts_prior: finishes.length,
        win_pct_prior: round(finishes.filter(f => f === 1).length / finishes.length),
        place_pct_prior: round(finishes.filter(placed).length / finishes.length),
        avg_finish_prior: round(mean(finishes), 3),
        last_finish: finishes[finishes.length - 1],
        avg_finish_last3: round(mean(last3), 3),
        best_finish_last3: Math.min(...last3),
        surface_place_pct: (sf && sf.starts >= 3) ? round(sf.places / sf.starts) : '',
        surface_win_pct: (sf && sf.starts >= 3) ? round(sf.wins / sf.starts) : '',
        surface_starts: sf ? sf.starts : 0,
        dist_place_pct: (ds && ds.starts >= 3) ? round(ds.places / ds.starts) : '',
        dist_win_pct: (ds && ds.starts >= 3) ? round(ds.wins / ds.starts) : '',
        dist_starts: ds ? ds.starts : 0,
        layoff_days: layoff ?? '',
        layoff_fresh: (layoff != null && layoff >= 61 && layoff <= 120) ? 1 : 0,
        surface_changed: surfaceChanged,
        switch_to_dirt: toDirt,
        distance_move: distMove ?? '',
        cutback: (distMove != null && distMove <= -200) ? 1 : 0,
        closing_top3_rate: closes.length ? round(mean(closes)) : '',
        closing_samples: closes.length,
        running_style: styles.length ? round(mean(styles)) : '',
        wet_place_edge: (wetStarts >= 3 && overallPlacePct != null) ? round(wetPlacePct - overallPlacePct) : '',
        wet_starts: wetStarts,
        placed: placed(r.f) ? 1 : 0,
      };
      lines.push(COLS.map(c => csvCell(row[c])).join(','));
      emitted++;
    }

    // ----- fold THIS run into the priors for later runs -----
    const b = distanceBucket(r.d);
    finishes.push(r.f);
    (surf[r.s] ??= { starts: 0, wins: 0, places: 0 });
    surf[r.s].starts++; if (r.f === 1) surf[r.s].wins++; if (placed(r.f)) surf[r.s].places++;
    if (b) { (dist[b] ??= { starts: 0, wins: 0, places: 0 }); dist[b].starts++; if (r.f === 1) dist[b].wins++; if (placed(r.f)) dist[b].places++; }
    if (r._closeTop3 != null) closes.push(r._closeTop3);
    if (r._earlyFrac != null) styles.push(r._earlyFrac);
    allStarts++; if (placed(r.f)) allPlaces++;
    if (r.going != null && r.going >= 3) { wetStarts++; if (placed(r.f)) wetPlaces++; }
    prevYmd = r.ymd; prevSurface = r.s; prevDist = r.d;
  }
}

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(OUT, lines.join('\n') + '\n');
console.log(`KEIBrain Phase 0: wrote ${emitted} feature rows × ${COLS.length} cols → ${path.relative(path.join(here, '..'), OUT)}`);
console.log(`(rows = runs with >=1 prior start; debuts excluded. Target = placed (top-3).)`);
