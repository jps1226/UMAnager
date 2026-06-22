// ============================================================
// Point-in-time stats — the fix for the look-ahead leak (TODO §0 Step 1).
//
// The historical /api/races payload computes a horse's Record / Surface_Win_Pct / Dist_Win_Pct (and
// Sire_Fit / Jockey_AE / Trainer_AE) as-of-NOW — i.e. including the race's own result + every later
// race. Replaying past races with those fields LEAKS the outcome. This module recomputes the horse's
// OWN-record fields from its runs strictly BEFORE each race, using the full clean finish history
// exported from the DB (fixtures/finish_history.json: {h:horseId, ymd:'YYYYMMDD', s:surface, d:distance, f:finish}).
//
// Fields it can honestly reconstruct: Record (career W/S), Surface_Win_Pct, Dist_Win_Pct (+ their start
// counts). Form_Score / Last3 are already date-gated in the controller, so they stay.
//
// Step 1.5 (this module also) rebuilds the THREE fields Step 1 had to null — jockey A/E, trainer A/E,
// and sire-fit — as point-in-time too, mirroring JockeyTrainerStatsService + SirePerformanceService but
// anchored strictly BEFORE each race instead of as-of-NOW. The richer finish_history export now also
// carries fav/jockeyCode/trainerCode/sireId per run, which is all these reconstructions need. The
// leakage-prone part is the A/E baseline: P(win | favRank) must ALSO be rebuilt from only-before-the-race
// runs, or the "expected" term silently sees the future. See jtAEAsOf below.
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const HIST = path.join(here, 'fixtures', 'finish_history.json');

// Mirror SirePerformanceService.DistanceBucket exactly.
export function distanceBucket(d) {
  if (d == null || d <= 0) return '';
  if (d <= 1400) return 'sprint';
  if (d <= 1800) return 'mile';
  if (d <= 2200) return 'middle';
  return 'long';
}

const MIN_SPLIT_STARTS = 3; // controller's MinHorseSplitStarts — below this the split is null

// Min-sample floors — mirror the controller (MinJockeyStarts=30, MinTrainerStarts=20) + the sire MV
// (MinSireStarts=10). Below these the field is surfaced as null and contributes nothing to scoring.
const MIN_JOCKEY_STARTS = 30;
const MIN_TRAINER_STARTS = 20;
const MIN_SIRE_STARTS = 10;

// One shared row load → every index the reconstructions need.
//   byHorse  : own-record splits (Step 1)
//   byJockey/byTrainer : a code's mounts (rolling A/E)
//   jockeyRuns/trainerRuns : ALL coded mounts, for the global P(win|favRank) baseline (Step 1.5)
//   bySire   : a sire's progeny finishes (sire-fit)
let _idx = null;
function index() {
  if (_idx) return _idx;
  const rows = JSON.parse(fs.readFileSync(HIST, 'utf8'));
  const byHorse = new Map(), byJockey = new Map(), byTrainer = new Map(), bySire = new Map();
  const jockeyRuns = [], trainerRuns = [];
  const push = (map, key, r) => { let a = map.get(key); if (!a) { a = []; map.set(key, a); } a.push(r); };
  for (const r of rows) {
    push(byHorse, r.h, r);
    if (r.j) { push(byJockey, r.j, r); jockeyRuns.push(r); }
    if (r.t) { push(byTrainer, r.t, r); trainerRuns.push(r); }
    if (r.sire) push(bySire, r.sire, r);
  }
  const byYmd = (a, b) => (a.ymd < b.ymd ? -1 : 1);
  for (const a of byHorse.values()) a.sort(byYmd);
  _idx = { byHorse, byJockey, byTrainer, bySire, jockeyRuns, trainerRuns };
  return _idx;
}

// 'YYYYMMDD' minus N days → 'YYYYMMDD' (UTC math; date-only, so no tz drift).
function shiftYmd(ymd, days) {
  const y = +ymd.slice(0, 4), m = +ymd.slice(4, 6), d = +ymd.slice(6, 8);
  const t = Date.UTC(y, m - 1, d) + days * 86400000;
  const dt = new Date(t);
  const p = n => String(n).padStart(2, '0');
  return `${dt.getUTCFullYear()}${p(dt.getUTCMonth() + 1)}${p(dt.getUTCDate())}`;
}

// Stats for a horse using ONLY its runs strictly before raceYmd (a 'YYYYMMDD' string).
// raceSurface/raceDistance describe the race being bet. Returns the fields to overwrite on the entry.
export function statsAsOf(horseId, raceYmd, raceSurface, raceDistance) {
  const all = index().byHorse.get(String(horseId).split('.')[0]) || [];
  const prior = all.filter(r => r.ymd < raceYmd);
  const starts = prior.length;
  const wins = prior.filter(r => r.f === 1).length;
  const record = `${wins}/${starts}`;

  const bucket = distanceBucket(raceDistance);
  const isJump = raceSurface === 'jump';

  let surfaceWinPct = null, surfaceStarts = null;
  if (raceSurface) {
    const sf = prior.filter(r => r.s === raceSurface);
    if (sf.length >= MIN_SPLIT_STARTS) {
      surfaceStarts = sf.length;
      surfaceWinPct = Math.round(1000 * sf.filter(r => r.f === 1).length / sf.length) / 10;
    }
  }

  let distWinPct = null, distStarts = null;
  if (bucket) {
    const ds = prior.filter(r => distanceBucket(r.d) === bucket && (isJump ? r.s === 'jump' : r.s !== 'jump'));
    if (ds.length >= MIN_SPLIT_STARTS) {
      distStarts = ds.length;
      distWinPct = Math.round(1000 * ds.filter(r => r.f === 1).length / ds.length) / 10;
    }
  }

  return { record, surfaceWinPct, surfaceStarts, distWinPct, distStarts };
}

// ── Step 1.5: point-in-time jockey/trainer A/E ────────────────────────────────
// Mirrors JockeyTrainerStatsService exactly, but anchored to the race date instead of CURRENT_DATE:
//   window      = runs in [raceYmd - windowDays, raceYmd)  (strictly before the race)
//   baseline    = P(win | favRank) over ALL coded mounts in that window  (the global market accuracy)
//   A/E (code)  = Σ wins ÷ Σ baseline[mount.favRank]  over that code's mounts in the window
// Floored like the controller (30 jockey / 20 trainer starts) → below the floor returns null.
const _aeBaseline = new Map(); // key `${raceYmd}|${windowDays}|${kind}` → {rank:p_win}
function baselineAsOf(allCodedRuns, raceYmd, windowDays, kind) {
  const key = `${raceYmd}|${windowDays}|${kind}`;
  let bl = _aeBaseline.get(key);
  if (bl) return bl;
  const lo = shiftYmd(raceYmd, -windowDays);
  const sums = new Map(); // rank → [wins, starts]
  for (const r of allCodedRuns) {
    if (r.ymd >= raceYmd || r.ymd < lo) continue;
    const rank = r.fav;
    if (rank == null || rank < 1 || rank > 18) continue;
    let s = sums.get(rank); if (!s) { s = [0, 0]; sums.set(rank, s); }
    if (r.f === 1) s[0]++;   // wins
    s[1]++;                  // starts (always)
  }
  bl = new Map();
  for (const [rank, [w, n]] of sums) bl.set(rank, n ? w / n : 0);
  _aeBaseline.set(key, bl);
  return bl;
}

const _aeCache = new Map(); // key `${kind}|${code}|${raceYmd}` → ae|null
export function jtAEAsOf(code, raceYmd, kind /* 'jockey' | 'trainer' */) {
  if (!code) return null;
  const idx = index();
  const isJockey = kind === 'jockey';
  const windowDays = isJockey ? 90 : 180;
  const floor = isJockey ? MIN_JOCKEY_STARTS : MIN_TRAINER_STARTS;
  const byCode = isJockey ? idx.byJockey : idx.byTrainer;
  const allCoded = isJockey ? idx.jockeyRuns : idx.trainerRuns;

  const cacheKey = `${kind}|${code}|${raceYmd}`;
  if (_aeCache.has(cacheKey)) return _aeCache.get(cacheKey);

  const lo = shiftYmd(raceYmd, -windowDays);
  const mounts = (byCode.get(code) || []).filter(r => r.ymd < raceYmd && r.ymd >= lo);
  let ae = null;
  if (mounts.length >= floor) {
    const bl = baselineAsOf(allCoded, raceYmd, windowDays, kind);
    let wins = 0, expected = 0;
    for (const r of mounts) {
      if (r.f === 1) wins++;
      const rank = r.fav;
      if (rank != null && rank >= 1 && rank <= 18) expected += (bl.get(rank) ?? 0);
    }
    ae = expected > 0 ? Math.round((wins / expected) * 10000) / 10000 : null;
  }
  _aeCache.set(cacheKey, ae);
  return ae;
}

// ── Step 1.5: point-in-time sire-fit ──────────────────────────────────────────
// Mirrors the sire_performance MV (sire × surface × distance bucket win%/place%), but counting only
// progeny finishes strictly BEFORE the race. Floored at MinSireStarts=10 like the controller lookup.
const _sireCache = new Map(); // key `${sire}|${raceYmd}|${surface}|${bucket}` → {win,place}|null
export function sireFitAsOf(sireId, raceYmd, raceSurface, raceDistance) {
  if (!sireId || !raceSurface) return { sireFit: null, sirePlaceFit: null, sireStarts: null };
  const bucket = distanceBucket(raceDistance);
  if (!bucket) return { sireFit: null, sirePlaceFit: null, sireStarts: null };

  const key = `${sireId}|${raceYmd}|${raceSurface}|${bucket}`;
  if (_sireCache.has(key)) return _sireCache.get(key);

  const runs = (index().bySire.get(sireId) || [])
    .filter(r => r.ymd < raceYmd && r.s === raceSurface && distanceBucket(r.d) === bucket);
  let out = { sireFit: null, sirePlaceFit: null, sireStarts: null };
  if (runs.length >= MIN_SIRE_STARTS) {
    const wins = runs.filter(r => r.f === 1).length;
    const places = runs.filter(r => r.f >= 1 && r.f <= 3).length;
    out = {
      sireFit: Math.round(10000 * wins / runs.length) / 100,
      sirePlaceFit: Math.round(10000 * places / runs.length) / 100,
      sireStarts: runs.length,
    };
  }
  _sireCache.set(key, out);
  return out;
}
