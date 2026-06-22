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
// counts). Fields it CANNOT cheaply reconstruct as-of-date (sire-fit MV, jockey/trainer rolling windows)
// are NULLED by the caller — dropping a low-weight signal is honest; keeping a leaky one is not.
// Form_Score / Last3 are already date-gated in the controller, so they stay.
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

let _byHorse = null;
function index() {
  if (_byHorse) return _byHorse;
  const rows = JSON.parse(fs.readFileSync(HIST, 'utf8'));
  _byHorse = new Map();
  for (const r of rows) {
    let arr = _byHorse.get(r.h);
    if (!arr) { arr = []; _byHorse.set(r.h, arr); }
    arr.push(r);
  }
  for (const arr of _byHorse.values()) arr.sort((a, b) => (a.ymd < b.ymd ? -1 : 1));
  return _byHorse;
}

// Stats for a horse using ONLY its runs strictly before raceYmd (a 'YYYYMMDD' string).
// raceSurface/raceDistance describe the race being bet. Returns the fields to overwrite on the entry.
export function statsAsOf(horseId, raceYmd, raceSurface, raceDistance) {
  const all = index().get(String(horseId).split('.')[0]) || [];
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
