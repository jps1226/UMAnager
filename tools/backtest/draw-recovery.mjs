// ============================================================
// DRAW / POST-POSITION RECOVERY (MONEY) TEST — Group-A factor (§0 Step 1.6 / Step 2).
//
// Does a horse's GATE (post position) move money the market hasn't already priced? Draw bias is real in
// racing (an outside gate, especially in a big field, often costs ground), and the crowd may under- or
// over-adjust for it. We test relative draw = (PP-1)/(field-1): inner / middle / outer third, plus the
// classic case (outer vs inner in BIG fields, ≥14 runners). If the market mis-prices a draw, that bucket
// out- or under-recovers the FIELD baseline WITHIN its market band.
//
// Point-in-time by construction (the draw is known pre-race) and full coverage. Flat ¥100 PLACE bet from
// stored dividends; recovery = ¥ back ÷ ¥ staked. Read-only.
// Run:  node tools/backtest/draw-recovery.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const bench = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'history.json'), 'utf8'));

const STAKE = 100;
const BIG_FIELD = +(process.env.DR_BIG ?? 14);
const favBand = f => (f == null ? null : f <= 3 ? 'chalk (1-3)' : f <= 8 ? 'mid (4-8)' : 'longshot (9+)');
const BANDS = ['chalk (1-3)', 'mid (4-8)', 'longshot (9+)'];

const drawBucket = frac => (frac == null ? null : frac <= 1 / 3 ? 'inner' : frac >= 2 / 3 ? 'outer' : 'middle');

// cell[dim][bucket][band] = {staked, returned, bets, hits}
const cell = {};
const grp = (dim, bk, b) => ((((cell[dim] ??= {})[bk] ??= {})[b] ??= { staked: 0, returned: 0, bets: 0, hits: 0 }));
const place = (dim, bk, b, pp, placeDivs) => {
  if (!bk) return;
  const c = grp(dim, bk, b); c.staked += STAKE; c.bets++;
  const win = placeDivs.find(d => d.combo && d.combo[0] === pp);
  if (win) { c.returned += win.payout * (STAKE / 100); c.hits++; }
};

const byDate = bench.past_races_by_date || {};
let settledRaces = 0;
for (const date of Object.keys(byDate)) {
  for (const race of byDate[date]) {
    let payouts = null; try { payouts = JSON.parse(race.info.results_json); } catch (_) {}
    const placeDivs = payouts && payouts.place;
    if (!placeDivs || !placeDivs.length) continue;
    settledRaces++;
    const runners = (race.entries || []).filter(e => e.Scratched !== true);
    const field = runners.length;
    if (field < 2) continue;
    for (const e of runners) {
      const pp = parseInt(e.PP, 10); if (!Number.isFinite(pp)) continue;
      const band = favBand(parseInt(e.Fav, 10)); if (!band) continue;
      const frac = (pp - 1) / (field - 1);
      place('FIELD', 'FIELD (all)', band, pp, placeDivs);
      place('DRAW (relative third)', drawBucket(frac), band, pp, placeDivs);
      if (field >= BIG_FIELD) {
        const bk = frac <= 1 / 3 ? `inner (≥${BIG_FIELD})` : frac >= 2 / 3 ? `outer (≥${BIG_FIELD})` : null;
        place('BIG FIELD draw', bk, band, pp, placeDivs);
      }
    }
  }
}

const rec = c => (c.staked ? (100 * c.returned / c.staked).toFixed(1) + '%' : '—');
const hr = c => (c.bets ? (100 * c.hits / c.bets).toFixed(0) + '%' : '—');
const fieldRef = b => cell.FIELD['FIELD (all)'][b];

function table(dim, buckets) {
  console.log(`\n── ${dim} : place recovery (hit, n) and Δ vs FIELD, by market band ──`);
  console.log('bucket'.padEnd(18) + BANDS.map(b => b.padStart(22)).join(''));
  let f = 'FIELD baseline'.padEnd(18);
  for (const b of BANDS) f += `${rec(fieldRef(b))} (${hr(fieldRef(b))}, n=${fieldRef(b).bets})`.padStart(22);
  console.log(f);
  for (const bk of buckets) {
    let line = bk.padEnd(18);
    for (const b of BANDS) {
      const c = cell[dim]?.[bk]?.[b];
      if (!c || !c.staked) { line += '—'.padStart(22); continue; }
      const d = (100 * c.returned / c.staked) - (100 * fieldRef(b).returned / fieldRef(b).staked);
      line += `${rec(c)} (${(d >= 0 ? '+' : '') + d.toFixed(0)}, n=${c.bets})`.padStart(22);
    }
    console.log(line);
  }
}

console.log(`\nDRAW / POST-POSITION RECOVERY (PLACE / 複勝) — point-in-time, ${settledRaces} settled races, ¥${STAKE}/bet`);
console.log('Δ = recovery minus the FIELD baseline in the SAME band. Big + with healthy n = candidate edge.');
table('DRAW (relative third)', ['inner', 'middle', 'outer']);
table('BIG FIELD draw', [`inner (≥${BIG_FIELD})`, `outer (≥${BIG_FIELD})`]);
console.log('');
