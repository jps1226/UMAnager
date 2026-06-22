// ============================================================
// SEGMENT RECOVERY — is the market less efficient in MAIDEN races or LATE-CARD races?
//
// Operator question: should we bet differently in maiden (unproven-form) races and in late-card races?
// A market that is LESS sharp in a segment shows up as favorites placing less reliably and/or longshots
// recovering BETTER than usual there — i.e. value leaks to the outsiders. We measure place (複勝) recovery
// + place hit-rate by market band (chalk/mid/long), split two ways:
//   1) race CLASS — maiden+debut (no win yet) vs proven (1win/2win/3win/open)
//   2) race NUMBER — early (R1-4) / mid (R5-8) / late (R9-12)
//
// Read-only, point-in-time by construction (uses only stored result dividends). Flat ¥100 place bet.
// Run:  node tools/backtest/segment-recovery.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const bench = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'history.json'), 'utf8'));

const STAKE = 100;
const favBand = f => (f == null ? null : f <= 3 ? 'chalk (1-3)' : f <= 8 ? 'mid (4-8)' : 'longshot (9+)');
const BANDS = ['chalk (1-3)', 'mid (4-8)', 'longshot (9+)'];

const classGroup = rc => (rc === 'maiden' || rc === 'debut') ? 'MAIDEN/debut' : (rc ? 'PROVEN (1win+)' : null);
const numGroup = n => (n <= 4 ? 'early R1-4' : n <= 8 ? 'mid R5-8' : 'late R9-12');

// cell[dimension][segment][band] = {staked, returned, bets, hits}
const cell = {};
const grp = (dim, seg, b) => ((((cell[dim] ??= {})[seg] ??= {})[b] ??= { staked: 0, returned: 0, bets: 0, hits: 0 }));
const place = (dim, seg, b, pp, placeDivs) => {
  const c = grp(dim, seg, b); c.staked += STAKE; c.bets++;
  const win = placeDivs.find(d => d.combo && d.combo[0] === pp);
  if (win) { c.returned += win.payout * (STAKE / 100); c.hits++; }
};

const byDate = bench.past_races_by_date || {};
for (const date of Object.keys(byDate)) {
  for (const race of byDate[date]) {
    let payouts = null; try { payouts = JSON.parse(race.info.results_json); } catch (_) {}
    const placeDivs = payouts && payouts.place;
    if (!placeDivs || !placeDivs.length) continue;
    const cg = classGroup(race.info.race_class);
    const ng = numGroup(parseInt(race.info.race_number, 10) || 0);
    for (const e of (race.entries || [])) {
      if (e.Scratched === true) continue;
      const pp = parseInt(e.PP, 10); if (!Number.isFinite(pp)) continue;
      const band = favBand(parseInt(e.Fav, 10)); if (!band) continue;
      if (cg) place('CLASS', cg, band, pp, placeDivs);
      place('RACE#', ng, band, pp, placeDivs);
    }
  }
}

const rec = c => (c.staked ? (100 * c.returned / c.staked).toFixed(1) + '%' : '—');
const hr = c => (c.bets ? (100 * c.hits / c.bets).toFixed(0) + '%' : '—');

function table(dim, segments) {
  console.log(`\n── ${dim} : place recovery / hit-rate by market band ──`);
  console.log('segment'.padEnd(16) + BANDS.map(b => b.padStart(20)).join(''));
  for (const seg of segments) {
    let line = seg.padEnd(16);
    for (const b of BANDS) {
      const c = cell[dim]?.[seg]?.[b] || { staked: 0, bets: 0 };
      line += `${rec(c)} (${hr(c)}, n=${c.bets})`.padStart(20);
    }
    console.log(line);
  }
}

console.log('\nSEGMENT RECOVERY (PLACE / 複勝) — flat ¥100, all settled bench races');
console.log('Higher longshot recovery / lower chalk hit = market less sharp in that segment = value to outsiders.');
table('CLASS', ['MAIDEN/debut', 'PROVEN (1win+)']);
table('RACE#', ['early R1-4', 'mid R5-8', 'late R9-12']);
console.log('');
