// ============================================================
// ODDS-DRIFT RECOVERY (MONEY) TEST — TARGET's 前日→直前 odds-movement / overlay angle (TARGET scope).
//
// TARGET surfaces how each horse's odds MOVE (previous → just-before-post). The handicapping lore: late
// money is informed — horses whose odds FIRM (shorten) are "steaming" and outperform; horses that DRIFT
// (lengthen) are being abandoned. The North-Star question (s51-52 lesson): the final price already reflects
// the move, so is there edge LEFT? We test it the only honest way on this bench — classify each horse by its
// move (known PRE-post), settle a flat PLACE bet at the REAL dividend, and ask whether, WITHIN the same final
// market band (FavRank), firmers out-recover the band (a late-steam overlay) or drifters under-recover (a fade).
//
// Method (point-in-time — the move is fully known before the race; settlement at stored `place` dividends):
//   • drift ratio = Odds / Prev_Odds. FIRM <= 1-MOVE (shortened), DRIFT >= 1+MOVE (lengthened), else STEADY.
//   • Stratify by FavRank band; the overlay (if any) should live in the longshots, like H7/H12.
//   • Baseline = the same entries that HAVE a prev-odds reading (so FIRM/DRIFT are compared like-for-like).
//
// ⚠ Coverage caveat: Prev_Odds is only populated on the live-polled weekends (~35% of bench entries), so this
// is a smaller, recency-biased sample than the full-coverage factors. Exploratory. Read-only.
// Run:  node tools/backtest/odds-drift-recovery.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const bench = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'history.json'), 'utf8'));

const MOVE = +(process.env.OD_MOVE ?? 0.15);   // ±15% move to count as firm/drift (env-overridable)
const STAKE = 100;
const favBand = f => (f == null ? null : f <= 3 ? 'chalk (1-3)' : f <= 8 ? 'mid (4-8)' : 'longshot (9+)');
const BANDS = ['chalk (1-3)', 'mid (4-8)', 'longshot (9+)'];

const cell = {};
const grp = (g, b) => (((cell[g] ??= {})[b] ??= { staked: 0, returned: 0, bets: 0, hits: 0 }));
const place = (g, b, pp, placeDivs, ret) => { const c = grp(g, b); c.staked += STAKE; c.bets++; if (ret) { c.returned += ret * (STAKE / 100); c.hits++; } };

const byDate = bench.past_races_by_date || {};
const diag = { FIRM: [], DRIFT: [], FIELD: [] };  // longshot-band per-bet {date, ret}
let settledRaces = 0, withPrev = 0;
for (const date of Object.keys(byDate)) {
  for (const race of byDate[date]) {
    let payouts = null; try { payouts = JSON.parse(race.info.results_json); } catch (_) {}
    const placeDivs = payouts && payouts.place;
    if (!placeDivs || !placeDivs.length) continue;
    settledRaces++;
    const retOf = pp => { const w = placeDivs.find(d => d.combo && d.combo[0] === pp); return w ? w.payout : 0; };
    for (const e of (race.entries || [])) {
      if (e.Scratched === true) continue;
      const pp = parseInt(e.PP, 10); if (!Number.isFinite(pp)) continue;
      const band = favBand(parseInt(e.Fav, 10)); if (!band) continue;
      const o = parseFloat(e.Odds), p = parseFloat(e.Prev_Odds);
      if (!(o > 0 && p > 0)) continue;           // only entries with a prev-odds reading
      withPrev++;
      const ret = retOf(pp);
      const ratio = o / p;
      place('FIELD (has prev-odds)', band, pp, placeDivs, ret);
      if (band === 'longshot (9+)') diag.FIELD.push({ date, ret });
      if (ratio <= 1 - MOVE)      { place('FIRM (shortened)', band, pp, placeDivs, ret); if (band === 'longshot (9+)') diag.FIRM.push({ date, ret }); }
      else if (ratio >= 1 + MOVE) { place('DRIFT (lengthened)', band, pp, placeDivs, ret); if (band === 'longshot (9+)') diag.DRIFT.push({ date, ret }); }
    }
  }
}

const rec = c => (c.staked ? (100 * c.returned / c.staked).toFixed(1) + '%' : '—');
const hr = c => (c.bets ? (100 * c.hits / c.bets).toFixed(0) + '%' : '—');
const roll = g => { const t = { staked: 0, returned: 0, bets: 0, hits: 0 }; for (const b of BANDS) { const c = cell[g]?.[b]; if (c) { t.staked += c.staked; t.returned += c.returned; t.bets += c.bets; t.hits += c.hits; } } return t; };

console.log(`\nODDS-DRIFT RECOVERY (PLACE / 複勝) — ${settledRaces} settled races, ${withPrev} entries with prev-odds, ¥${STAKE}/bet`);
console.log(`FIRM = final <= ${(100 * (1 - MOVE)).toFixed(0)}% of prev · DRIFT = final >= ${(100 * (1 + MOVE)).toFixed(0)}% of prev\n`);
const GROUPS = ['FIELD (has prev-odds)', 'FIRM (shortened)', 'DRIFT (lengthened)'];
const w = 24;
console.log('group'.padEnd(w) + 'band'.padEnd(16) + 'recovery'.padStart(10) + 'hit'.padStart(7) + 'bets'.padStart(9));
for (const g of GROUPS) {
  for (const b of BANDS) { const c = cell[g]?.[b] || { staked: 0, returned: 0, bets: 0, hits: 0 }; console.log(g.padEnd(w) + b.padEnd(16) + rec(c).padStart(10) + hr(c).padStart(7) + String(c.bets).padStart(9)); }
  const t = roll(g); console.log(''.padEnd(w) + 'ALL'.padEnd(16) + rec(t).padStart(10) + hr(t).padStart(7) + String(t.bets).padStart(9) + '\n');
}

// robustness on the longshot band
const recOf = a => a.length ? (a.reduce((s, x) => s + x.ret, 0) / a.length).toFixed(1) + '%' : '—';
const dropTop = (a, k) => { const s = [...a].sort((x, y) => y.ret - x.ret).slice(k); return s.length ? (s.reduce((t, x) => t + x.ret, 0) / s.length).toFixed(1) + '%' : '—'; };
const dates = [...new Set([].concat(...Object.values(diag)).map(x => x.date))].sort();
const mid = dates[Math.floor(dates.length / 2)];
const half = (a, lo, hi) => a.filter(x => x.date >= lo && x.date <= hi);
console.log('── ROBUSTNESS (longshot band) ─────────────────────────────────────');
console.log('group        H1 recov   H2 recov   full   drop-top3   drop-top5     bets');
for (const g of ['FIRM', 'DRIFT', 'FIELD']) {
  const a = diag[g]; const h1 = half(a, dates[0], mid), h2 = half(a, mid, dates[dates.length - 1]);
  console.log(g.padEnd(12) + recOf(h1).padStart(8) + recOf(h2).padStart(11) + recOf(a).padStart(9) + dropTop(a, 3).padStart(12) + dropTop(a, 5).padStart(12) + String(a.length).padStart(9));
}
console.log('\nRead: within the longshot band, FIRM >> FIELD = late steam is a usable overlay; DRIFT << FIELD =');
console.log('a fade. If both ~= FIELD, the move is already in the final price (priced-in). Watch the coverage caveat.\n');
