// ============================================================
// FAVORITE-BIAS LAB — the targeted follow-up to value-lab.mjs (TODO §0 Step 2, narrowed hunt).
//
// The broad scan said raw merit has no global edge, and the one real signal is the favourite-longshot
// bias (short prices under-bet, longshots over-bet). This tool asks the sharper question:
//   1. QUANTIFY the bias: Win + Place recovery by market favourite-rank and by odds band.
//   2. Does our point-in-time MERIT add value where the market is sharpest? Among the market's top
//      choices, split into merit-AGREES (we also rate it top) vs merit-SKEPTIC (we rate it lower) —
//      "false favourites" — and compare. If merit-agree favourites recover better, the cold engine has
//      a real job (back solid favs, fade hollow ones). If not, merit adds nothing even here.
//
// Honesty rails: point-in-time forced on; flat stakes; settled vs stored Win/Place dividends.
// Read-only. Run:  node tools/backtest/favorite-bias.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { loadEngine, loadWeekend, pad, fixturesDir } from './harness.mjs';

const HIST = path.join(fixturesDir, 'history.json');
const STAKE = 100;
const pct = x => (100 * x).toFixed(1) + '%';

const BT = loadEngine();
const raw = JSON.parse(fs.readFileSync(HIST, 'utf8'));
const dates = Object.keys(raw.past_races_by_date).sort();
const { races } = loadWeekend(BT, dates, HIST, { pointInTime: true });

// Build one row per priced runner: favRank, meritRank (within race), odds, won, placed, win/place dividend.
const rows = [];
for (const r of races) {
  if (!r.settled) continue;
  const t3 = r.t3, winPp = t3[0], t3set = new Set(t3);
  const winDiv = {}, placeDiv = {};
  for (const w of (r.payouts.win || [])) winDiv[w.combo[0]] = w.payout;
  for (const p of (r.payouts.place || [])) placeDiv[p.combo[0]] = p.payout;
  const pool = [];
  for (const e of BT.entries(r.rid)) {
    const odds = parseFloat(e.Odds), merit = BT.merit(e), pp = parseInt(e.PP, 10), fav = parseInt(e.Fav, 10);
    if (!Number.isFinite(odds) || odds <= 1 || !Number.isFinite(merit) || !Number.isFinite(pp) || !Number.isFinite(fav)) continue;
    pool.push({ pp, fav, odds, merit, won: pp === winPp, placed: t3set.has(pp), winDiv: winDiv[pp] || 0, placeDiv: placeDiv[pp] || 0 });
  }
  if (pool.length < 4) continue;
  // merit rank within the priced field (1 = highest merit)
  [...pool].sort((a, b) => b.merit - a.merit).forEach((h, i) => { h.meritRank = i + 1; });
  rows.push(...pool);
}

// ── helpers ──
const winRec = set => { let s = 0, w = 0; for (const h of set) { s += STAKE; if (h.won) w += STAKE * h.winDiv / 100; } return { n: set.length, rec: s ? w / s : 0, hit: set.length ? set.filter(h => h.won).length / set.length : 0 }; };
const plcRec = set => { let s = 0, w = 0; for (const h of set) { s += STAKE; if (h.placed) w += STAKE * h.placeDiv / 100; } return { n: set.length, rec: s ? w / s : 0, hit: set.length ? set.filter(h => h.placed).length / set.length : 0 }; };

console.log(`\nFAVOURITE-BIAS LAB — ${races.filter(r => r.settled).length} settled races, ${rows.length} priced runners, point-in-time merit\n`);

// 1. recovery by favourite rank
console.log('── 1. Recovery by market favourite-rank (the bias curve) ──');
console.log(' favRank   n      win%    WIN-rec   plc%    PLACE-rec');
for (let f = 1; f <= 10; f++) {
  const set = rows.filter(h => h.fav === f);
  if (!set.length) continue;
  const w = winRec(set), p = plcRec(set);
  console.log(`  ${pad(f, 2)}     ${pad(set.length, 5)}   ${pad(pct(w.hit), 6)}  ${pad(pct(w.rec), 7)}   ${pad(pct(p.hit), 6)}  ${pad(pct(p.rec), 7)}`);
}
const longshots = rows.filter(h => h.odds >= 20);
console.log(`  (longshots ≥20.0: ${longshots.length} runners, WIN-rec ${pct(winRec(longshots).rec)}, PLACE-rec ${pct(plcRec(longshots).rec)})`);

// 2. does merit separate true vs false favourites?
console.log('\n── 2. Does merit add value among the market\'s top choices? ──');
console.log('   (merit-AGREE = our merit ALSO ranks it that high; merit-SKEPTIC = we rate it lower)');
const bands = [[1, 1, 'the favourite (fav 1)'], [1, 2, 'top-2 market'], [1, 3, 'top-3 market']];
for (const [lo, hi, label] of bands) {
  const set = rows.filter(h => h.fav >= lo && h.fav <= hi);
  const agree = set.filter(h => h.meritRank <= hi);      // we also rank it within the top band
  const skept = set.filter(h => h.meritRank > hi);       // false favourite — we rate it lower
  const wa = winRec(agree), ws = winRec(skept), pa = plcRec(agree), ps = plcRec(skept);
  console.log(`\n  ${label}: ${set.length} runners`);
  console.log(`    merit-AGREE  (${pad(agree.length, 4)}):  WIN ${pct(wa.hit)} hit / ${pct(wa.rec)} rec    PLACE ${pct(pa.hit)} hit / ${pct(pa.rec)} rec`);
  console.log(`    merit-SKEPTIC(${pad(skept.length, 4)}):  WIN ${pct(ws.hit)} hit / ${pct(ws.rec)} rec    PLACE ${pct(ps.hit)} hit / ${pct(ps.rec)} rec`);
  const edge = wa.rec - ws.rec;
  console.log(`    → WIN recovery gap (agree − skeptic): ${(edge >= 0 ? '+' : '') + (100 * edge).toFixed(1)} pts ${Math.abs(edge) > 0.05 ? (edge > 0 ? '— merit helps: back solid favs, fade false ones' : '— merit HURTS here') : '— negligible (merit adds nothing where the market is sharp)'}`);
}

console.log('\n(Replayed history, final-poll odds, flat stakes, point-in-time merit. One bench — prior, not proof.)\n');
