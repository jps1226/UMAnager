// ============================================================
// Counterfactual backtest — RISK SWEEP (project #2, first cut).
//
// Replays a SETTLED race weekend through the REAL engine at every risk level and settles the
// resulting tickets against the REAL stored dividends + finishing order, to answer:
// "does turning the RISK slider up actually make more money?"  Reuses the shipping pick/price/
// settle code via harness.mjs, so the numbers can't drift from what the live app would do.
//
// MODELLING CHOICE (first cut): the bet PRESET is held fixed (default "balanced") for every race
// so that RISK is the ONLY moving variable — the cleanest read of "does risk help". Pass a
// different preset id as argv[3] to hold another preset instead.
// CAVEAT: stored odds are the last poll before each race, not the exact bet-time tick.
//
// Run:  node tools/backtest/risk-sweep.mjs [date,date,...] [presetId]
//   e.g. node tools/backtest/risk-sweep.mjs 2026-06-20,2026-06-21 balanced
//   BT_DEBUG=1 prefix dumps the first 3 races' marks → tickets → settlement for eyeballing.
// ============================================================
import { loadEngine, loadWeekend, runRace, fmt, pad, median } from './harness.mjs';

const argDates = (process.argv[2] || '2026-06-20,2026-06-21').split(',').map(s => s.trim()).filter(Boolean);
const PRESET = process.argv[3] || 'balanced';
const RISKS = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100];

const BT = loadEngine();
const { races, totalRaces, unsettled } = loadWeekend(BT, argDates);
const comp = BT.comp(PRESET);

console.log(`\nCounterfactual risk sweep — preset held at "${PRESET}"`);
console.log(`Weekend: ${argDates.join(' + ')}  |  ${totalRaces} races (${unsettled} unsettled, skipped)\n`);
console.log('risk   bet  abst   staked        won          net        ROI    hit%');
console.log('────  ────  ────  ──────────   ──────────   ──────────   ─────  ─────');

const rows = [];
for (const risk of RISKS) {
  let staked = 0, won = 0, bet = 0, hits = 0, abstain = 0;
  for (const m of races) {
    if (!m.settled) continue;
    const r = runRace(BT, m, comp, risk);
    if (r.status !== 'bet') { abstain++; continue; }
    staked += r.staked; won += r.won; bet++; if (r.won > 0) hits++;
    if (process.env.BT_DEBUG && bet <= 3) {
      console.log(`\n[debug ${m.rid}] risk ${risk}  finish(top3 pp)=${m.t3.join('-')}  marks=${r.plan.assignments.map(a => a.symbol + a.h_id.slice(-3) + '(pp' + m.ppByHorse[a.h_id] + ')').join(' ')}`);
      r.lineDetail.forEach(l => console.log('   ' + l));
      console.log(`   staked ${r.staked}  won ${r.won}`);
    }
  }
  const net = won - staked;
  const roi = staked ? (won / staked * 100) : 0;
  const hitRate = bet ? (hits / bet * 100) : 0;
  rows.push({ risk, bet, abstain, staked, won, net, roi, hitRate });
  console.log(
    `${pad(risk, 4)}  ${pad(bet, 4)}  ${pad(abstain, 4)}  ${pad(fmt(staked), 10)}   ${pad(fmt(won), 10)}   ${pad(fmt(net), 10)}   ${pad(roi.toFixed(1), 5)}  ${pad(hitRate.toFixed(1), 5)}`
  );
}

// ── Verdict: did net trend UP as risk rose? (the H1 question) ──
// MEDIAN, not mean — a single longshot-driven weekend can post one huge net spike on a tiny
// hit rate (variance), and a mean would let that outlier flip the conclusion.
const best = rows.reduce((a, b) => (b.net > a.net ? b : a), rows[0]);
const worst = rows.reduce((a, b) => (b.net < a.net ? b : a), rows[0]);
const safe = rows.filter(r => r.risk <= 40);
const bold = rows.filter(r => r.risk >= 60);
const safeMed = median(safe.map(r => r.net));
const boldMed = median(bold.map(r => r.net));
const safeHitAvg = safe.reduce((s, r) => s + r.hitRate, 0) / safe.length;
const spikes = rows.filter(r => r.net > 0 && r.hitRate < safeHitAvg * 0.5);

// Recovery (ROI) by risk THIRD, via median so the variance spikes don't distort it. This reveals
// the SHAPE of the risk→return curve — often an inverted-U (mid beats both extremes), which the
// two-half net split below can't show.
const zoneRoi = (lo, hi) => median(rows.filter(r => r.risk >= lo && r.risk <= hi).map(r => r.roi));
const loRoi = zoneRoi(0, 30), midRoi = zoneRoi(40, 60), hiRoi = zoneRoi(70, 100);

console.log('\n── Read ──');
console.log(`Best net at risk ${best.risk} (${fmt(best.net)}); worst at risk ${worst.risk} (${fmt(worst.net)}).`);
console.log(`Median recovery by zone — low (0-30): ${loRoi.toFixed(0)}%   mid (40-60): ${midRoi.toFixed(0)}%   high (70-100): ${hiRoi.toFixed(0)}%.`);
const peakZone = midRoi >= loRoi && midRoi >= hiRoi ? 'MID (≈40-50)' : loRoi >= hiRoi ? 'LOW (≈0-30)' : 'HIGH (≈70+)';
console.log(`→ Best-recovering zone this sample: ${peakZone}.`);
console.log(`MEDIAN net — safe half (risk 0-40): ${fmt(safeMed)}   vs   bold half (risk 60-100): ${fmt(boldMed)}.`);
for (const s of spikes)
  console.log(`  ⚠ risk ${s.risk}: ${fmt(s.net)} on only ${s.hitRate.toFixed(1)}% hits — a VARIANCE spike (rare big longshots), not a reliable edge.`);
console.log(boldMed > safeMed
  ? '→ By median, higher risk did BETTER this weekend (a point FOR raising RISK — watch it hold).'
  : '→ By median, higher risk did WORSE this weekend (consistent with H1 = rejected).');
console.log('\n(One weekend only — do not change engine math off a single read. Log + watch ≥3 weekends.)\n');
