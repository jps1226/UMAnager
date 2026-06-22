// ============================================================
// DISCIPLINE-MODE RECOVERY — does the cold "Discipline" engine actually preserve capital? (§0 Step 2)
//
// Replays every settled bench weekend in DISCIPLINE MODE (slider pinned to DISCIPLINE_RISK, bet type
// auto-chosen SAFE/place-biased per race) and settles vs stored dividends, then compares recovery to
// two baselines at the operator's default risk 30: Balanced and Win+Place. Pure offline measurement.
//
// Point-in-time forced on. Read-only. Run:  node tools/backtest/discipline-sweep.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { loadEngine, loadWeekend, runRace, fmt, pad, fixturesDir } from './harness.mjs';

const HIST = path.join(fixturesDir, 'history.json');
const BT = loadEngine();
const raw = JSON.parse(fs.readFileSync(HIST, 'utf8'));
const dates = Object.keys(raw.past_races_by_date).sort();
const { races } = loadWeekend(BT, dates, HIST, { pointInTime: true });
const settled = races.filter(r => r.settled);

// Tally a full replay: cb(m) → composition for that race; risk via the mode. Returns totals.
function replay(label, getComp, disciplineOn) {
  BT.setDisciplineMode(disciplineOn);
  let staked = 0, won = 0, bet = 0, hits = 0, abst = 0;
  for (const m of settled) {
    const comp = getComp(m);
    const res = runRace(BT, m, comp);
    if (res.status !== 'bet') { abst++; continue; }
    staked += res.staked; won += res.won; bet++;
    if (res.won > 0) hits++;
  }
  BT.setDisciplineMode(false);
  return { label, staked, won, net: won - staked, bet, abst, hits,
           rec: staked ? won / staked : 0, hitPct: bet ? hits / bet : 0 };
}

const disciplineRow = replay('Discipline mode', m => BT.resolveComp(m.rid), true);
// Baselines need a fixed risk 30; runRace's default reads the (stubbed) slider, so force via riskOverride.
function replayAtRisk(label, comp, risk) {
  let staked = 0, won = 0, bet = 0, hits = 0, abst = 0;
  for (const m of settled) {
    const res = runRace(BT, m, comp, risk);
    if (res.status !== 'bet') { abst++; continue; }
    staked += res.staked; won += res.won; bet++; if (res.won > 0) hits++;
  }
  return { label, staked, won, net: won - staked, bet, abst, hits, rec: staked ? won / staked : 0, hitPct: bet ? hits / bet : 0 };
}
const clean = [
  disciplineRow,
  replayAtRisk('Balanced @ risk 30',  BT.comp('balanced'),  30),
  replayAtRisk('Win+Place @ risk 30', BT.comp('win_place'), 30),
];

const pct = x => (100 * x).toFixed(1) + '%';
console.log(`\nDISCIPLINE-MODE RECOVERY — ${settled.length} settled races, point-in-time\n`);
console.log(' strategy               bet   abst   hit%    staked        returned      net           recovery');
for (const r of clean) {
  console.log('  ' + pad(r.label, 20) + ' ' + pad(r.bet, 4) + '  ' + pad(r.abst, 4) + '  ' + pad(pct(r.hitPct), 6) +
    '  ' + pad(fmt(r.staked), 12) + '  ' + pad(fmt(r.won), 12) + '  ' + pad(fmt(r.net), 12) + '  ' + pad(pct(r.rec), 7));
}
console.log(`\n── Read ──`);
const d = clean[0];
console.log(`Discipline recovers ${pct(d.rec)} (loses ${pct(1 - d.rec)}). The bench's place-recovery ceiling is ~80-85%.`);
console.log(`It is a LOSS-MINIMIZER, not a profit engine — judge it by recovery vs the baselines, not by net.`);
console.log(`(Replayed history, final-poll odds, flat preset stakes. One bench — a prior, not live proof.)\n`);
