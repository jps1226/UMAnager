// ============================================================
// Counterfactual backtest — GROUND-TRUTH VALIDATION.
//
// Replays a single past race day in the SAME mode the operator actually ran ("🧪 Auto — engine
// picks per race" at a fixed risk) and compares the tool's net/recovery to the KNOWN reconciled
// actual, to prove the replay pipeline (pick → price → settle) is faithful before we trust its
// numbers for tuning decisions.
//
// Known ground truth (from the 2026-06-21 OrePro reconciliation, dev_log s48):
//   2026-06-21 (Sun, 36 races): staked ¥349,600 · won ¥137,290 · net −¥212,310 · 39% recovery · 16/36 hits.
// Auto-per-race chooses each race's preset from its field SHAPE (SHAPE_TO_PRESET), small fields get
// the ¥2,000 Q+Wide token — replicated here via the engine's own map.
//
// NOTE: an EXACT match isn't expected — the live run had a mid-card cookie expiry/re-run, some manual
// marks, and per-race stake nuances the replay doesn't carry. A net + recovery in the same neighborhood
// (and matching direction/magnitude) is the validation: the engine path settles real cards realistically.
//
// Run:  node tools/backtest/validate-actual.mjs [date] [risk]
// ============================================================
import { loadEngine, loadWeekend, runRace, fmt } from './harness.mjs';

const DATE = process.argv[2] || '2026-06-21';
const RISK = Number.isFinite(parseInt(process.argv[3], 10)) ? parseInt(process.argv[3], 10) : 30;

const BT = loadEngine();
const { races } = loadWeekend(BT, [DATE]);
const bal = BT.comp('balanced');
const shapeMap = BT.shapeToPreset();

// Pick each race's preset the way the live Auto-per-race mode does: probe the field SHAPE with a
// neutral Balanced read, map shape→preset; a small field gets the token; genuine skips (no odds/
// empty) return no preset → not bet.
function autoComp(rid) {
  const probe = BT.plan(rid, { compositionOverride: bal, riskOverride: RISK });
  if (probe.shape === 'small-field') return { comp: BT.smallTokenComp(), shape: 'small-field' };
  const presetId = shapeMap[probe.shape];
  return presetId ? { comp: BT.comp(presetId), shape: probe.shape, presetId } : { comp: null, shape: probe.shape };
}

let staked = 0, won = 0, bet = 0, hits = 0, abstain = 0;
const byPreset = {};
for (const m of races) {
  if (!m.settled) continue;
  const a = autoComp(m.rid);
  if (!a.comp) { abstain++; continue; }
  const r = runRace(BT, m, a.comp, RISK);
  if (r.status !== 'bet') { abstain++; continue; }
  staked += r.staked; won += r.won; bet++; if (r.won > 0) hits++;
  const key = a.presetId || a.shape;
  byPreset[key] = byPreset[key] || { bet: 0, staked: 0, won: 0 };
  byPreset[key].bet++; byPreset[key].staked += r.staked; byPreset[key].won += r.won;
}
const net = won - staked, roi = staked ? won / staked * 100 : 0, hitRate = bet ? hits / bet * 100 : 0;

console.log(`\nGround-truth validation — ${DATE}, Auto-per-race @ RISK ${RISK}\n`);
console.log(`REPLAY :  ${bet} bet (${abstain} skip) · staked ${fmt(staked)} · won ${fmt(won)} · net ${fmt(net)} · ${roi.toFixed(0)}% recovery · ${hits}/${bet} hits`);
if (DATE === '2026-06-21')
  console.log(`ACTUAL :  36 bet · staked ¥349,600 · won ¥137,290 · net -¥212,310 · 39% recovery · 16/36 hits`);
console.log('\nReplay by chosen preset (bet · staked · won · recovery):');
for (const [k, v] of Object.entries(byPreset).sort((a, b) => b[1].staked - a[1].staked))
  console.log(`  ${k.padEnd(15)} ${String(v.bet).padStart(2)} · ${fmt(v.staked).padStart(9)} · ${fmt(v.won).padStart(9)} · ${(v.staked ? v.won / v.staked * 100 : 0).toFixed(0)}%`);
console.log('\n(Not an exact match by design — see header. Same ballpark + direction = the engine path is faithful.)\n');
