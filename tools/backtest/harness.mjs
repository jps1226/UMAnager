// ============================================================
// Shared backtest harness — loads the REAL wwwroot/static/script.js engine into a sandbox
// and replays settled races through it. Used by risk-sweep.mjs and preset-sweep.mjs so the
// sandbox + settlement code lives in ONE place and can't drift between tools.
//
// READ-ONLY. The only input is a saved /api/races payload (fixtures/races_raw.json), which
// carries each race's entries (odds + form), finishing order, and results_json (the actual
// win/quinella/wide/trio dividends). See README.md.
// ============================================================
import fs from 'node:fs';
import vm from 'node:vm';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { statsAsOf, jtAEAsOf, sireFitAsOf } from './point-in-time.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');
const scriptPath = path.join(repoRoot, 'src', 'UMAnager.Nexus', 'wwwroot', 'static', 'script.js');
export const fixturesDir = path.join(here, 'fixtures');
export const fixturePath = path.join(fixturesDir, 'races_raw.json');

// ── formatting / stats helpers shared by both sweeps ──
export const fmt = n => '¥' + Math.round(n).toLocaleString('en-US');
export const pad = (s, w) => String(s).padStart(w);
export const median = arr => {
  if (!arr.length) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};

// Load the REAL script.js into a sandbox (universal-DOM-stub trick from the parity harness),
// then append an epilogue that runs IN script.js's own module scope so it can read/populate the
// `let`-declared globals the engine reads + expose the engine helpers we drive.
export function loadEngine() {
  const stub = () => new Proxy(function () {}, {
    get: (_t, p) => (p === 'length' ? 0 : (p === Symbol.toPrimitive ? () => 0 : stub())),
    apply: () => stub(),
    construct: () => stub(),
  });
  const ctx = {
    console, JSON, Math, parseInt, parseFloat, isNaN, Number, String, Array, Object, Set, Map, Date, Proxy, Symbol, Infinity,
    setTimeout: () => {}, setInterval: () => {}, clearTimeout: () => {}, clearInterval: () => {},
    fetch: () => Promise.resolve(stub()),
  };
  ctx.window = ctx; ctx.globalThis = ctx; ctx.self = ctx;
  ctx.document = stub(); ctx.localStorage = stub(); ctx.navigator = stub(); ctx.location = stub();
  ctx.WebSocket = function () { return stub(); };
  ctx.requestAnimationFrame = () => {}; ctx.matchMedia = () => stub();

  const EPILOGUE = `
;(function () {
  try { if (typeof appConfig === 'object' && appConfig) appConfig.ui = appConfig.ui || {}; } catch (_) {}
  const replace = (target, src) => { for (const k in target) delete target[k]; Object.assign(target, src); };
  globalThis.__bt = {
    setData(entriesByRace, infoByRace, classByRace) {
      replace(globalRaceEntries, entriesByRace);
      replace(globalRaceInfo, infoByRace);
      replace(globalRaceClass, classByRace);
      replace(globalMarks, {});
      replace(globalRaceMeta, {});
      replace(globalDayBetCompositions, {});
    },
    raceClassFlags: (rc) => raceClassFlags(rc),
    plan: (rid, opts) => getEngineMarkPlanForRace(rid, opts),
    comp: (presetId) => compositionFromPreset(presetId),
    presetId: (comp) => compositionPresetId(comp),
    buildLines: (comp, runners) => buildLinesFromComposition(comp, runners),
    scoreLine: (line, t3, t3set, payouts) => scoreBetLine(line, t3, t3set, payouts),
    // For replaying the live "🧪 Auto — engine picks per race" mode: the shape→preset map +
    // the small-field token composition, straight from the engine so they can't drift.
    shapeToPreset: () => SHAPE_TO_PRESET,
    smallTokenComp: () => smallFieldTokenComposition(),
    // For the upset autopsy: the cleaned entry rows for a race, and a horse's STATS-ONLY grade
    // (the engine's own form + pedigree merit, with the market/odds term factored OUT).
    entries: (rid) => globalRaceEntries[rid] || [],
    merit: (row) => { const e = explainPowerScore(row, 30); return e.form.subtotal + e.pedigree.subtotal; },
    // s54: the post-race "Why It Won" autopsy compute (pure logic, no DOM) — winner grade
    // (chalk/catchable/semi/freak), field ranks, and the engine's ◎ result. Same code the UI runs.
    autopsy: (rid) => computeRaceAutopsy(rid),
    // For the stats-tilt test: override the live formula weights (the real Settings lever) so we can
    // replay with the picker trusting form/breeding more vs odds. getFormulaWeights reads this.
    setFormulaWeights: (w) => { appConfig.ui = appConfig.ui || {}; appConfig.ui.formulaWeights = w; },
    // Discipline mode (cold engine) — toggle it + read what it derives, so we can backtest it.
    setDisciplineMode: (on) => { appConfig.ui = appConfig.ui || {}; appConfig.ui.disciplineMode = !!on; },
    disciplineRisk: () => getCurrentAutoPickRisk(),
    resolveComp: (rid) => resolveBetComposition(rid),
    isDiscipline: () => isDisciplineMode(),
  };
})();
`;
  vm.createContext(ctx);
  try {
    vm.runInContext(fs.readFileSync(scriptPath, 'utf8') + EPILOGUE, ctx, { filename: 'script.js' });
  } catch (e) {
    // Top-level DOM side-effects may throw on the stubs — fine, hoisted fns + epilogue still bind.
  }
  const BT = ctx.__bt;
  if (!BT || typeof BT.plan !== 'function') {
    console.error('FATAL: engine helpers not bound — the sandbox or a function name moved.');
    process.exit(2);
  }
  return BT;
}

// Read the fixture, build the engine-input globals + per-race settlement data for the given
// JST dates, push the globals into the engine, and return the per-race list + counts.
// opts.pointInTime (default = env BT_PIT==='1'): rebuild each horse's Record/Surface/Distance stats from
// its runs BEFORE the race and NULL the un-reconstructable leaky fields (sire-fit / jockey & trainer A/E),
// so the replay can't see the future. Form_Score/Last3 stay (already date-gated). See point-in-time.mjs.
export function loadWeekend(BT, dates, fixtureFile = fixturePath, opts = {}) {
  const pit = opts.pointInTime ?? (process.env.BT_PIT === '1');
  const raw = JSON.parse(fs.readFileSync(fixtureFile, 'utf8'));
  const byDate = raw.past_races_by_date || {};
  const entriesByRace = {}, infoByRace = {}, classByRace = {};
  const races = [];
  let totalRaces = 0, unsettled = 0;

  for (const date of dates) {
    for (const race of (byDate[date] || [])) {
      totalRaces++;
      const rid = race.info.race_id;
      // Scratched horses (取消/除外) aren't bettable — drop them so the engine never picks one.
      const entries = (race.entries || []).filter(e => e.Scratched !== true);
      entries.forEach((row, idx) => { row._raceId = rid; row.original_index = idx; });
      if (pit) {
        const ymd = String(rid).slice(0, 8);
        for (const row of entries) {
          const s = statsAsOf(row.Horse_ID, ymd, race.info.surface, race.info.distance);
          row.Record = s.record;
          row.Surface_Win_Pct = s.surfaceWinPct; row.Surface_Starts = s.surfaceStarts;
          row.Dist_Win_Pct = s.distWinPct; row.Dist_Starts = s.distStarts;
          // Step 1.5: rebuild jockey/trainer A/E + sire-fit point-in-time (mounts/progeny strictly
          // before the race; A/E baseline rebuilt from the same window). These were nulled in Step 1.
          row.Jockey_AE = jtAEAsOf(row.Jockey_Code, ymd, 'jockey');
          row.Trainer_AE = jtAEAsOf(row.Trainer_Code, ymd, 'trainer');
          const sf = sireFitAsOf(row.Sire_ID, ymd, race.info.surface, race.info.distance);
          row.Sire_Fit = sf.sireFit; row.Sire_Place_Fit = sf.sirePlaceFit; row.Sire_Starts = sf.sireStarts;
          // Own-record place splits aren't read by the scorer (win-pct only) → leave null.
          row.Surface_Place_Pct = null; row.Dist_Place_Pct = null;
        }
      }
      entriesByRace[rid] = entries;
      infoByRace[rid] = { ...race.info, _timeline: 'past' };
      classByRace[rid] = BT.raceClassFlags(race.info.race_class);

      let payouts = null;
      try { payouts = JSON.parse(race.info.results_json); } catch (_) {}
      const ppByHorse = {};
      const fin = {};
      (race.entries || []).forEach(e => {
        const hid = String(e.Horse_ID).split('.')[0];
        const pp = parseInt(e.PP, 10);
        if (Number.isFinite(pp)) ppByHorse[hid] = pp;
        const rank = parseInt(String(e.Finish).trim(), 10);
        if (rank >= 1 && rank <= 3 && fin[rank] == null && Number.isFinite(pp)) fin[rank] = pp;
      });
      const settled = !!(fin[1] && fin[2] && fin[3] && payouts);
      if (!settled) unsettled++;
      races.push({ rid, ppByHorse, t3: [fin[1], fin[2], fin[3]], payouts, settled,
                   surface: race.info.surface, field: entries.length });
    }
  }
  BT.setData(entriesByRace, infoByRace, classByRace);
  return { races, totalRaces, unsettled };
}

// Run ONE settled race through the engine at a given composition + risk, build the tickets,
// and settle them against the stored result. Returns { status:'bet'|'abstain', staked, won,
// plan, lineDetail }. Caller decides how to tally. The single settlement seam both sweeps share.
export function runRace(BT, m, comp, riskOverride) {
  const plan = BT.plan(m.rid, { riskOverride, compositionOverride: comp });
  if (!plan || plan.count === 0) return { status: 'abstain', reason: plan?.shape, staked: 0, won: 0, plan };
  const runners = plan.assignments
    .map(a => ({ symbol: a.symbol, pp: m.ppByHorse[a.h_id], horseId: a.h_id }))
    .filter(r => Number.isFinite(r.pp) && r.pp > 0);
  if (runners.length === 0) return { status: 'abstain', reason: 'no-pp', staked: 0, won: 0, plan };
  const built = BT.buildLines(comp, runners);
  if (!built.lines.length) return { status: 'abstain', reason: 'no-lines', staked: 0, won: 0, plan };
  const t3set = new Set(m.t3);
  let won = 0;
  const lineDetail = [];
  for (const line of built.lines) {
    const w = BT.scoreLine(line, m.t3, t3set, m.payouts);
    won += w;
    lineDetail.push(`${line.label} [${(line.horses || []).map(h => h.pp).join(',')}]${line.axisPp ? ' ax' + line.axisPp : ''} ×${line.comboCount}@${line.stakePerCombo} → ${w}`);
  }
  return { status: 'bet', staked: built.staked, won, plan, lineDetail };
}
