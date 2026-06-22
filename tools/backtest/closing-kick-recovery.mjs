// ============================================================
// CLOSING-KICK RECOVERY (MONEY) TEST — the confirmation for Group-B factor #2 (§0 Step 2).
//
// pace-edge.mjs proved a PLACE-RATE signal: horses with a proven closing kick (a high prior rate of
// top-3 last-3F finishes) out-PLACE weak closers within the same market band. That is predictiveness,
// not yet money. This test answers the money question: if you actually BET proven closers to PLACE
// (複勝) across the whole settled bench, how much of your stake comes back — net of the ~20-25% takeout
// that is already baked into the dividends — and does it beat just betting the field?
//
// Method (all point-in-time, no look-ahead):
//   • Closing profile per (race, horse): from finish_history, a horse's prior rate of posting a top-3
//     last-3F, using ONLY its runs strictly before that race (>= MIN_SAMPLES). Identical thresholds to
//     pace-edge: strong-closer >= 0.50, weak-closer <= 0.25.
//   • Settle: a flat ¥100 PLACE bet on each qualifying horse, paid from the stored `place` dividends
//     (payout is per ¥100). Recovery = 100 * returned / staked.
//   • Baselines to beat: betting the WHOLE field to place, and betting every PROFILED horse to place.
//     Stratified by market band (chalk / mid / longshot) because the edge was biggest mid-band.
//
// Read-only. Run:  node tools/backtest/closing-kick-recovery.mjs
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const hist = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'finish_history.json'), 'utf8'));
const bench = JSON.parse(fs.readFileSync(path.join(here, 'fixtures', 'history.json'), 'utf8'));

const MIN_SAMPLES = +(process.env.CK_MIN ?? 3);   // min prior closing samples before a horse has a profile
const STRONG = +(process.env.CK_STRONG ?? 0.50);  // env-overridable so the conclusion can be stress-tested
const WEAK = +(process.env.CK_WEAK ?? 0.25);
const STAKE = 100;              // ¥ per place bet; dividends are per ¥100

const l3f = s => { const v = parseInt(s, 10); return Number.isFinite(v) && v >= 250 && v <= 500 ? v : null; };
const favBand = f => (f == null ? null : f <= 3 ? 'chalk (1-3)' : f <= 8 ? 'mid (4-8)' : 'longshot (9+)');
const BANDS = ['chalk (1-3)', 'mid (4-8)', 'longshot (9+)'];

// ── 1) closing rank within each race (fastest last-3F = rank 1) ───────────────
const byRace = new Map();
for (const r of hist) { let a = byRace.get(r.rid); if (!a) { a = []; byRace.set(r.rid, a); } a.push(r); r._l3f = l3f(r.l3f); }
for (const field of byRace.values()) {
  const valid = field.filter(r => r._l3f != null).sort((a, b) => a._l3f - b._l3f);
  valid.forEach((r, i) => { r._closeTop3 = (i + 1) <= 3 ? 1 : 0; });
}

// ── 2) per-horse chronological → prior closer-rate AS OF each run, keyed by `rid|h` ──
const byHorse = new Map();
for (const r of hist) { let a = byHorse.get(r.h); if (!a) { a = []; byHorse.set(r.h, a); } a.push(r); }
for (const a of byHorse.values()) a.sort((x, y) => (x.ymd < y.ymd ? -1 : 1));

const profile = new Map(); // `${rid}|${h}` → { rate, samples }
for (const arr of byHorse.values()) {
  const closes = [];
  for (const r of arr) {
    if (closes.length >= MIN_SAMPLES) {
      const rate = closes.reduce((a, b) => a + b, 0) / closes.length;
      profile.set(`${r.rid}|${r.h}`, { rate, samples: closes.length });
    }
    if (r._closeTop3 != null) closes.push(r._closeTop3);
  }
}

// ── 3) settle place bets across the bench ─────────────────────────────────────
// cell[group][band] = { staked, returned, bets, hits }
const cell = {};
const grp = (g, b) => (((cell[g] ??= {})[b] ??= { staked: 0, returned: 0, bets: 0, hits: 0 }));
const place = (g, b, pp, placeDivs) => {
  const c = grp(g, b);
  c.staked += STAKE; c.bets++;
  const win = placeDivs.find(d => d.combo && d.combo[0] === pp);
  if (win) { c.returned += win.payout * (STAKE / 100); c.hits++; }
};

const byDate = bench.past_races_by_date || {};
let settledRaces = 0, profiledHorses = 0;
for (const date of Object.keys(byDate)) {
  for (const race of byDate[date]) {
    const rid = race.info.race_id;
    let payouts = null;
    try { payouts = JSON.parse(race.info.results_json); } catch (_) {}
    const placeDivs = payouts && payouts.place;
    if (!placeDivs || !placeDivs.length) continue; // no place pool settled → skip race
    settledRaces++;

    for (const e of (race.entries || [])) {
      if (e.Scratched === true) continue;
      const pp = parseInt(e.PP, 10);
      if (!Number.isFinite(pp)) continue;
      const band = favBand(parseInt(e.Fav, 10));
      if (!band) continue;
      const hid = String(e.Horse_ID).split('.')[0];

      place('FIELD (all runners)', band, pp, placeDivs); // pure baseline: bet everyone to place

      const prof = profile.get(`${rid}|${hid}`);
      if (!prof) continue; // no closing profile yet
      profiledHorses++;
      place('PROFILED (has kick data)', band, pp, placeDivs);
      if (prof.rate >= STRONG) place('STRONG closer', band, pp, placeDivs);
      else if (prof.rate <= WEAK) place('WEAK closer', band, pp, placeDivs);
    }
  }
}

// ── 4) report ─────────────────────────────────────────────────────────────────
const rec = c => (c.staked ? (100 * c.returned / c.staked).toFixed(1) + '%' : '—');
const hr = c => (c.bets ? (100 * c.hits / c.bets).toFixed(0) + '%' : '—');
const roll = g => { const t = { staked: 0, returned: 0, bets: 0, hits: 0 }; for (const b of BANDS) { const c = cell[g]?.[b]; if (c) { t.staked += c.staked; t.returned += c.returned; t.bets += c.bets; t.hits += c.hits; } } return t; };

console.log(`\nCLOSING-KICK RECOVERY (PLACE / 複勝) — point-in-time, ${settledRaces} settled races, ¥${STAKE}/bet`);
console.log(`profile: prior top-3 last-3F rate, >=${MIN_SAMPLES} samples · strong>=${STRONG} · weak<=${WEAK}\n`);

const GROUPS = ['FIELD (all runners)', 'PROFILED (has kick data)', 'STRONG closer', 'WEAK closer'];
const w = 26;
console.log('group'.padEnd(w) + 'band'.padEnd(16) + 'recovery'.padStart(10) + 'hit'.padStart(7) + 'bets'.padStart(9));
for (const g of GROUPS) {
  for (const b of BANDS) {
    const c = cell[g]?.[b] || { staked: 0, returned: 0, bets: 0, hits: 0 };
    console.log(g.padEnd(w) + b.padEnd(16) + rec(c).padStart(10) + hr(c).padStart(7) + String(c.bets).padStart(9));
  }
  const t = roll(g);
  console.log(''.padEnd(w) + 'ALL'.padEnd(16) + rec(t).padStart(10) + hr(t).padStart(7) + String(t.bets).padStart(9));
  console.log('');
}

console.log('Read: STRONG-closer recovery vs FIELD/PROFILED in the SAME band = the money edge (or its');
console.log('absence). ~80%+ AND beating the field baseline = a genuine, bettable edge. The place pool');
console.log('already nets the takeout, so recovery is real money back, not gross.\n');
