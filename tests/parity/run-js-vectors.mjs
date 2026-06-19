// Golden-vector parity runner — JS side (tech-debt T2-1).
//
// Loads the REAL wwwroot/static/script.js into a sandboxed vm context (so we test the
// shipping code, not a copy that could drift), then runs the shared `scoring` vectors and
// the `jsBuilding` vectors from bet_vectors.json and asserts each against its frozen expected.
// Exits non-zero on any mismatch. Run: `node tests/parity/run-js-vectors.mjs`.
//
// The C# side (tests/UMAnager.Tests/BetParityTests.cs) asserts the SAME `scoring` expected, so a
// pass on both = the two languages score identically. `jsBuilding` locks the JS-only builder.

import fs from 'node:fs';
import vm from 'node:vm';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');
const scriptPath = path.join(repoRoot, 'src', 'UMAnager.Nexus', 'wwwroot', 'static', 'script.js');
const vectorsPath = path.join(here, 'bet_vectors.json');

// Universal no-op stub: any browser/DOM access during top-level execution returns a chainable
// stub and never throws, so execution reaches every const + function declaration in script.js.
const stub = () => new Proxy(function () {}, {
  get: (_t, p) => (p === 'length' ? 0 : (p === Symbol.toPrimitive ? () => 0 : stub())),
  apply: () => stub(),
  construct: () => stub(),
});

const ctx = {
  console, JSON, Math, parseInt, parseFloat, isNaN, Number, String, Array, Object, Set, Map, Date,
  setTimeout: () => {}, setInterval: () => {}, clearTimeout: () => {}, clearInterval: () => {},
  fetch: () => Promise.resolve(stub()),
};
ctx.window = ctx; ctx.globalThis = ctx; ctx.self = ctx;
ctx.document = stub(); ctx.localStorage = stub(); ctx.navigator = stub(); ctx.location = stub();
ctx.WebSocket = function () { return stub(); };
ctx.requestAnimationFrame = () => {}; ctx.matchMedia = () => stub();

vm.createContext(ctx);
try {
  vm.runInContext(fs.readFileSync(scriptPath, 'utf8'), ctx, { filename: 'script.js' });
} catch (e) {
  // Top-level side effects may throw on the DOM stubs — fine, the hoisted functions are bound.
}

for (const fn of ['scoreBetLine', 'buildLinesFromComposition']) {
  if (typeof ctx[fn] !== 'function') {
    console.error(`FATAL: ${fn} not found after loading script.js — the sandbox or the function moved.`);
    process.exit(2);
  }
}

const vectors = JSON.parse(fs.readFileSync(vectorsPath, 'utf8'));
const sorted = a => [...a].sort();
const eq = (a, b) => JSON.stringify(sorted(a)) === JSON.stringify(sorted(b));
let failures = 0;
const fail = (name, msg) => { failures++; console.error(`  ✗ ${name}: ${msg}`); };

// ── scoring (shared with C#) ──────────────────────────────────────────────────
console.log('scoring:');
for (const c of vectors.scoring) {
  const t3 = [c.result.pp1, c.result.pp2, c.result.pp3];
  const t3set = new Set(t3);
  let won = 0; const labels = [];
  for (const line of c.lines) {
    const w = ctx.scoreBetLine(line, t3, t3set, c.payouts);
    if (w > 0) { won += w; labels.push(line.label); }
  }
  if (won !== c.expect.won) fail(c.name, `won ${won} ≠ expected ${c.expect.won}`);
  else if (!eq(labels, c.expect.labels)) fail(c.name, `labels ${JSON.stringify(sorted(labels))} ≠ ${JSON.stringify(sorted(c.expect.labels))}`);
  else console.log(`  ✓ ${c.name} (won=${won})`);
}

// ── jsBuilding (JS-only) ──────────────────────────────────────────────────────
console.log('jsBuilding:');
for (const c of vectors.jsBuilding) {
  const b = ctx.buildLinesFromComposition(c.composition, c.runners);
  const lines = b.lines.map(l => `${l.label}|${l.stakePerCombo}|${l.comboCount}`);
  if (b.staked !== c.expect.staked) fail(c.name, `staked ${b.staked} ≠ expected ${c.expect.staked}`);
  else if (!eq(lines, c.expect.lines)) fail(c.name, `lines ${JSON.stringify(sorted(lines))} ≠ ${JSON.stringify(sorted(c.expect.lines))}`);
  else console.log(`  ✓ ${c.name} (staked=${b.staked})`);
}

if (failures > 0) { console.error(`\nJS parity FAILED: ${failures} mismatch(es).`); process.exit(1); }
console.log('\nJS parity OK.');
