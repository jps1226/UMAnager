// ============================================================
// Handoff dev_log archiver — automates step 2 of the /handoff skill.
//
// Keeps the N most-recent session entries in dev_log.md and moves everything older into
// docs/archive/ (one file per session), so Claude never has to hand-move verbatim text blocks
// (the slow, error-prone part of a handoff). The operator's workflow is unchanged — this runs
// AS PART OF /handoff, invoked by Claude after it writes the new session entry.
//
// dev_log.md format assumed: a header/preamble, then session blocks each starting with a line
// "## Session ..." and separated by a "---" rule. Newest entry is at the TOP.
//
// Usage:
//   node tools/handoff-archive.mjs            # keep 5 newest, archive the rest
//   node tools/handoff-archive.mjs --keep 4   # keep a different number
//   node tools/handoff-archive.mjs --dry-run  # report what it WOULD do, write nothing
// ============================================================
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..');
const devLogPath = path.join(repoRoot, 'dev_log.md');
const archiveDir = path.join(repoRoot, 'docs', 'archive');

const args = process.argv.slice(2);
const dryRun = args.includes('--dry-run');
const keepIdx = args.indexOf('--keep');
const KEEP = keepIdx >= 0 ? Math.max(1, parseInt(args[keepIdx + 1], 10) || 5) : 5;

const SEP = '\n\n---\n'; // canonical inter-entry separator
const SESSION_SPLIT = /\n+---\n+(?=## Session )/; // split between entries, keep the heading
// A session's id for its archive filename: prefer a digit ("## Session 51 — ..."), else the
// ISO date in the heading line, else a slug of the heading.
function sessionId(block) {
  const head = block.split('\n', 1)[0];
  const num = head.match(/##\s*Session\s+(\d+)/);
  if (num) return num[1].padStart(2, '0');
  const date = head.match(/(\d{4}-\d{2}-\d{2})/);
  if (date) return date[1];
  return head.replace(/[^a-z0-9]+/gi, '-').slice(0, 40).replace(/^-|-$/g, '') || 'unknown';
}

// Normalize CRLF→LF for processing (dev_log.md is often CRLF on Windows); we write LF back.
const raw = fs.readFileSync(devLogPath, 'utf8').replace(/\r\n/g, '\n');
const firstIdx = raw.search(/## Session /);
if (firstIdx < 0) { console.log('[handoff-archive] No "## Session" entries found — nothing to do.'); process.exit(0); }

// head = everything before the first (newest) session block, minus the trailing separator rule.
const head = raw.slice(0, firstIdx).replace(/\n+---\n+$/, '').trimEnd();
const blocks = raw.slice(firstIdx).split(SESSION_SPLIT).map(b => b.trimEnd()).filter(Boolean);

console.log(`[handoff-archive] dev_log.md has ${blocks.length} session entries; keeping ${KEEP} newest.`);
if (blocks.length <= KEEP) {
  console.log('[handoff-archive] Under the limit — no archiving needed.');
  process.exit(0);
}

const keep = blocks.slice(0, KEEP);
const archive = blocks.slice(KEEP); // older entries (the file is newest-first)

const moved = [];
for (const block of archive) {
  const id = sessionId(block);
  const dest = path.join(archiveDir, `dev_log_session_${id}.md`);
  const content =
    `# Developer Log — Archived Session ${id}\n\n` +
    `*Archived ${new Date().toISOString().slice(0, 10)} by tools/handoff-archive.mjs to keep dev_log.md lean. ` +
    `Permanent chronological journal — do NOT load at session start.*\n\n` +
    block + '\n';
  if (dryRun) { moved.push(`${id} -> ${path.relative(repoRoot, dest)} (dry-run)`); continue; }
  fs.mkdirSync(archiveDir, { recursive: true });
  fs.writeFileSync(dest, content);            // write archive FIRST (so data is safe before trim)
  moved.push(`${id} -> ${path.relative(repoRoot, dest)}`);
}

const newDevLog = head + SEP + keep.join(SEP) + '\n';
if (!dryRun) fs.writeFileSync(devLogPath, newDevLog);

console.log(`[handoff-archive] Archived ${archive.length} entr${archive.length === 1 ? 'y' : 'ies'}:`);
moved.forEach(m => console.log('  ' + m));
console.log(dryRun
  ? `[handoff-archive] DRY RUN — wrote nothing. dev_log.md would drop to ${keep.length} entries (${newDevLog.length} bytes).`
  : `[handoff-archive] dev_log.md now holds ${keep.length} entries (${newDevLog.length} bytes).`);
