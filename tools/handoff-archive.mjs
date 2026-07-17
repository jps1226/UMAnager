#!/usr/bin/env node
// ============================================================
// tools/handoff-archive.mjs
// Mechanical half of the /handoff dev_log archive step (see CLAUDE.md "Handoff" section).
// Keeps the 5 newest session entries in dev_log.md; moves any older entries into
// docs/archive/dev_log_sessions_<lo>_<hi>.md (creating or appending to a contiguous file),
// verbatim, in chronological order. Verifies the archive copy is byte-intact BEFORE
// touching dev_log.md — if verification fails, nothing is written and the script exits
// non-zero. dev_log.md and docs/archive/ are both git-ignored (all markdown is local-only,
// see CLAUDE.md) so there is no git safety net; this script is the safety net.
//
// Usage: node tools/handoff-archive.mjs [--keep N] [--dry-run]
//   --keep N     how many newest entries to leave in dev_log.md (default 5, matches CLAUDE.md)
//   --dry-run    report what WOULD happen, write nothing
// ============================================================
import { readFileSync, writeFileSync, existsSync, readdirSync, unlinkSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const DEV_LOG = join(ROOT, 'dev_log.md');
const ARCHIVE_DIR = join(ROOT, 'docs', 'archive');

const args = process.argv.slice(2);
const dryRun = args.includes('--dry-run');
const keepIdx = args.indexOf('--keep');
const KEEP = keepIdx >= 0 ? parseInt(args[keepIdx + 1], 10) : 5;
if (!Number.isFinite(KEEP) || KEEP < 1) {
  console.error(`Invalid --keep value.`);
  process.exit(1);
}

if (!existsSync(DEV_LOG)) {
  console.error(`dev_log.md not found at ${DEV_LOG}`);
  process.exit(1);
}

// Normalize CRLF -> LF defensively (a Windows editor could reintroduce \r\n; the parser below
// matches literal "\n" patterns and would otherwise silently fail to find the entry boundaries).
const raw = readFileSync(DEV_LOG, 'utf8').replace(/\r\n/g, '\n');

// dev_log.md format: a preamble ("# Developer Log\n\n*...*\n"), then a "---\n" separator,
// then session entries separated by "\n---\n", newest-first. Split on the FIRST "---\n" to
// isolate the preamble, then split the remainder on "\n---\n" to get individual entries.
const firstSep = raw.indexOf('\n---\n');
if (firstSep === -1) {
  console.error('Could not find the preamble/---/first-entry structure in dev_log.md. Aborting — format may have changed; do not blind-run this script.');
  process.exit(1);
}
const preamble = raw.slice(0, firstSep + '\n---\n'.length); // includes the trailing "---\n"
const body = raw.slice(firstSep + '\n---\n'.length);
const entries = body.split('\n---\n').map(e => e.trim()).filter(Boolean); // newest-first

const sessionNumOf = (entry) => {
  const m = entry.match(/^##\s*Session\s+(\d+)/);
  return m ? parseInt(m[1], 10) : null;
};

const numbers = entries.map(sessionNumOf);
if (numbers.some(n => n === null)) {
  console.error('At least one entry does not start with "## Session <N>". Aborting — do not blind-run this script on a non-conforming file.');
  process.exit(1);
}

console.log(`Found ${entries.length} session entries: ${numbers.join(', ')}`);

if (entries.length <= KEEP) {
  console.log(`Nothing to archive (${entries.length} <= keep ${KEEP}). dev_log.md unchanged.`);
  process.exit(0);
}

const kept = entries.slice(0, KEEP);          // newest-first
const excess = entries.slice(KEEP);           // still newest-first among themselves
const excessChrono = [...excess].reverse();   // oldest-first, for the archive file

const excessNums = excess.map(sessionNumOf);
const lo = Math.min(...excessNums);
const hi = Math.max(...excessNums);
console.log(`Archiving sessions ${lo}-${hi} (${excess.length} entries), keeping ${kept.length} newest (${numbers.slice(0, KEEP).join(', ')}).`);

// Find a contiguous existing archive file to append to (dev_log_sessions_<a>_<b>.md where b+1 === lo).
let oldFile = null;   // the existing file to remove after a successful append-rename
let isAppend = false;
let newLo = lo, newHi = hi;
if (existsSync(ARCHIVE_DIR)) {
  const re = /^dev_log_sessions_(\d+)_(\d+)\.md$/;
  for (const f of readdirSync(ARCHIVE_DIR)) {
    const m = f.match(re);
    if (!m) continue;
    const a = parseInt(m[1], 10), b = parseInt(m[2], 10);
    if (b + 1 === lo) { oldFile = join(ARCHIVE_DIR, f); isAppend = true; newLo = a; newHi = hi; break; }
  }
}
let targetFile = isAppend
  ? join(ARCHIVE_DIR, `dev_log_sessions_${newLo}_${newHi}.md`)
  : join(ARCHIVE_DIR, `dev_log_sessions_${lo}_${hi}.md`);

const today = new Date().toISOString().slice(0, 10);
const archiveEntriesText = excessChrono.join('\n\n---\n\n') + '\n';

let archiveContent;
if (isAppend) {
  const existing = readFileSync(oldFile, 'utf8').replace(/\s*$/, '\n');
  // Update the header's range in-place (first line only) to keep the filename/header range accurate.
  const rangedHeader = existing.replace(
    /^#\s*Developer Log — Archived Sessions \d+–?-?\d+/,
    `# Developer Log — Archived Sessions ${newLo}–${newHi}`
  );
  archiveContent = rangedHeader.replace(/\n$/, '') + '\n\n---\n\n' + archiveEntriesText;
} else {
  const header =
    `# Developer Log — Archived Sessions ${lo}–${hi}\n\n` +
    `*Archived ${today} (session ${numbers[0]}) to keep \`dev_log.md\` lean. Permanent chronological journal — ` +
    `do NOT load at session start. These entries are LOCAL-ONLY — all markdown is gitignored ` +
    `(\`.gitignore\`: \`*.md\`, only \`README.md\` tracked), so this file is the ONLY copy. ` +
    `Never delete dev_log entries; move them here.*\n\n`;
  archiveContent = header + archiveEntriesText;
}

// --- Verify BEFORE writing anything: every excess entry's exact text must survive in the archive,
// AND (when appending) every pre-existing archived entry in the old file must ALSO still be present
// verbatim — catches a bad header-regex rewrite silently corrupting prior content. ---
const missingNew = excess.filter(e => !archiveContent.includes(e));
if (missingNew.length > 0) {
  console.error(`VERIFICATION FAILED: ${missingNew.length} new entr${missingNew.length === 1 ? 'y' : 'ies'} would NOT survive verbatim in the archive content. Aborting — nothing written.`);
  process.exit(1);
}
if (isAppend) {
  // Every non-blank line of the OLD file must appear somewhere in the new content, EXCEPT the
  // header title line — its "Sessions <lo>-<hi>" range is deliberately rewritten to cover the
  // newly-appended sessions, so it's expected to differ verbatim.
  const headerLineRe = /^#\s*Developer Log — Archived Sessions/;
  const priorLines = readFileSync(oldFile, 'utf8').split('\n').filter(l => l.trim() && !headerLineRe.test(l));
  const missingPrior = priorLines.filter(l => !archiveContent.includes(l));
  if (missingPrior.length > 0) {
    console.error(`VERIFICATION FAILED: appending to ${oldFile} would lose ${missingPrior.length} pre-existing line(s). Aborting — nothing written.`);
    console.error(missingPrior.map(l => `  - ${l}`).join('\n'));
    process.exit(1);
  }
  // Confirm the header rewrite actually took (not a silently-failed regex leaving the old range).
  if (!archiveContent.startsWith(`# Developer Log — Archived Sessions ${newLo}–${newHi}`)) {
    console.error(`VERIFICATION FAILED: header rewrite did not produce the expected "Sessions ${newLo}–${newHi}" range. Aborting — nothing written.`);
    process.exit(1);
  }
}
console.log(`Verified: all ${excess.length} archived entries${isAppend ? ' + all pre-existing archived content' : ''} present verbatim in the new archive content.`);

const newDevLog = preamble + kept.join('\n---\n') + '\n';

if (dryRun) {
  console.log(`[dry-run] Would write archive -> ${targetFile} (${Buffer.byteLength(archiveContent, 'utf8')} bytes, ${isAppend ? 'append/update range' : 'new file'})`);
  console.log(`[dry-run] Would trim dev_log.md -> ${Buffer.byteLength(newDevLog, 'utf8')} bytes, ${kept.length} entries (${numbers.slice(0, KEEP).join(', ')})`);
  process.exit(0);
}

// Write archive first, verify on-disk, THEN (if renamed) remove the old file, THEN trim dev_log.md.
// Each step only proceeds once the previous one is confirmed on disk — a crash mid-way leaves
// dev_log.md untouched (worst case) rather than silently losing an entry.
writeFileSync(targetFile, archiveContent, 'utf8');
const onDisk = readFileSync(targetFile, 'utf8');
if (onDisk !== archiveContent) {
  console.error('POST-WRITE VERIFICATION FAILED: archive file on disk does not match intended content. dev_log.md NOT touched. Investigate manually.');
  process.exit(1);
}
console.log(`Wrote ${targetFile} (${Buffer.byteLength(archiveContent, 'utf8')} bytes) — verified on disk.`);

if (isAppend && oldFile !== targetFile) {
  unlinkSync(oldFile);
  console.log(`Removed superseded ${oldFile} (content now lives in ${targetFile}).`);
}

writeFileSync(DEV_LOG, newDevLog, 'utf8');
console.log(`Trimmed dev_log.md to ${kept.length} entries (${numbers.slice(0, KEEP).join(', ')}), ${Buffer.byteLength(newDevLog, 'utf8')} bytes.`);
console.log('Done.');
