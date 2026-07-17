# tools/backup-docs.ps1
# Periodic local backup of UMAnager2's git-ignored markdown docs (dev_log.md, current_state.md,
# TODO.md, CLAUDE.md, docs/archive/, etc.) — these files are LOCAL-ONLY (all markdown is
# gitignored, see CLAUDE.md's Handoff section) with no git history, so this is the only safety
# net against a bad edit or accidental delete. NOT off-VM protection (see gotcha below) — just
# "protect against ourselves." Registered as a daily scheduled task; see tools/register-docs-backup-task.ps1.
#
# GOTCHA (2026-07-17): this VM's LAN peer routing is currently hijacked by a Tailscale subnet
# route for 192.168.40.0/24 (metric 0, beats the direct Ethernet route at metric 256), so a
# network destination (servarr/arch-server) is NOT reachable right now without fixing that
# routing first — deliberately deferred (don't touch VPN/Tailscale settings near a live weekend).
# This script targets a SAME-VM destination in the meantime. Revisit once the routing is fixed.

$ErrorActionPreference = 'Stop'

$RepoRoot   = Split-Path -Parent $PSScriptRoot
$BackupRoot = 'C:\Users\UMAnager\DocsBackup\UMAnager2'
$RetainDays = 60

$stamp = Get-Date -Format 'yyyy-MM-dd'
$dest  = Join-Path $BackupRoot $stamp

New-Item -ItemType Directory -Force -Path $dest | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $dest 'docs\archive') | Out-Null

# Root-level .md files (dev_log.md, current_state.md, TODO.md, CLAUDE.md, TECH_DEBT.md,
# tuning_hypotheses.md, ORACLE_ANSWERS.md, etc. — everything the repo's own *.md gitignore rule
# treats as local-only; README.md is harmless to include too).
Get-ChildItem -Path $RepoRoot -Filter '*.md' -File | Copy-Item -Destination $dest -Force

# docs/archive/ (the permanent journal).
$archiveSrc = Join-Path $RepoRoot 'docs\archive'
if (Test-Path $archiveSrc) {
    Copy-Item -Path (Join-Path $archiveSrc '*') -Destination (Join-Path $dest 'docs\archive') -Recurse -Force
}

$fileCount = (Get-ChildItem -Path $dest -Recurse -File).Count
Write-Output "Backed up $fileCount file(s) to $dest"

# Prune snapshots older than $RetainDays.
$cutoff = (Get-Date).AddDays(-$RetainDays)
Get-ChildItem -Path $BackupRoot -Directory -ErrorAction SilentlyContinue | ForEach-Object {
    try {
        $d = [DateTime]::ParseExact($_.Name, 'yyyy-MM-dd', [System.Globalization.CultureInfo]::InvariantCulture)
        if ($d -lt $cutoff) {
            Remove-Item -Path $_.FullName -Recurse -Force
            Write-Output "Pruned old snapshot: $($_.Name)"
        }
    } catch {
        # Name doesn't match yyyy-MM-dd (unrelated folder) — leave it alone.
    }
}
