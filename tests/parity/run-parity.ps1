# Runs BOTH sides of the bet-math golden-vector parity check (tech-debt T2-1) and fails if either
# side mismatches its frozen expected in tests/parity/bet_vectors.json.
#   - C#  : dotnet test  (TemplateBetEvaluator — scoring + structure builder)
#   - JS  : node runner  (real script.js — scoring + composition builder)
# The shared `scoring` vectors use identical expected on both sides, so a green run = the two
# languages score frozen bet lines identically. Safe to run anytime (builds Debug, never touches
# the live Release binary).
$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")

Write-Host "== C# parity (dotnet test) ==" -ForegroundColor Cyan
& dotnet test (Join-Path $root "tests\UMAnager.Tests\UMAnager.Tests.csproj") --nologo -v minimal
$cs = $LASTEXITCODE

Write-Host "`n== JS parity (node) ==" -ForegroundColor Cyan
& node (Join-Path $root "tests\parity\run-js-vectors.mjs")
$js = $LASTEXITCODE

Write-Host ""
if ($cs -eq 0 -and $js -eq 0) {
    Write-Host "PARITY OK (C# + JS agree on the golden vectors)." -ForegroundColor Green
    exit 0
}
Write-Host "PARITY FAILED (C#=$cs, JS=$js). A bet-math change drifted from the golden vectors." -ForegroundColor Red
exit 1
