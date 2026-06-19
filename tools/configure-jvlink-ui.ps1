# ============================================================
# One-time JV-Link UI configuration — disable the blocking "notice" dialog.
#
# JV-Link's お知らせメッセージダイアログ (Notice Message Dialog) pops on JVOpen/JVRTOpen whenever
# JRA-VAN publishes a new system notice or a new JV-Link version, and it BLOCKS the Sidecar's single
# COM thread until a human clicks it (the cause of the 2026-06-16 "stuck Streaming" hangs). Per the
# Oracle (ORACLE_ANSWERS.md, 2026-06-19) there is no code/registry switch — the supported fix is to
# open JVSetUIProperties() ONCE and uncheck the notice display. That writes the zero-dialog config to
# the registry permanently, for all future automated runs.
#
# In the dialog that appears:
#   • UNCHECK  「JRA-VANからのお知らせを表示する」   (show notices from JRA-VAN)
#   • UNCHECK  「払戻速報を表示する」                 (show refund flash — optional but recommended)
#   • click OK
#
# Safe + reversible: it only opens a settings dialog. Run it on the server desktop (it needs a UI).
# ============================================================

# JV-Link's COM server (JVDTLab.dll) is 32-bit and the dialog needs an STA. Re-launch self in a
# 32-bit STA PowerShell if we're not already there.
if ([Environment]::Is64BitProcess -or
    [System.Threading.Thread]::CurrentThread.GetApartmentState() -ne 'STA') {
    $ps32 = Join-Path $env:SystemRoot 'SysWOW64\WindowsPowerShell\v1.0\powershell.exe'
    & $ps32 -STA -NoProfile -ExecutionPolicy Bypass -File $PSCommandPath
    exit $LASTEXITCODE
}

Write-Host "32-bit STA host OK ($([Environment]::Is64BitProcess), $([System.Threading.Thread]::CurrentThread.GetApartmentState()))."
try {
    Write-Host "Creating JVDTLab.JVLink COM object..."
    $jv = New-Object -ComObject JVDTLab.JVLink

    # Same software-id the Sidecar uses (appsettings.json JvLink:SoftwareId = "UNKNOWN").
    Write-Host "JVInit('UNKNOWN')..."
    $rc = $jv.JVInit('UNKNOWN')
    Write-Host "JVInit returned: $rc  (0 = success)"
    if ($rc -lt 0) {
        Write-Host "JVInit failed ($rc). If the Sidecar is running it may hold the session — stop it" -ForegroundColor Yellow
        Write-Host "(.\launch-services.ps1 -Action stop) and re-run this. Aborting." -ForegroundColor Yellow
        exit 1
    }

    Write-Host ""
    Write-Host "Opening the JV-Link settings dialog now. In it:" -ForegroundColor Cyan
    Write-Host "   - UNCHECK  JRA-VANからのお知らせを表示する" -ForegroundColor Cyan
    Write-Host "   - UNCHECK  払戻速報を表示する  (optional)" -ForegroundColor Cyan
    Write-Host "   - click OK" -ForegroundColor Cyan
    $null = $jv.JVSetUIProperties()
    Write-Host ""
    Write-Host "Dialog closed. Settings written to the registry. Done." -ForegroundColor Green
}
catch {
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
