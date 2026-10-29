# UMAnager Preview launcher
# Starts only the Preview Nexus build. This script is invoked by an InteractiveToken
# scheduled task so it never launches from the elevated SSH Session 0 shell.
param(
    [ValidateSet("start", "stop", "status")]
    [string]$Action = "start"
)

$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
$ProductionRoot = Split-Path (Split-Path $Root -Parent) -Parent
$LogDir = Join-Path $Root "logs"
$PidFile = Join-Path $Root ".preview-nexus-pid.json"
$NexusProject = Join-Path $Root "src\UMAnager.Nexus\UMAnager.Nexus.csproj"
$NexusExe = Join-Path $Root "src\UMAnager.Nexus\bin\Release\net8.0\UMAnager.Nexus.exe"
$NexusLog = Join-Path $LogDir "preview-nexus.log"
$ProductionConfig = Join-Path $ProductionRoot "src\UMAnager.Nexus\appsettings.json"
$PreviewConfig = Join-Path (Split-Path $NexusExe -Parent) "appsettings.json"

function Get-PreviewProcess {
    if (-not (Test-Path $PidFile)) { return $null }
    try {
        $processId = (Get-Content $PidFile -Raw | ConvertFrom-Json).pid
        if (-not $processId) { return $null }
        $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
        if ($null -eq $process -or $process.Path -ne $NexusExe) { return $null }
        return $process
    }
    catch { return $null }
}

function Write-PreviewStatus {
    param([string]$Message)
    Write-Output ("[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message)
}

function Start-Preview {
    $existing = Get-PreviewProcess
    if ($null -ne $existing) {
        Write-PreviewStatus "Preview Nexus is already running (PID $($existing.Id))."
        return
    }

    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    Write-PreviewStatus "Building Preview Nexus..."
    & dotnet build $NexusProject -c Release
    if ($LASTEXITCODE -ne 0) { throw "Preview Nexus build failed (exit $LASTEXITCODE)." }
    if (-not (Test-Path $ProductionConfig)) { throw "Production appsettings.json is unavailable; Preview cannot read the card data." }
    Copy-Item -Force $ProductionConfig $PreviewConfig
    if (Test-Path $NexusLog) { Remove-Item -Force $NexusLog }

    $psi = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = $NexusExe
    $psi.WorkingDirectory = $Root
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.EnvironmentVariables["ASPNETCORE_ENVIRONMENT"] = "Preview"
    $process = [System.Diagnostics.Process]::Start($psi)
    if ($null -eq $process) { throw "Could not start Preview Nexus." }
    $process.StandardOutput.BaseStream.CopyToAsync([System.IO.File]::Open($NexusLog, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)) | Out-Null
    $process.StandardError.BaseStream.CopyToAsync([System.IO.File]::Open($NexusLog, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)) | Out-Null
    @{ pid = $process.Id; startedAt = (Get-Date).ToUniversalTime().ToString("o") } | ConvertTo-Json | Set-Content $PidFile

    $deadline = (Get-Date).AddSeconds(20)
    do {
        Start-Sleep -Milliseconds 500
        try {
            $response = Invoke-RestMethod -Uri "http://127.0.0.1:5001/api/runtime-mode" -TimeoutSec 2
            if ($response.preview -eq $true) {
                Write-PreviewStatus "Preview Nexus ready on http://127.0.0.1:5001 (PID $($process.Id))."
                return
            }
        }
        catch { }
    } while ((Get-Date) -lt $deadline)

    if ($process.HasExited) { throw "Preview Nexus exited during startup. See $NexusLog" }
    throw "Preview Nexus did not report read-only mode on port 5001 within 20 seconds. See $NexusLog"
}

function Stop-Preview {
    $process = Get-PreviewProcess
    if ($null -eq $process) {
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
        Write-PreviewStatus "Preview Nexus is not running."
        return
    }
    Stop-Process -Id $process.Id -Force
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    Write-PreviewStatus "Preview Nexus stopped (PID $($process.Id))."
}

function Get-PreviewStatus {
    $process = Get-PreviewProcess
    if ($null -eq $process) {
        Write-PreviewStatus "Preview Nexus is stopped."
        return
    }
    try {
        $mode = Invoke-RestMethod -Uri "http://127.0.0.1:5001/api/runtime-mode" -TimeoutSec 3
        Write-PreviewStatus "Preview Nexus running (PID $($process.Id)); mode=$($mode.mode)."
    }
    catch {
        Write-PreviewStatus "Preview Nexus process is running (PID $($process.Id)) but its read-only API is not responding."
    }
}

switch ($Action) {
    "start"  { Start-Preview }
    "stop"   { Stop-Preview }
    "status" { Get-PreviewStatus }
}
