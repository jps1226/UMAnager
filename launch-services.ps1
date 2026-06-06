# UMAnager v2.0 — PowerShell Service Launcher
# Launches Sidecar (x86) and Nexus (x64) with full output capture and monitoring

param(
    [ValidateSet("start", "stop", "restart", "status")]
    [string]$Action = "start"
)

$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
$LogDir = Join-Path $Root "logs"
$SidecarLog = Join-Path $LogDir "sidecar.log"
$NexusLog = Join-Path $LogDir "nexus.log"
$PidFile = Join-Path $Root ".service-pids.json"

# Ensure log directory exists
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Force -Path $LogDir | Out-Null }

function Write-Status {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$timestamp] [$Level] $Message" -ForegroundColor $(if ($Level -eq "ERROR") { "Red" } elseif ($Level -eq "WARN") { "Yellow" } else { "Green" })
}

function Start-Services {
    Write-Status "Starting UMAnager services..."

    # Build in Release mode first.
    # NOTE (session 41 bug): build output used to be piped to Out-Null and $LASTEXITCODE
    # was never checked. When a running process held the Release DLL locked, the build
    # FAILED silently and the script relaunched a stale DLL for two days. Now: output is
    # visible, exit codes are checked, and we abort loudly on failure.
    $nexusDll = Join-Path $Root "src/UMAnager.Nexus/bin/Release/net8.0/UMAnager.Nexus.dll"
    $dllBefore = if (Test-Path $nexusDll) { (Get-Item $nexusDll).LastWriteTime } else { $null }

    Write-Status "Building Sidecar (Release)..."
    & dotnet build src/UMAnager.Sidecar -c Release
    if ($LASTEXITCODE -ne 0) {
        Write-Status "ERROR: Sidecar build FAILED (exit $LASTEXITCODE). Aborting — not relaunching stale binary." ERROR
        exit 1
    }

    Write-Status "Building Nexus (Release)..."
    & dotnet build src/UMAnager.Nexus -c Release
    if ($LASTEXITCODE -ne 0) {
        Write-Status "ERROR: Nexus build FAILED (exit $LASTEXITCODE). Aborting — not relaunching stale binary." ERROR
        exit 1
    }
    Write-Status "Build complete."

    # Confirm the Nexus DLL actually got rewritten. If LastWriteTime didn't advance,
    # the DLL was almost certainly locked and we're about to launch stale code — warn loudly.
    $dllAfter = if (Test-Path $nexusDll) { (Get-Item $nexusDll).LastWriteTime } else { $null }
    if ($null -eq $dllAfter) {
        Write-Status "ERROR: Nexus DLL not found after build: $nexusDll" ERROR
        exit 1
    }
    if ($null -ne $dllBefore -and $dllAfter -le $dllBefore) {
        Write-Status "WARNING: Nexus DLL LastWriteTime did not advance ($dllAfter). Build may have been a no-op or the DLL was locked." WARN
    }
    else {
        Write-Status "Nexus DLL fresh: $dllAfter"
    }

    # Clear old logs
    if (Test-Path $SidecarLog) { Remove-Item $SidecarLog -Force }
    if (Test-Path $NexusLog) { Remove-Item $NexusLog -Force }

    # Launch Sidecar (x86)
    Write-Status "Launching Sidecar (x86)..."
    $sidecarExe = Join-Path $Root "src/UMAnager.Sidecar/bin/Release/net8.0-windows/win-x86/UMAnager.Sidecar.exe"
    $sidecarProcess = Start-Process `
        -FilePath $sidecarExe `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $SidecarLog `
        -PassThru `
        -NoNewWindow
    Write-Status "Sidecar started (PID: $($sidecarProcess.Id))"

    # Wait 2 seconds for Sidecar to initialize
    Start-Sleep -Seconds 2

    # Launch Nexus (x64)
    Write-Status "Launching Nexus (x64)..."
    $nexusExe = Join-Path $Root "src/UMAnager.Nexus/bin/Release/net8.0/UMAnager.Nexus.exe"
    $nexusProcess = Start-Process `
        -FilePath $nexusExe `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $NexusLog `
        -PassThru `
        -NoNewWindow
    Write-Status "Nexus started (PID: $($nexusProcess.Id))"

    # Save PIDs for later cleanup
    @{
        sidecar = $sidecarProcess.Id
        nexus = $nexusProcess.Id
    } | ConvertTo-Json | Set-Content $PidFile

    # Monitor startup (wait for handshake completion)
    Write-Status "Waiting for services to initialize..."
    $maxWait = 15
    $elapsed = 0
    $ready = $false

    while ($elapsed -lt $maxWait) {
        Start-Sleep -Seconds 1
        $elapsed++

        # Check if both processes are still running
        if (-not (Get-Process -Id $sidecarProcess.Id -ErrorAction SilentlyContinue)) {
            Write-Status "ERROR: Sidecar crashed! Check logs:" ERROR
            Get-Content $SidecarLog | ForEach-Object { Write-Host "  $_" }
            Stop-Services
            exit 1
        }
        if (-not (Get-Process -Id $nexusProcess.Id -ErrorAction SilentlyContinue)) {
            Write-Status "ERROR: Nexus crashed! Check logs:" ERROR
            Get-Content $NexusLog | ForEach-Object { Write-Host "  $_" }
            Stop-Services
            exit 1
        }

        # Check for successful handshake in logs
        $sidecarReady = (Get-Content $SidecarLog -ErrorAction SilentlyContinue | Select-String -Pattern "Waiting for Nexus connection" -ErrorAction SilentlyContinue)
        $nexusReady = (Get-Content $NexusLog -ErrorAction SilentlyContinue | Select-String -Pattern "Sidecar responded" -ErrorAction SilentlyContinue)

        if ($sidecarReady -and $nexusReady) {
            $ready = $true
            break
        }

        # Show progress
        Write-Host -NoNewline "."
    }

    if (-not $ready) {
        Write-Status "WARNING: Services may still be initializing. Proceeding with caution." WARN
        Write-Status "Sidecar log:" INFO
        Get-Content $SidecarLog | ForEach-Object { Write-Host "  $_" }
        Write-Status "Nexus log:" INFO
        Get-Content $NexusLog | ForEach-Object { Write-Host "  $_" }
    }
    else {
        Write-Status "✓ Services ready! Handshake complete."
    }

    # Test API connectivity
    Write-Status "Testing API connectivity..."
    try {
        $statusResponse = Invoke-RestMethod -Uri "http://localhost:5000/api/jvlink/status" -TimeoutSec 5 -ErrorAction Stop
        Write-Status "✓ API responding. JV-Link status: $($statusResponse.message)"
    }
    catch {
        Write-Status "WARNING: API not responding yet. Services may still be initializing." WARN
    }

    Write-Status ""
    Write-Status "======================================"
    Write-Status "Services are running!"
    Write-Status "======================================"
    Write-Status "Sidecar Log: $SidecarLog"
    Write-Status "Nexus Log:   $NexusLog"
    Write-Status ""
    Write-Status "API endpoint: http://localhost:5000"
    Write-Status "To stop:      .\launch-services.ps1 -Action stop"
    Write-Status "To view logs: Get-Content $SidecarLog -Tail 20 -Wait"
    Write-Status "======================================"
}

function Stop-Services {
    Write-Status "Stopping services..."

    # Stop by PID file first (if present) — targets the exact processes we launched.
    if (Test-Path $PidFile) {
        $pids = Get-Content $PidFile | ConvertFrom-Json
        if ($pids.sidecar) {
            Write-Status "Stopping Sidecar (PID: $($pids.sidecar))..."
            Stop-Process -Id $pids.sidecar -ErrorAction SilentlyContinue -Force
        }
        if ($pids.nexus) {
            Write-Status "Stopping Nexus (PID: $($pids.nexus))..."
            Stop-Process -Id $pids.nexus -ErrorAction SilentlyContinue -Force
        }
        Remove-Item $PidFile -ErrorAction SilentlyContinue
    }
    else {
        Write-Status "No PID file found — falling back to stop-by-name only." WARN
    }

    # ALWAYS stop by name too. This is the robust path: it catches processes started
    # outside this script (the bug that silently locked the Release DLL for two days,
    # session 41). Without this, a stale/missing PID file means we kill nothing, the
    # running process keeps the DLL locked, and the next build silently fails.
    $byName = Get-Process -Name "UMAnager.Nexus", "UMAnager.Sidecar" -ErrorAction SilentlyContinue
    if ($byName) {
        foreach ($p in $byName) {
            Write-Status "Stopping $($p.ProcessName) by name (PID: $($p.Id))..."
            Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
        }
    }

    # Give the OS a moment to release the file locks before any rebuild.
    Start-Sleep -Milliseconds 500

    # Verify nothing survived — a leftover process WILL relock the DLL.
    $survivors = Get-Process -Name "UMAnager.Nexus", "UMAnager.Sidecar" -ErrorAction SilentlyContinue
    if ($survivors) {
        Write-Status "ERROR: processes still alive after stop: $($survivors.Id -join ', '). DLL may stay locked." ERROR
    }
    else {
        Write-Status "✓ Services stopped."
    }
}

function Get-ServiceStatus {
    if (-not (Test-Path $PidFile)) {
        Write-Status "No running services (PID file not found)"
        return
    }

    $pids = Get-Content $PidFile | ConvertFrom-Json

    Write-Status "Service Status:"
    Write-Status "  Sidecar (PID $($pids.sidecar)): $( if (Get-Process -Id $pids.sidecar -ErrorAction SilentlyContinue) { 'RUNNING' } else { 'STOPPED' })"
    Write-Status "  Nexus   (PID $($pids.nexus)): $( if (Get-Process -Id $pids.nexus -ErrorAction SilentlyContinue) { 'RUNNING' } else { 'STOPPED' })"
}

# Execute requested action
switch ($Action) {
    "start"   { Start-Services }
    "stop"    { Stop-Services }
    "restart" { Stop-Services; Start-Sleep -Seconds 1; Start-Services }
    "status"  { Get-ServiceStatus }
}
