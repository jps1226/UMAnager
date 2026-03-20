param(
    [ValidateSet('open', 'focus')]
    [string]$Action = 'open',
    [string]$Url = 'https://orepro.netkeiba.com/bet/race_list.html'
)

$ErrorActionPreference = 'Stop'
$statePath = Join-Path $env:TEMP 'umanager-orepro-window.json'
$debugPort = 9222
$userDataDir = Join-Path $env:TEMP 'umanager-orepro-browser-profile'

Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class Win32 {
    [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hWnd);
    [DllImport("user32.dll")] public static extern bool ShowWindowAsync(IntPtr hWnd, int nCmdShow);
    [DllImport("user32.dll")] public static extern bool IsIconic(IntPtr hWnd);
    [DllImport("user32.dll")] public static extern bool BringWindowToTop(IntPtr hWnd);
    [DllImport("user32.dll", SetLastError=true)] public static extern bool SetWindowPos(IntPtr hWnd, IntPtr hWndInsertAfter, int X, int Y, int cx, int cy, uint uFlags);
    public static readonly IntPtr HWND_TOPMOST = new IntPtr(-1);
    public const UInt32 SWP_NOSIZE = 0x0001;
    public const UInt32 SWP_NOMOVE = 0x0002;
    public const UInt32 SWP_SHOWWINDOW = 0x0040;
}
"@

function Write-JsonAndExit {
    param(
        [int]$Code,
        [string]$Status,
        [string]$Message,
        [bool]$Reused = $false,
        [int]$ProcessId = 0
    )

    @{
        status = $Status
        action = $Action
        message = $Message
        reused = $Reused
        pid = $ProcessId
    } | ConvertTo-Json -Compress
    exit $Code
}

function Save-WindowState {
    param([System.Diagnostics.Process]$Process)

    if (-not $Process) { return }
    @{
        pid = $Process.Id
        processName = $Process.ProcessName
    } | ConvertTo-Json -Compress | Set-Content -Path $statePath -Encoding ASCII
}

function Get-StoredWindowProcess {
    if (-not (Test-Path $statePath)) { return $null }

    try {
        $saved = Get-Content -Path $statePath -Raw | ConvertFrom-Json
        if (-not $saved.pid) { return $null }
        $proc = Get-Process -Id ([int]$saved.pid) -ErrorAction SilentlyContinue
        if ($proc -and $proc.MainWindowHandle -ne 0) {
            return $proc
        }
    } catch {
        return $null
    }

    return $null
}

function Get-OreProWindow {
    $stored = Get-StoredWindowProcess
    if ($stored) {
        return $stored
    }

    return Get-Process -ErrorAction SilentlyContinue |
        Where-Object {
            $_.MainWindowHandle -ne 0 -and (
                $_.MainWindowTitle -match '(?i)(orepro|netkeiba|race_list)'
            )
        } |
        Sort-Object StartTime -Descending |
        Select-Object -First 1
}

function Find-ChromiumBrowserPath {
    $candidates = @(
        (Join-Path $env:ProgramFiles 'Microsoft\Edge\Application\msedge.exe'),
        (Join-Path ${env:ProgramFiles(x86)} 'Microsoft\Edge\Application\msedge.exe'),
        (Join-Path $env:ProgramFiles 'Google\Chrome\Application\chrome.exe'),
        (Join-Path ${env:ProgramFiles(x86)} 'Google\Chrome\Application\chrome.exe'),
        (Join-Path $env:ProgramFiles 'BraveSoftware\Brave-Browser\Application\brave.exe'),
        (Join-Path ${env:ProgramFiles(x86)} 'BraveSoftware\Brave-Browser\Application\brave.exe')
    ) | Where-Object { $_ }

    foreach ($path in $candidates) {
        if (Test-Path $path) {
            return $path
        }
    }

    foreach ($cmd in @('msedge.exe', 'chrome.exe', 'brave.exe')) {
        $found = Get-Command $cmd -ErrorAction SilentlyContinue
        if ($found -and $found.Source) {
            return $found.Source
        }
    }

    return $null
}

function Get-NewBrowserWindow {
    param(
        [datetime]$LaunchTime,
        [string[]]$ProcessNames
    )

    return Get-Process -ErrorAction SilentlyContinue |
        Where-Object {
            $_.MainWindowHandle -ne 0 -and
            $ProcessNames -contains $_.ProcessName -and
            $_.StartTime -ge $LaunchTime.AddSeconds(-2)
        } |
        Sort-Object StartTime -Descending |
        Select-Object -First 1
}

function Promote-Window {
    param([System.Diagnostics.Process]$Process)

    if (-not $Process) { return $false }

    $Process.Refresh()
    if ($Process.MainWindowHandle -eq 0) { return $false }

    $hwnd = [IntPtr]$Process.MainWindowHandle
    if ([Win32]::IsIconic($hwnd)) {
        [Win32]::ShowWindowAsync($hwnd, 9) | Out-Null
    } else {
        [Win32]::ShowWindowAsync($hwnd, 5) | Out-Null
    }

    [Win32]::SetWindowPos(
        $hwnd,
        [Win32]::HWND_TOPMOST,
        0,
        0,
        0,
        0,
        [Win32]::SWP_NOMOVE -bor [Win32]::SWP_NOSIZE -bor [Win32]::SWP_SHOWWINDOW
    ) | Out-Null
    [Win32]::BringWindowToTop($hwnd) | Out-Null
    Start-Sleep -Milliseconds 120
    [Win32]::SetForegroundWindow($hwnd) | Out-Null
    return $true
}

function Test-CdpReady {
    param([int]$Port = 9222)

    try {
        $resp = Invoke-WebRequest -Uri ("http://127.0.0.1:{0}/json/version" -f $Port) -UseBasicParsing -TimeoutSec 1
        return ($resp.StatusCode -eq 200)
    } catch {
        return $false
    }
}

$existing = Get-OreProWindow
if ($existing -and $Action -eq 'focus') {
    Save-WindowState -Process $existing
    [void](Promote-Window -Process $existing)
    Write-JsonAndExit -Code 0 -Status 'ok' -Message 'Focused the existing OrePro companion window and kept it topmost.' -Reused $true -ProcessId $existing.Id
}

# For "open", if a compatible managed window/debug target is already alive, just reuse it.
if ($existing -and $Action -eq 'open' -and (Test-CdpReady -Port $debugPort)) {
    Save-WindowState -Process $existing
    [void](Promote-Window -Process $existing)
    Write-JsonAndExit -Code 0 -Status 'ok' -Message 'Reused existing managed OrePro companion window.' -Reused $true -ProcessId $existing.Id
}

$browserPath = Find-ChromiumBrowserPath
$browserNames = @('msedge', 'chrome', 'brave')
$launchTime = Get-Date

if (-not (Test-Path $userDataDir)) {
    New-Item -ItemType Directory -Path $userDataDir -Force | Out-Null
}

if ($browserPath) {
    Start-Process -FilePath $browserPath -ArgumentList @(
        "--remote-debugging-port=$debugPort",
        "--user-data-dir=$userDataDir",
        '--new-window',
        "--app=$Url"
    ) | Out-Null
} else {
    Start-Process $Url | Out-Null
}

for ($i = 0; $i -lt 40; $i++) {
    Start-Sleep -Milliseconds 500

    $window = Get-OreProWindow
    if (-not $window) {
        $window = Get-NewBrowserWindow -LaunchTime $launchTime -ProcessNames $browserNames
    }

    if ($window) {
        Save-WindowState -Process $window
        [void](Promote-Window -Process $window)
        Write-JsonAndExit -Code 0 -Status 'ok' -Message 'Opened OrePro in a native topmost companion window.' -Reused $false -ProcessId $window.Id
    }
}

    Write-JsonAndExit -Code 1 -Status 'error' -Message 'Timed out waiting for the OrePro companion window to appear.' -Reused $false -ProcessId 0