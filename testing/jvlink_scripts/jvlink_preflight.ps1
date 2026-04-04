param(
    [string]$WorkspaceRoot = (Get-Location).Path
)

$ErrorActionPreference = "SilentlyContinue"

function Write-Check {
    param(
        [string]$Name,
        [bool]$Ok,
        [string]$Detail
    )

    $status = if ($Ok) { "PASS" } else { "FAIL" }
    Write-Output ("[{0}] {1} - {2}" -f $status, $Name, $Detail)
}

Write-Output "== JRA-VAN JV-Link Preflight =="
Write-Output ("Workspace: " + $WorkspaceRoot)

# 1) Install location check
$candidateInstallPaths = @(
    "C:\Program Files (x86)\JRA-VAN\Data Lab",
    "C:\Program Files\JRA-VAN\Data Lab"
)

$installPath = $candidateInstallPaths | Where-Object { Test-Path $_ } | Select-Object -First 1
$installDetail = "not found"
if ($installPath) { $installDetail = $installPath }
Write-Check -Name "JV-Link install directory" -Ok ([bool]$installPath) -Detail $installDetail

# 2) COM registration check (64-bit vs 32-bit)
$clsid = "{2AB1774D-0C41-11D7-916F-0003479BEB3F}"
$clsid64Path = "Registry::HKEY_CLASSES_ROOT\CLSID\$clsid"
$clsid32Path = "Registry::HKEY_CLASSES_ROOT\Wow6432Node\CLSID\$clsid"

$has64 = Test-Path $clsid64Path
$has32 = Test-Path $clsid32Path

$detail64 = "missing"
if ($has64) { $detail64 = "registered" }
$detail32 = "missing"
if ($has32) { $detail32 = "registered" }
Write-Check -Name "COM CLSID (64-bit host)" -Ok $has64 -Detail $detail64
Write-Check -Name "COM CLSID (32-bit host)" -Ok $has32 -Detail $detail32

# 3) COM object creation in current (likely 64-bit) host
try {
    $obj = New-Object -ComObject "JVDTLab.JVLink"
    $ver = ""
    try { $ver = $obj.m_JVLinkVersion } catch { $ver = "unknown" }
    [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($obj)
    Write-Check -Name "COM instantiate in current host" -Ok $true -Detail ("version=" + $ver)
}
catch {
    Write-Check -Name "COM instantiate in current host" -Ok $false -Detail $_.Exception.Message
}

# 4) COM object creation in forced 32-bit host
$pwsh32 = Join-Path $env:WINDIR "SysWOW64\WindowsPowerShell\v1.0\powershell.exe"
if (Test-Path $pwsh32) {
    $cmd = "try { `$obj = New-Object -ComObject 'JVDTLab.JVLink'; try { 'OK version=' + `$obj.m_JVLinkVersion } catch { 'OK version=unknown' }; [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject(`$obj) } catch { 'FAIL ' + `$_.Exception.Message }"
    $result = & $pwsh32 -NoProfile -Command $cmd
    $ok32 = ($result -match "^OK")
    Write-Check -Name "COM instantiate in 32-bit host" -Ok $ok32 -Detail ($result -join " ")
}
else {
    Write-Check -Name "COM instantiate in 32-bit host" -Ok $false -Detail "32-bit PowerShell not found"
}

# 5) Python architecture check (workspace venv)
$py = Join-Path $WorkspaceRoot ".venv\Scripts\python.exe"
if (Test-Path $py) {
    $pyOut = & $py -c "import struct,sys; print(sys.executable); print(struct.calcsize('P')*8)"
    $pyBits = ($pyOut | Select-Object -Last 1)
    $pyOk = ($pyBits -eq "32")
    Write-Check -Name "Python architecture for direct COM" -Ok $pyOk -Detail (("bits=" + $pyBits + "; exe=" + ($pyOut | Select-Object -First 1)))
}
else {
    Write-Check -Name "Python architecture for direct COM" -Ok $false -Detail "workspace .venv not found"
}

Write-Output "== Suggested next action =="
if ($has32 -and -not $has64) {
    Write-Output "Use a 32-bit bridge process (recommended) or a 32-bit Python environment for direct COM access."
}
elseif ($has64) {
    Write-Output "Direct COM from current host should be possible."
}
else {
    Write-Output "Run JV-Link setup/registration from installed Data Lab location."
}
