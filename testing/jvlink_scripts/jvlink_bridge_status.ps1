$ErrorActionPreference = "Stop"

function To-Bool($value) {
    if ($value) { return $true }
    return $false
}

$result = @{
    ok = $false
    version = ""
    progId = "JVDTLab.JVLink"
    clsid = "{2AB1774D-0C41-11D7-916F-0003479BEB3F}"
    bitness = "32"
    clsidRegistered = $false
    error = ""
}

try {
    $clsidPath = "Registry::HKEY_CLASSES_ROOT\\CLSID\\$($result.clsid)"
    $result.clsidRegistered = To-Bool (Test-Path $clsidPath)

    $obj = New-Object -ComObject $result.progId
    try {
        $result.version = [string]$obj.m_JVLinkVersion
    }
    catch {
        $result.version = ""
    }

    [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($obj)
    $result.ok = $true
}
catch {
    $result.ok = $false
    $result.error = [string]$_.Exception.Message
}

$result | ConvertTo-Json -Compress
