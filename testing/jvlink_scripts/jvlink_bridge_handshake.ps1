param(
    [string]$Sid = "UMANAGER",
    [string]$ServiceKey = "",
    [string]$SavePath = "",
    [string]$ProbeDataSpec = "",
    [string]$ProbeFromDate = "",
    [int]$DataOption = 1,
    [switch]$SkipServiceKey
)

$ErrorActionPreference = "Stop"

$result = @{
    ok = $false
    sid = $Sid
    version = ""
    initCode = $null
    setSavePathCode = $null
    setServiceKeyCode = $null
    openCode = $null
    closeCode = $null
    probe = $false
    error = ""
}

$obj = $null
try {
    $obj = New-Object -ComObject "JVDTLab.JVLink"

    try {
        $result.version = [string]$obj.m_JVLinkVersion
    }
    catch {
        $result.version = ""
    }

    $result.initCode = [int]$obj.JVInit($Sid)

    if ($SavePath) {
        if (-not (Test-Path $SavePath)) {
            New-Item -ItemType Directory -Path $SavePath -Force | Out-Null
        }
        $result.setSavePathCode = [int]$obj.JVSetSavePath($SavePath)
        [void]$obj.JVSetSaveFlag(1)
    }

    if (-not $SkipServiceKey -and $ServiceKey) {
        $result.setServiceKeyCode = [int]$obj.JVSetServiceKey($ServiceKey)
    }

    if ($ProbeDataSpec -and $ProbeFromDate) {
        $result.probe = $true
        $readCount = 0
        $downloadCount = 0
        $lastFileTimestamp = ""
        $safeDataOption = $DataOption
        if ($safeDataOption -lt 1 -or $safeDataOption -gt 3) {
            $safeDataOption = 1
        }
        $result.openCode = [int]$obj.JVOpen($ProbeDataSpec, $ProbeFromDate, $safeDataOption, [ref]$readCount, [ref]$downloadCount, [ref]$lastFileTimestamp)
    }

    $result.ok = $true
}
catch {
    $result.ok = $false
    $result.error = [string]$_.Exception.Message
}
finally {
    if ($null -ne $obj) {
        try {
            $result.closeCode = [int]$obj.JVClose()
        }
        catch {
            if (-not $result.error) {
                $result.error = [string]$_.Exception.Message
            }
        }
        [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($obj)
    }
}

$result | ConvertTo-Json -Compress
