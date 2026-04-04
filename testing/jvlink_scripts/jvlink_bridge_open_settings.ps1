param(
    [string]$Sid = "UNKNOWN"
)

$ErrorActionPreference = "Stop"

$result = @{
    ok = $false
    sid = $Sid
    initCode = $null
    setUIPropertiesCode = $null
    closeCode = $null
    error = ""
}

$obj = $null
try {
    $obj = New-Object -ComObject "JVDTLab.JVLink"
    $result.initCode = [int]$obj.JVInit($Sid)
    $result.setUIPropertiesCode = [int]$obj.JVSetUIProperties()
    $result.ok = ($result.setUIPropertiesCode -eq 0)
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
        catch {}
        [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($obj)
    }
}

$result | ConvertTo-Json -Compress
