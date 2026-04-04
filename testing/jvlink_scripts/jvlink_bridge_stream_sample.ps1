param(
    [string]$Sid = "UNKNOWN",
    [string]$ServiceKey = "",
    [string]$SavePath = "",
    [string]$DataSpec,
    [string]$FromDate,
    [int]$DataOption = 1,
    [int]$MaxRecords = 100,
    [int]$MaxStatusWaitSeconds = 12,
    [switch]$SkipServiceKey
)

$ErrorActionPreference = "Stop"

if (-not $DataSpec) { throw "DataSpec is required" }
if (-not $FromDate) { throw "FromDate is required" }

function Read-JVRecord {
    param(
        $Obj,
        [int]$BufferCapacity = 120000
    )

    $filename = ""

    try {
        [string]$buff = ""
        $size = 0
        $ret = [int]$Obj.JVRead([ref]$buff, [ref]$size, [ref]$filename)
        $hexPrefix = ""
        $nonZero = 0
        if ($ret -gt 0 -and $size -gt 0 -and $buff) {
            try {
                $bytes = [System.Text.Encoding]::GetEncoding(932).GetBytes($buff)
                $take = [Math]::Min(48, $bytes.Length)
                if ($take -gt 0) {
                    $hexPrefix = ([System.BitConverter]::ToString($bytes, 0, $take)).Replace('-', ' ')
                }
                foreach ($b in $bytes) {
                    if ([byte]$b -ne 0) { $nonZero++ }
                }
            }
            catch {}
        }
        return @{
            ok = $true
            ret = $ret
            size = [int]$size
            filename = [string]$filename
            text = [string]$buff
            transport = "JVRead"
            hexPrefix = $hexPrefix
            nonZeroCount = [int]$nonZero
            error = ""
        }
    }
    catch {
        $readErr = [string]$_.Exception.Message
        $hasJVGets = $null -ne ($Obj | Get-Member -Name "JVGets" -MemberType Method -ErrorAction SilentlyContinue)
        if (-not $hasJVGets) {
            return @{
                ok = $false
                ret = -999
                size = 0
                filename = [string]$filename
                text = ""
                transport = "JVRead"
                error = "JVRead failed and JVGets not available: $readErr"
            }
        }

        try {
            $bytes = New-Object byte[] $BufferCapacity
            $ret = [int]$Obj.JVGets($bytes, [int]$BufferCapacity, [ref]$filename)
            $used = 0
            if ($ret -gt 0) { $used = [Math]::Min([int]$ret, $bytes.Length) }

            $txt = ""
            $hexPrefix = ""
            $nonZero = 0
            if ($used -gt 0) {
                for ($k = 0; $k -lt $used; $k++) {
                    if ([byte]$bytes[$k] -ne 0) { $nonZero++ }
                }
                $take = [Math]::Min(48, $used)
                if ($take -gt 0) {
                    $hexPrefix = ([System.BitConverter]::ToString($bytes, 0, $take)).Replace('-', ' ')
                }
                if ($nonZero -gt 0) {
                    $txt = [System.Text.Encoding]::GetEncoding(932).GetString($bytes, 0, $used)
                }
            }
            return @{
                ok = $true
                ret = $ret
                size = $used
                filename = [string]$filename
                text = $txt
                transport = "JVGets"
                hexPrefix = $hexPrefix
                nonZeroCount = [int]$nonZero
                error = ""
            }
        }
        catch {
            return @{
                ok = $false
                ret = -999
                size = 0
                filename = [string]$filename
                text = ""
                transport = "JVGets"
                hexPrefix = ""
                nonZeroCount = 0
                error = "JVRead failed ($readErr); JVGets failed: $([string]$_.Exception.Message)"
            }
        }
    }
}

function Get-RaceKeyCandidate {
    param([string]$text)
    if (-not $text -or $text.Length -lt 27) { return "" }
    # Record header (11 bytes) + RACE_ID(16 bytes) is a common pattern.
    $candidate = $text.Substring(11, 16)
    if ($candidate -match '^(19|20)\d{14}$') { return $candidate }
    $m = [regex]::Match($text, '(?:19|20)\d{14}')
    if ($m.Success) { return $m.Value }
    return ""
}

function Normalize-RecordSpec {
    param(
        [string]$spec,
        [string]$fileName
    )
    if ($spec -match '^[A-Z0-9]{2}$') { return $spec }
    $stem = [System.IO.Path]::GetFileNameWithoutExtension([string]$fileName).ToUpperInvariant()
    if ($stem.Length -ge 2) { return $stem.Substring(0, 2) }
    return ""
}

function Normalize-DataKubun {
    param(
        [string]$kubun,
        [string]$fileName
    )
    if ($kubun -match '^[A-Z0-9]$') { return $kubun }
    $stem = [System.IO.Path]::GetFileNameWithoutExtension([string]$fileName).ToUpperInvariant()
    if ($stem.Length -ge 3) { return $stem.Substring(2, 1) }
    return ""
}

$result = @{
    ok = $false
    openOk = $false
    readOk = $false
    readTransport = ""
    sid = $Sid
    dataSpec = $DataSpec
    fromDate = $FromDate
    dataOption = $DataOption
    maxRecords = $MaxRecords
    maxStatusWaitSeconds = $MaxStatusWaitSeconds
    version = ""
    initCode = $null
    setSavePathCode = $null
    setServiceKeyCode = $null
    openCode = $null
    statusCode = $null
    statusPollCount = 0
    readCount = 0
    downloadCount = 0
    lastFileTimestamp = ""
    closeCode = $null
    records = @()
    warnings = @()
    error = ""
}

$obj = $null
try {
    $obj = New-Object -ComObject "JVDTLab.JVLink"
    try { $result.version = [string]$obj.m_JVLinkVersion } catch { $result.version = "" }

    $result.initCode = [int]$obj.JVInit($Sid)

    if ($SavePath) {
        if (-not (Test-Path $SavePath)) { New-Item -ItemType Directory -Path $SavePath -Force | Out-Null }
        $result.setSavePathCode = [int]$obj.JVSetSavePath($SavePath)
        [void]$obj.JVSetSaveFlag(1)
    }

    if (-not $SkipServiceKey -and $ServiceKey) {
        $result.setServiceKeyCode = [int]$obj.JVSetServiceKey($ServiceKey)
    }

    $safeOpt = $DataOption
    if ($safeOpt -lt 1 -or $safeOpt -gt 3) { $safeOpt = 1 }

    $readCount = 0
    $downloadCount = 0
    $lastFileTimestamp = ""
    $result.openCode = [int]$obj.JVOpen($DataSpec, $FromDate, $safeOpt, [ref]$readCount, [ref]$downloadCount, [ref]$lastFileTimestamp)
    $result.readCount = [int]$readCount
    $result.downloadCount = [int]$downloadCount
    $result.lastFileTimestamp = [string]$lastFileTimestamp

    if ($result.openCode -lt 0) {
        $result.error = "JVOpen returned code $($result.openCode)"
    }

    # Poll briefly if downloads are pending.
    if ($result.openCode -ge 0 -and $downloadCount -gt 0) {
        $waitSec = $MaxStatusWaitSeconds
        if ($waitSec -lt 1) { $waitSec = 1 }
        if ($waitSec -gt 600) { $waitSec = 600 }
        $maxPoll = [Math]::Max(1, [int]([Math]::Ceiling(($waitSec * 1000.0) / 200.0)))

        for ($poll = 1; $poll -le $maxPoll; $poll++) {
            $status = [int]$obj.JVStatus()
            $result.statusCode = $status
            $result.statusPollCount = $poll
            if ($status -lt 0) {
                $result.error = "JVStatus returned code $status"
                break
            }
            if ($status -ge $downloadCount) { break }
            Start-Sleep -Milliseconds 200
        }

        if (-not $result.error -and [int]$result.statusCode -lt [int]$downloadCount) {
            $result.warnings += "JVStatus did not reach downloadCount within poll window; skipping JVRead for now."
        }
    }

    $canRead = ($result.openCode -ge 0 -and [string]::IsNullOrEmpty($result.error))
    if ($canRead -and ($downloadCount -le 0 -or [int]$result.statusCode -ge [int]$downloadCount)) {
        $safeMax = $MaxRecords
        if ($safeMax -lt 1) { $safeMax = 1 }
        if ($safeMax -gt 500) { $safeMax = 500 }

        for ($i = 1; $i -le $safeMax; $i++) {
            $read = Read-JVRecord -Obj $obj -BufferCapacity 120000
            if (-not $read.ok) {
                $result.error = "Read exception on record $($i): $($read.error)"
                break
            }

            $ret = [int]$read.ret
            $size = [int]$read.size
            $filename = [string]$read.filename
            $txt = [string]$read.text
            $result.readTransport = [string]$read.transport

            $recSpec = ""
            $dataKubun = ""
            $raceKey16 = ""

            if ($ret -gt 0 -and $size -ge 3 -and $txt.Length -ge 3) {
                if ($txt.Length -ge 3) {
                    $recSpec = $txt.Substring(0, 2)
                    $dataKubun = $txt.Substring(2, 1)
                }
                $raceKey16 = Get-RaceKeyCandidate -text $txt
            }

            $recSpec = Normalize-RecordSpec -spec $recSpec -fileName $filename
            $dataKubun = Normalize-DataKubun -kubun $dataKubun -fileName $filename

            $result.records += @{
                index = $i
                ret = $ret
                size = [int]$size
                fileName = [string]$filename
                transport = [string]$read.transport
                hexPrefix = [string]$read.hexPrefix
                nonZeroCount = [int]$read.nonZeroCount
                recordSpec = $recSpec
                dataKubun = $dataKubun
                raceKey16 = $raceKey16
                text = $txt
            }

            if ($ret -le 0) { break }
        }
    }

    $result.openOk = ($result.openCode -ge 0)
    $result.readOk = [string]::IsNullOrEmpty($result.error)
    $result.ok = ($result.openCode -ge 0)
}
catch {
    $result.ok = $false
    $result.error = [string]$_.Exception.Message
}
finally {
    if ($null -ne $obj) {
        try { $result.closeCode = [int]$obj.JVClose() } catch {}
        [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($obj)
    }
}

$result | ConvertTo-Json -Depth 7 -Compress
