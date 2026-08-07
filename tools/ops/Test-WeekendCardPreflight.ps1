[CmdletBinding()]
param(
    [string]$ApiBase = 'http://127.0.0.1:5000'
)

# Read-only Friday/weekend readiness check. It makes no settings, service, database, or stream changes.
$today = (Get-Date).Date
$daysToSaturday = (6 - [int]$today.DayOfWeek + 7) % 7
if ($daysToSaturday -eq 0) { $daysToSaturday = 7 }
$expectedSaturday = $today.AddDays($daysToSaturday).ToString('yyyy-MM-dd')
$expectedSunday = $today.AddDays($daysToSaturday + 1).ToString('yyyy-MM-dd')
$checks = [ordered]@{}

$jv = Get-Service -Name JVLinkAgent -ErrorAction SilentlyContinue
$checks['JVLinkAgent'] = if ($jv -and $jv.Status -eq 'Running') { 'PASS' } else { "FAIL ($($jv.Status))" }

try {
    $ip = Resolve-DnsName -Name 'dl.cdn.jra-van.ne.jp' -Type A -ErrorAction Stop |
        Where-Object IPAddress | Select-Object -First 1 -ExpandProperty IPAddress
    $checks['JRA-VAN DNS'] = "PASS ($ip)"
} catch {
    $checks['JRA-VAN DNS'] = "FAIL ($($_.Exception.Message))"
}

try {
    $status = Invoke-RestMethod "$ApiBase/api/orchestrator/status" -ErrorAction Stop
    $races = Invoke-RestMethod "$ApiBase/api/races" -ErrorAction Stop
    $dates = @($races.upcoming_races_by_date.PSObject.Properties.Name)
    $checks['Saturday card'] = if ($dates -contains $expectedSaturday) { "PASS ($expectedSaturday)" } else { "FAIL expected $expectedSaturday; got $($dates -join ', ')" }
    $checks['Sunday card'] = if ($dates -contains $expectedSunday) { "PASS ($expectedSunday)" } else { "FAIL expected $expectedSunday; got $($dates -join ', ')" }
    $checks['Phase'] = if ($status.phase -eq 'WAITING_FOR_RACES') { 'FAIL (WAITING_FOR_RACES)' } else { "PASS ($($status.phase))" }
} catch {
    $checks['API'] = "FAIL ($($_.Exception.Message))"
}

[pscustomobject]@{
    ExpectedSaturday = $expectedSaturday
    ExpectedSunday = $expectedSunday
    Checks = $checks
} | ConvertTo-Json -Depth 4

if (($checks.Values | Where-Object { $_ -like 'FAIL*' }).Count -gt 0) { exit 1 }
