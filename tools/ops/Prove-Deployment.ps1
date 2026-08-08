[CmdletBinding()]
param(
    [string]$Root = 'C:\Users\UMAnager\UMAnager2',
    [ValidateSet('Nexus','Sidecar')]
    [string]$Project = 'Nexus',
    [string]$Dll,
    [string]$EvidencePath
)

# Read-only. Exit code is informational only; consume the JSON verdict.
$ErrorActionPreference = 'Stop'
$projectMap = @{
    Nexus   = @{ Source = 'src\UMAnager.Nexus'; Dll = 'src\UMAnager.Nexus\bin\Release\net8.0\UMAnager.Nexus.dll'; Process = 'UMAnager.Nexus.exe' }
    Sidecar = @{ Source = 'src\UMAnager.Sidecar'; Dll = 'src\UMAnager.Sidecar\bin\Release\net8.0\UMAnager.Sidecar.dll'; Process = 'UMAnager.Sidecar.exe' }
}
$spec = $projectMap[$Project]
if (-not $Dll) { $Dll = Join-Path $Root $spec.Dll } else { $Dll = [IO.Path]::GetFullPath($Dll) }
$sourceRoot = Join-Path $Root $spec.Source
$record = [ordered]@{
    collected_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
    root = $Root
    project = $Project
    source_scope = "$($spec.Source)/**/*.cs and $($spec.Source)/*.cs only"
    excluded_from_source_verdict = @('wwwroot assets','project files such as *.csproj','other repository paths')
    process_name = $spec.Process
    dll_path = $Dll
    source_root = $sourceRoot
    head_sha = $null
    source_max_mtime = $null
    dll_mtime = $null
    process = @()
    project_git_diff_head_empty = $null
    repo_git_diff_head_empty = $null
    repo_status_short = @()
    binary_matches_worktree = $null
    project_source_matches_head = $null
    process_loaded_this_dll = $null
    verdict = 'INDETERMINATE'
    missing_inputs = @()
    exit_code_semantics = 'informational_only; consume verdict'
}

function Add-Missing([string]$Name) { $record.missing_inputs += $Name }
function Iso([object]$Value) { if ($null -eq $Value) { return $null }; return ([DateTime]$Value).ToString('o') }

try {
    if (-not (Test-Path -LiteralPath $Root -PathType Container)) { Add-Missing 'root' }
    if (-not (Test-Path -LiteralPath $Dll -PathType Leaf)) { Add-Missing 'dll' }
    if (-not (Test-Path -LiteralPath $sourceRoot -PathType Container)) { Add-Missing 'source_root' }

    Push-Location $Root
    try {
        $head = (& git rev-parse HEAD)
        if ($LASTEXITCODE -eq 0) { $record.head_sha = ($head -join '').Trim() } else { Add-Missing 'git_head' }

        $null = & git diff HEAD --quiet -- $spec.Source 2>$null
        if ($LASTEXITCODE -eq 0) { $record.project_git_diff_head_empty = $true }
        elseif ($LASTEXITCODE -eq 1) { $record.project_git_diff_head_empty = $false }
        else { Add-Missing 'project_git_diff' }

        $null = & git diff HEAD --quiet -- . 2>$null
        if ($LASTEXITCODE -eq 0) { $record.repo_git_diff_head_empty = $true }
        elseif ($LASTEXITCODE -eq 1) { $record.repo_git_diff_head_empty = $false }
        else { Add-Missing 'repo_git_diff' }
        $record.repo_status_short = @(& git status --short)

        $tracked = @(& git ls-files -- "$($spec.Source)/*.cs" "$($spec.Source)/**/*.cs")
        if ($LASTEXITCODE -ne 0 -or $tracked.Count -eq 0) { Add-Missing 'tracked_source_files' }
        else {
            $sourceFiles = @($tracked | ForEach-Object { Join-Path $Root ($_ -replace '/', '\') } | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf })
            if ($sourceFiles.Count -eq 0) { Add-Missing 'source_files_on_disk' }
            else {
                $maxSource = $sourceFiles | ForEach-Object { Get-Item -LiteralPath $_ } | Sort-Object LastWriteTime | Select-Object -Last 1
                $record.source_max_mtime = Iso $maxSource.LastWriteTime
            }
        }
    }
    finally { Pop-Location }

    if (Test-Path -LiteralPath $Dll -PathType Leaf) { $record.dll_mtime = Iso (Get-Item -LiteralPath $Dll).LastWriteTime }

    $procs = @(Get-CimInstance Win32_Process -Filter "Name='$($spec.Process)'" | Sort-Object ProcessId)
    foreach ($proc in $procs) {
        $owner = Invoke-CimMethod -InputObject $proc -MethodName GetOwner
        $record.process += [ordered]@{
            pid = $proc.ProcessId
            session_id = $proc.SessionId
            owner_rc = $owner.ReturnValue
            owner = if ($owner.User) { "$($owner.Domain)\$($owner.User)" } else { $null }
            creation_time = Iso $proc.CreationDate
            executable_path = $proc.ExecutablePath
        }
    }
    if ($procs.Count -eq 0) { Add-Missing 'running_process' }
    elseif ($procs.Count -ne 1) { Add-Missing 'single_running_process' }

    if ($record.source_max_mtime -and $record.dll_mtime) {
        $sourceTime = [DateTime]::Parse($record.source_max_mtime)
        $dllTime = [DateTime]::Parse($record.dll_mtime)
        $record.binary_matches_worktree = ($sourceTime -le $dllTime)
    }
    if ($null -ne $record.project_git_diff_head_empty) { $record.project_source_matches_head = $record.project_git_diff_head_empty }
    if ($record.process.Count -eq 1 -and $record.dll_mtime -and $record.process[0].creation_time) {
        $processTime = [DateTime]::Parse($record.process[0].creation_time)
        $dllTime = [DateTime]::Parse($record.dll_mtime)
        $record.process_loaded_this_dll = ($dllTime -le $processTime)
    }

    if ($record.missing_inputs.Count -eq 0) {
        if (-not $record.binary_matches_worktree) { $record.verdict = 'BINARY_PREDATES_SOURCE' }
        elseif (-not $record.process_loaded_this_dll) { $record.verdict = 'PROCESS_PREDATES_BINARY' }
        elseif (-not $record.project_source_matches_head) { $record.verdict = 'MATCHES_WORKTREE_NOT_HEAD' }
        else { $record.verdict = 'DEPLOYED_MATCHES_HEAD' }
    }
}
catch {
    $record.missing_inputs += 'unreadable_input'
}

$record.process = @($record.process)
$record.missing_inputs = @($record.missing_inputs | Sort-Object -Unique)
$record.repo_status_short = @($record.repo_status_short)
$text = ($record | ConvertTo-Json -Depth 8)
if ($EvidencePath) { Add-Content -LiteralPath $EvidencePath -Value $text -Encoding UTF8 }
Write-Output $text
exit 0
