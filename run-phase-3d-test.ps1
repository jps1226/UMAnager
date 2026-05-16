# Phase 3D Full Parsing Test — PowerShell Automation Script
# Prerequisites: PostgreSQL running, psql in PATH, services not already running

param(
    [ValidateSet("reset", "start", "parse", "monitor", "verify", "all")]
    [string]$Action = "all"
)

$ErrorActionPreference = "Stop"

function Write-Info {
    Write-Host "[INFO] $args" -ForegroundColor Cyan
}

function Write-Success {
    Write-Host "[✓] $args" -ForegroundColor Green
}

function Write-Error {
    Write-Host "[✗] $args" -ForegroundColor Red
}

function Reset-Database {
    Write-Info "Resetting database..."

    $sqlScript = @"
DELETE FROM race_entries;
DELETE FROM races;
DELETE FROM horses;
UPDATE raw_staging SET "IsProcessed" = false WHERE "RecordType" = 'UM';

SELECT 'horses' as table_name, COUNT(*) as count FROM horses
UNION ALL
SELECT 'races', COUNT(*) FROM races
UNION ALL
SELECT 'race_entries', COUNT(*) FROM race_entries
UNION ALL
SELECT 'unprocessed UM records', COUNT(*) FROM raw_staging WHERE "RecordType" = 'UM' AND "IsProcessed" = false;
"@

    # Write SQL to temp file and execute via psql
    $tempSqlFile = [System.IO.Path]::GetTempFileName()
    try {
        $sqlScript | Out-File -FilePath $tempSqlFile -Encoding UTF8 -NoNewline

        $env:PGPASSWORD = "dude!!"
        $result = & psql -h localhost -U postgres -d umanager -f $tempSqlFile 2>&1
        $env:PGPASSWORD = ""

        if ($LASTEXITCODE -eq 0) {
            Write-Success "Database reset complete"
            Write-Host ($result | Select-Object -Last 15 | Out-String)
        } else {
            Write-Error "Database reset failed"
            Write-Host $result
            exit 1
        }
    } finally {
        if (Test-Path $tempSqlFile) {
            Remove-Item $tempSqlFile -Force
        }
    }
}

function Start-Services {
    Write-Info "Starting services..."

    if (!(Test-Path ".\launch-services.ps1")) {
        Write-Error "launch-services.ps1 not found in current directory"
        exit 1
    }

    & .\launch-services.ps1 -Action start

    Write-Info "Waiting for services to start (15 seconds)..."
    Start-Sleep -Seconds 15

    # Verify services are running
    $healthCheck = try {
        $response = Invoke-WebRequest -Uri "http://localhost:5000/api/jvlink/status" -ErrorAction Stop
        $response.StatusCode -eq 200
    } catch {
        $false
    }

    if ($healthCheck) {
        Write-Success "Services started successfully"
    } else {
        Write-Error "Services failed to start or are not responding"
        exit 1
    }
}

function Trigger-Parsing {
    Write-Info "Triggering DIFN record parsing..."

    try {
        $response = Invoke-WebRequest -Uri "http://localhost:5000/api/jvlink/parse-records" `
            -Method POST `
            -ContentType "application/json" `
            -ErrorAction Stop

        $result = $response.Content | ConvertFrom-Json
        Write-Success "Parse command accepted (HTTP 200)"
        Write-Host "Response: $($result | ConvertTo-Json -Depth 2)"
    } catch {
        Write-Error "Failed to trigger parsing: $_"
        exit 1
    }
}

function Monitor-Logs {
    Write-Info "Monitoring parsing progress (press Ctrl+C to stop)..."
    Write-Info "Watching: .\logs\nexus.log"

    if (!(Test-Path ".\logs")) {
        Write-Error "Logs directory not found"
        exit 1
    }

    $logFile = ".\logs\nexus.log"
    $lastLineCount = 0
    $startTime = Get-Date
    $lastBatchTime = $startTime

    while ($true) {
        try {
            if (Test-Path $logFile) {
                $lines = @(Get-Content $logFile -ErrorAction SilentlyContinue)
                $currentLineCount = $lines.Count

                if ($currentLineCount -gt $lastLineCount) {
                    # Show new lines
                    $newLines = $lines[($lastLineCount)..($currentLineCount - 1)]
                    foreach ($line in $newLines) {
                        if ($line -match "Batch \d+:|Completed \d+ batches|parsing complete") {
                            Write-Success $line
                            $lastBatchTime = Get-Date
                        } else {
                            Write-Host $line
                        }
                    }
                    $lastLineCount = $currentLineCount
                }

                # Check for completion
                if ($lines -join "`n" -match "DIFN parsing complete") {
                    Write-Success "Parsing completed!"
                    break
                }

                # Timeout check (30 minutes)
                $elapsedMinutes = ((Get-Date) - $startTime).TotalMinutes
                if ($elapsedMinutes -gt 30) {
                    Write-Error "Parsing timeout (30 minutes exceeded)"
                    break
                }
            }

            Start-Sleep -Seconds 2
        } catch {
            Write-Error "Error reading logs: $_"
            break
        }
    }

    $totalTime = ((Get-Date) - $startTime).TotalMinutes
    Write-Info "Total elapsed time: $([Math]::Round($totalTime, 2)) minutes"
}

function Verify-Results {
    Write-Info "Verifying parsing results..."

    $sqlQuery = @"
SELECT
    (SELECT COUNT(*) FROM horses) as parsed_horses,
    (SELECT COUNT(*) FROM races) as parsed_races,
    (SELECT COUNT(*) FROM race_entries) as parsed_entries,
    (SELECT COUNT(*) FROM raw_staging WHERE "IsProcessed" = true) as marked_processed;
"@

    $tempSqlFile = [System.IO.Path]::GetTempFileName()
    try {
        $sqlQuery | Out-File -FilePath $tempSqlFile -Encoding UTF8 -NoNewline

        $env:PGPASSWORD = "dude!!"
        $result = & psql -h localhost -U postgres -d umanager -f $tempSqlFile -t 2>&1
        $env:PGPASSWORD = ""

        if ($LASTEXITCODE -eq 0) {
            Write-Success "Database verification:"
            Write-Host $result

            # Parse the numeric result - it comes as a single line with pipe separators
            $cleanResult = $result -replace '\s+', ''
            $counts = $cleanResult -split '\|' | ForEach-Object { $_.Trim() } | Where-Object { $_ -match '^\d+$' }

            if ($counts.Count -ge 4) {
                $horses = [int]$counts[0]
                $races = [int]$counts[1]
                $entries = [int]$counts[2]
                $processed = [int]$counts[3]

                if ($horses -gt 0 -and $races -gt 0 -and $entries -gt 0) {
                    Write-Success "All record types parsed successfully!"
                    Write-Success "Horses: $horses | Races: $races | Entries: $entries | Processed: $processed"
                } else {
                    Write-Error "Some record types have 0 rows (parsing may have failed)"
                }
            } else {
                Write-Info "Database has records (raw output): $result"
            }
        } else {
            Write-Error "Database verification failed"
            Write-Host $result
            exit 1
        }
    } finally {
        if (Test-Path $tempSqlFile) {
            Remove-Item $tempSqlFile -Force
        }
    }
}

# Main execution
Write-Host "========================================" -ForegroundColor Blue
Write-Host "Phase 3D Full Parsing Test" -ForegroundColor Blue
Write-Host "========================================" -ForegroundColor Blue

try {
    switch ($Action) {
        "reset" { Reset-Database }
        "start" { Start-Services }
        "parse" { Trigger-Parsing }
        "monitor" { Monitor-Logs }
        "verify" { Verify-Results }
        "all" {
            Reset-Database
            Start-Services
            Trigger-Parsing
            Monitor-Logs
            Verify-Results
        }
    }

    Write-Success "Phase 3D test complete!"
} catch {
    Write-Error "Test failed: $_"
    exit 1
}
