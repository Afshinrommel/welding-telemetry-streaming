# ============================================================
# Welding Digital Twin - Kafka Recovery Test
# ============================================================
# Tests Kafka buffering and Consumer replay after MongoDB outage.
#
# IMPORTANT:
# - Does NOT access or modify the extension directory.
# - Does NOT delete MongoDB data or Docker volumes.
# - Does NOT rebuild Docker images.
# - Uses a separate result_data_welding/recovery_test directory.
# - Does NOT modify Producer or Consumer source code.
# ============================================================

$ErrorActionPreference = "Stop"

# ---------------- Configuration ----------------

$MongoService = "mongodb"
$ConsumerService = "consumer"
$KafkaService = "kafka"

$KafkaTopic = "welding_telemetry"
$DatabaseName = "welding_digital_twin"
$CollectionName = "welding_telemetry"

$MessageCount = 20
$RecoveryTimeoutSeconds = 120

$TestDirectory = Join-Path $PSScriptRoot "result_data_welding\recovery_test"

$RunId = [guid]::NewGuid().ToString("N")
$MessageFile = Join-Path $TestDirectory "recovery_$RunId.jsonl"

$EventIds = @()

$ConsumerStopped = $false
$MongoStopped = $false
$TestPassed = $false

# ---------------- Helper functions ----------------

function Write-Info($Message) {
    Write-Host "[INFO] $Message" -ForegroundColor Cyan
}

function Write-Success($Message) {
    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Write-WarningMessage($Message) {
    Write-Host "[WARNING] $Message" -ForegroundColor Yellow
}

function Write-Failure($Message) {
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Wait-MongoHealthy {
    param([int]$TimeoutSeconds = 90)

    Write-Info "Waiting for MongoDB to become available..."

    $StartTime = Get-Date

    while (((Get-Date) - $StartTime).TotalSeconds -lt $TimeoutSeconds) {

        docker compose exec -T mongodb mongosh --quiet --eval "db.adminCommand('ping').ok" 2>$null | Out-Null

        if ($LASTEXITCODE -eq 0) {
            Write-Success "MongoDB ping successful."
            return $true
        }

        Start-Sleep -Seconds 2
    }

    return $false
}

function Get-RecoveredCount {

    $IdsForQuery = ($EventIds | ForEach-Object {
        "'" + $_ + "'"
    }) -join ","

    $MongoQuery = 'db.' + $CollectionName +
        '.countDocuments({event_id: {$in: [' +
        $IdsForQuery + ']}})'

    $Result = docker compose exec -T mongodb mongosh $DatabaseName --quiet --eval $MongoQuery

    if ($LASTEXITCODE -ne 0) {
        throw "Could not query MongoDB."
    }

    $CountText = ($Result | Select-Object -Last 1).ToString().Trim()

    return [int]$CountText
}

# ---------------- Start test ----------------

try {

    Write-Host ""
    Write-Host "============================================"
    Write-Host " WELDING DIGITAL TWIN - RECOVERY TEST"
    Write-Host "============================================"
    Write-Host ""

    # 1. Check required services

    Write-Info "Checking Docker services..."

    $RunningServices = @(docker compose ps --status running --services)

    if ($LASTEXITCODE -ne 0) {
        throw "Could not inspect Docker Compose services."
    }

    foreach ($Service in @("kafka", "mongodb", "consumer")) {
        if ($RunningServices -notcontains $Service) {
            throw "Required service '$Service' is not running. Start it before testing."
        }
    }

    Write-Success "Kafka, MongoDB and Consumer are running."

    # 2. Create isolated test directory

    New-Item -ItemType Directory -Force -Path $TestDirectory | Out-Null

    Write-Info "Test directory: $TestDirectory"

    # 3. Generate test messages

    Write-Info "Generating $MessageCount test messages..."

    $Messages = @()

    for ($i = 1; $i -le $MessageCount; $i++) {

        $EventId = "recovery-$RunId-$i"

        $EventIds += $EventId

        $Record = [ordered]@{
            time         = [double](1000 + $i)
            event_id     = $EventId
            test_marker  = "KAFKA_RECOVERY_TEST"
            test_run_id  = $RunId
            test_sequence = $i
            source       = "recovery_test"
        }

        $Messages += ($Record | ConvertTo-Json -Compress)
    }

    # UTF-8 without BOM
    $Utf8NoBom = New-Object System.Text.UTF8Encoding($false)

    [System.IO.File]::WriteAllLines(
        $MessageFile,
        $Messages,
        $Utf8NoBom
    )

    Write-Success "$MessageCount unique messages generated."
    Write-Info "Test file: $MessageFile"

    # 4. Stop Consumer gracefully

    Write-Info "Stopping Consumer to preserve its committed offset..."

    docker compose stop $ConsumerService

    if ($LASTEXITCODE -ne 0) {
        throw "Could not stop Consumer."
    }

    $ConsumerStopped = $true

    Write-Success "Consumer stopped."

    # 5. Stop MongoDB

    Write-Info "Stopping MongoDB..."

    docker compose stop $MongoService

    if ($LASTEXITCODE -ne 0) {
        throw "Could not stop MongoDB."
    }

    $MongoStopped = $true

    Write-Success "MongoDB stopped."

    # 6. Copy test data into Kafka container

    Write-Info "Copying test messages into Kafka container..."

    docker cp $MessageFile "telemetry_kafka:/tmp/recovery_test.jsonl"

    if ($LASTEXITCODE -ne 0) {
        throw "Could not copy test file into Kafka container."
    }

    # 7. Publish messages directly to Kafka

    Write-Info "Publishing $MessageCount messages to Kafka topic '$KafkaTopic'..."

    docker compose exec -T kafka bash -lc "kafka-console-producer --bootstrap-server kafka:29092 --topic $KafkaTopic < /tmp/recovery_test.jsonl"

    if ($LASTEXITCODE -ne 0) {
        throw "Kafka message publishing failed."
    }

    Write-Success "Test messages published to Kafka."

    # 8. Restore MongoDB

    Write-Info "Starting MongoDB..."

    docker compose up -d mongodb

    if ($LASTEXITCODE -ne 0) {
        throw "Could not start MongoDB."
    }

    $MongoStopped = $false

    if (-not (Wait-MongoHealthy -TimeoutSeconds 90)) {
        throw "MongoDB did not become healthy within 90 seconds."
    }

    # 9. Restart Consumer

    Write-Info "Starting Consumer..."

    docker compose start consumer

    if ($LASTEXITCODE -ne 0) {
        throw "Could not start Consumer."
    }

    $ConsumerStopped = $false

    Write-Success "Consumer started."

    # 10. Verify recovered messages

    Write-Info "Waiting for all test messages to be persisted..."

    $StartTime = Get-Date
    $RecoveredCount = 0

    while (((Get-Date) - $StartTime).TotalSeconds -lt $RecoveryTimeoutSeconds) {

        $RecoveredCount = Get-RecoveredCount

        Write-Host "Recovered: $RecoveredCount / $MessageCount"

        if ($RecoveredCount -eq $MessageCount) {
            $TestPassed = $true
            break
        }

        Start-Sleep -Seconds 3
    }

    # 11. Final result

    Write-Host ""
    Write-Host "============================================"

    if ($TestPassed) {

        Write-Success "RECOVERY TEST PASSED"

        Write-Host "Published messages : $MessageCount"
        Write-Host "Recovered messages : $RecoveredCount"

        Write-Host "All test event IDs were found in MongoDB."

    }
    else {

        Write-Failure "RECOVERY TEST FAILED OR TIMED OUT"

        Write-Host "Published messages : $MessageCount"
        Write-Host "Recovered messages : $RecoveredCount"

        $MissingIds = @()

        $IdsForQuery = ($EventIds | ForEach-Object {
            "'" + $_ + "'"
        }) -join ","

        $MissingQuery = 'db.' + $CollectionName +
            '.find({event_id: {$in: [' + $IdsForQuery +
            '] }}, {_id: 0, event_id: 1}).toArray()'

        $FoundRecords = docker compose exec -T mongodb mongosh $DatabaseName --quiet --eval $MissingQuery

        $FoundText = ($FoundRecords -join "`n")

        foreach ($Id in $EventIds) {
            if ($FoundText -notmatch [regex]::Escape($Id)) {
                $MissingIds += $Id
            }
        }

        Write-Host "Missing event IDs:"
        $MissingIds | ForEach-Object { Write-Host $_ }
    }

    Write-Host "============================================"

}
catch {

    Write-Failure $_.Exception.Message

}
finally {

    Write-Host ""
    Write-Info "Restoring services..."

    # Restore MongoDB first

    if ($MongoStopped) {

        docker compose up -d mongodb

        if ($LASTEXITCODE -eq 0) {

            $MongoStopped = $false

            if (-not (Wait-MongoHealthy -TimeoutSeconds 90)) {
                Write-Failure "MongoDB could not be restored."
            }
        }
    }

    # Restore Consumer only when MongoDB is healthy

    if ($ConsumerStopped -and -not $MongoStopped) {

        docker compose start consumer

        if ($LASTEXITCODE -eq 0) {
            $ConsumerStopped = $false
            Write-Success "Consumer restored."
        }
        else {
            Write-Failure "Could not restore Consumer."
        }
    }

    # Remove temporary file inside Kafka container

    docker compose exec -T -u 0 kafka rm -f /tmp/recovery_test.jsonl 2>$null | Out-Null


    Write-Info "Host test file retained for inspection."
    Write-Info "No extension files or MongoDB volumes were modified."

}
