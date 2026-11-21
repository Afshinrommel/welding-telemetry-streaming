# ==========================================
# Welding Digital Twin - Automation Script
# ==========================================

$ErrorActionPreference = "Stop"

function Stop-Test {
    param([string]$Message)

    Write-Host "`nERROR: $Message" -ForegroundColor Red
    exit 1
}

function Test-HttpService {
    param(
        [string]$Name,
        [string]$Url,
        [int]$TimeoutSeconds = 120
    )

    Write-Host "Checking $Name..." -ForegroundColor Yellow

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest `
                -Uri $Url `
                -TimeoutSec 5 `
                -UseBasicParsing

            if ($response.StatusCode -ge 200 -and
                $response.StatusCode -lt 400) {

                Write-Host "$Name is ready." -ForegroundColor Green
                return $true
            }
        }
        catch {
            Start-Sleep -Seconds 3
        }
    }

    return $false
}

# ==========================================
# 1. Stop Existing Containers
# ==========================================

Write-Host "---------------------------------------------------" -ForegroundColor Cyan
Write-Host "1. Stopping existing containers..." -ForegroundColor Yellow
Write-Host "---------------------------------------------------" -ForegroundColor Cyan

docker compose down -v

if ($LASTEXITCODE -ne 0) {
    Stop-Test "Docker Compose down failed."
}

# ==========================================
# 2. Provision Grafana Dashboard
# ==========================================

Write-Host "`n---------------------------------------------------" -ForegroundColor Cyan
Write-Host "2. Provisioning 11-Panel Grafana Dashboard..." -ForegroundColor Yellow
Write-Host "---------------------------------------------------" -ForegroundColor Cyan

python setup_grafana.py

if ($LASTEXITCODE -ne 0) {
    Stop-Test "Grafana provisioning failed."
}

# ==========================================
# 3. Build and Start Services
# ==========================================

Write-Host "`n---------------------------------------------------" -ForegroundColor Cyan
Write-Host "3. Building and starting Docker services..." -ForegroundColor Yellow
Write-Host "---------------------------------------------------" -ForegroundColor Cyan

if (-not $env:RUN_ID) { $env:RUN_ID = "RUN-$(Get-Date -Format 'yyyyMMdd-HHmmss')" }
Write-Host "Active Pipeline RUN_ID: $env:RUN_ID" -ForegroundColor Cyan
docker compose up -d --build

if ($LASTEXITCODE -ne 0) {
    Stop-Test "Docker Compose startup failed."
}

# ==========================================
# 4. Check Container Status
# ==========================================

Write-Host "`n---------------------------------------------------" -ForegroundColor Cyan
Write-Host "4. Checking container status..." -ForegroundColor Yellow
Write-Host "---------------------------------------------------" -ForegroundColor Cyan

docker compose ps

if ($LASTEXITCODE -ne 0) {
    Stop-Test "Could not retrieve Docker service status."
}

# ==========================================
# 5. Wait for Web Services
# ==========================================

Write-Host "`n---------------------------------------------------" -ForegroundColor Cyan
Write-Host "5. Waiting for services to become ready..." -ForegroundColor Yellow
Write-Host "---------------------------------------------------" -ForegroundColor Cyan

if (-not (Test-HttpService `
    -Name "Prometheus" `
    -Url "http://localhost:9090/-/ready")) {

    Stop-Test "Prometheus did not become ready."
}

if (-not (Test-HttpService `
    -Name "Grafana" `
    -Url "http://localhost:3000/api/health")) {

    Stop-Test "Grafana did not become ready."
}

if (-not (Test-HttpService `
    -Name "Streamlit" `
    -Url "http://localhost:8501/_stcore/health")) {

    Stop-Test "Streamlit did not become ready."
}

# ==========================================
# 6. Check Prometheus Targets
# ==========================================

Write-Host "`n---------------------------------------------------" -ForegroundColor Cyan
Write-Host "6. Checking Prometheus monitoring targets..." -ForegroundColor Yellow
Write-Host "---------------------------------------------------" -ForegroundColor Cyan

try {
    $targets = Invoke-RestMethod `
        -Uri "http://localhost:9090/api/v1/targets" `
        -TimeoutSec 10

    $activeTargets = $targets.data.activeTargets

    $activeTargets |
        Select-Object job, health, lastError |
        Format-Table -AutoSize

    $unhealthy = @(
        $activeTargets | Where-Object { $_.health -ne "up" }
    )

    if ($unhealthy.Count -gt 0) {
        Write-Host "Warning: Some Prometheus targets are not UP." `
            -ForegroundColor Yellow
    }
}
catch {
    Stop-Test "Could not retrieve Prometheus targets."
}

# ==========================================
# 7. Launch Dashboards
# ==========================================

Write-Host "`n---------------------------------------------------" -ForegroundColor Cyan
Write-Host "7. Launching dashboards in default browser..." -ForegroundColor Yellow
Write-Host "---------------------------------------------------" -ForegroundColor Cyan

Start-Process "http://localhost:3000"
Start-Process "http://localhost:8501"

# ==========================================
# 8. Completion
# ==========================================

Write-Host "`n===================================================" -ForegroundColor Green
Write-Host "   SERVICES STARTED AND BASIC CHECKS COMPLETED   " -ForegroundColor Green
Write-Host "   READY FOR MANUAL RECOVERY TEST.                " -ForegroundColor Green
Write-Host "===================================================" -ForegroundColor Green